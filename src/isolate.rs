//! One worker: a V8 isolate on its own thread, running one project's bundle.
//!
//! The worker's `Deno.serve` is pinned to a Unix socket in a directory only this worker may
//! read or write, and the supervisor proxies requests to it. So the function is served by Deno's
//! own HTTP server (streaming bodies both ways, WebSocket upgrades, trailers), and the only
//! JavaScript of ours that runs in the isolate is the few lines of `INSTALL` below.
//!
//! What a worker may do, decided here and nowhere else: read its own bundle, read its own socket
//! directory, reach the network (which the container's network then polices: the
//! metadata service and private ranges are refused there), and nothing else: no environment of
//! the process, no subprocess, no FFI, and no file written at all: the socket is bound by our own
//! code before the function's runs, and write permission is gone before the function's first line
//! (audit 5-B: a function could otherwise fill the host's disk). `Deno.env` is replaced by the project's
//! own variables, so a worker sees exactly what its manifest says and never the runtime's own.
//!
//! An isolate is BOOTED before anyone needs it and CLAIMED by a function later (`Spares`): what
//! it may touch (its bundle, its socket, its permissions) is bound at the claim, so a spare can
//! read nothing and reach nothing while it waits.

use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use deno_core::{ModuleCodeString, ModuleSpecifier, v8};
use deno_resolver::npm::{DenoInNpmPackageChecker, NpmResolver};
use deno_runtime::deno_fs::RealFs;
use deno_runtime::deno_permissions::{Permissions, PermissionsContainer, PermissionsOptions};
use deno_runtime::permissions::RuntimePermissionDescriptorParser;
use deno_runtime::worker::{MainWorker, WorkerOptions, WorkerServiceOptions};
use tokio::sync::oneshot;

use crate::loader::{BundleLoader, Root, unbound_root};
use crate::manifest::Limits;

type Sys = sys_traits::impls::RealSys;

pub struct Spec {
	pub label: String,
	pub bundle: PathBuf,
	pub socket_dir: PathBuf,
	pub env: BTreeMap<String, String>,
	pub limits: Limits,
}

/// What the supervisor holds for a running worker.
pub struct Worker {
	pub label: String,
	pub socket: PathBuf,
	pub limits: Limits,
	isolate: v8::IsolateHandle,
	/// When this worker's event loop last turned, in the process's uptime milliseconds.
	beat_ms: Arc<AtomicU64>,
	alive: Arc<AtomicBool>,
	stopped_because: Arc<Mutex<Option<String>>>,
	/// Wakes the worker's own thread to drop the isolate. Terminating execution alone stops
	/// the JavaScript but leaves the event loop waiting for a wake-up that never comes, and the
	/// requests it holds hang until their wall clock.
	stopping: Arc<tokio::sync::Notify>,
	/// V8's heap in use, sampled on the worker's own thread every `SAMPLE` while its event loop
	/// turns; with the allocator's ArrayBuffers, what the worker holds (`memory_bytes`).
	heap_bytes: Arc<AtomicU64>,
	budget: Arc<crate::v8_memory::Budget>,
	/// How many requests the worker's `Deno.serve` wrapper has started (INSTALL), the last count
	/// `stalled_ms` saw, and when it saw it change. A request starting is the thread coming
	/// back to the worker as surely as the event loop turning.
	///
	/// Why it exists: the heartbeat runs only when the event loop hands the thread back, and
	/// when several requests are ready together one turn runs them back to back. Sixty requests
	/// of one second's CPU each, to one function, stopped its worker for "CPU" twice (2026-10-02,
	/// docs/cloud/QA-RETEST.md §3f): the limit read four healthy requests in a row as one that
	/// had held the thread for four seconds. A request that holds it past the limit by itself
	/// still moves no counter, and is still stopped.
	requests: Option<crate::v8_memory::SharedCounter>,
	requests_seen: AtomicI32,
	request_ms: AtomicU64,
	/// Its CPU over its whole life, against a credit per request (cpu.rs); None where the platform
	/// has no thread CPU clock.
	cpu: Option<Arc<crate::cpu::Meter>>,
}

impl Worker {
	pub fn alive(&self) -> bool {
		self.alive.load(Ordering::Acquire)
	}

	/// Stop the isolate now, whatever it is doing, and say why to whoever asks next.
	pub fn stop(&self, reason: &str) {
		debug!("{}: stop requested ({reason})", self.label);
		if let Ok(mut held) = self.stopped_because.lock()
			&& held.is_none()
		{
			*held = Some(reason.to_owned());
		}
		self.isolate.terminate_execution();
		self.stopping.notify_one();
	}

	/// A request is given to this worker, and brings its CPU limit to the worker's credit.
	pub fn begin_request(&self) {
		if let Some(cpu) = &self.cpu {
			cpu.begin();
		}
	}

	pub fn end_request(&self) {
		if let Some(cpu) = &self.cpu {
			cpu.end();
		}
	}

	pub fn stopped_because(&self) -> Option<String> {
		self.stopped_because.lock().ok().and_then(|held| held.clone())
	}

	/// How long this worker's thread has gone without its event loop turning: JavaScript that
	/// has not yielded for that long.
	/// What this worker holds: its heap as last sampled, and its ArrayBuffers now.
	pub fn memory_bytes(&self) -> u64 {
		self.heap_bytes.load(Ordering::Acquire) + u64::try_from(self.budget.held()).unwrap_or(u64::MAX)
	}

	pub fn stalled_ms(&self) -> u64 {
		let now = now_ms();
		if let Some(requests) = &self.requests {
			let count = requests.load();
			if self.requests_seen.swap(count, Ordering::AcqRel) != count {
				self.request_ms.store(now, Ordering::Release);
			}
		}
		now.saturating_sub(self.beat_ms.load(Ordering::Acquire).max(self.request_ms.load(Ordering::Acquire)))
	}
}

/// Our own few lines, run before the customer's module. They close over what they need and
/// leave nothing on `globalThis`.
const INSTALL: &str = r#"((socketPath, vars) => {
	// [0] bumped as each request starts (`Worker::stalled_ms`); [1] set once the function serves.
	const counters = new Int32Array(new SharedArrayBuffer(8));
	const listen = Deno.listen;
	const serveOn = Deno[Deno.internal].serveHttpOnListener;
	const AddrInUse = Deno.errors.AddrInUse;
	// The worker's socket, bound here, before any of the function's code runs: the only write this
	// worker is ever allowed, and `run` takes the permission away as soon as this returns.
	let bound = listen({ transport: "unix", path: socketPath });
	const take = () => {
		if (bound === null) throw new AddrInUse("Address already in use (os error 98)");
		const listener = bound;
		bound = null;
		Atomics.store(counters, 1, 1);
		return listener;
	};
	const failed = (error) => {
		console.error(error);
		return new Response(new TextEncoder().encode("Internal Server Error"), { status: 500 });
	};
	Object.defineProperty(Deno, "serve", {
		configurable: true,
		writable: true,
		value: function snoutServe(a, b) {
			let options = {};
			let handler;
			if (typeof a === "function") {
				handler = a;
				if (b && typeof b === "object") options = b;
			} else if (typeof b === "function") {
				options = a ?? {};
				handler = b;
			} else {
				options = a ?? {};
				handler = options.handler;
			}
			const { signal, onError, automaticCompression } = options;
			// Served on a Unix socket, a request's URL would read `http+unix://`; the function
			// sees the address it was called at instead, as it would on a TCP listener.
			const called = (request, info) => {
				Atomics.add(counters, 0, 1);
				const at = new URL(request.url);
				const url = "http://" + (request.headers.get("host") ?? "localhost") + at.pathname + at.search;
				// Not `new Request(url, request)`: that copies the signal, and reading it asks Deno
				// to abort it on success (a warning on every worker). Disconnects are wired later.
				const body = request.body;
				const forwarded = new Request(url, { method: request.method, headers: request.headers, body, redirect: request.redirect, duplex: body ? "half" : undefined });
				// A function that set its own onError handles its own failures, through Deno.
				if (onError) return handler(forwarded, info);
				// Otherwise a failure is answered the way clients expect: a handler that throws
				// gets a plain-text 500, one that returns something that is not a Response a 502.
				return (async () => {
					let response;
					try {
						response = await handler(forwarded, info);
					} catch (error) {
						console.error(error);
						return new Response("Internal Server Error", { status: 500, headers: { "content-type": "text/plain;charset=UTF-8" } });
					}
					if (!(response instanceof Response)) {
						console.error(new TypeError("the handler did not return a Response"));
						return new Response("Bad Gateway", { status: 502, headers: { "content-type": "text/plain;charset=UTF-8" } });
					}
					return response;
				})();
			};
			// What Deno.serve does with a `path`, on the socket bound above.
			return serveOn(take(), signal, called, onError ?? failed, () => {}, automaticCompression);
		},
	});
	const store = new Map(Object.entries(vars));
	const env = {
		get: (key) => store.get(String(key)),
		set: (key, value) => { store.set(String(key), String(value)); },
		delete: (key) => { store.delete(String(key)); },
		has: (key) => store.has(String(key)),
		toObject: () => Object.fromEntries(store),
	};
	Object.defineProperty(Deno, "env", { configurable: true, writable: false, value: env });
	// Older functions serve with std/http's serve(), which listens on a port and hands each
	// connection to Deno.serveHttp. That listener is the same socket.
	Object.defineProperty(Deno, "listen", {
		configurable: true,
		writable: true,
		value: function snoutListen(options) {
			if (options && options.transport === "unix") return listen(options);
			return take();
		},
	});
	return counters.buffer;
})"#;

type Ready = oneshot::Sender<Result<Arc<Worker>, String>>;

/// How often a serving worker records the memory its heap holds.
const SAMPLE: Duration = Duration::from_millis(250);
type Claim = (Spec, Ready);

/// Isolates booted ahead of need, by heap limit (fixed when an isolate is created), each waiting
/// on its own thread for a function to claim it. A cold request then pays for loading its module
/// and nothing of the boot (about 16 ms from Deno's snapshot, spike 1). One per limit that has
/// been asked for, replaced as soon as it is taken; `SNOUT_FUNCTIONS_SPARES=0` turns it off.
///
/// Cold requests in a burst outrun one spare: the next claim finds the replacement still booting
/// and waits for the rest of it (a cold p50 of 29 ms against a 10 ms target, bench 2026-09-29). So
/// a claim within `BURST_MS` of the previous one asks for one spare more, up to `BURST_MAX`, and
/// `trim_spares` lets the extras go once claims stop for `BURST_MS` ten times over.
struct Spares {
	by_limit: Mutex<HashMap<u32, Pool>>,
	per_limit: AtomicUsize,
}

#[derive(Default)]
struct Pool {
	waiting: Vec<oneshot::Sender<Claim>>,
	/// How many this limit keeps now: `per_limit`, plus one for each claim in a burst.
	target: usize,
	last_claim_ms: u64,
}

const BURST_MS: u64 = 1_000;
const BURST_MAX: usize = 4;

fn spares() -> &'static Spares {
	static SPARES: OnceLock<Spares> = OnceLock::new();
	SPARES.get_or_init(|| Spares {
		by_limit: Mutex::new(HashMap::new()),
		per_limit: AtomicUsize::new(std::env::var("SNOUT_FUNCTIONS_SPARES").ok().and_then(|v| v.parse().ok()).unwrap_or(1)),
	})
}

/// Boot a spare for this heap limit, if there is room for one.
pub fn keep_spare(memory_mb: u32) {
	let pool = spares();
	let Ok(mut held) = pool.by_limit.lock() else { return };
	let spares = held.entry(memory_mb).or_default();
	spares.target = spares.target.max(pool.per_limit.load(Ordering::Relaxed));
	spares.waiting.retain(|claim| !claim.is_closed());
	while spares.waiting.len() < spares.target {
		spares.waiting.push(start_thread(memory_mb, true));
	}
}

/// Let a burst's extra spares go once claims have stopped: dropping one ends its thread.
pub fn trim_spares() {
	let pool = spares();
	let Ok(mut held) = pool.by_limit.lock() else { return };
	let now = now_ms();
	let per_limit = pool.per_limit.load(Ordering::Relaxed);
	for spares in held.values_mut() {
		if spares.target > per_limit && now.saturating_sub(spares.last_claim_ms) > BURST_MS * 10 {
			spares.target = per_limit;
			spares.waiting.retain(|claim| !claim.is_closed());
			spares.waiting.truncate(per_limit);
		}
	}
}

/// How many spares each limit is refilled to from now on. One already booted stays until it is
/// claimed: a project's own process (project.rs) keeps refilling only while the project has a
/// second function to start, and its first request still takes the isolate it booted unassigned.
pub fn set_spares(count: usize) {
	let pool = spares();
	pool.per_limit.store(count, Ordering::Relaxed);
	let Ok(mut held) = pool.by_limit.lock() else { return };
	for spares in held.values_mut() {
		spares.target = count;
	}
}

pub fn spawn(spec: Spec) -> oneshot::Receiver<Result<Arc<Worker>, String>> {
	let (ready, answer) = oneshot::channel();
	let memory_mb = spec.limits.memory_mb;
	let pool = spares();
	let spare = pool.by_limit.lock().ok().and_then(|mut held| {
		let spares = held.get_mut(&memory_mb)?;
		let now = now_ms();
		let per_limit = pool.per_limit.load(Ordering::Relaxed);
		if per_limit > 0 && now.saturating_sub(spares.last_claim_ms) < BURST_MS {
			spares.target = (spares.target + 1).min(BURST_MAX.max(per_limit));
		}
		spares.last_claim_ms = now;
		spares.waiting.retain(|claim| !claim.is_closed());
		// The oldest first: it has had longest to finish booting.
		(!spares.waiting.is_empty()).then(|| spares.waiting.remove(0))
	});
	let claim = spare.unwrap_or_else(|| start_thread(memory_mb, false));
	if let Err((_, ready)) = claim.send((spec, ready)) {
		let _ = ready.send(Err("the worker's thread ended before it was claimed".into()));
	}
	keep_spare(memory_mb);
	answer
}

/// How long a booted spare waits unclaimed before it rehearses. In a burst of cold requests
/// spares are claimed faster than this, and a rehearsal's CPU would be what the next claim waits
/// on (cold-probe.sh, 2 cores: 9.7 ms back to back rehearsing at once, against 5.3 without).
const REHEARSE_AFTER: Duration = Duration::from_millis(100);

/// A thread with an isolate booted for this heap limit, waiting to be claimed; rehearsed
/// (`rehearse`) if it may be and nobody claims it for `REHEARSE_AFTER`.
fn start_thread(memory_mb: u32, may_rehearse: bool) -> oneshot::Sender<Claim> {
	let (claim, claimed) = oneshot::channel::<Claim>();
	let spawned = std::thread::Builder::new().name(format!("fn-{memory_mb}mb")).stack_size(8 << 20).spawn(move || {
		let runtime = match tokio::runtime::Builder::new_current_thread().enable_all().build() {
			Ok(runtime) => runtime,
			Err(error) => {
				eprintln!("a worker's runtime could not start: {error}");
				return;
			}
		};
		runtime.block_on(async move {
			let mut claimed = claimed;
			let mut booted = boot_isolate(memory_mb);
			let mut early = None;
			if may_rehearse && rehearsing() && booted.is_ok() {
				tokio::select! {
					claim = &mut claimed => early = Some(claim),
					() = tokio::time::sleep(REHEARSE_AFTER) => {
						booted = match booted {
							Ok(isolate) => rehearsed(isolate, memory_mb).await,
							failed => failed,
						};
					}
				}
			}
			let claim = match early {
				Some(claim) => claim,
				None => claimed.await,
			};
			let Ok((spec, ready)) = claim else { return };
			match booted {
				Ok(booted) => run(booted, spec, ready).await,
				Err(error) => {
					let _ = ready.send(Err(error));
				}
			}
		});
	});
	if let Err(error) = spawned {
		eprintln!("a worker thread could not be started: {error}");
	}
	claim
}

/// An isolate booted and not yet anyone's: it may read nothing and reach nothing.
struct Booted {
	worker: MainWorker,
	budget: Arc<crate::v8_memory::Budget>,
	root: Root,
	parser: Arc<RuntimePermissionDescriptorParser<Sys>>,
}

async fn run(booted: Booted, spec: Spec, ready: Ready) {
	let Booted { mut worker, budget, root, parser } = booted;
	let socket = spec.socket_dir.join("s.sock");
	let claimed_at = std::time::Instant::now();
	debug!("{}: claimed", spec.label);
	if let Some(error) = preparation_failed(&spec.bundle) {
		let _ = ready.send(Err(format!("its imports could not be prepared: {error}")));
		return;
	}
	if let Err(error) = std::fs::create_dir_all(&spec.socket_dir) {
		let _ = ready.send(Err(format!("the worker's directory could not be made: {error}")));
		return;
	}
	// What this worker may touch, now that it is somebody's.
	*root.borrow_mut() = spec.bundle.clone();
	match permissions_for(&spec, parser.clone(), true) {
		Ok(permissions) => worker.js_runtime.op_state().borrow_mut().put(permissions),
		Err(error) => {
			let _ = ready.send(Err(error));
			return;
		}
	}
	let alive = Arc::new(AtomicBool::new(true));
	let beat_ms = Arc::new(AtomicU64::new(now_ms()));
	// The heartbeat: a task on this thread's own runtime, so it runs only when the event loop
	// yields. JavaScript that holds the thread holds the beat.
	let heartbeat = {
		let beat_ms = beat_ms.clone();
		tokio::spawn(async move {
			loop {
				beat_ms.store(now_ms(), Ordering::Release);
				tokio::time::sleep(Duration::from_millis(10)).await;
			}
		})
	};
	let stopping = Arc::new(tokio::sync::Notify::new());
	let stopped_because = Arc::new(Mutex::new(None));
	let heap_bytes = Arc::new(AtomicU64::new(0));

	{
		// ArrayBuffer memory past the limit is refused where it is asked for (v8_memory.rs).
		let isolate = worker.js_runtime.v8_isolate().thread_safe_handle();
		let because = stopped_because.clone();
		let wake = stopping.clone();
		budget.on_refusal(move || {
			if let Ok(mut held) = because.lock()
				&& held.is_none()
			{
				*held = Some("memory".to_owned());
			}
			isolate.terminate_execution();
			wake.notify_one();
		});
	}
	{
		let isolate = worker.js_runtime.v8_isolate().thread_safe_handle();
		let because = stopped_because.clone();
		let wake = stopping.clone();
		worker.js_runtime.add_near_heap_limit_callback(move |current, _initial| {
			if let Ok(mut held) = because.lock()
				&& held.is_none()
			{
				*held = Some("memory".to_owned());
			}
			debug!("near the heap limit ({current} bytes): terminating");
			isolate.terminate_execution();
			wake.notify_one();
			// Room to unwind; the isolate is being torn down either way.
			current * 2
		});
	}

	let install = format!(
		"{INSTALL}({}, {});",
		serde_json::to_string(&socket.to_string_lossy()).unwrap_or_default(),
		serde_json::to_string(&spec.env).unwrap_or_else(|_| "{}".into())
	);
	let installed = match worker.execute_script("[snout-functions]", ModuleCodeString::from(install)) {
		Ok(installed) => installed,
		Err(error) => {
			let _ = ready.send(Err(format!("the worker could not be prepared: {error}")));
			return;
		}
	};
	// The socket is bound: from here the worker may write nothing (audit 5-B).
	match permissions_for(&spec, parser, false) {
		Ok(permissions) => worker.js_runtime.op_state().borrow_mut().put(permissions),
		Err(error) => {
			let _ = ready.send(Err(error));
			return;
		}
	}
	let (requests, served) = {
		let runtime = &mut worker.js_runtime;
		deno_core::scope!(scope, runtime);
		let value = v8::Local::new(scope, &installed);
		let store = v8::Local::<v8::SharedArrayBuffer>::try_from(value).ok().map(|buffer| buffer.get_backing_store());
		let counter = |index| store.clone().and_then(|store| crate::v8_memory::SharedCounter::new(store, index));
		(counter(0), counter(1))
	};
	let Some(served) = served else {
		let _ = ready.send(Err("the worker could not be prepared: no counters".into()));
		return;
	};
	// Its CPU, from before the function's first line to its last (cpu.rs, audit 5-A).
	let cpu = {
		let isolate = worker.js_runtime.v8_isolate().thread_safe_handle();
		let because = stopped_because.clone();
		let wake = stopping.clone();
		let label = spec.label.clone();
		let unit_ms = spec.limits.hard_cpu();
		crate::cpu::meter(unit_ms, move || {
			eprintln!("{label}: stopped for CPU, past {unit_ms} ms for each request it was given");
			if let Ok(mut held) = because.lock()
				&& held.is_none()
			{
				*held = Some("CPU".to_owned());
			}
			isolate.terminate_execution();
			wake.notify_one();
		})
	};
	let _metered = Metered(cpu.clone());
	let handle = Arc::new(Worker {
		label: spec.label.clone(),
		socket: socket.clone(),
		limits: spec.limits,
		isolate: worker.js_runtime.v8_isolate().thread_safe_handle(),
		beat_ms: beat_ms.clone(),
		alive: alive.clone(),
		stopped_because: stopped_because.clone(),
		stopping: stopping.clone(),
		heap_bytes: heap_bytes.clone(),
		budget: budget.clone(),
		requests,
		requests_seen: AtomicI32::new(0),
		request_ms: AtomicU64::new(0),
		cpu,
	});


	let main = match ModuleSpecifier::from_file_path(entry(&spec.bundle)) {
		Ok(main) => main,
		Err(()) => {
			let _ = ready.send(Err("the bundle's path is not absolute".into()));
			return;
		}
	};
	debug!("{}: installed after {} us", spec.label, claimed_at.elapsed().as_micros());
	// A module that holds the thread is stopped by its CPU credit; the watchdog's wake-up ends the
	// wait for one that yields.
	let evaluated = tokio::select! {
		evaluated = tokio::time::timeout(Duration::from_millis(spec.limits.wall_ms), worker.execute_main_module(&main)) => Some(evaluated),
		() = stopping.notified() => None,
	};
	let stopped = stopped_because.lock().ok().and_then(|held| held.clone());
	if stopped.as_deref() == Some("CPU") {
		let _ = ready.send(Err("the function's module used more than its CPU limit while it loaded".into()));
		return;
	}
	let Some(evaluated) = evaluated else {
		let _ = ready.send(Err(format!("the function's module failed to load: it was stopped ({})", stopped.as_deref().unwrap_or("unknown"))));
		return;
	};
	match evaluated {
		Err(_) => {
			let _ = ready.send(Err("the function's module did not finish loading within its wall-clock limit".into()));
			return;
		}
		Ok(Err(error)) => {
			let _ = ready.send(Err(format!("the function's module failed to load: {error}")));
			return;
		}
		Ok(Ok(())) => {}
	}
	debug!("{}: module evaluated after {} us", spec.label, claimed_at.elapsed().as_micros());
	if served.load() == 0 {
		// An error thrown while the module loaded can surface only when the event loop runs; let
		// it run a moment, so the caller hears that rather than "did not serve".
		let surfaced = tokio::time::timeout(Duration::from_millis(100), worker.run_event_loop(false)).await;
		let reason = match surfaced {
			Ok(Err(error)) => format!("the function's module failed to load: {error}"),
			_ => "the function did not call Deno.serve".to_owned(),
		};
		let _ = ready.send(Err(reason));
		return;
	}
	if crate::debugging() {
		let stats = worker.js_runtime.v8_isolate().get_heap_statistics();
		debug!(
			"{}: serving; heap used {} KB of {} KB total ({} KB physical), external {} KB, malloced {} KB",
			spec.label,
			stats.used_heap_size() / 1024,
			stats.total_heap_size() / 1024,
			stats.total_physical_size() / 1024,
			stats.external_memory() / 1024,
			stats.malloced_memory() / 1024
		);
	}
	debug!("{}: ready after {} us", spec.label, claimed_at.elapsed().as_micros());
	let _ = ready.send(Ok(handle));

	// The event loop, interrupted every `SAMPLE` to record the heap, which only this thread may
	// read. A worker running JavaScript without yielding is not sampled, and is stopped by its CPU
	// limit long before its heap has grown far.
	let mut sample = tokio::time::interval(SAMPLE);
	loop {
		tokio::select! {
			ended = worker.run_event_loop(false) => {
				if let Err(error) = ended
					&& stopped_because.lock().ok().and_then(|held| held.clone()).is_none()
				{
					eprintln!("{}: {error}", spec.label);
				}
				break;
			}
			() = stopping.notified() => break,
			_ = sample.tick() => {
				let used = worker.js_runtime.v8_isolate().get_heap_statistics().used_heap_size();
				heap_bytes.store(u64::try_from(used).unwrap_or(u64::MAX), Ordering::Release);
			}
		}
	}
	heartbeat.abort();
	// Dropping the worker disposes of the isolate and closes its server, so every request it
	// held is answered by the supervisor with the reason, now rather than at its wall clock.
	drop(worker);
	debug!("{}: stopped ({:?})", spec.label, stopped_because.lock().ok().and_then(|held| held.clone()));
	alive.store(false, Ordering::Release);
	let _ = std::fs::remove_dir_all(&spec.socket_dir);
}

/// Lets the CPU watchdog go when `run` returns, by whatever path.
struct Metered(Option<Arc<crate::cpu::Meter>>);

impl Drop for Metered {
	fn drop(&mut self) {
		if let Some(meter) = &self.0 {
			meter.finish();
		}
	}
}

/// This container's own addresses, by name, which no worker may reach.
const LOOPBACK: [&str; 5] = ["127.0.0.1", "localhost", "0.0.0.0", "[::1]", "[::]"];

/// What a worker may do: read its own bundle and its own socket directory, reach the network
/// except this container's loopback, and nothing else. `bind` adds the one write it is ever
/// allowed, its socket's own path, for our code to bind it before the function's code runs.
fn permissions_for(spec: &Spec, parser: Arc<RuntimePermissionDescriptorParser<Sys>>, bind: bool) -> Result<PermissionsContainer, String> {
	let bundle = spec.bundle.to_string_lossy().into_owned();
	let sockets = spec.socket_dir.to_string_lossy().into_owned();
	let socket = spec.socket_dir.join("s.sock").to_string_lossy().into_owned();
	let permissions = Permissions::from_options(
		parser.as_ref(),
		&PermissionsOptions {
			allow_read: Some(vec![bundle, sockets]),
			allow_write: bind.then(|| vec![socket]),
			// Empty is "every host": the network the container is on is what refuses the metadata
			// service and private ranges, measured in functions.pod.ts.
			allow_net: Some(vec![]),
			// Never this container's own loopback, where the runtime's port answers any project
			// named in a header. By name here; by resolved address in the network policy.
			deny_net: Some(LOOPBACK.map(String::from).to_vec()),
			prompt: false,
			..Default::default()
		},
	)
	.map_err(|error| format!("the worker's permissions could not be set: {error}"))?;
	Ok(PermissionsContainer::new(parser, permissions))
}

/// A spare after its rehearsal (`rehearse`), or, if that failed, a fresh one without.
async fn rehearsed(mut booted: Booted, memory_mb: u32) -> Result<Booted, String> {
	let started = std::time::Instant::now();
	let cpu = thread_cpu_us();
	match rehearse(&mut booted).await {
		Ok(()) => {
			debug!("spare for {memory_mb} MB rehearsed in {} us ({} us CPU)", started.elapsed().as_micros(), thread_cpu_us() - cpu);
			Ok(booted)
		}
		// A spare that could not rehearse may be left holding a server: start again without one.
		// The isolate goes first: two on one thread at once is not something deno_core allows.
		Err(error) => {
			eprintln!("a spare's rehearsal failed, booting it again without one: {error}");
			drop(booted);
			boot_isolate(memory_mb)
		}
	}
}

/// Boot an isolate from the snapshot with this heap limit, belonging to no function yet.
fn boot_isolate(memory_mb: u32) -> Result<Booted, String> {
	let started = std::time::Instant::now();
	let cpu = thread_cpu_us();
	let parser = Arc::new(RuntimePermissionDescriptorParser::new(Sys::default()));
	let root = unbound_root();
	let limit = usize::try_from(memory_mb).unwrap_or(128) << 20;
	// Deno.mainModule names this until a function's module is loaded as main.
	let placeholder = ModuleSpecifier::parse("file:///snoutfn/unclaimed.js").map_err(|error| error.to_string())?;
	let (allocator, budget) = crate::v8_memory::allocator(limit);
	let options = WorkerOptions {
		startup_snapshot: deno_snapshots::CLI_SNAPSHOT,
		residual_lazy_js_sources: deno_snapshots::RESIDUAL_LAZY_JS,
		residual_lazy_esm_sources: deno_snapshots::RESIDUAL_LAZY_ESM,
		create_params: Some(v8::CreateParams::default().heap_limits(0, limit).array_buffer_allocator(allocator.make_shared())),
		..Default::default()
	};
	let worker = MainWorker::bootstrap_from_options::<DenoInNpmPackageChecker, NpmResolver<Sys>, Sys>(
		&placeholder,
		WorkerServiceOptions {
			deno_rt_native_addon_loader: None,
			module_loader: Rc::new(BundleLoader::new(root.clone())),
			permissions: PermissionsContainer::new(parser.clone(), Permissions::none_without_prompt()),
			blob_store: Arc::new(deno_runtime::deno_web::BlobStore::default()),
			broadcast_channel: Default::default(),
			feature_checker: Default::default(),
			node_services: Some(crate::node::services(root.clone())),
			npm_process_state_provider: Default::default(),
			root_cert_store_provider: Default::default(),
			fetch_dns_resolver: Default::default(),
			shared_array_buffer_store: Default::default(),
			compiled_wasm_module_store: Default::default(),
			v8_code_cache: Default::default(),
			fs: Arc::new(RealFs),
			bundle_provider: None,
		},
		options,
	);
	let mut worker = worker;
	// Deno loads Deno.serve's JavaScript (the HTTP server, Request/Response and web streams) the
	// first time the property is read. A spare reads it while it waits, so the function that
	// claims it does not compile it on its first request.
	worker
		.execute_script("[snout-functions:warm]", ModuleCodeString::from_static("void Deno.serve; void Deno.listen; void new Response('');"))
		.map_err(|error| format!("the worker could not be warmed: {error}"))?;
	debug!("spare for {memory_mb} MB booted in {} us ({} us CPU)", started.elapsed().as_micros(), thread_cpu_us() - cpu);
	Ok(Booted { worker, budget, root, parser })
}

/// CPU time this thread has had, from the scheduler's own count; 0 where there is none.
fn thread_cpu_us() -> u64 {
	std::fs::read_to_string("/proc/thread-self/schedstat")
		.ok()
		.and_then(|s| s.split_whitespace().next().and_then(|ns| ns.parse::<u64>().ok()))
		.map_or(0, |ns| ns / 1000)
}

fn rehearsing() -> bool {
	static ON: OnceLock<bool> = OnceLock::new();
	*ON.get_or_init(|| std::env::var("SNOUT_FUNCTIONS_REHEARSE").map_or(true, |v| v != "0"))
}

/// What a spare serves to itself: the shape of a cold request, so the JavaScript a function's
/// first request runs through (Deno.serve, Request, URL, a JSON body both ways) is compiled while
/// the spare waits rather than while a caller does. Deno's own server is used directly; nothing is
/// left on `globalThis`, and the server is shut down before the spare can be claimed.
const REHEARSAL: &str = r#"((path) => {
	const server = Deno.serve({ path, onListen() {} }, async (request) => {
		const at = new URL(request.url);
		const url = "http://" + (request.headers.get("host") ?? "localhost") + at.pathname + at.search;
		const forwarded = new Request(url, { method: request.method, headers: request.headers, body: request.body, duplex: "half" });
		const body = await forwarded.json();
		setTimeout(() => server.shutdown(), 0);
		return Response.json({ body, path: at.pathname, type: forwarded.headers.get("content-type") });
	});
})"#;

/// One request through a spare's own server, on a socket in a directory of its own, with only
/// that directory readable and writable (and the network, which Deno asks of a Unix socket, on a
/// claimed worker's terms) for as long as it takes. Nothing of any function is in
/// the isolate yet, so nothing of one can be left behind.
async fn rehearse(booted: &mut Booted) -> Result<(), String> {
	static NEXT: AtomicU64 = AtomicU64::new(0);
	let dir = std::env::temp_dir()
		.join("snout-functions-rehearsal")
		.join(format!("{}-{}", std::process::id(), NEXT.fetch_add(1, Ordering::Relaxed)));
	std::fs::create_dir_all(&dir).map_err(|error| format!("{}: {error}", dir.display()))?;
	let rehearsed = rehearse_in(booted, &dir).await;
	booted.worker.js_runtime.op_state().borrow_mut().put(PermissionsContainer::new(booted.parser.clone(), Permissions::none_without_prompt()));
	let _ = std::fs::remove_dir_all(&dir);
	rehearsed
}

async fn rehearse_in(booted: &mut Booted, dir: &std::path::Path) -> Result<(), String> {
	let place = dir.to_string_lossy().into_owned();
	let permissions = Permissions::from_options(
		booted.parser.as_ref(),
		&PermissionsOptions {
			allow_read: Some(vec![place.clone()]),
			allow_write: Some(vec![place]),
			allow_net: Some(vec![]),
			deny_net: Some(LOOPBACK.map(String::from).to_vec()),
			prompt: false,
			..Default::default()
		},
	)
	.map_err(|error| error.to_string())?;
	booted.worker.js_runtime.op_state().borrow_mut().put(PermissionsContainer::new(booted.parser.clone(), permissions));
	let socket = dir.join("s.sock");
	let script = format!("{REHEARSAL}({});", serde_json::to_string(&socket.to_string_lossy()).unwrap_or_default());
	booted.worker.execute_script("[snout-functions:rehearsal]", ModuleCodeString::from(script)).map_err(|error| error.to_string())?;
	let client = tokio::spawn(ask_rehearsal(socket));
	match tokio::time::timeout(Duration::from_secs(2), booted.worker.run_event_loop(false)).await {
		Err(_) => return Err("its server did not shut down".into()),
		Ok(Err(error)) => return Err(error.to_string()),
		Ok(Ok(())) => {}
	}
	// What the request left behind is garbage: collect it now, while nobody waits, so a spare
	// holds its compiled code and not the heap pages the rehearsal grew.
	// Without it a worker keeps ~1.5 MB more (8.5 against 6.7, memory-probe.sh), for ~6 ms of the
	// spare's CPU.
	booted.worker.js_runtime.v8_isolate().low_memory_notification();
	match client.await {
		Ok(Ok(())) => Ok(()),
		Ok(Err(error)) => Err(error),
		Err(error) => Err(error.to_string()),
	}
}

async fn ask_rehearsal(socket: PathBuf) -> Result<(), String> {
	use http_body_util::{BodyExt, Full};
	let stream = tokio::net::UnixStream::connect(&socket).await.map_err(|error| error.to_string())?;
	let (mut sender, connection) =
		hyper::client::conn::http1::handshake(hyper_util::rt::TokioIo::new(stream)).await.map_err(|error| error.to_string())?;
	tokio::spawn(async move {
		let _ = connection.await;
	});
	let request = hyper::Request::post("/rehearsal?cold=1")
		.header("host", "localhost")
		.header("content-type", "application/json")
		.body(Full::new(bytes::Bytes::from_static(br#"{"rehearsal":true}"#)))
		.map_err(|error| error.to_string())?;
	let response = sender.send_request(request).await.map_err(|error| error.to_string())?;
	if !response.status().is_success() {
		return Err(format!("it answered {}", response.status()));
	}
	response.into_body().collect().await.map_err(|error| error.to_string())?;
	Ok(())
}

/// What a worker runs: the bundle as prepared on the host (`.built/main.js`, its imports from
/// registries and URLs resolved into one file when the bundle arrived), or its own `index.ts`
/// when it has no imports to resolve.
fn entry(bundle: &std::path::Path) -> PathBuf {
	let built = bundle.join(".built").join("main.js");
	if built.exists() { built } else { bundle.join("index.ts") }
}

/// The builder's own words, when the host could not prepare this bundle and has no earlier
/// build of it to serve.
fn preparation_failed(bundle: &std::path::Path) -> Option<String> {
	let built = bundle.join(".built");
	if built.join("main.js").exists() {
		return None;
	}
	std::fs::read_to_string(built.join("error.txt")).ok().map(|text| text.trim().to_owned())
}

fn now_ms() -> u64 {
	u64::try_from(crate::uptime_ms()).unwrap_or(u64::MAX)
}
