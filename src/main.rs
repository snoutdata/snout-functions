//! snout-functions: the Snout Functions runtime.
//!
//! `start` is the front process: it answers the port, holds the door's secret and the manifests,
//! and runs each project's functions in a process of that project's own, confined to what is that
//! project's (router.rs, project.rs, confine.rs). `SNOUT_FUNCTIONS_PROCESSES=one` keeps the layout
//! before 0.2.0, every project in this one process, for the probes that measure one process. It
//! accepts the command line hosts already pass (`start --main-service /snoutfn/main --port 9000`);
//! the main service is ignored, because routing a request to its function is this binary's own work.
//!
//!   snout-functions start [--main-service <ignored>] [--port 9000] [--root /snoutfn]
//!   snout-functions project        (started by `start`, never by hand)
//!   snout-functions boot-bench [n]

/// A line on stderr when SNOUT_FUNCTIONS_DEBUG is set: worker starts and stops, limit decisions.
macro_rules! debug {
	($($arg:tt)*) => {
		if $crate::debugging() {
			eprintln!("{:>10} {}", $crate::uptime_ms(), format!($($arg)*));
		}
	};
}

mod compress;
mod compressible;
mod confine;
mod http;
mod isolate;
mod jwt;
mod loader;
mod manifest;
mod node;
mod project;
mod router;
mod supervisor;
mod v8_memory;

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

fn main() {
	let args: Vec<String> = std::env::args().collect();
	// V8 flags for every isolate, set once before the first one exists: ours, then any in
	// SNOUT_FUNCTIONS_V8_FLAGS (space-separated), which win. Ours are for many small isolates in one
	// process, measured with tests/memory-probe.sh on a c7g (2026-09-29): a paged young generation
	// (--minor-ms) commits 256 KB pages as it fills, where two semi-spaces committed 2 MB a worker
	// with 332 KB in use, and --optimize-for-size; together 8.6 to 6.2 MB a warm worker (with the
	// arena cap in the Containerfile), and CPU-bound work unchanged (tests/cpu-probe.sh, 41.1 ms).
	let mut all = vec![String::from("snout-functions"), String::from("--minor-ms"), String::from("--optimize-for-size")];
	if let Ok(flags) = std::env::var("SNOUT_FUNCTIONS_V8_FLAGS") {
		all.extend(flags.split_whitespace().map(str::to_owned));
	}
	let unknown = deno_core::v8_set_flags(all);
	if unknown.len() > 1 {
		eprintln!("unknown V8 flags: {:?}", &unknown[1..]);
	}
	match args.get(1).map(String::as_str) {
		Some("start") => start(&args[2..]),
		Some("project") => {
			let (idle, max_workers, max_replicas) = sizes();
			project::run(idle, max_workers, max_replicas);
		}
		Some("boot-bench") => boot_bench(args.get(2).and_then(|v| v.parse().ok()).unwrap_or(30)),
		_ => {
			eprintln!("usage: snout-functions start [--port 9000] [--root /snoutfn]");
			std::process::exit(2);
		}
	}
}

fn start(args: &[String]) {
	let mut port: u16 = 9000;
	let mut root = PathBuf::from("/snoutfn");
	let mut sockets = std::env::temp_dir().join("snout-functions");
	let mut i = 0;
	while i < args.len() {
		let value = args.get(i + 1).cloned().unwrap_or_default();
		match args[i].as_str() {
			"--port" => port = value.parse().unwrap_or(port),
			"--root" => root = PathBuf::from(value),
			"--sockets" => sockets = PathBuf::from(value),
			// Accepted for the catalogue's command line; the routing it named is ours now.
			"--main-service" => {}
			other => {
				eprintln!("unknown argument {other}");
				std::process::exit(2);
			}
		}
		i += 2;
	}
	let (idle, max_workers, max_replicas) = sizes();
	if std::env::var("SNOUT_FUNCTIONS_PROCESSES").map_or(true, |v| v != "one") {
		start_front(port, root, idle);
		return;
	}
	let _ = std::fs::remove_dir_all(&sockets);
	if let Err(error) = std::fs::create_dir_all(&sockets) {
		eprintln!("{}: {error}", sockets.display());
		std::process::exit(1);
	}
	let runtime = match tokio::runtime::Builder::new_multi_thread().enable_all().build() {
		Ok(runtime) => runtime,
		Err(error) => {
			eprintln!("{error}");
			std::process::exit(1);
		}
	};
	runtime.block_on(async move {
		let listener = match tokio::net::TcpListener::bind(("0.0.0.0", port)).await {
			Ok(listener) => listener,
			Err(error) => {
				eprintln!("port {port}: {error}");
				std::process::exit(1);
			}
		};
		let supervisor = supervisor::Supervisor::new(sockets, idle, max_workers, max_replicas, true);
		tokio::spawn(supervisor.clone().watch());
		// One isolate booted ahead for the default limit, so the first cold request after a start
		// does not pay the boot either.
		isolate::keep_spare(manifest::Limits::default().memory_mb);
		let door = std::env::var(http::DOOR_ENV).ok().filter(|v| !v.is_empty()).map(String::into_bytes);
		if door.is_none() {
			eprintln!("WARNING: {} is not set, so a request is not asked to prove it came through the front door", http::DOOR_ENV);
		}
		let state = Arc::new(http::State { manifests: manifest::Manifests::new(root), supervisor, door });
		eprintln!("snout-functions listening on {port}");
		let supervisor = state.supervisor.clone();
		let mut terminate = match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
			Ok(signal) => signal,
			Err(error) => {
				eprintln!("SIGTERM: {error}");
				std::process::exit(1);
			}
		};
		tokio::select! {
			() = http::serve(listener, state) => {}
			_ = terminate.recv() => {}
		}
		// Stopped: no new connection is accepted, and what is in flight gets to finish, up to a
		// bound under the engine's own stop timeout, before the process leaves.
		let drain = Duration::from_millis(env_number("SNOUT_FUNCTIONS_DRAIN_MS", 25_000));
		eprintln!("SIGTERM: draining {} request(s) in flight", supervisor.in_flight());
		let deadline = std::time::Instant::now() + drain;
		while supervisor.in_flight() > 0 && std::time::Instant::now() < deadline {
			tokio::time::sleep(Duration::from_millis(50)).await;
		}
		std::process::exit(0);
	});
}

/// How long a worker is kept idle, how many one process may run, and how many one function may.
fn sizes() -> (Duration, usize, usize) {
	let idle = Duration::from_millis(env_number("SNOUT_FUNCTIONS_IDLE_MS", 60_000));
	let max_workers = usize::try_from(env_number("SNOUT_FUNCTIONS_MAX_WORKERS", 256)).unwrap_or(256);
	// How many workers one function may have when each is busy on CPU: one a core, since more
	// could only take turns.
	let cores = std::thread::available_parallelism().map_or(1, std::num::NonZero::get);
	let max_replicas = usize::try_from(env_number("SNOUT_FUNCTIONS_MAX_REPLICAS", u64::try_from(cores).unwrap_or(1))).unwrap_or(1);
	(idle, max_workers, max_replicas)
}

/// The front process (router.rs): no isolate of its own, a process per project.
fn start_front(port: u16, root: PathBuf, idle: Duration) {
	let runtime = match tokio::runtime::Builder::new_multi_thread().enable_all().build() {
		Ok(runtime) => runtime,
		Err(error) => {
			eprintln!("{error}");
			std::process::exit(1);
		}
	};
	runtime.block_on(async move {
		let listener = match tokio::net::TcpListener::bind(("0.0.0.0", port)).await {
			Ok(listener) => listener,
			Err(error) => {
				eprintln!("port {port}: {error}");
				std::process::exit(1);
			}
		};
		let door = std::env::var(http::DOOR_ENV).ok().filter(|v| !v.is_empty()).map(String::into_bytes);
		if door.is_none() {
			eprintln!("WARNING: {} is not set, so a request is not asked to prove it came through the front door", http::DOOR_ENV);
		}
		let max_projects = usize::try_from(env_number("SNOUT_FUNCTIONS_MAX_PROJECTS", 64)).unwrap_or(64);
		let router = match router::Router::new(root, door, idle, max_projects) {
			Ok(router) => router,
			Err(error) => {
				eprintln!("{error}");
				std::process::exit(1);
			}
		};
		router.keep_spares();
		tokio::spawn(router.clone().watch());
		eprintln!("snout-functions listening on {port}, a process per project");
		let mut terminate = match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
			Ok(signal) => signal,
			Err(error) => {
				eprintln!("SIGTERM: {error}");
				std::process::exit(1);
			}
		};
		tokio::select! {
			() = router::serve(listener, router.clone()) => {}
			_ = terminate.recv() => {}
		}
		let drain = Duration::from_millis(env_number("SNOUT_FUNCTIONS_DRAIN_MS", 25_000));
		eprintln!("SIGTERM: draining {} request(s) in flight", router.in_flight());
		let deadline = std::time::Instant::now() + drain;
		while router.in_flight() > 0 && std::time::Instant::now() < deadline {
			tokio::time::sleep(Duration::from_millis(50)).await;
		}
		// Leaving closes every project's stdin, which ends their processes too.
		std::process::exit(0);
	});
}

static STARTED: std::sync::OnceLock<std::time::Instant> = std::sync::OnceLock::new();
static DEBUG: std::sync::OnceLock<bool> = std::sync::OnceLock::new();

pub fn debugging() -> bool {
	*DEBUG.get_or_init(|| std::env::var_os("SNOUT_FUNCTIONS_DEBUG").is_some())
}

pub fn uptime_ms() -> u128 {
	STARTED.get_or_init(std::time::Instant::now).elapsed().as_millis()
}

fn env_number(name: &str, fallback: u64) -> u64 {
	std::env::var(name).ok().and_then(|v| v.parse().ok()).unwrap_or(fallback)
}

fn anon_kb() -> u64 {
	std::fs::read_to_string("/proc/self/status")
		.ok()
		.and_then(|s| s.lines().find(|l| l.starts_with("RssAnon:")).map(str::to_owned))
		.and_then(|l| l.split_whitespace().nth(1).and_then(|v| v.parse().ok()))
		.unwrap_or(0)
}

/// How long an isolate takes to boot from the snapshot and what each costs: spike 1's question,
/// kept as a subcommand because it is the number every cold-start change moves.
fn boot_bench(n: usize) {
	let dir = std::env::temp_dir().join("snout-boot-bench");
	let _ = std::fs::create_dir_all(dir.join("bundle"));
	let _ = std::fs::write(dir.join("bundle/index.ts"), "Deno.serve(() => new Response('ok'));\n");
	let runtime = match tokio::runtime::Builder::new_multi_thread().enable_all().build() {
		Ok(runtime) => runtime,
		Err(error) => {
			eprintln!("{error}");
			return;
		}
	};
	runtime.block_on(async move {
		let before = anon_kb();
		let mut kept = Vec::new();
		let mut times = Vec::new();
		for i in 0..=n {
			let started = std::time::Instant::now();
			let spec = isolate::Spec {
				label: format!("bench/{i}"),
				bundle: dir.join("bundle"),
				socket_dir: dir.join(format!("w{i}")),
				env: Default::default(),
				limits: manifest::Limits::default(),
			};
			match isolate::spawn(spec).await {
				Ok(Ok(worker)) => kept.push(worker),
				Ok(Err(error)) => {
					eprintln!("{error}");
					return;
				}
				Err(_) => return,
			}
			if i > 0 {
				times.push(started.elapsed());
			} else {
				println!("first (V8 platform init + a serving worker): {:?}", started.elapsed());
			}
		}
		let after = anon_kb();
		times.sort();
		println!("a serving worker, {n} kept alive: p50 {:?} p95 {:?} max {:?}", times[n / 2], times[(n * 95) / 100], times[n - 1]);
		println!("anon: {} MB before, {} MB after, {:.2} MB per worker", before / 1024, after / 1024, (after.saturating_sub(before)) as f64 / 1024.0 / (n + 1) as f64);
		for worker in &kept {
			worker.stop("bench over");
		}
		tokio::time::sleep(std::time::Duration::from_millis(200)).await;
	});
}
