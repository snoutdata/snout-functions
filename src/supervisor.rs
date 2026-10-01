//! Which workers exist, starting them, and stopping them.
//!
//! A worker is keyed by EVERYTHING that decides what it runs: the project, the function, the
//! bundle's digest, the environment and the limits. So a changed secret or limit is a new key,
//! and the very next request meets a worker that has it, never a warm worker still serving the
//! old values. The old worker is simply no longer found and is reaped when idle.
//!
//! A key usually has one worker, and one worker serves any number of requests that wait on I/O
//! (100 held at once in the bench). It gets another, up to `max_replicas`, only when EVERY worker it
//! has is stalled with a request in flight: its event loop has not turned for `STALL_MS`, which is
//! what CPU-bound work looks like and I/O never does. Without it, four CPU-bound requests to one
//! function shared one core (16 a second on a 2-core t4g.medium; 28 with it, 2026-09-29).
//!
//! The wall clock is the REQUEST's (the HTTP layer enforces it), not the worker's: a warm worker
//! is not retired part-way through its limit with requests still arriving. What the supervisor
//! enforces is CPU, as the longest a request may hold its worker's thread WITHOUT YIELDING: the
//! worker thread beats a heartbeat whenever its event loop turns, and a heartbeat stalled longer
//! than the project's hard CPU limit, with a request in flight, stops the isolate. That is what a
//! CPU limit protects (one busy loop starving every request its worker holds); work that yields
//! is bounded by the wall clock instead. (Per busy period, as first built, charged a stream of
//! short requests their sum and stopped a healthy worker under steady load: 4 errors in the
//! 2026-09-29 bench. Per request since arrival would charge a long stream everyone else's CPU.)
//!
//! Room for a worker is counted twice: by number (`max_workers`) and by MEMORY. Every project on
//! the host is in one container with one memory cap (1 GB on the fleet), and the kernel's answer to
//! a container over its cap is to kill what is in it, every project at once. So past
//! `MEMORY_ROOM` of the cap a new worker starts only by stopping an idle one; with none idle, a
//! request waits on a worker its function already has, or hears 503 and `Retry-After`. And past
//! `MEMORY_GUARD`, when workers already running have grown, the one holding the most is stopped:
//! its requests hear why, and the line it logs ("memory guard") is the one to watch for.
//!
//! Each project runs in a process of its own (router.rs), and only the front process sees the
//! whole container, so a project's supervisor is built not to weigh (`weigh: false`): the front
//! process counts, and asks the process holding the most to stop its largest worker
//! (`stop_largest`). A supervisor that weighs is the single-process layout (`start --shared`).

use std::collections::{BTreeMap, HashMap};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tokio::sync::OnceCell;

use crate::isolate::{self, Spec, Worker};
use crate::manifest::Limits;

/// How long a worker's event loop may go without turning, with a request in flight, before it
/// counts as busy on CPU. The heartbeat beats every 10 ms on a free loop.
const STALL_MS: u64 = 30;

/// The share of the container's memory past which a new worker must make room first, in percent.
/// A worker costs ~7 MB before its function allocates (tests/memory-probe.sh), so this leaves a
/// cap of 1 GB about 150 MB for the functions already running to grow into.
pub const MEMORY_ROOM: u64 = 85;

/// The share of the container's memory past which the worker holding the most is stopped, so one
/// function pays for the host running short rather than the kernel killing every project. 90, not
/// higher: memory is sampled every 100 ms and a heap every 250 ms, and a function allocating fast
/// reached 98% of a 120 MB cap before a guard at 95 acted (tests/memory-guard-probe.sh). At most
/// one a `GUARD_EVERY_MS`, which gives a stopped worker's memory time to come back, and only a
/// worker holding at least `GUARD_SHARE` of the cap: past it the memory is elsewhere, and
/// stopping a small worker would free nothing.
pub const MEMORY_GUARD: u64 = 90;
pub const GUARD_EVERY_MS: u64 = 1_000;
const GUARD_SHARE: u64 = 20;

#[derive(Clone, PartialEq, Eq, Hash)]
pub struct Key {
	project: String,
	name: String,
	digest: String,
	env: u64,
	limits: Limits,
}

impl Key {
	pub fn new(project: &str, name: &str, digest: &str, env: &BTreeMap<String, String>, limits: Limits) -> Self {
		let mut hasher = DefaultHasher::new();
		env.hash(&mut hasher);
		Key { project: project.to_owned(), name: name.to_owned(), digest: digest.to_owned(), env: hasher.finish(), limits }
	}
}

pub struct Entry {
	pub worker: Arc<Worker>,
	inflight: AtomicUsize,
	last_used_ms: AtomicU64,
}

impl Entry {
	fn busy_on_cpu(&self) -> bool {
		self.inflight.load(Ordering::Acquire) > 0 && self.worker.stalled_ms() > STALL_MS
	}
}

type Slot = Arc<OnceCell<Result<Arc<Entry>, String>>>;

pub struct Supervisor {
	sockets: PathBuf,
	/// Each key's workers, one slot each; a slot is empty while its worker starts.
	slots: Mutex<HashMap<Key, Vec<Slot>>>,
	serial: AtomicU64,
	started: Instant,
	pub idle: Duration,
	pub max_workers: usize,
	pub max_replicas: usize,
	/// The memory cap in bytes, and whether it is the cgroup's (else `SNOUT_FUNCTIONS_MEMORY_MB`,
	/// counted against this process alone); None where there is none.
	memory_cap: Option<(u64, bool)>,
	/// Anonymous memory in use, sampled by `watch` every 100 ms, or as the front process last
	/// told it (`set_memory`).
	memory_used: AtomicU64,
	/// The container's cap as the front process told it, for a project's process, which can see
	/// neither the cgroup nor the others; 0 until it has. Used only to make room, never to guard.
	told_cap: AtomicU64,
}

/// A request's claim on a worker: counted in flight while it lives.
pub struct Lease {
	pub entry: Arc<Entry>,
	supervisor: Arc<Supervisor>,
}

impl Drop for Lease {
	fn drop(&mut self) {
		self.entry.last_used_ms.store(self.supervisor.now_ms(), Ordering::Release);
		self.entry.inflight.fetch_sub(1, Ordering::AcqRel);
	}
}

pub enum Refusal {
	/// The function's own code failed to start: the caller should hear why.
	Start(String),
	/// Every worker slot on this host is busy: try again shortly.
	Full,
}

impl Supervisor {
	pub fn new(sockets: PathBuf, idle: Duration, max_workers: usize, max_replicas: usize, weigh: bool) -> Arc<Self> {
		let memory_cap = if weigh { memory_cap() } else { None };
		match memory_cap {
			Some((cap, cgroup)) => eprintln!("memory cap {} MB ({}): new workers make room past {MEMORY_ROOM}%", cap >> 20, if cgroup { "the container's" } else { "SNOUT_FUNCTIONS_MEMORY_MB, this process" }),
			None if weigh => eprintln!("no memory cap found: workers are counted, not weighed"),
			None => {}
		}
		Arc::new(Supervisor {
			sockets,
			slots: Mutex::new(HashMap::new()),
			serial: AtomicU64::new(0),
			started: Instant::now(),
			idle,
			max_workers,
			max_replicas: max_replicas.max(1),
			memory_cap,
			memory_used: AtomicU64::new(0),
			told_cap: AtomicU64::new(0),
		})
	}

	/// The container's memory as the front process read it: a project's process makes room by it.
	pub fn set_memory(&self, used: u64, cap: u64) {
		self.memory_used.store(used, Ordering::Release);
		self.told_cap.store(cap, Ordering::Release);
	}

	/// Whether one more worker fits under the memory cap without stopping another.
	fn memory_allows_another(&self) -> bool {
		let cap = self.memory_cap.map_or_else(|| self.told_cap.load(Ordering::Acquire), |(cap, _)| cap);
		cap == 0 || self.memory_used.load(Ordering::Acquire) < cap / 100 * MEMORY_ROOM
	}

	fn now_ms(&self) -> u64 {
		u64::try_from(self.started.elapsed().as_millis()).unwrap_or(u64::MAX)
	}

	/// Which of a key's workers takes this request, or a new one for it. `concurrency` is the
	/// function's own cap on workers (sql/100), within the host's.
	fn choose(&self, key: &Key, concurrency: Option<usize>) -> Result<Slot, Refusal> {
		let max_replicas = concurrency.map_or(self.max_replicas, |wanted| wanted.clamp(1, self.max_replicas));
		let mut slots = self.slots.lock().map_err(|_| Refusal::Full)?;
		let total: usize = slots.values().map(Vec::len).sum();
		let held = slots.get(key).map(Vec::as_slice).unwrap_or_default();
		// A worker still starting is as good as a free one: the request waits for it either way.
		let free = held
			.iter()
			.filter(|slot| slot.get().is_none_or(|started| started.as_ref().is_ok_and(|entry| !entry.busy_on_cpu())))
			.min_by_key(|slot| slot.get().and_then(|started| started.as_ref().ok()).map_or(0, |entry| entry.inflight.load(Ordering::Acquire)));
		if let Some(slot) = free {
			return Ok(slot.clone());
		}
		let room = (total < self.max_workers && self.memory_allows_another()) || self.evict_one_idle(&mut slots);
		let held = slots.get(key).map(Vec::as_slice).unwrap_or_default();
		if held.len() < max_replicas && room {
			let slot = Slot::default();
			slots.entry(key.clone()).or_default().push(slot.clone());
			return Ok(slot);
		}
		// Every worker is busy and no other may start: queue on the one with least in flight.
		held.iter()
			.min_by_key(|slot| slot.get().and_then(|started| started.as_ref().ok()).map_or(0, |entry| entry.inflight.load(Ordering::Acquire)))
			.cloned()
			.ok_or(Refusal::Full)
	}

	pub async fn lease(self: &Arc<Self>, key: Key, bundle: PathBuf, env: &BTreeMap<String, String>, concurrency: Option<usize>) -> Result<Lease, Refusal> {
		for _ in 0..2 {
			let slot = self.choose(&key, concurrency)?;
			let started = slot
				.get_or_init(|| async {
					let serial = self.serial.fetch_add(1, Ordering::Relaxed);
					let spec = Spec {
						label: format!("{}/{}", key.project, key.name),
						bundle: bundle.clone(),
						socket_dir: self.sockets.join(format!("w{serial}")),
						env: env.clone(),
						limits: key.limits,
					};
					match isolate::spawn(spec).await {
						Ok(Ok(worker)) => Ok(Arc::new(Entry {
							worker,
							inflight: AtomicUsize::new(0),
							last_used_ms: AtomicU64::new(self.now_ms()),
						})),
						Ok(Err(error)) => Err(error),
						Err(_) => Err("the worker stopped while starting".to_owned()),
					}
				})
				.await
				.clone();
			match started {
				Ok(entry) if entry.worker.alive() => {
					entry.inflight.fetch_add(1, Ordering::AcqRel);
					return Ok(Lease { entry, supervisor: self.clone() });
				}
				Ok(_) => {
					// It died since it was found: forget it and start again, once.
					self.forget(&key, &slot);
				}
				Err(error) => {
					// A start that failed is not remembered, so the next request tries again.
					self.forget(&key, &slot);
					return Err(Refusal::Start(error));
				}
			}
		}
		Err(Refusal::Start("the function's worker kept stopping as it started".into()))
	}

	/// Requests in flight across every worker: what a stop waits for.
	pub fn in_flight(&self) -> usize {
		self.slots
			.lock()
			.map(|slots| {
				slots.values().flatten().filter_map(|slot| slot.get()?.as_ref().ok().map(|e| e.inflight.load(Ordering::Acquire))).sum()
			})
			.unwrap_or(0)
	}

	fn forget(&self, key: &Key, slot: &Slot) {
		if let Ok(mut slots) = self.slots.lock()
			&& let Some(held) = slots.get_mut(key)
		{
			held.retain(|one| !Arc::ptr_eq(one, slot));
			if held.is_empty() {
				slots.remove(key);
			}
		}
	}

	/// Make room by stopping the least recently used worker with nothing in flight (a start
	/// that failed holds no worker and goes first).
	fn evict_one_idle(&self, slots: &mut HashMap<Key, Vec<Slot>>) -> bool {
		let victim = slots
			.iter()
			.flat_map(|(key, held)| held.iter().map(move |slot| (key, slot)))
			.filter_map(|(key, slot)| match slot.get() {
				Some(Ok(entry)) if entry.inflight.load(Ordering::Acquire) == 0 => Some((key.clone(), slot.clone(), entry.last_used_ms.load(Ordering::Acquire))),
				Some(Err(_)) => Some((key.clone(), slot.clone(), 0)),
				_ => None,
			})
			.min_by_key(|(_, _, used)| *used);
		let Some((key, slot, _)) = victim else { return false };
		if let Some(Ok(entry)) = slot.get() {
			entry.worker.stop("evicted to make room");
		}
		if let Some(held) = slots.get_mut(&key) {
			held.retain(|one| !Arc::ptr_eq(one, &slot));
			if held.is_empty() {
				slots.remove(&key);
			}
		}
		true
	}

	/// Every 20 ms: stop workers over their CPU limit, reap the dead and the long idle.
	pub async fn watch(self: Arc<Self>) {
		let mut tick = tokio::time::interval(Duration::from_millis(20));
		let mut sweeps: u64 = 0;
		let mut guarded_ms: u64 = 0;
		loop {
			tick.tick().await;
			sweeps += 1;
			if sweeps.is_multiple_of(50) {
				isolate::trim_spares();
			}
			if let Some((_, cgroup)) = self.memory_cap
				&& sweeps.is_multiple_of(5)
			{
				self.memory_used.store(memory_used(cgroup), Ordering::Release);
			}
			let now = self.now_ms();
			let over = self.memory_cap.and_then(|(cap, _)| {
				let used = self.memory_used.load(Ordering::Acquire);
				(used > cap / 100 * MEMORY_GUARD && now.saturating_sub(guarded_ms) > GUARD_EVERY_MS).then_some((used, cap))
			});
			let idle_ms = u64::try_from(self.idle.as_millis()).unwrap_or(u64::MAX);
			let Ok(mut slots) = self.slots.lock() else { continue };
			for held in slots.values_mut() {
				held.retain(|slot| match slot.get() {
					None => true,
					Some(Err(_)) => false,
					Some(Ok(entry)) => {
						if !entry.worker.alive() {
							return false;
						}
						let inflight = entry.inflight.load(Ordering::Acquire);
						if inflight > 0 {
							let stalled = entry.worker.stalled_ms();
							if stalled > entry.worker.limits.hard_cpu() {
								entry.worker.stop("CPU");
								return false;
							}
							true
						} else if sweeps.is_multiple_of(50) && now.saturating_sub(entry.last_used_ms.load(Ordering::Acquire)) > idle_ms {
							entry.worker.stop("idle");
							false
						} else {
							true
						}
					}
				});
			}
			slots.retain(|_, held| !held.is_empty());
			if let Some((used, cap)) = over
				&& stop_largest_in(&slots, used, cap)
			{
				guarded_ms = now;
			}
		}
	}

	/// The memory guard, asked for by the front process, which sees the container: stop this
	/// process's largest worker if it holds a share worth stopping. Whether one was stopped.
	pub fn stop_largest(&self, used: u64, cap: u64) -> bool {
		self.slots.lock().is_ok_and(|slots| stop_largest_in(&slots, used, cap))
	}
}

fn stop_largest_in(slots: &HashMap<Key, Vec<Slot>>, used: u64, cap: u64) -> bool {
	let largest = slots
		.values()
		.flatten()
		.filter_map(|slot| slot.get()?.as_ref().ok().cloned())
		.filter(|entry| entry.worker.alive())
		.max_by_key(|entry| entry.worker.memory_bytes())
		.filter(|entry| entry.worker.memory_bytes() >= cap / GUARD_SHARE);
	let Some(entry) = largest else { return false };
	eprintln!(
		"{}: stopped by the memory guard, holding {} MB, with the container at {}% of its {} MB",
		entry.worker.label,
		entry.worker.memory_bytes() >> 20,
		used * 100 / cap.max(1),
		cap >> 20
	);
	entry.worker.stop("the host ran short of memory");
	true
}

/// The container's memory cap: `SNOUT_FUNCTIONS_MEMORY_MB` if set, else its cgroup's
/// `memory.max` (the container's own, under a private cgroup namespace), else none.
pub fn memory_cap() -> Option<(u64, bool)> {
	if let Some(mb) = std::env::var("SNOUT_FUNCTIONS_MEMORY_MB").ok().and_then(|v| v.parse::<u64>().ok()) {
		return Some((mb << 20, false));
	}
	std::fs::read_to_string("/sys/fs/cgroup/memory.max").ok().and_then(|v| v.trim().parse().ok()).map(|cap| (cap, true))
}

/// Anonymous memory in use: the cgroup's `anon` when the cap is the cgroup's (what the cap is
/// reached by; file pages are reclaimed first), else this process's own.
pub fn memory_used(cgroup: bool) -> u64 {
	let counted = cgroup
		.then(|| std::fs::read_to_string("/sys/fs/cgroup/memory.stat").ok())
		.flatten()
		.and_then(|stat| stat.lines().find_map(|line| line.strip_prefix("anon ").and_then(|v| v.trim().parse().ok())));
	counted.unwrap_or_else(|| {
		std::fs::read_to_string("/proc/self/status")
			.ok()
			.and_then(|status| status.lines().find_map(|line| line.strip_prefix("RssAnon:").and_then(|v| v.split_whitespace().next()?.parse::<u64>().ok())))
			.map_or(0, |kb| kb << 10)
	})
}
