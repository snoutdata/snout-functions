//! A worker's CPU, counted for the whole of its life (audit 5-A).
//!
//! The supervisor's stall check stops a REQUEST that holds its worker's thread without yielding,
//! and only while a request is in flight. That left three ways to hold a core with no stop at all:
//! a module whose top-level code never returns (its worker is still starting, so the supervisor
//! has nothing to check), work after the response (`setTimeout(() => { while (true) {} })`), and
//! work that yields between slices shorter than the limit. Each pinned a core until the worker had
//! been idle for a minute, or forever while it kept being called.
//!
//! So every claimed worker also has a `Meter`: its thread's own CPU clock, read from a watchdog
//! thread every `TICK` whether or not the worker is serving, against a CREDIT of CPU. A worker
//! starts with one unit (the project's hard CPU limit) for loading its module, gains one unit for
//! each request it is given, and may bank no more than one unit per request in flight plus one.
//! A worker past its credit is stopped, as the memory limits stop one. So a function may use, on
//! average, the CPU limit per request, which is what the limit promises; a stream of short requests
//! never comes near it, and nothing done after the last response may use more than one unit.
//!
//! The clock is the kernel's per-thread CPU clock (`pthread_getcpuclockid`), user and system time
//! together, and works in a project's process with no /proc (its root has none). This module is
//! those two system calls and the arithmetic; `unsafe` is denied everywhere else (Cargo.toml).

#![allow(unsafe_code)]

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

/// How often the watchdog reads every worker's clock.
const TICK: Duration = Duration::from_millis(20);

/// The calling thread's CPU clock, readable from any thread of the process while it lives.
#[derive(Clone, Copy)]
pub struct ThreadClock(#[cfg(target_os = "linux")] libc::clockid_t);

impl ThreadClock {
	/// The clock of the thread that calls this; None where the platform has no such clock.
	#[cfg(target_os = "linux")]
	pub fn current() -> Option<Self> {
		let mut clock: libc::clockid_t = 0;
		// SAFETY: pthread_self names the calling thread, which is alive; `clock` outlives the call.
		let failed = unsafe { libc::pthread_getcpuclockid(libc::pthread_self(), &raw mut clock) };
		(failed == 0).then_some(ThreadClock(clock))
	}

	#[cfg(not(target_os = "linux"))]
	pub fn current() -> Option<Self> {
		None
	}

	/// The CPU the thread has used, in microseconds; None once it has gone.
	#[cfg(target_os = "linux")]
	pub fn read_us(self) -> Option<u64> {
		let mut now = libc::timespec {
			tv_sec: 0,
			tv_nsec: 0,
		};
		// SAFETY: a plain system call on a clock id and a timespec that outlives it. A clock whose
		// thread has ended answers EINVAL rather than anything unsafe.
		if unsafe { libc::clock_gettime(self.0, &raw mut now) } != 0 {
			return None;
		}
		let secs = u64::try_from(now.tv_sec).ok()?;
		let nanos = u64::try_from(now.tv_nsec).ok()?;
		Some(secs * 1_000_000 + nanos / 1_000)
	}

	#[cfg(not(target_os = "linux"))]
	pub fn read_us(self) -> Option<u64> {
		None
	}
}

/// The arithmetic of the credit, apart from any clock, so it can be tested.
#[derive(Debug)]
pub struct Ledger {
	unit_us: i64,
	credit_us: i64,
	seen_us: u64,
}

impl Ledger {
	/// A worker whose clock now reads `cpu_us`: one unit, for loading its module.
	pub fn new(unit_us: u64, cpu_us: u64) -> Self {
		let unit_us = i64::try_from(unit_us).unwrap_or(i64::MAX / 4);
		Ledger {
			unit_us,
			credit_us: unit_us,
			seen_us: cpu_us,
		}
	}

	/// A request given to the worker brings one unit.
	pub fn grant(&mut self) {
		self.credit_us = self.credit_us.saturating_add(self.unit_us);
	}

	/// Charge what the clock has moved since it was last read, with `inflight` requests held now.
	/// False once the credit is spent.
	pub fn charge(&mut self, cpu_us: u64, inflight: usize) -> bool {
		let used = i64::try_from(cpu_us.saturating_sub(self.seen_us)).unwrap_or(i64::MAX);
		self.seen_us = self.seen_us.max(cpu_us);
		let cap = self.unit_us.saturating_mul(
			i64::try_from(inflight)
				.unwrap_or(i64::MAX)
				.saturating_add(1),
		);
		self.credit_us = self.credit_us.saturating_sub(used).min(cap);
		self.credit_us >= 0
	}
}

/// One worker's meter, as the supervisor and the watchdog hold it.
pub struct Meter {
	clock: ThreadClock,
	ledger: Mutex<Ledger>,
	inflight: AtomicUsize,
	done: AtomicBool,
	spent: Box<dyn Fn() + Send + Sync>,
}

impl Meter {
	/// A request starts on this worker.
	pub fn begin(&self) {
		self.inflight.fetch_add(1, Ordering::AcqRel);
		if let Ok(mut ledger) = self.ledger.lock() {
			ledger.grant();
		}
	}

	/// A request on this worker is over.
	pub fn end(&self) {
		let _ = self
			.inflight
			.fetch_update(Ordering::AcqRel, Ordering::Acquire, |held| {
				held.checked_sub(1)
			});
	}

	/// The worker has stopped: the watchdog lets it go.
	pub fn finish(&self) {
		self.done.store(true, Ordering::Release);
	}

	fn check(&self) {
		let Some(now) = self.clock.read_us() else {
			self.finish();
			return;
		};
		let within = self
			.ledger
			.lock()
			.is_ok_and(|mut ledger| ledger.charge(now, self.inflight.load(Ordering::Acquire)));
		if !within {
			self.finish();
			(self.spent)();
		}
	}
}

/// Meter the CALLING thread from now, with `unit_ms` of CPU per request; `spent` is called once,
/// from the watchdog's thread, if the credit runs out. None where there is no thread clock.
pub fn meter(unit_ms: u64, spent: impl Fn() + Send + Sync + 'static) -> Option<Arc<Meter>> {
	let clock = ThreadClock::current()?;
	let ledger = Ledger::new(unit_ms.saturating_mul(1_000), clock.read_us()?);
	let meter = Arc::new(Meter {
		clock,
		ledger: Mutex::new(ledger),
		inflight: AtomicUsize::new(0),
		done: AtomicBool::new(false),
		spent: Box::new(spent),
	});
	watched().lock().ok()?.push(meter.clone());
	Some(meter)
}

/// Every metered worker, and the thread that reads them, started with the first.
fn watched() -> &'static Mutex<Vec<Arc<Meter>>> {
	static WATCHED: OnceLock<Mutex<Vec<Arc<Meter>>>> = OnceLock::new();
	WATCHED.get_or_init(|| {
		let spawned = std::thread::Builder::new()
			.name("cpu-watchdog".into())
			.spawn(|| {
				loop {
					std::thread::sleep(TICK);
					let meters: Vec<Arc<Meter>> = match watched().lock() {
						Ok(mut held) => {
							held.retain(|meter| !meter.done.load(Ordering::Acquire));
							held.clone()
						}
						Err(_) => continue,
					};
					for meter in meters {
						meter.check();
					}
				}
			});
		if let Err(error) = spawned {
			eprintln!("the CPU watchdog could not be started: {error}");
		}
		Mutex::new(Vec::new())
	})
}

#[cfg(test)]
mod tests {
	use super::Ledger;

	const UNIT: u64 = 3_500_000;

	#[test]
	fn a_module_that_never_returns_is_stopped_after_one_unit() {
		let mut ledger = Ledger::new(UNIT, 100);
		assert!(ledger.charge(100 + UNIT, 0));
		assert!(!ledger.charge(100 + UNIT + 1, 0));
	}

	#[test]
	fn work_after_the_response_gets_one_unit_whatever_was_banked() {
		let mut ledger = Ledger::new(UNIT, 0);
		// A thousand cheap requests, one at a time: nothing is banked past what is in flight.
		let mut cpu = 0;
		for _ in 0..1_000 {
			ledger.grant();
			cpu += 1_000;
			assert!(ledger.charge(cpu, 1));
		}
		// Every response sent; a busy loop in a timer from here on.
		assert!(ledger.charge(cpu, 0));
		assert!(ledger.charge(cpu + UNIT, 0));
		assert!(!ledger.charge(cpu + UNIT + 20_000, 0));
	}

	#[test]
	fn slices_that_yield_are_counted_together() {
		// A request that burns CPU in slices shorter than the limit, yielding between them: the
		// stall check never sees it, and the credit does.
		let mut ledger = Ledger::new(UNIT, 0);
		ledger.grant();
		let mut cpu = 0;
		let mut stopped = false;
		for _ in 0..100 {
			cpu += 1_000_000;
			if !ledger.charge(cpu, 1) {
				stopped = true;
				break;
			}
		}
		assert!(stopped);
		assert!(cpu <= 2 * UNIT + 1_000_000);
	}

	#[test]
	fn a_steady_stream_of_requests_within_the_limit_is_never_stopped() {
		// Sixty requests of a second's CPU each, all queued on one worker at once, and
		// then a thousand more, one after another.
		let mut ledger = Ledger::new(UNIT, 0);
		for _ in 0..60 {
			ledger.grant();
		}
		let mut cpu = 0;
		for held in (0..60).rev() {
			cpu += 1_000_000;
			assert!(
				ledger.charge(cpu, held),
				"stopped with {held} still in flight"
			);
		}
		for _ in 0..1_000 {
			ledger.grant();
			cpu += 3_000_000;
			assert!(ledger.charge(cpu, 1));
			assert!(ledger.charge(cpu, 0));
		}
	}

	#[test]
	fn a_request_may_use_its_whole_limit_on_top_of_what_loading_left() {
		let mut ledger = Ledger::new(UNIT, 0);
		ledger.grant();
		assert!(ledger.charge(2 * UNIT, 1));
		assert!(!ledger.charge(2 * UNIT + 1, 1));
	}

	#[cfg(target_os = "linux")]
	#[test]
	fn the_thread_clock_moves_with_work_and_is_read_from_another_thread() {
		let clock = super::ThreadClock::current().expect("a thread CPU clock");
		let before = clock.read_us().unwrap();
		let mut x: u64 = 0;
		let started = std::time::Instant::now();
		while started.elapsed() < std::time::Duration::from_millis(50) {
			x = std::hint::black_box(x.wrapping_add(1));
		}
		let read_elsewhere = std::thread::spawn(move || clock.read_us())
			.join()
			.unwrap()
			.unwrap();
		assert!(
			read_elsewhere >= before + 20_000,
			"{before} then {read_elsewhere}"
		);
	}
}
