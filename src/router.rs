//! The front process: every project's functions in a process of that project's own.
//!
//! One process per host used to run every project's isolates, so the only wall between two
//! customers was V8's. A bug that let code out of its isolate would have been in a process holding
//! every project's manifest on the host: their secrets, their service keys. Since 0.2.0 this
//! process runs no customer code at all. It holds the door's secret and reads the manifests, and
//! for each project it hands a process of its own (project.rs) that project's manifest and nothing
//! else, in a directory holding only that project's bundles, as a user of its own (confine.rs).
//! Code that escapes its isolate is then in a process that can read only its own project's
//! secrets, and cannot reach the others' processes, sockets or files, nor this one's.
//!
//! What it costs, and how that is kept small:
//!   * a process per project with functions running: about 10 MB, V8's platform and one spare
//!     isolate; a project's process is stopped once nothing has asked it for `idle`;
//!   * a process's start: one is kept booted and unassigned (`SNOUT_FUNCTIONS_SPARE_PROCESSES`,
//!     default 1), so a project's first request finds it waiting, as a function finds a spare
//!     isolate;
//!   * one more hop, over a Unix socket, for every request.
//!
//! Memory is weighed here, since only this process sees the whole container: past `MEMORY_ROOM`
//! of the cap a new project's process starts only by stopping an idle one, and past
//! `MEMORY_GUARD` the process holding the most is told to stop its largest worker.

use std::collections::{HashMap, HashSet};
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::process::Stdio;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::{Duration, SystemTime};

use bytes::Bytes;
use http_body::{Body, Frame};
use http_body_util::combinators::BoxBody;
use hyper::body::Incoming;
use hyper::service::service_fn;
use hyper::{Request, StatusCode, Uri};
use hyper_util::rt::TokioIo;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, Lines};
use tokio::net::{TcpListener, UnixStream};
use tokio::process::{ChildStdin, ChildStdout};
use tokio::sync::OnceCell;

use crate::http::{DOOR_HEADER, Error, HEALTH_PATH, Reply, refuse, text, through_door};
use crate::manifest::{self, Manifest, Manifests};
use crate::supervisor::{GUARD_EVERY_MS, MEMORY_GUARD, MEMORY_ROOM};

/// The first uid a project's process runs as; each project on this host gets its own, in order.
const FIRST_UID: u32 = 20_000;

/// The variables a project's process is started with; everything else of this process's
/// environment, the door's secret first, stays here.
const PASSED: [&str; 10] = [
	"MALLOC_ARENA_MAX",
	"MALLOC_MMAP_THRESHOLD_",
	"SNOUT_FUNCTIONS_IDLE_MS",
	"SNOUT_FUNCTIONS_MAX_WORKERS",
	"SNOUT_FUNCTIONS_MAX_REPLICAS",
	"SNOUT_FUNCTIONS_SPARES",
	"SNOUT_FUNCTIONS_REHEARSE",
	"SNOUT_FUNCTIONS_V8_FLAGS",
	"SNOUT_FUNCTIONS_DEBUG",
	"SNOUT_FUNCTIONS_DRAIN_MS",
];

/// A project's process, as this one holds it.
pub struct Project {
	name: String,
	socket: PathBuf,
	pid: u32,
	/// The user it runs as, where it is confined: the only one whose socket it may be.
	uid: Option<u32>,
	/// Its orders; dropped to stop it (its stdin closing ends it).
	orders: tokio::sync::Mutex<Option<ChildStdin>>,
	/// The manifest file's time when it was last sent, and the bundles in its directory.
	sent: Mutex<(Option<SystemTime>, HashSet<String>)>,
	/// Held while its directory is written, so a changed manifest and the refresh in `watch`
	/// never copy the same bundle at once.
	preparing: tokio::sync::Mutex<()>,
	inflight: AtomicUsize,
	last_used_ms: AtomicU64,
	alive: Arc<AtomicBool>,
}

type Slot = Arc<OnceCell<Result<Arc<Project>, String>>>;

/// A process started and waiting for a project.
struct Spare {
	pid: u32,
	stdin: ChildStdin,
	stdout: Lines<BufReader<ChildStdout>>,
	alive: Arc<AtomicBool>,
}

pub struct Router {
	manifests: Manifests,
	bundles: PathBuf,
	views: PathBuf,
	door: Option<Vec<u8>>,
	confine: bool,
	binary: PathBuf,
	projects: Mutex<HashMap<String, Slot>>,
	spares: Mutex<Vec<Spare>>,
	spare_count: usize,
	uids: Mutex<HashMap<String, u32>>,
	next_uid: AtomicU32,
	idle_ms: u64,
	max_projects: usize,
	memory_cap: Option<(u64, bool)>,
	memory_used: AtomicU64,
}

enum Refusal {
	Full,
	Start(String),
}

impl Router {
	pub fn new(root: PathBuf, door: Option<Vec<u8>>, idle: Duration, max_projects: usize) -> Result<Arc<Self>, String> {
		let views = std::env::temp_dir().join("snout-functions-projects");
		let _ = std::fs::remove_dir_all(&views);
		std::fs::create_dir_all(&views).map_err(|error| format!("{}: {error}", views.display()))?;
		let binary = std::env::current_exe().map_err(|error| format!("this binary's own path: {error}"))?;
		let confine = crate::confine::can_confine();
		if !confine {
			eprintln!("WARNING: not running as root, so each project's process shares this process's user and files: it is separate but not confined");
		}
		let memory_cap = crate::supervisor::memory_cap();
		match memory_cap {
			Some((cap, _)) => eprintln!("memory cap {} MB: a new project's process makes room past {MEMORY_ROOM}%", cap >> 20),
			None => eprintln!("no memory cap found: projects are counted, not weighed"),
		}
		Ok(Arc::new(Router {
			bundles: root.join("bundles"),
			manifests: Manifests::new(root),
			views,
			door,
			confine,
			binary,
			projects: Mutex::new(HashMap::new()),
			spares: Mutex::new(Vec::new()),
			spare_count: usize::try_from(crate::env_number("SNOUT_FUNCTIONS_SPARE_PROCESSES", 1)).unwrap_or(1),
			uids: Mutex::new(HashMap::new()),
			next_uid: AtomicU32::new(FIRST_UID),
			idle_ms: u64::try_from(idle.as_millis()).unwrap_or(u64::MAX),
			max_projects,
			memory_cap,
			memory_used: AtomicU64::new(0),
		}))
	}

	fn now_ms(&self) -> u64 {
		now_ms()
	}

	/// Requests in flight in every project's process: what a stop waits for.
	pub fn in_flight(&self) -> usize {
		self.live().iter().map(|project| project.inflight.load(Ordering::Acquire)).sum()
	}

	fn live(&self) -> Vec<Arc<Project>> {
		self.projects
			.lock()
			.map(|projects| projects.values().filter_map(|slot| slot.get()?.as_ref().ok().cloned()).filter(|p| p.alive.load(Ordering::Acquire)).collect())
			.unwrap_or_default()
	}

	/// Start a process that waits for a project.
	fn spawn_spare(&self) -> Result<Spare, String> {
		let mut command = tokio::process::Command::new(&self.binary);
		command.arg("project").env_clear().stdin(Stdio::piped()).stdout(Stdio::piped()).stderr(Stdio::inherit());
		for name in PASSED {
			if let Some(value) = std::env::var_os(name) {
				command.env(name, value);
			}
		}
		let mut child = command.spawn().map_err(|error| format!("a project's process could not be started: {error}"))?;
		let pid = child.id().unwrap_or(0);
		let stdin = child.stdin.take().ok_or("no stdin")?;
		let stdout = BufReader::new(child.stdout.take().ok_or("no stdout")?).lines();
		let alive = Arc::new(AtomicBool::new(true));
		let dead = alive.clone();
		tokio::spawn(async move {
			let _ = child.wait().await;
			dead.store(false, Ordering::Release);
		});
		Ok(Spare { pid, stdin, stdout, alive })
	}

	/// Keep `spare_count` processes waiting.
	pub fn keep_spares(&self) {
		let Ok(mut spares) = self.spares.lock() else { return };
		spares.retain(|spare| spare.alive.load(Ordering::Acquire));
		while spares.len() < self.spare_count {
			match self.spawn_spare() {
				Ok(spare) => spares.push(spare),
				Err(error) => {
					eprintln!("{error}");
					break;
				}
			}
		}
	}

	fn uid_for(&self, project: &str) -> u32 {
		let Ok(mut uids) = self.uids.lock() else { return FIRST_UID };
		*uids.entry(project.to_owned()).or_insert_with(|| self.next_uid.fetch_add(1, Ordering::Relaxed))
	}

	/// This project's process, started if it has none, and told of a changed manifest.
	async fn project(self: &Arc<Self>, name: &str, manifest: &Manifest) -> Result<Arc<Project>, Refusal> {
		let stamp = self.manifests.stamp(name);
		let slot = {
			let mut projects = self.projects.lock().map_err(|_| Refusal::Full)?;
			let usable = projects.get(name).is_some_and(|slot| match slot.get() {
				None => true,
				Some(Ok(project)) => project.alive.load(Ordering::Acquire),
				Some(Err(_)) => false,
			});
			if !usable {
				projects.insert(name.to_owned(), Slot::default());
			}
			projects.get(name).cloned().ok_or(Refusal::Full)?
		};
		let started = slot.get_or_init(|| self.start(name, manifest, stamp)).await.clone();
		let project = match started {
			Ok(project) => project,
			Err(error) if error == "full" => return Err(Refusal::Full),
			Err(error) => return Err(Refusal::Start(error)),
		};
		let changed = project.sent.lock().is_ok_and(|sent| sent.0 != stamp);
		if changed {
			self.send_manifest(&project, manifest, stamp).await.map_err(Refusal::Start)?;
		}
		Ok(project)
	}

	async fn start(self: &Arc<Self>, name: &str, manifest: &Manifest, stamp: Option<SystemTime>) -> Result<Arc<Project>, String> {
		if !self.make_room(name) {
			return Err("full".into());
		}
		let uid = self.uid_for(name);
		let view = self.views.join(name);
		let digests = digests(manifest);
		{
			let (bundles, view, confine, digests) = (self.bundles.clone(), view.clone(), self.confine, digests.clone());
			tokio::task::spawn_blocking(move || prepare(&view, &bundles, &digests, confine.then_some(uid)))
				.await
				.map_err(|error| error.to_string())??;
		}
		let taken = self.spares.lock().ok().and_then(|mut spares| {
			spares.retain(|spare| spare.alive.load(Ordering::Acquire));
			(!spares.is_empty()).then(|| spares.remove(0))
		});
		let mut spare = match taken {
			Some(spare) => spare,
			None => self.spawn_spare()?,
		};
		self.keep_spares();
		let assign = serde_json::json!({ "assign": { "project": name, "uid": uid, "view": view, "confine": self.confine, "manifest": manifest } });
		spare.stdin.write_all(format!("{assign}\n").as_bytes()).await.map_err(|error| format!("its process did not take the project: {error}"))?;
		let ready = tokio::time::timeout(Duration::from_secs(10), spare.stdout.next_line()).await;
		if !matches!(ready, Ok(Ok(Some(ref line))) if line == "ready") {
			return Err("its process did not start".into());
		}
		debug!("{name}: a process of its own (pid {}, uid {uid})", spare.pid);
		Ok(Arc::new(Project {
			name: name.to_owned(),
			socket: view.join("sockets").join("front.sock"),
			pid: spare.pid,
			uid: self.confine.then_some(uid),
			orders: tokio::sync::Mutex::new(Some(spare.stdin)),
			sent: Mutex::new((stamp, digests.into_iter().collect())),
			preparing: tokio::sync::Mutex::new(()),
			inflight: AtomicUsize::new(0),
			last_used_ms: AtomicU64::new(self.now_ms()),
			alive: spare.alive,
		}))
	}

	/// A changed manifest: its new bundles into the project's directory first, then the manifest.
	async fn send_manifest(&self, project: &Arc<Project>, manifest: &Manifest, stamp: Option<SystemTime>) -> Result<(), String> {
		let missing: Vec<String> = {
			let sent = project.sent.lock().map_err(|_| "poisoned")?;
			digests(manifest).into_iter().filter(|digest| !sent.1.contains(digest)).collect()
		};
		if !missing.is_empty() {
			let _preparing = project.preparing.lock().await;
			let (bundles, view, confine, uid, todo) = (self.bundles.clone(), self.views.join(&project.name), self.confine, self.uid_for(&project.name), missing.clone());
			tokio::task::spawn_blocking(move || prepare(&view, &bundles, &todo, confine.then_some(uid))).await.map_err(|error| error.to_string())??;
		}
		let line = format!("{}\n", serde_json::json!({ "manifest": manifest.for_project() }));
		let mut orders = project.orders.lock().await;
		let stdin = orders.as_mut().ok_or("its process was stopped")?;
		stdin.write_all(line.as_bytes()).await.map_err(|error| format!("its process did not take the change: {error}"))?;
		if let Ok(mut sent) = project.sent.lock() {
			sent.0 = stamp;
			sent.1.extend(missing);
		}
		Ok(())
	}

	/// Room for one more project's process: under the cap on their number and past none of the
	/// memory, or made by stopping the one idle longest.
	fn make_room(&self, starting: &str) -> bool {
		let live = self.live();
		let over_memory = self.memory_cap.is_some_and(|(cap, _)| self.memory_used.load(Ordering::Acquire) >= cap / 100 * MEMORY_ROOM);
		if live.len() < self.max_projects && !over_memory {
			return true;
		}
		let idle = live
			.iter()
			.filter(|project| project.name != starting && project.inflight.load(Ordering::Acquire) == 0)
			.min_by_key(|project| project.last_used_ms.load(Ordering::Acquire))
			.cloned();
		match idle {
			Some(project) => {
				self.stop(&project, "to make room");
				true
			}
			None => false,
		}
	}

	fn stop(&self, project: &Arc<Project>, why: &str) {
		debug!("{}: its process stopped ({why})", project.name);
		project.alive.store(false, Ordering::Release);
		// Its stdin closing is what ends it.
		let closing = project.clone();
		tokio::spawn(async move {
			closing.orders.lock().await.take();
		});
		if let Ok(mut projects) = self.projects.lock() {
			projects.retain(|_, slot| !matches!(slot.get(), Some(Ok(held)) if Arc::ptr_eq(held, project)));
		}
	}

	/// Every 100 ms: weigh the container, and past the guard ask the heaviest project's process to
	/// stop its largest worker. Every second: stop projects' processes nothing has asked for in
	/// `idle`, and keep the spares.
	pub async fn watch(self: Arc<Self>) {
		let mut tick = tokio::time::interval(Duration::from_millis(100));
		let mut ticks: u64 = 0;
		let mut guarded_ms: u64 = 0;
		loop {
			tick.tick().await;
			ticks += 1;
			let now = self.now_ms();
			if let Some((cap, cgroup)) = self.memory_cap {
				let live = self.live();
				// The cgroup's count where the cap is the cgroup's; else this process and every one
				// it started, which is what SNOUT_FUNCTIONS_MEMORY_MB caps.
				let used = if cgroup {
					crate::supervisor::memory_used(true)
				} else {
					let spares: Vec<u32> = self.spares.lock().map(|spares| spares.iter().map(|spare| spare.pid).collect()).unwrap_or_default();
					anon_bytes(std::process::id()) + live.iter().map(|project| anon_bytes(project.pid)).sum::<u64>() + spares.into_iter().map(anon_bytes).sum::<u64>()
				};
				self.memory_used.store(used, Ordering::Release);
				// Each project's process makes room by this reading, as one process did by its own.
				if ticks.is_multiple_of(2) {
					let line = format!("{}\n", serde_json::json!({ "memory": { "used": used, "cap": cap } }));
					for project in &live {
						if let Ok(mut orders) = project.orders.try_lock()
							&& let Some(stdin) = orders.as_mut()
						{
							let _ = stdin.write_all(line.as_bytes()).await;
						}
					}
				}
				if used > cap / 100 * MEMORY_GUARD && now.saturating_sub(guarded_ms) > GUARD_EVERY_MS {
					let heaviest = live.into_iter().max_by_key(|project| anon_bytes(project.pid));
					if let Some(project) = heaviest {
						let line = format!("{}\n", serde_json::json!({ "guard": { "used": used, "cap": cap } }));
						let mut orders = project.orders.lock().await;
						if let Some(stdin) = orders.as_mut() {
							let _ = stdin.write_all(line.as_bytes()).await;
						}
						guarded_ms = now;
					}
				}
			}
			if ticks.is_multiple_of(10) {
				for project in self.live() {
					if project.inflight.load(Ordering::Acquire) == 0 && now.saturating_sub(project.last_used_ms.load(Ordering::Acquire)) > self.idle_ms {
						self.stop(&project, "idle");
					}
				}
				self.keep_spares();
			}
			if ticks.is_multiple_of(REFRESH_TICKS) {
				self.refresh().await;
			}
		}
	}

	/// Each running project's copies, against what the host has built since they were taken.
	///
	/// A manifest that has not changed is never sent again, so without this a bundle the host
	/// rebuilds (after a failed build, or under a new builder) would be served from the copy taken
	/// before, for as long as the project's process lived. The check is a few stats per bundle.
	async fn refresh(&self) {
		for project in self.live() {
			let Ok(_preparing) = project.preparing.try_lock() else {
				continue;
			};
			let digests: Vec<String> = project.sent.lock().map(|sent| sent.1.iter().cloned().collect()).unwrap_or_default();
			let (bundles, view, confine, uid) = (self.bundles.clone(), self.views.join(&project.name), self.confine, self.uid_for(&project.name));
			match tokio::task::spawn_blocking(move || prepare(&view, &bundles, &digests, confine.then_some(uid))).await {
				Ok(Ok(replaced)) => {
					for digest in replaced {
						eprintln!("{}: bundle {digest} was built again on the host, so its copy was replaced", project.name);
					}
				}
				Ok(Err(error)) => eprintln!("{}: its bundles could not be refreshed: {error}", project.name),
				Err(error) => eprintln!("{}: its bundles could not be refreshed: {error}", project.name),
			}
		}
	}
}

/// How often `refresh` runs, in `watch`'s 100 ms ticks.
const REFRESH_TICKS: u64 = 50;

fn now_ms() -> u64 {
	u64::try_from(crate::uptime_ms()).unwrap_or(u64::MAX)
}

/// The bundles a manifest names, those that are valid.
fn digests(manifest: &Manifest) -> Vec<String> {
	let mut digests: Vec<String> = manifest.functions.iter().map(|f| f.digest.clone()).filter(|d| manifest::valid_digest(d)).collect();
	digests.sort();
	digests.dedup();
	digests
}

/// A project's directory: its bundles (copied from the mount, which it cannot see), a /tmp and
/// its socket's directory, and the two files the resolver reads.
///
/// Where it is confined, the project's user owns `tmp` and `sockets` and NOTHING else: the
/// directory itself, `bundles`, `etc` and `proc` stay this process's (root's), readable by the
/// project's group and writable by nobody but root. This process writes here as root every few
/// seconds (`refresh`), so a project that owned what it writes into could plant a symlink (its
/// `/etc/hosts` pointing at a host library, or a directory swapped for a link while it is walked)
/// and have root truncate or chown a file outside it (audit 5-C). Root never writes into `tmp` or
/// `sockets` after it has made them, and refuses a path here that is not what it made.
///
/// A copy is replaced when the host has prepared its bundle again since (`build_state`), and
/// the digests replaced are returned. A bundle is named by its source, not by its build, so a
/// copy taken before the host had built it stayed unbuilt for as long as this process ran: on
/// 2026-10-02 a function copied in the moment before its build landed was refused ("has not been
/// prepared yet") through three control-plane deploys, each of which rebuilt it correctly, until
/// the container was restarted.
fn prepare(view: &Path, bundles: &Path, digests: &[String], uid: Option<u32>) -> Result<Vec<String>, String> {
	let fail = |what: &Path, error: std::io::Error| format!("{}: {error}", what.display());
	if let Some(parent) = view.parent() {
		std::fs::create_dir_all(parent).map_err(|e| fail(parent, e))?;
	}
	for dir in ["", "bundles", "etc", "proc", "proc/self"] {
		let path = if dir.is_empty() { view.to_path_buf() } else { view.join(dir) };
		shared_dir(&path, uid).map_err(|e| fail(&path, e))?;
	}
	for dir in ["tmp", "sockets"] {
		let path = view.join(dir);
		owned_dir(&path, uid).map_err(|e| fail(&path, e))?;
	}
	for file in ["resolv.conf", "hosts"] {
		let from = Path::new("/etc").join(file);
		if from.exists() {
			let to = view.join("etc").join(file);
			// Copied beside it and renamed over it, so whatever is at `to` is replaced, never
			// written through.
			let next = view.join("etc").join(format!(".{file}.next"));
			let _ = std::fs::remove_file(&next);
			std::fs::copy(&from, &next).map_err(|e| fail(&next, e))?;
			share(&next, uid, 0o640).map_err(|e| fail(&next, e))?;
			std::fs::rename(&next, &to).map_err(|e| fail(&to, e))?;
		}
	}
	// There is no /proc in a project's root, and Deno's `Deno.execPath()` (Node's `process.execPath`,
	// which npm packages read as they load) unwraps the link /proc/self/exe: without it the whole
	// project's process aborts (functions.pod.ts, 2026-09-30). The link, and only the link: it names
	// this binary's path, which the project's root does not hold. Every other /proc read in the
	// Deno crates is allowed to fail.
	let exe = view.join("proc").join("self").join("exe");
	if std::fs::symlink_metadata(&exe).is_err() {
		let binary = std::env::current_exe().unwrap_or_else(|_| PathBuf::from("/snout-functions"));
		std::os::unix::fs::symlink(binary, &exe).map_err(|e| fail(&exe, e))?;
	}
	let mut replaced = Vec::new();
	for digest in digests {
		let to = view.join("bundles").join(digest);
		let from = bundles.join(digest);
		if !from.is_dir() {
			continue;
		}
		let stale = to.exists();
		if stale && build_state(&to) == build_state(&from) {
			continue;
		}
		let partial = view.join("bundles").join(format!(".{digest}.partial"));
		let _ = std::fs::remove_dir_all(&partial);
		copy_dir(&from, &partial, uid).map_err(|e| fail(&from, e))?;
		// Swapped whole, so a worker starting now reads the old copy or the new one.
		let old = view.join("bundles").join(format!(".{digest}.old"));
		if stale {
			let _ = std::fs::remove_dir_all(&old);
			std::fs::rename(&to, &old).map_err(|e| fail(&to, e))?;
		}
		std::fs::rename(&partial, &to).map_err(|e| fail(&to, e))?;
		if stale {
			let _ = std::fs::remove_dir_all(&old);
			replaced.push(digest.clone());
		}
	}
	Ok(replaced)
}

/// A directory of the view that only this process writes: made if missing, refused if what is
/// there is not a directory (a symlink is not followed), and, where the project is confined,
/// readable and searchable by the project's group alone.
fn shared_dir(path: &Path, uid: Option<u32>) -> std::io::Result<()> {
	match std::fs::create_dir(path) {
		Ok(()) => {}
		Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => real_dir(path)?,
		Err(error) => return Err(error),
	}
	share(path, uid, 0o750)
}

/// A directory the project's user owns (its /tmp, its socket's): handed over the moment it is
/// made, empty, and never touched by this process again.
fn owned_dir(path: &Path, uid: Option<u32>) -> std::io::Result<()> {
	match std::fs::create_dir(path) {
		Ok(()) => {
			if let Some(uid) = uid {
				std::os::unix::fs::lchown(path, Some(uid), Some(uid))?;
				std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700))?;
			}
			Ok(())
		}
		Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => real_dir(path),
		Err(error) => Err(error),
	}
}

/// That `path` is a directory itself, not a link to one.
fn real_dir(path: &Path) -> std::io::Result<()> {
	if std::fs::symlink_metadata(path)?.file_type().is_dir() {
		Ok(())
	} else {
		Err(std::io::Error::other("not a directory of this process's own (a link is never followed here)"))
	}
}

/// Something this process made in the view, readable by the project's group: `lchown` to the
/// group alone (the owner stays root), and a mode with no write for anyone but the owner.
fn share(path: &Path, uid: Option<u32>, mode: u32) -> std::io::Result<()> {
	let Some(gid) = uid else { return Ok(()) };
	std::os::unix::fs::lchown(path, None, Some(gid))?;
	std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode))
}

/// What the host has made of a bundle's source (functionRuntime.ts writes `.built`): the builder
/// that made it, the build's size, and the builder's error if it failed. Equal on a faithful copy,
/// and different once the host builds it again, which it does after a failure and when the
/// builder's pin moves.
fn build_state(bundle: &Path) -> (String, Option<u64>, String) {
	let built = bundle.join(".built");
	let read = |name: &str| std::fs::read_to_string(built.join(name)).map(|text| text.trim().to_owned()).unwrap_or_default();
	let size = std::fs::metadata(built.join("main.js")).ok().map(|meta| meta.len());
	(read("builder"), size, read("error.txt"))
}

/// A bundle copied into the view, in a directory only this process writes; links are skipped.
fn copy_dir(from: &Path, to: &Path, uid: Option<u32>) -> std::io::Result<()> {
	std::fs::create_dir(to)?;
	share(to, uid, 0o750)?;
	for entry in std::fs::read_dir(from)? {
		let entry = entry?;
		let kind = entry.file_type()?;
		let target = to.join(entry.file_name());
		if kind.is_dir() {
			copy_dir(&entry.path(), &target, uid)?;
		} else if kind.is_file() {
			std::fs::copy(entry.path(), &target)?;
			share(&target, uid, 0o640)?;
		}
	}
	Ok(())
}

/// A process's anonymous memory, from /proc, which this process can read for its children.
fn anon_bytes(pid: u32) -> u64 {
	std::fs::read_to_string(format!("/proc/{pid}/status"))
		.ok()
		.and_then(|status| status.lines().find_map(|line| line.strip_prefix("RssAnon:").and_then(|v| v.split_whitespace().next()?.parse::<u64>().ok())))
		.map_or(0, |kb| kb << 10)
}

pub async fn serve(listener: TcpListener, router: Arc<Router>) {
	loop {
		let (stream, _) = match listener.accept().await {
			Ok(accepted) => accepted,
			Err(error) => {
				eprintln!("accept: {error}");
				continue;
			}
		};
		let _ = stream.set_nodelay(true);
		let router = router.clone();
		tokio::spawn(async move {
			let service = service_fn(move |request: Request<Incoming>| {
				let router = router.clone();
				async move {
					// What this process answers itself is compressed by the same rules as before
					// (twin, 2026-09-30: an unknown function's 404 was gzipped and then was not); what
					// a project's process answered, it compressed already.
					let encoding = crate::compress::requested(request.headers().get(hyper::header::ACCEPT_ENCODING).and_then(|v| v.to_str().ok()));
					let method = request.method().clone();
					let reply = answer(request, router).await;
					let reply = if reply.extensions().get::<Forwarded>().is_some() { reply } else { crate::compress::reply(reply, encoding, &method) };
					Ok::<_, std::convert::Infallible>(reply)
				}
			});
			let _ = hyper::server::conn::http1::Builder::new().serve_connection(TokioIo::new(stream), service).with_upgrades().await;
		});
	}
}

async fn answer(request: Request<Incoming>, router: Arc<Router>) -> Reply {
	if request.uri().path() == HEALTH_PATH {
		return text(StatusCode::OK, "ok\n");
	}
	if let Some(door) = &router.door
		&& !through_door(request.headers().get(DOOR_HEADER).map(|v| v.as_bytes()), door)
	{
		return refuse(StatusCode::FORBIDDEN, "This request did not arrive through the front door.", None);
	}
	let Some(name) = request.headers().get("x-snoutdata-ref").and_then(|v| v.to_str().ok()).map(str::to_owned) else {
		return refuse(StatusCode::BAD_REQUEST, "This request did not arrive through the front door.", Some("Call https://<ref>.api.snoutdata.com/functions/v1/<name>"));
	};
	if !manifest::valid_ref(&name) {
		return refuse(StatusCode::BAD_REQUEST, "This request did not arrive through the front door.", None);
	}
	let Some(function) = manifest::function_name(request.uri().path()).map(str::to_owned) else {
		return refuse(StatusCode::NOT_FOUND, "No function was named in this request.", Some("The path is /functions/v1/<name>"));
	};
	// Decided here, so no process is started for a function that does not exist.
	let Some((manifest, wall_ms, refused)) = router.manifests.get(&name).and_then(|m| {
		let deployed = m.functions.iter().find(|f| f.name == function)?;
		let wall_ms = m.limits_for(deployed).wall_ms;
		let refused = m.refusal(deployed, request.headers().get(hyper::header::AUTHORIZATION).map(|v| v.as_bytes()));
		Some((m, wall_ms, refused))
	}) else {
		return refuse(StatusCode::NOT_FOUND, &format!("There is no function called {function} in this project."), Some(&format!("Deploy it with: snoutdata functions deploy {function}")));
	};
	// Decided here too, so a stranger's token never starts a process either.
	if let Some(refused) = refused {
		return refuse(StatusCode::UNAUTHORIZED, refused.message(), None);
	}
	// Past the project's own wall clock, which its process enforces and answers for, a margin.
	let deadline = tokio::time::Instant::now() + Duration::from_millis(wall_ms) + Duration::from_secs(5);
	let mut request = request;
	request.headers_mut().remove(DOOR_HEADER);
	for attempt in 0..2 {
		let project = match router.project(&name, &manifest).await {
			Ok(project) => project,
			Err(Refusal::Full) => {
				let mut reply = refuse(StatusCode::SERVICE_UNAVAILABLE, "This host is running as many functions as it can.", Some("Try again in a moment."));
				reply.headers_mut().insert("retry-after", hyper::header::HeaderValue::from_static("1"));
				return reply;
			}
			Err(Refusal::Start(error)) => {
				eprintln!("{name}: {error}");
				return refuse(StatusCode::INTERNAL_SERVER_ERROR, "This function could not be run.", Some(&error));
			}
		};
		match UnixStream::connect(&project.socket).await {
			Ok(stream) if answered_by(&stream, project.uid) => return forward(request, stream, project, deadline).await,
			// Its socket's directory is the project's own, so code that got out of its isolate could
			// leave a link there to another project's socket: a request that passed THIS project's
			// checks would then reach that one's functions. Only the project's own user may answer.
			Ok(_) => {
				eprintln!("{name}: its socket was answered by a process not its own, so its process was stopped");
				router.stop(&project, "its socket was not its own");
				break;
			}
			// A process that has gone since it was found: once more, with a new one.
			Err(_) if attempt == 0 => router.stop(&project, "its socket did not answer"),
			Err(error) => {
				eprintln!("{name}: {error}");
				break;
			}
		}
	}
	refuse(StatusCode::INTERNAL_SERVER_ERROR, "This function could not be run.", Some("its project's process did not answer"))
}

/// Whether the process listening on `stream` runs as `uid` (any process where none is confined).
fn answered_by(stream: &UnixStream, uid: Option<u32>) -> bool {
	uid.is_none_or(|uid| stream.peer_cred().is_ok_and(|peer| peer.uid() == uid))
}

async fn forward(mut request: Request<Incoming>, stream: UnixStream, project: Arc<Project>, deadline: tokio::time::Instant) -> Reply {
	let held = Held::new(project);
	let (mut sender, connection) = match tokio::time::timeout_at(deadline, hyper::client::conn::http1::handshake(TokioIo::new(stream))).await {
		Ok(Ok(pair)) => pair,
		_ => return refuse(StatusCode::INTERNAL_SERVER_ERROR, "This function could not be run.", Some("its project's process did not answer")),
	};
	tokio::spawn(async move {
		let _ = connection.with_upgrades().await;
	});
	let path = request.uri().path_and_query().map(|p| p.as_str().to_owned()).unwrap_or_else(|| "/".into());
	*request.uri_mut() = path.parse::<Uri>().unwrap_or_default();
	let response = match tokio::time::timeout_at(deadline, sender.send_request(request)).await {
		Ok(Ok(response)) => response,
		Err(_) => return refuse(StatusCode::INTERNAL_SERVER_ERROR, "This function could not be run.", Some("it reached its wall clock limit")),
		Ok(Err(_)) => return refuse(StatusCode::INTERNAL_SERVER_ERROR, "This function could not be run.", Some("its project's process stopped")),
	};
	let (parts, body) = response.into_parts();
	let mut reply = hyper::Response::from_parts(parts, BoxBody::new(Passing { inner: body, _held: Some(held) }));
	reply.extensions_mut().insert(Forwarded);
	reply
}

/// A reply a project's process gave, passed on as it is.
#[derive(Clone, Copy)]
struct Forwarded;

/// A request counted in flight at its project's process for as long as its answer is being sent.
struct Held(Arc<Project>);

impl Held {
	fn new(project: Arc<Project>) -> Self {
		project.inflight.fetch_add(1, Ordering::AcqRel);
		Held(project)
	}
}

impl Drop for Held {
	fn drop(&mut self) {
		self.0.last_used_ms.store(now_ms(), Ordering::Release);
		self.0.inflight.fetch_sub(1, Ordering::AcqRel);
	}
}

struct Passing {
	inner: Incoming,
	_held: Option<Held>,
}

impl Body for Passing {
	type Data = Bytes;
	type Error = Error;

	fn poll_frame(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Result<Frame<Bytes>, Error>>> {
		let this = self.get_mut();
		match Pin::new(&mut this.inner).poll_frame(cx) {
			Poll::Ready(None) => {
				this._held = None;
				Poll::Ready(None)
			}
			Poll::Ready(Some(frame)) => Poll::Ready(Some(frame.map_err(Into::into))),
			Poll::Pending => Poll::Pending,
		}
	}
}

#[cfg(test)]
mod tests {
	use super::{answered_by, prepare};
	use std::os::unix::fs::{MetadataExt, PermissionsExt};
	use std::path::{Path, PathBuf};

	fn scratch(name: &str) -> PathBuf {
		let dir = std::env::temp_dir().join(format!("snout-router-{name}-{}", std::process::id()));
		let _ = std::fs::remove_dir_all(&dir);
		std::fs::create_dir_all(dir.join("bundles").join("d1")).unwrap();
		std::fs::write(dir.join("bundles").join("d1").join("index.ts"), "import './src/f/index.ts';\n").unwrap();
		dir
	}

	fn build(bundle: &Path, builder: &str, main: Option<&str>, error: Option<&str>) {
		let built = bundle.join(".built");
		let _ = std::fs::remove_dir_all(&built);
		std::fs::create_dir_all(&built).unwrap();
		std::fs::write(built.join("builder"), format!("{builder}\n")).unwrap();
		if let Some(main) = main {
			std::fs::write(built.join("main.js"), main).unwrap();
		}
		if let Some(error) = error {
			std::fs::write(built.join("error.txt"), error).unwrap();
		}
	}

	#[test]
	fn a_copy_taken_before_the_build_is_replaced_once_it_lands() {
		let root = scratch("before");
		let (bundles, view) = (root.join("bundles"), root.join("view"));
		let digests = vec!["d1".to_owned()];
		assert!(prepare(&view, &bundles, &digests, None).unwrap().is_empty());
		assert!(!view.join("bundles/d1/.built/main.js").exists());

		build(&bundles.join("d1"), "deno:2.9.7", Some("export {};"), None);
		assert_eq!(prepare(&view, &bundles, &digests, None).unwrap(), digests);
		assert_eq!(std::fs::read_to_string(view.join("bundles/d1/.built/main.js")).unwrap(), "export {};");
		assert!(!view.join("bundles/.d1.old").exists());

		// Settled: nothing more to copy.
		assert!(prepare(&view, &bundles, &digests, None).unwrap().is_empty());
		let _ = std::fs::remove_dir_all(&root);
	}

	#[test]
	fn a_failed_build_that_later_succeeds_is_replaced() {
		let root = scratch("retry");
		let (bundles, view) = (root.join("bundles"), root.join("view"));
		let digests = vec!["d1".to_owned()];
		build(&bundles.join("d1"), "deno:2.9.7", None, Some("registry unreachable"));
		prepare(&view, &bundles, &digests, None).unwrap();
		assert!(view.join("bundles/d1/.built/error.txt").exists());

		build(&bundles.join("d1"), "deno:2.9.7", Some("export {};"), None);
		assert_eq!(prepare(&view, &bundles, &digests, None).unwrap(), digests);
		assert!(!view.join("bundles/d1/.built/error.txt").exists());
		assert!(view.join("bundles/d1/.built/main.js").exists());
		let _ = std::fs::remove_dir_all(&root);
	}

	#[test]
	fn a_new_builder_pin_replaces_the_copy() {
		let root = scratch("pin");
		let (bundles, view) = (root.join("bundles"), root.join("view"));
		let digests = vec!["d1".to_owned()];
		build(&bundles.join("d1"), "deno:2.9.7", Some("export {};"), None);
		prepare(&view, &bundles, &digests, None).unwrap();

		build(&bundles.join("d1"), "deno:2.9.8", Some("export {};"), None);
		assert_eq!(prepare(&view, &bundles, &digests, None).unwrap(), digests);
		assert_eq!(std::fs::read_to_string(view.join("bundles/d1/.built/builder")).unwrap().trim(), "deno:2.9.8");
		let _ = std::fs::remove_dir_all(&root);
	}

	// Audit 5-C: root writes into the view every few seconds, so nothing the project could have
	// put there may be followed out of it.
	#[test]
	fn a_link_left_at_etc_hosts_is_replaced_and_never_written_through() {
		if !Path::new("/etc/hosts").exists() {
			return;
		}
		let root = scratch("hosts-link");
		let (bundles, view) = (root.join("bundles"), root.join("view"));
		prepare(&view, &bundles, &[], None).unwrap();
		let victim = root.join("victim");
		std::fs::write(&victim, "not the resolver's").unwrap();
		std::fs::remove_file(view.join("etc/hosts")).unwrap();
		std::os::unix::fs::symlink(&victim, view.join("etc/hosts")).unwrap();

		prepare(&view, &bundles, &[], None).unwrap();
		assert_eq!(std::fs::read_to_string(&victim).unwrap(), "not the resolver's");
		assert!(std::fs::symlink_metadata(view.join("etc/hosts")).unwrap().file_type().is_file());
		let _ = std::fs::remove_dir_all(&root);
	}

	#[test]
	fn a_directory_swapped_for_a_link_is_refused_and_nothing_is_written_through_it() {
		let root = scratch("dir-link");
		let (bundles, view) = (root.join("bundles"), root.join("view"));
		let digests = vec!["d1".to_owned()];
		prepare(&view, &bundles, &[], None).unwrap();
		let elsewhere = root.join("elsewhere");
		std::fs::create_dir_all(&elsewhere).unwrap();
		for dir in ["etc", "bundles", "tmp", "sockets", "proc"] {
			let path = view.join(dir);
			std::fs::rename(&path, root.join(format!("{dir}.was"))).unwrap();
			std::os::unix::fs::symlink(&elsewhere, &path).unwrap();
			assert!(prepare(&view, &bundles, &digests, None).is_err(), "{dir} as a link was followed");
			std::fs::remove_file(&path).unwrap();
			std::fs::rename(root.join(format!("{dir}.was")), &path).unwrap();
		}
		assert_eq!(std::fs::read_dir(&elsewhere).unwrap().count(), 0);
		let _ = std::fs::remove_dir_all(&root);
	}

	#[test]
	fn a_confined_project_owns_its_tmp_and_sockets_and_can_only_read_the_rest() {
		let root = scratch("modes");
		let (bundles, view) = (root.join("bundles"), root.join("view"));
		std::fs::set_permissions(bundles.join("d1/index.ts"), std::fs::Permissions::from_mode(0o600)).unwrap();
		// The test's own user stands for the project's: lchown to it needs no privilege.
		let me = std::fs::metadata(&root).unwrap();
		if me.uid() != me.gid() {
			return;
		}
		prepare(&view, &bundles, &["d1".to_owned()], Some(me.uid())).unwrap();
		let mode = |path: &str| std::fs::symlink_metadata(view.join(path)).unwrap().mode() & 0o777;
		for dir in ["", "bundles", "bundles/d1", "etc", "proc", "proc/self"] {
			assert_eq!(mode(dir), 0o750, "{dir}");
		}
		assert_eq!(mode("bundles/d1/index.ts"), 0o640);
		assert_eq!(mode("tmp"), 0o700);
		assert_eq!(mode("sockets"), 0o700);
		let _ = std::fs::remove_dir_all(&root);
	}

	#[test]
	fn only_the_projects_own_user_may_answer_its_socket() {
		let root = scratch("peer");
		let path = root.join("front.sock");
		let listener = std::os::unix::net::UnixListener::bind(&path).unwrap();
		let me = std::fs::metadata(&path).unwrap().uid();
		let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
		runtime.block_on(async {
			let stream = tokio::net::UnixStream::connect(&path).await.unwrap();
			assert!(answered_by(&stream, None));
			assert!(answered_by(&stream, Some(me)));
			assert!(!answered_by(&stream, Some(me + 1)));
		});
		drop(listener);
		let _ = std::fs::remove_dir_all(&root);
	}
}
