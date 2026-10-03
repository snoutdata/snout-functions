//! What the host agent writes under the functions mount, and reading it.
//!
//! The layout is the host agent's, and it is the contract this runtime reads: a manifest per
//! project at
//! `<root>/projects/<ref>.json`, written by rename so a reader sees the whole old one or the whole
//! new one, and each bundle at `<root>/bundles/<digest>/`, where a one-line `index.ts` imports the
//! declared entrypoint under `src/`. The mount is read-only; nothing here writes to it.

use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::SystemTime;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Manifest {
	#[serde(default)]
	pub functions: Vec<Deployed>,
	#[serde(default)]
	pub limits: Limits,
	#[serde(default)]
	pub env: BTreeMap<String, String>,
	/// The project's JWT signing secret, for `verify_jwt` (jwt.rs). Absent from a control plane
	/// older than 0.2.2's, which leaves the front door's key check the only one, as before. Never
	/// sent on to a project's own process.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub jwt_secret: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Deployed {
	pub name: String,
	pub digest: String,
	/// This function's own size within its plan (sql/100): the memory one worker may use, and
	/// how many workers it may run at once. Absent from a control plane older than 100, which
	/// leaves the project's `limits` and the host's own cap on workers.
	#[serde(default)]
	pub memory_mb: Option<u32>,
	#[serde(default)]
	pub concurrency: Option<usize>,
	/// Whether a call needs a token signed with the project's secret (`verify_jwt`). True when
	/// absent: the function is open only when it was deployed open.
	#[serde(default = "yes")]
	pub verify_jwt: bool,
}

fn yes() -> bool {
	true
}

impl Manifest {
	/// Why a call to `deployed` with this `Authorization` may not run, if it may not.
	pub fn refusal(&self, deployed: &Deployed, authorization: Option<&[u8]>) -> Option<crate::jwt::Refused> {
		let secret = self.jwt_secret.as_deref().filter(|_| deployed.verify_jwt)?;
		crate::jwt::check(authorization, secret, crate::jwt::now()).err()
	}

	/// This manifest as a project's own process gets it: without the signing secret.
	pub fn for_project(&self) -> Manifest {
		Manifest { jwt_secret: None, ..self.clone() }
	}

	/// The limits a function runs under: the project's, with the function's own memory.
	pub fn limits_for(&self, deployed: &Deployed) -> Limits {
		let mut limits = self.limits;
		if let Some(memory_mb) = deployed.memory_mb {
			limits.memory_mb = memory_mb.clamp(16, 1024);
		}
		limits
	}
}

/// A project's limits, per invocation. The defaults are what a manifest without `limits` has
/// always meant.
#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "camelCase", default)]
pub struct Limits {
	pub memory_mb: u32,
	pub wall_ms: u64,
	pub cpu_ms: u64,
	pub hard_cpu_ms: Option<u64>,
}

impl Default for Limits {
	fn default() -> Self {
		Limits { memory_mb: 128, wall_ms: 10_000, cpu_ms: 2_000, hard_cpu_ms: None }
	}
}

impl Limits {
	/// The CPU a request may use before its worker is stopped. The main worker's default
	/// when the manifest does not derive one.
	pub fn hard_cpu(&self) -> u64 {
		self.hard_cpu_ms.unwrap_or(3_500).max(self.cpu_ms)
	}
}

/// Where a process's manifests come from.
///
/// The front process reads the files the host agent writes, cached by each file's modification
/// time: exact, unlike a TTL, and cheaper than re-reading a file with secrets in it on every
/// invocation. A project's own process is HANDED its one manifest by the front process and can
/// read no file of any other project's (project.rs), so it holds that one in memory.
pub struct Manifests {
	bundles: PathBuf,
	source: Source,
}

enum Source {
	Files { root: PathBuf, held: Mutex<HashMap<String, (SystemTime, Manifest)>> },
	Held(Mutex<Option<(String, Manifest)>>),
}

impl Manifests {
	pub fn new(root: PathBuf) -> Self {
		Manifests { bundles: root.join("bundles"), source: Source::Files { root, held: Mutex::new(HashMap::new()) } }
	}

	/// One project's manifest, held rather than read, with its bundles under `bundles`.
	pub fn held(bundles: PathBuf, project: &str, manifest: Manifest) -> Self {
		Manifests { bundles, source: Source::Held(Mutex::new(Some((project.to_owned(), manifest)))) }
	}

	/// Replace a held manifest (a changed secret, limit or deployment reaches the next request).
	pub fn set(&self, manifest: Manifest) {
		if let Source::Held(held) = &self.source
			&& let Ok(mut held) = held.lock()
			&& let Some((_, current)) = held.as_mut()
		{
			*current = manifest;
		}
	}

	pub fn bundle_dir(&self, digest: &str) -> PathBuf {
		self.bundles.join(digest)
	}

	/// The manifest's file and its modification time: what the front process watches for change.
	pub fn stamp(&self, project: &str) -> Option<SystemTime> {
		match &self.source {
			Source::Files { root, .. } => std::fs::metadata(root.join("projects").join(format!("{project}.json"))).and_then(|m| m.modified()).ok(),
			Source::Held(_) => None,
		}
	}

	pub fn get(&self, project: &str) -> Option<Manifest> {
		match &self.source {
			Source::Held(held) => held.lock().ok()?.as_ref().filter(|(owner, _)| owner == project).map(|(_, manifest)| manifest.clone()),
			Source::Files { root, held } => {
				let path = root.join("projects").join(format!("{project}.json"));
				let stamp = std::fs::metadata(&path).and_then(|m| m.modified()).ok()?;
				if let Some((held_stamp, manifest)) = held.lock().ok()?.get(project)
					&& *held_stamp == stamp
				{
					return Some(manifest.clone());
				}
				let manifest = read(&path)?;
				held.lock().ok()?.insert(project.to_owned(), (stamp, manifest.clone()));
				Some(manifest)
			}
		}
	}
}

fn read(path: &Path) -> Option<Manifest> {
	let text = std::fs::read_to_string(path).ok()?;
	match serde_json::from_str(&text) {
		Ok(manifest) => Some(manifest),
		Err(error) => {
			eprintln!("manifest {} could not be read: {error}", path.display());
			None
		}
	}
}

/// A project ref as the front door writes it: lowercase letters and digits. Anything else is
/// refused before it becomes part of a path.
pub fn valid_ref(value: &str) -> bool {
	!value.is_empty() && value.len() <= 40 && value.bytes().all(|b| b.is_ascii_lowercase() || b.is_ascii_digit())
}

/// The function a path names, by the same rule the control package applies
/// (`functionNameFromPath`): letters, digits, hyphens and underscores, no dot and no slash,
/// because the front door decides whether this function may be called without a key by that
/// name, and a name that meant two things in the two places is a way past it.
pub fn function_name(path: &str) -> Option<&str> {
	let first = path.trim_start_matches('/').split('/').next().unwrap_or("");
	let mut bytes = first.bytes();
	let lead = bytes.next()?;
	let ok = lead.is_ascii_alphanumeric()
		&& first.len() <= 48
		&& first.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-');
	ok.then_some(first)
}

pub fn valid_digest(value: &str) -> bool {
	value.len() == 64 && value.bytes().all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn a_function_runs_at_its_own_memory_within_the_project_limits() {
		let manifest: Manifest = serde_json::from_str(
			r#"{ "functions": [
				{ "name": "a", "digest": "x", "memoryMb": 256, "concurrency": 4 },
				{ "name": "b", "digest": "y" },
				{ "name": "c", "digest": "z", "memoryMb": 4096 } ],
			  "limits": { "memoryMb": 512, "wallMs": 30000, "cpuMs": 8000 } }"#,
		)
		.expect("a manifest");
		let [a, b, c] = [&manifest.functions[0], &manifest.functions[1], &manifest.functions[2]];
		assert_eq!(manifest.limits_for(a).memory_mb, 256);
		assert_eq!(a.concurrency, Some(4));
		assert_eq!(manifest.limits_for(a).wall_ms, 30_000);
		// Older control planes send no size: the project's limits and the host's cap.
		assert_eq!(manifest.limits_for(b).memory_mb, 512);
		assert_eq!(b.concurrency, None);
		assert_eq!(manifest.limits_for(c).memory_mb, 1024);
	}

	#[test]
	fn verify_jwt_is_checked_against_the_secret_and_the_secret_stays_in_the_front_process() {
		let manifest: Manifest = serde_json::from_str(
			r#"{ "functions": [
				{ "name": "keyed", "digest": "x", "verifyJwt": true },
				{ "name": "open", "digest": "y", "verifyJwt": false },
				{ "name": "older", "digest": "z" } ],
			  "jwtSecret": "s3cret" }"#,
		)
		.expect("a manifest");
		let [keyed, open, older] = [&manifest.functions[0], &manifest.functions[1], &manifest.functions[2]];
		let forged = b"Bearer eyJhbGciOiJIUzI1NiJ9.eyJyb2xlIjoic2VydmljZV9yb2xlIn0.AAAA";
		assert_eq!(manifest.refusal(keyed, Some(forged)), Some(crate::jwt::Refused::Invalid));
		assert_eq!(manifest.refusal(keyed, None), Some(crate::jwt::Refused::Missing));
		// A function deployed open is run for anyone, and one from before the flag needs a key.
		assert_eq!(manifest.refusal(open, Some(forged)), None);
		assert_eq!(manifest.refusal(older, Some(forged)), Some(crate::jwt::Refused::Invalid));
		// A project's own process never receives the secret.
		let sent = serde_json::to_string(&manifest.for_project()).expect("json");
		assert!(!sent.contains("s3cret"));
		// And a control plane that sends no secret leaves the door's key check the only one.
		let older_plane: Manifest = serde_json::from_str(r#"{ "functions": [ { "name": "keyed", "digest": "x", "verifyJwt": true } ] }"#).expect("a manifest");
		assert_eq!(older_plane.refusal(&older_plane.functions[0], Some(forged)), None);
	}

	#[test]
	fn names_follow_the_control_packages_rule() {
		assert_eq!(function_name("/hello"), Some("hello"));
		assert_eq!(function_name("/hello/world"), Some("hello"));
		assert_eq!(function_name("/a_b-c"), Some("a_b-c"));
		assert_eq!(function_name("/"), None);
		assert_eq!(function_name("/.env"), None);
		assert_eq!(function_name("/-x"), None);
		assert_eq!(function_name("/x.y"), None);
		assert_eq!(function_name(&format!("/{}", "a".repeat(49))), None);
	}

	#[test]
	fn refs_and_digests_are_checked_before_they_are_paths() {
		assert!(valid_ref("abcdefgh12345"));
		assert!(!valid_ref("../etc"));
		assert!(!valid_ref(""));
		assert!(valid_digest(&"a".repeat(64)));
		assert!(!valid_digest(&"A".repeat(64)));
		assert!(!valid_digest("abc"));
	}
}
