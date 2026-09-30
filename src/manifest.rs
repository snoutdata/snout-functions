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

use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Manifest {
	#[serde(default)]
	pub functions: Vec<Deployed>,
	#[serde(default)]
	pub limits: Limits,
	#[serde(default)]
	pub env: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Deserialize)]
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
}

impl Manifest {
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
#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq, Hash)]
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

/// Manifests, cached by the file's modification time: exact, unlike a TTL, and cheaper than
/// re-reading a file with secrets in it on every invocation.
pub struct Manifests {
	root: PathBuf,
	held: Mutex<HashMap<String, (SystemTime, Manifest)>>,
}

impl Manifests {
	pub fn new(root: PathBuf) -> Self {
		Manifests { root, held: Mutex::new(HashMap::new()) }
	}

	pub fn bundle_dir(&self, digest: &str) -> PathBuf {
		self.root.join("bundles").join(digest)
	}

	pub fn get(&self, project: &str) -> Option<Manifest> {
		let path = self.root.join("projects").join(format!("{project}.json"));
		let stamp = std::fs::metadata(&path).and_then(|m| m.modified()).ok()?;
		if let Some((held_stamp, manifest)) = self.held.lock().ok()?.get(project)
			&& *held_stamp == stamp
		{
			return Some(manifest.clone());
		}
		let manifest = read(&path)?;
		self.held.lock().ok()?.insert(project.to_owned(), (stamp, manifest.clone()));
		Some(manifest)
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
