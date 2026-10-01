//! Shutting a project's process into what is its own, before it runs any of the project's code.
//!
//! The front process prepares a directory holding only this project's bundles, a /tmp and the
//! resolver's two files (router.rs), then hands it over. Here the process makes that directory its
//! whole filesystem and becomes a user of its own, so that nothing it runs, even code that got out
//! of its V8 isolate, can read another project's manifest (its secrets), bundles or sockets, reach
//! the front process's own socket, or signal another project's process:
//!
//!   1. `chroot` into the directory, then `chdir("/")`, so no path leads out of it;
//!   2. no supplementary groups, then the project's own gid and uid, real, effective and saved,
//!      which leaves no capability behind (the kernel clears them on leaving uid 0);
//!   3. `no_new_privs`, so no later exec can gain any;
//!   4. and a check that becoming root again now fails, since a confinement that did not take is
//!      worse than none: it looks like one.
//!
//! glibc applies the set*id calls to every thread of the process, which matters because V8's and
//! tokio's threads already exist by now. `unsafe` is denied everywhere else in the crate
//! (Cargo.toml); this module is the system calls, and nothing else.

#![allow(unsafe_code)]

use std::ffi::CString;
use std::os::unix::ffi::OsStrExt;
use std::path::Path;

fn failed(what: &str) -> String {
	format!("{what}: {}", std::io::Error::last_os_error())
}

pub fn confine(view: &Path, uid: u32) -> Result<(), String> {
	let path = CString::new(view.as_os_str().as_bytes()).map_err(|_| "the directory's path holds a NUL".to_owned())?;
	// SAFETY: plain system calls on arguments that outlive them; each result is checked.
	unsafe {
		if libc::chroot(path.as_ptr()) != 0 {
			return Err(failed("chroot"));
		}
		if libc::chdir(c"/".as_ptr()) != 0 {
			return Err(failed("chdir"));
		}
		if libc::setgroups(0, std::ptr::null()) != 0 {
			return Err(failed("setgroups"));
		}
		if libc::setresgid(uid, uid, uid) != 0 {
			return Err(failed("setresgid"));
		}
		if libc::setresuid(uid, uid, uid) != 0 {
			return Err(failed("setresuid"));
		}
		if libc::prctl(libc::PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0) != 0 {
			return Err(failed("no_new_privs"));
		}
		if libc::setuid(0) == 0 || libc::geteuid() != uid {
			return Err("the process could become root again after dropping to its own user".into());
		}
	}
	Ok(())
}

/// Whether this process may confine a project's process at all: it must be root (in its user
/// namespace, as in a rootless container). Anywhere else the processes are separate but share
/// this one's user and files, which is said once at start.
pub fn can_confine() -> bool {
	// SAFETY: geteuid cannot fail.
	unsafe { libc::geteuid() == 0 }
}
