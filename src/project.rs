//! One project's process: `snout-functions project`, started by the front process (router.rs) and
//! never by anyone else.
//!
//! It starts UNASSIGNED, as a spare: V8 up and one isolate booted, belonging to nobody. The front
//! process gives it a project with one line on its stdin, and only then does it learn anything:
//! the project's ref, the directory prepared for it and its manifest. It shuts itself into that
//! directory as a user of its own (confine.rs) before running any of the project's code, binds a
//! socket there that only the front process reaches, and says `ready` on stdout.
//!
//! After that, stdin carries the front process's orders, one JSON line each: a changed manifest
//! (a new secret, limit or deployment, for the next request), or the memory guard's "stop your
//! largest worker". Its stdin closing means the front process has gone, and so does this one.
//!
//! What it never has: the front door's secret (not in its environment, and removed from every
//! request before it is forwarded here), any other project's manifest or bundle, and the files
//! the host agent writes.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use serde::Deserialize;
use tokio::io::{AsyncBufReadExt, BufReader};

use crate::manifest::{Limits, Manifest, Manifests};

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum Order {
	Assign(Assign),
	Manifest(Manifest),
	Guard { used: u64, cap: u64 },
	Memory { used: u64, cap: u64 },
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Assign {
	pub project: String,
	pub uid: u32,
	pub view: PathBuf,
	pub confine: bool,
	pub manifest: Manifest,
}

pub fn run(idle: Duration, max_workers: usize, max_replicas: usize) {
	let runtime = match tokio::runtime::Builder::new_multi_thread().enable_all().build() {
		Ok(runtime) => runtime,
		Err(error) => {
			eprintln!("{error}");
			std::process::exit(1);
		}
	};
	runtime.block_on(async move {
		// Booted while this process belongs to nobody, so a project's first request pays neither.
		crate::isolate::keep_spare(Limits::default().memory_mb);
		let mut orders = BufReader::new(tokio::io::stdin()).lines();
		let assign = loop {
			match orders.next_line().await {
				Ok(Some(line)) => match serde_json::from_str::<Order>(&line) {
					Ok(Order::Assign(assign)) => break assign,
					Ok(_) => {}
					Err(error) => eprintln!("an order this process could not read: {error}"),
				},
				// The front process went before giving this one a project.
				_ => std::process::exit(0),
			}
		};
		let project = assign.project.clone();
		if assign.confine
			&& let Err(error) = crate::confine::confine(&assign.view, assign.uid)
		{
			eprintln!("{project}: this project's process could not be confined, so it will not run: {error}");
			std::process::exit(1);
		}
		let root = if assign.confine { PathBuf::from("/") } else { assign.view.clone() };
		// A process before this one for the project may have died holding its workers' sockets:
		// the directory is the project's and outlives a process, its sockets do not.
		let sockets = root.join("tmp").join("w");
		let _ = std::fs::remove_dir_all(&sockets);
		spares_for(&assign.manifest);
		let supervisor = crate::supervisor::Supervisor::new(sockets, idle, max_workers, max_replicas, false);
		tokio::spawn(supervisor.clone().watch());
		let state = Arc::new(crate::http::State { manifests: Manifests::held(root.join("bundles"), &project, assign.manifest), supervisor, door: None });
		let socket = root.join("sockets").join("front.sock");
		let _ = std::fs::remove_file(&socket);
		let listener = match tokio::net::UnixListener::bind(&socket) {
			Ok(listener) => listener,
			Err(error) => {
				eprintln!("{project}: {}: {error}", socket.display());
				std::process::exit(1);
			}
		};
		println!("ready");
		let listening = state.clone();
		tokio::spawn(async move { crate::http::serve_unix(listener, listening).await });

		let mut terminate = match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
			Ok(signal) => signal,
			Err(error) => {
				eprintln!("SIGTERM: {error}");
				std::process::exit(1);
			}
		};
		loop {
			tokio::select! {
				line = orders.next_line() => match line {
					Ok(Some(line)) => match serde_json::from_str::<Order>(&line) {
						Ok(Order::Manifest(manifest)) => {
							spares_for(&manifest);
							state.manifests.set(manifest);
						}
						Ok(Order::Guard { used, cap }) => {
							state.supervisor.stop_largest(used, cap);
						}
						Ok(Order::Memory { used, cap }) => state.supervisor.set_memory(used, cap),
						Ok(Order::Assign(_)) => eprintln!("{project}: this process already serves a project"),
						Err(error) => eprintln!("{project}: an order this process could not read: {error}"),
					},
					// The front process has gone: nothing can reach this one any more.
					_ => std::process::exit(0),
				},
				_ = terminate.recv() => break,
			}
		}
		// Stopped by the front process: it sends nothing new here, so what is in flight finishes.
		let deadline = std::time::Instant::now() + Duration::from_millis(crate::env_number("SNOUT_FUNCTIONS_DRAIN_MS", 25_000));
		while state.supervisor.in_flight() > 0 && std::time::Instant::now() < deadline {
			tokio::time::sleep(Duration::from_millis(50)).await;
		}
		std::process::exit(0);
	});
}

/// A spare isolate is kept for a project's second function, and none for a project with one: its
/// first request took the isolate this process booted before it had a project, and a spare more
/// would be ~7 MB held for every project on the host for nothing (tests/project-cost-probe.sh).
fn spares_for(manifest: &Manifest) {
	if std::env::var_os("SNOUT_FUNCTIONS_SPARES").is_none() {
		crate::isolate::set_spares(usize::from(manifest.functions.len() > 1));
	}
}
