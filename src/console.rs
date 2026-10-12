//! What a function prints, and which function printed it.
//!
//! Every `console` call in a worker ends in Deno's one print op, `op_print`, already formatted.
//! This module swaps that op's implementation (an op middleware, `extension`), so a line leaves
//! the worker as one JSON object on stderr naming the project and function that printed it,
//! instead of bare text nobody can attribute. The host collects these lines and shows them to the
//! project's owner. Nothing goes to stdout: a project's process answers the front process there.
//!
//! A worker owns its thread, so whose output it is is a thread-local set when the worker is
//! claimed (`claim`). A thread with none (a spare rehearsing before anyone claims it, the bench)
//! prints plain text to stderr, as before.
//!
//! Bounded, because the output of somebody else's code is a stream we pay to carry: a line is cut
//! at `MAX_LINE` bytes, and a worker may print `BURST` lines at once and `RATE` a second after
//! that. What is refused is counted and said on the next line that is let through, so a gap in a
//! log is never silent.

use std::cell::RefCell;
use std::io::Write;
use std::time::Instant;

use deno_core::{Extension, OpDecl, op2};

/// The longest line kept, in bytes. Longer ones are cut at a character boundary and marked.
const MAX_LINE: usize = 4096;
/// Lines a worker may print at once.
const BURST: f64 = 200.0;
/// Lines a second a worker may print after its burst.
const RATE: f64 = 50.0;

struct Printer {
	project: String,
	function: String,
	tokens: f64,
	at: Instant,
	dropped: u64,
}

thread_local! {
	static PRINTER: RefCell<Option<Printer>> = const { RefCell::new(None) };
}

/// From now on, what this thread prints is `label`'s: `<project>/<function>`.
pub fn claim(label: &str) {
	let (project, function) = label.split_once('/').unwrap_or(("", label));
	PRINTER.with(|printer| {
		*printer.borrow_mut() = Some(Printer { project: project.to_owned(), function: function.to_owned(), tokens: BURST, at: Instant::now(), dropped: 0 });
	});
}

/// The extension that puts `op_print_attributed` where `op_print` was. It has no ops and no
/// JavaScript of its own, so it changes nothing a snapshot holds.
pub fn extension() -> Extension {
	Extension {
		name: "snout_console",
		middleware_fn: Some(Box::new(|op: OpDecl| if op.name == "op_print" { op.with_implementation_from(&op_print_attributed()) } else { op })),
		..Default::default()
	}
}

/// `msg` cut to at most `MAX_LINE` bytes, never inside a character.
fn cut(msg: &str) -> (&str, bool) {
	if msg.len() <= MAX_LINE {
		return (msg, false);
	}
	let mut end = MAX_LINE;
	while !msg.is_char_boundary(end) {
		end -= 1;
	}
	(&msg[..end], true)
}

/// The JSON line for one print, or None when the worker is past its rate.
fn attributed(printer: &mut Printer, msg: &str, is_err: bool, now: Instant, at_ms: u128) -> Option<String> {
	let elapsed = now.duration_since(printer.at).as_secs_f64();
	printer.at = now;
	printer.tokens = (printer.tokens + elapsed * RATE).min(BURST);
	if printer.tokens < 1.0 {
		printer.dropped += 1;
		return None;
	}
	printer.tokens -= 1.0;
	// Deno ends each console line with a newline; the collector adds its own.
	let msg = msg.strip_suffix('\n').unwrap_or(msg);
	let (kept, was_cut) = cut(msg);
	// `snout_log` is the key a collector looks for: a line without it is the runtime's own.
	let mut line = serde_json::json!({
		"snout_log": 1,
		"project": printer.project,
		"function": printer.function,
		"level": if is_err { "error" } else { "log" },
		"at": u64::try_from(at_ms).unwrap_or(u64::MAX),
		"msg": kept,
	});
	if was_cut {
		line["cut"] = serde_json::json!(msg.len());
	}
	if printer.dropped > 0 {
		line["dropped"] = serde_json::json!(printer.dropped);
		printer.dropped = 0;
	}
	Some(format!("{line}\n"))
}

/// `op_print`, attributed. Same arguments and result, so it drops in without a change to the
/// JavaScript that calls it.
#[op2(fast)]
fn op_print_attributed(#[string] msg: &str, is_err: bool) -> Result<(), std::io::Error> {
	let at_ms = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_millis()).unwrap_or(0);
	let line = PRINTER.with(|printer| {
		let mut printer = printer.borrow_mut();
		match printer.as_mut() {
			Some(printer) => attributed(printer, msg, is_err, Instant::now(), at_ms).map(Some),
			None => Some(None),
		}
	});
	let mut out = std::io::stderr().lock();
	match line {
		// Past its rate: counted, and said on the next line let through.
		None => Ok(()),
		Some(Some(line)) => out.write_all(line.as_bytes()),
		Some(None) => out.write_all(msg.as_bytes()),
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use std::time::Duration;

	fn printer() -> Printer {
		Printer { project: "abc".into(), function: "hello".into(), tokens: BURST, at: Instant::now(), dropped: 0 }
	}

	#[test]
	fn a_line_names_its_project_and_function() {
		let mut p = printer();
		let now = p.at;
		let line = attributed(&mut p, "hi there\n", false, now, 7).expect("let through");
		let value: serde_json::Value = serde_json::from_str(line.trim_end()).expect("json");
		assert_eq!(value["snout_log"], 1);
		assert_eq!(value["project"], "abc");
		assert_eq!(value["function"], "hello");
		assert_eq!(value["level"], "log");
		assert_eq!(value["msg"], "hi there");
		assert_eq!(value["at"], 7);
		assert!(line.ends_with('\n') && line.matches('\n').count() == 1, "one line, whatever the message holds");
	}

	#[test]
	fn a_multi_line_message_stays_one_line() {
		let mut p = printer();
		let now = p.at;
		let line = attributed(&mut p, "a\nb\n", true, now, 0).expect("let through");
		assert_eq!(line.matches('\n').count(), 1);
		let value: serde_json::Value = serde_json::from_str(line.trim_end()).expect("json");
		assert_eq!(value["msg"], "a\nb");
		assert_eq!(value["level"], "error");
	}

	#[test]
	fn a_long_line_is_cut_at_a_character() {
		let long = "é".repeat(MAX_LINE);
		let (kept, was_cut) = cut(&long);
		assert!(was_cut && kept.len() <= MAX_LINE && kept.chars().all(|c| c == 'é'));
	}

	#[test]
	fn past_the_burst_lines_are_counted_then_said() {
		let mut p = printer();
		let now = p.at;
		let mut let_through = 0;
		for _ in 0..(BURST as usize + 10) {
			if attributed(&mut p, "x", false, now, 0).is_some() {
				let_through += 1;
			}
		}
		assert_eq!(let_through, BURST as usize);
		let later = now + Duration::from_secs(1);
		let line = attributed(&mut p, "after", false, later, 0).expect("refilled");
		let value: serde_json::Value = serde_json::from_str(line.trim_end()).expect("json");
		assert_eq!(value["dropped"], 10);
	}
}
