//! The one place this crate talks to V8 through raw pointers: a counting ArrayBuffer allocator.
//!
//! V8's heap limit (`CreateParams::heap_limits`, and the near-heap-limit callback in
//! `isolate.rs`) covers JavaScript objects but not the memory behind ArrayBuffers and typed
//! arrays, which V8 asks the embedder's allocator for. So a function could hold gigabytes in
//! `Uint8Array`s under a 128 MB limit, and the first thing to notice would be the kernel killing
//! the whole runtime, every project on the host with it. This allocator counts what each isolate
//! holds and REFUSES the allocation that would cross the limit, synchronously, before the memory
//! exists (V8 then throws `RangeError: Array buffer allocation failed`), and asks the worker to
//! stop, so the caller hears "memory limit".
//!
//! `unsafe` is denied everywhere else in the crate (Cargo.toml); this module is the V8
//! embedding boundary, the way pgrx is the Postgres one for the extensions (X13). Every block
//! states what it relies on.

#![allow(unsafe_code)]

use std::alloc::Layout;
use std::ffi::c_void;
use std::sync::atomic::{AtomicI32, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};

use deno_core::v8;

/// What one isolate's allocator knows: its limit, what it holds, and who to tell.
pub struct Budget {
	limit: usize,
	held: AtomicUsize,
	on_refusal: OnceLock<Box<dyn Fn() + Send + Sync>>,
}

impl Budget {
	/// Called once the worker exists: what to do when an allocation is refused.
	pub fn on_refusal(&self, act: impl Fn() + Send + Sync + 'static) {
		let _ = self.on_refusal.set(Box::new(act));
	}

	fn reserve(&self, len: usize) -> bool {
		let mut held = self.held.load(Ordering::Relaxed);
		loop {
			let next = match held.checked_add(len) {
				Some(next) if next <= self.limit => next,
				_ => {
					if let Some(act) = self.on_refusal.get() {
						act();
					}
					return false;
				}
			};
			match self.held.compare_exchange_weak(held, next, Ordering::AcqRel, Ordering::Relaxed) {
				Ok(_) => return true,
				Err(actual) => held = actual,
			}
		}
	}

	/// The ArrayBuffer memory this isolate holds now.
	pub fn held(&self) -> usize {
		self.held.load(Ordering::Acquire)
	}

	fn release(&self, len: usize) {
		self.held.fetch_sub(len, Ordering::AcqRel);
	}
}

/// V8 aligns nothing it asks for beyond what malloc would; 16 is malloc's guarantee on arm64.
const ALIGN: usize = 16;

fn layout(len: usize) -> Option<Layout> {
	// A zero-length buffer still gets a distinct allocation: std::alloc forbids size 0.
	Layout::from_size_align(len.max(1), ALIGN).ok()
}

unsafe extern "C" fn allocate(budget: &Budget, len: usize) -> *mut c_void {
	if !budget.reserve(len) {
		return std::ptr::null_mut();
	}
	match layout(len) {
		// SAFETY: the layout has a non-zero size.
		Some(layout) => {
			let ptr = unsafe { std::alloc::alloc_zeroed(layout) };
			if ptr.is_null() {
				budget.release(len);
			}
			ptr.cast()
		}
		None => {
			budget.release(len);
			std::ptr::null_mut()
		}
	}
}

unsafe extern "C" fn allocate_uninitialized(budget: &Budget, len: usize) -> *mut c_void {
	if !budget.reserve(len) {
		return std::ptr::null_mut();
	}
	match layout(len) {
		// SAFETY: the layout has a non-zero size.
		Some(layout) => {
			let ptr = unsafe { std::alloc::alloc(layout) };
			if ptr.is_null() {
				budget.release(len);
			}
			ptr.cast()
		}
		None => {
			budget.release(len);
			std::ptr::null_mut()
		}
	}
}

unsafe extern "C" fn free(budget: &Budget, data: *mut c_void, len: usize) {
	if data.is_null() {
		return;
	}
	if let Some(layout) = layout(len) {
		// SAFETY: V8 frees with the length it allocated with, and every pointer it holds came
		// from `allocate` or `allocate_uninitialized` above with `layout(len)`.
		unsafe { std::alloc::dealloc(data.cast(), layout) };
	}
	budget.release(len);
}

unsafe extern "C" fn drop_budget(budget: *const Budget) {
	// SAFETY: the pointer is the one `Arc::into_raw` produced in `allocator`, and V8 calls this
	// exactly once, when the allocator itself is released.
	drop(unsafe { Arc::from_raw(budget) });
}

static VTABLE: v8::RustAllocatorVtable<Budget> = v8::RustAllocatorVtable {
	allocate,
	allocate_uninitialized,
	free,
	drop: drop_budget,
};

/// An allocator for one isolate that holds at most `limit` bytes of ArrayBuffer memory.
pub fn allocator(limit: usize) -> (v8::UniqueRef<v8::Allocator>, Arc<Budget>) {
	let budget = Arc::new(Budget { limit, held: AtomicUsize::new(0), on_refusal: OnceLock::new() });
	let handle = Arc::into_raw(budget.clone());
	// SAFETY: `handle` stays valid until V8 calls `drop_budget`, which releases the reference
	// `into_raw` took; the vtable is 'static.
	let allocator = unsafe { v8::new_rust_allocator(handle, &VTABLE) };
	(allocator, budget)
}

/// A counter JavaScript writes with `Atomics` and Rust reads from any thread: the first four
/// bytes of a SharedArrayBuffer, with the backing store that keeps them alive. The worker's
/// request counter (isolate.rs, `Worker::stalled_ms`).
pub struct SharedCounter {
	_store: v8::SharedRef<v8::BackingStore>,
	count: *const AtomicI32,
}

// SAFETY: `count` points into the backing store `_store` keeps alive, allocated by `allocate`
// above and so aligned to `ALIGN`, and is only ever read through an atomic, as the JavaScript
// side writes it with Atomics. Dropping a SharedRef releases a reference count, which V8 makes
// thread-safe.
unsafe impl Send for SharedCounter {}
// SAFETY: as above: every access is an atomic load.
unsafe impl Sync for SharedCounter {}

impl SharedCounter {
	/// None for a buffer too short to hold a counter.
	pub fn new(store: v8::SharedRef<v8::BackingStore>) -> Option<Self> {
		if store.byte_length() < std::mem::size_of::<AtomicI32>() {
			return None;
		}
		let count = store.data()?.as_ptr().cast::<AtomicI32>().cast_const();
		Some(Self { _store: store, count })
	}

	pub fn load(&self) -> i32 {
		// SAFETY: see the type.
		unsafe { &*self.count }.load(Ordering::Acquire)
	}
}
