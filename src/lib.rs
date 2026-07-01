#![allow(clippy::manual_is_multiple_of)]
#![allow(clippy::if_same_then_else)]
#![allow(unsafe_op_in_unsafe_fn)]

mod board;
mod evaluation;
mod history;
mod lookup;
mod misc;
mod movepick;
pub mod nnue;
mod numa;
mod parameters;
mod search;
mod stack;
mod thread;
mod threadpool;
mod time;
mod tools;
mod transposition;
mod types;
pub mod uci;

#[cfg(feature = "syzygy")]
mod tb;

#[cfg(feature = "syzygy")]
#[allow(warnings)]
mod bindings;

// ── Per-instance output sink ──────────────────────────────────────────────────
//
// A process-wide `Mutex` that holds the current output function.  When set,
// ALL UCI output (from both the message-loop thread and the search worker
// threads that call `print_uci_info`) is routed through this closure instead
// of going to fd 1 (stdout).  When `None`, output falls back to `println!`.
//
// Why not `thread_local!`?
//   Search worker threads are long-lived threads pre-created by `ThreadPool`.
//   Thread-locals are per-thread, so a value set on the UCI message-loop thread
//   would not be visible on worker threads.  A process-wide static mutex gives
//   all threads the same view.  This is safe because only one engine instance
//   may be running at a time (same invariant that the old fd-redirect model
//   relied on).
//
// The macro `uci_out!(fmt, args...)` is the ONLY way UCI-protocol lines should
// be emitted.  `eprintln!` for diagnostics is always left alone.

use std::sync::{Mutex, OnceLock};

// The actual sink storage.
static OUTPUT_SINK: OnceLock<Mutex<Option<Box<dyn Fn(&str) + Send + Sync>>>> = OnceLock::new();

fn sink() -> &'static Mutex<Option<Box<dyn Fn(&str) + Send + Sync>>> {
    OUTPUT_SINK.get_or_init(|| Mutex::new(None))
}

/// Set the per-instance output sink.  Replaces any previously set sink.
/// Passing `None` restores fallback-to-stdout behaviour.
pub fn set_output_sink(f: Option<Box<dyn Fn(&str) + Send + Sync>>) {
    *sink().lock().unwrap() = f;
}

/// Route a UCI output line.  Called by the `uci_out!` macro.
/// If a sink is installed, the line is passed to it; otherwise it is
/// written to stdout via `println!` (the binary/stdin path).
#[doc(hidden)]
pub fn _uci_emit(line: &str) {
    let guard = sink().lock().unwrap();
    match &*guard {
        Some(f) => f(line),
        None => println!("{line}"),
    }
}

/// Emit a UCI-protocol output line.
///
/// Usage mirrors `println!`: `uci_out!("uciok")` or `uci_out!("info depth {d}")`.
/// Routes to the installed output sink (if any) or falls back to `println!`.
/// Never use `println!` directly for UCI-protocol lines — use this macro.
#[macro_export]
macro_rules! uci_out {
    ($fmt:literal $(, $args:expr)* $(,)?) => {
        $crate::_uci_emit(&format!($fmt $(, $args)*))
    };
}

// ── Public API ─────────────────────────────────────────────────────────────────

/// Run the Reckless UCI engine in-process using the standard stdin/stdout path.
///
/// `buffer` is drained first (CLI-mode commands); when empty the engine
/// switches to reading stdin (UCI-mode).  This is the original entry point
/// used by the binary.  No output sink is installed — output goes to fd 1.
pub fn run(buffer: std::collections::VecDeque<String>) {
    lookup::initialize();
    nnue::initialize();
    uci::message_loop(buffer);
}

/// Run the Reckless UCI engine with per-instance I/O injection.
///
/// # Arguments
/// * `initial` — commands to process first (drained before reading from `rx`).
/// * `rx`      — channel to receive subsequent UCI command strings from the host.
/// * `out`     — callback invoked for every UCI output line (no trailing newline
///               in the string; called from the engine thread or search workers).
///
/// # Threading
/// This function **blocks** until the engine exits (on `quit` or channel close).
/// The caller must spawn it on a dedicated thread.
///
/// The output closure is installed as the process-wide sink for the duration of
/// this call, then cleared on return.  Only one call to `run_io` (or `run`) must
/// be active at a time.
pub fn run_io(
    initial: std::collections::VecDeque<String>,
    rx: std::sync::mpsc::Receiver<String>,
    out: Box<dyn Fn(&str) + Send + Sync>,
) {
    lookup::initialize();
    nnue::initialize();

    // Install the output sink for this engine instance.
    set_output_sink(Some(out));

    // Run the message loop using the injected channel as the input source.
    uci::message_loop_with_channel(initial, rx);

    // Clear the output sink so that the next engine instance starts clean.
    set_output_sink(None);
}
