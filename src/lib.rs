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

/// Run the Reckless UCI engine in-process.
///
/// `buffer` is drained first (CLI-mode commands); when empty the engine
/// switches to reading stdin (UCI-mode).  For the in-process FFI model
/// the caller redirects fd 0/1 to pipes before calling this function.
pub fn run(buffer: std::collections::VecDeque<String>) {
    lookup::initialize();
    nnue::initialize();
    uci::message_loop(buffer);
}
