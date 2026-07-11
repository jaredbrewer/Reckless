use std::collections::HashMap;
use std::collections::VecDeque;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
    mpsc::{Receiver, RecvTimeoutError, Sender, TryRecvError},
};
use std::thread::JoinHandle;
use std::time::Duration;

use crate::{
    board::{Board, NullBoardObserver},
    search::Report,
    thread::{SharedContext, Status, ThreadData},
    threadpool::ThreadPool,
    time::{Limits, TimeManager},
    tools,
    transposition::DEFAULT_TT_SIZE,
    types::{Color, MAX_MOVES, Move, Score, is_decisive, is_loss, is_win},
    uci_out,
};

#[derive(Copy, Clone, PartialEq, Eq)]
enum Mode {
    Cli,
    Uci,
}

struct Settings {
    frc: bool,
    multi_pv: usize,
    move_overhead: u64,
    report: Report,
}

impl Default for Settings {
    fn default() -> Self {
        Self {
            frc: false,
            multi_pv: 1,
            move_overhead: 100,
            report: Report::Full,
        }
    }
}

/// A command delivered to the synchronous message loop. Injected `go`
/// commands carry a one-shot acknowledgement that is fulfilled only after the
/// search has changed `SharedContext.status` to `RUNNING`. The channel listener
/// waits for that acknowledgement before consuming a following `stop`/`quit`,
/// preventing an eagerly queued stop from being overwritten by search startup.
struct SearchLifecycle {
    started: Sender<()>,
    finished: Sender<()>,
}

struct QueuedMessage {
    text: String,
    search_lifecycle: Option<SearchLifecycle>,
}

impl QueuedMessage {
    fn plain(text: String) -> Self {
        Self { text, search_lifecycle: None }
    }
}

struct SearchCompletion(Option<Sender<()>>);

impl Drop for SearchCompletion {
    fn drop(&mut self) {
        if let Some(finished) = self.0.take() {
            let _ = finished.send(());
        }
    }
}

// Host sends wake `recv_timeout` immediately; this long interval is only the
// fallback that lets an unusual CLI-buffer exit join the listener. Avoid a
// high-frequency idle poll in the process-lifetime mobile engine.
const CHANNEL_LISTENER_POLL_INTERVAL: Duration = Duration::from_secs(1);

struct ChannelListener {
    receiver: Receiver<QueuedMessage>,
    shutdown: Arc<AtomicBool>,
    handle: JoinHandle<()>,
}

/// Original entry point: reads commands from stdin via a spawned listener thread.
/// This path is used by the binary (`main.rs`).  Output goes to stdout (via the
/// `uci_out!` macro, which falls back to `println!` when no sink is installed).
pub fn message_loop(buffer: VecDeque<String>) {
    let shared = Arc::new(SharedContext::default());
    let mut settings = Settings::default();
    let mut threads = ThreadPool::new(shared.clone());

    let rx = spawn_listener(shared.clone());

    let mode = if buffer.is_empty() { Mode::Uci } else { Mode::Cli };
    run_loop(buffer, rx, mode, &mut threads, &mut settings, &shared);
}

/// Per-instance I/O entry point: commands come from an injected channel `rx`.
/// No stdin listener is spawned; no fd redirection is performed.
/// Called by `reckless::run_io`.
pub fn message_loop_with_channel(buffer: VecDeque<String>, rx: std::sync::mpsc::Receiver<String>) {
    let shared = Arc::new(SharedContext::default());
    let mut settings = Settings::default();
    let mut threads = ThreadPool::new(shared.clone());

    // Match the stdin architecture: a dedicated listener remains responsive
    // while `run_loop` is synchronously blocked in `go()`. Besides fixing
    // stop/quit, this preserves UCI's requirement that `isready` answer while
    // the engine is calculating.
    let ChannelListener { receiver, shutdown, handle } = spawn_channel_listener(rx, shared.clone());
    let mode = if buffer.is_empty() { Mode::Uci } else { Mode::Cli };
    run_loop(buffer, receiver, mode, &mut threads, &mut settings, &shared);

    // `run_loop` can also exit after a CLI buffer, without a host-side `quit`.
    // Wake and join the listener so no prior engine instance can retain its
    // receiver or emit through a later instance's output sink.
    shutdown.store(true, Ordering::Release);
    if handle.join().is_err() {
        eprintln!("Injected UCI channel listener panicked");
    }
}

/// Core message loop, shared by both stdin and channel-based entry points.
///
/// `buffer` supplies pre-queued commands (CLI mode); `rx` supplies subsequent
/// commands (UCI mode).  `mode` starts as `Cli` if the buffer is non-empty,
/// `Uci` otherwise.  The loop switches to `Uci` when the `uci` command is seen.
fn run_loop(
    mut buffer: VecDeque<String>, rx: Receiver<QueuedMessage>, mut mode: Mode, threads: &mut ThreadPool,
    settings: &mut Settings, shared: &Arc<SharedContext>,
) {
    loop {
        let queued = if let Some(cmd) = buffer.pop_front() {
            QueuedMessage::plain(cmd)
        } else if mode == Mode::Uci {
            match rx.recv() {
                Ok(cmd) => cmd,
                Err(_) => break,
            }
        } else {
            break;
        };

        let QueuedMessage { text: message, search_lifecycle } = queued;

        let tokens = message.split_whitespace().collect::<Vec<_>>();
        match tokens.as_slice() {
            ["uci"] => {
                uci();
                mode = Mode::Uci;
            }

            ["isready"] => uci_out!("readyok"),

            ["go", tokens @ ..] => go(threads, settings, shared, tokens, search_lifecycle),
            ["position", tokens @ ..] => position(threads, settings, tokens),
            ["setoption", tokens @ ..] => set_option(threads, settings, shared, tokens),
            ["ucinewgame"] => reset(threads, shared),

            ["stop"] => shared.status.set(Status::STOPPED),
            ["quit"] => {
                break;
            }

            // Non-UCI commands
            ["compiler"] => compiler(),
            ["eval"] => eval(threads.main_thread()),
            ["d"] => println!("{}", threads.main_thread().board),
            ["bench", args @ ..] => match mode {
                Mode::Uci => tools::bench::<true>(args),
                Mode::Cli => tools::bench::<false>(args),
            },
            ["perft", depth] => tools::perft(depth.parse().unwrap(), &mut threads.main_thread().board),
            ["perft"] => eprintln!("Usage: perft <depth>"),

            // Ignore empty lines
            [] => (),

            _ => eprintln!("Unknown command: '{}'", message.trim_end()),
        }

        // Auto-exit after last CLI command
        if matches!(mode, Mode::Cli) && buffer.is_empty() {
            break;
        }
    }
}

fn spawn_listener(shared: Arc<SharedContext>) -> Receiver<QueuedMessage> {
    let (tx, rx) = std::sync::mpsc::channel();

    std::thread::spawn(move || {
        loop {
            let mut message = String::new();

            if std::io::stdin().read_line(&mut message).unwrap() == 0 {
                // EOF received
                if shared.status.get() != Status::RUNNING {
                    let _ = tx.send(QueuedMessage::plain("quit".to_string()));
                }
            }

            match message.trim_end() {
                "isready" => uci_out!("readyok"),
                "stop" => shared.status.set(Status::STOPPED),
                "quit" => {
                    shared.status.set(Status::STOPPED);
                    let _ = tx.send(QueuedMessage::plain("quit".to_string()));
                    break;
                }
                _ => {
                    // According to the UCI specs, commands that are unexpected
                    // in the current state should be ignored silently.
                    // (https://backscattering.de/chess/uci/#unexpected)
                    if shared.status.get() != Status::RUNNING {
                        let _ = tx.send(QueuedMessage::plain(message));
                    }
                }
            }
        }
    });

    rx
}

/// Bridge an injected host channel into the same two-thread architecture as
/// stdin. Control commands are consumed here so a synchronous search cannot
/// starve them; state-changing commands are forwarded while idle or queued
/// behind a search whose stop has already been requested. Channel disconnect
/// is equivalent to `quit` and also stops an infinite search, satisfying
/// `run_io`'s documented close behavior.
fn spawn_channel_listener(incoming: Receiver<String>, shared: Arc<SharedContext>) -> ChannelListener {
    let (command_tx, receiver) = std::sync::mpsc::channel();
    let shutdown = Arc::new(AtomicBool::new(false));
    let listener_shutdown = shutdown.clone();

    let handle = std::thread::spawn(move || {
        let mut active_search_finished: Option<Receiver<()>> = None;
        let mut stop_requested = false;

        'listen: loop {
            if listener_shutdown.load(Ordering::Acquire) {
                break;
            }

            let message = match incoming.recv_timeout(CHANNEL_LISTENER_POLL_INTERVAL) {
                Ok(message) => message,
                Err(RecvTimeoutError::Timeout) => continue,
                Err(RecvTimeoutError::Disconnected) => {
                    shared.status.set(Status::STOPPED);
                    let _ = command_tx.send(QueuedMessage::plain("quit".to_string()));
                    break;
                }
            };

            // Search completion may race the blocking host receive. Refresh
            // after a command wakes us so a position/new go arriving just
            // after bestmove is not mistaken for an unexpected in-search
            // command and silently discarded.
            if let Some(finished) = &active_search_finished {
                match finished.try_recv() {
                    Ok(()) | Err(TryRecvError::Disconnected) => {
                        active_search_finished = None;
                        stop_requested = false;
                    }
                    Err(TryRecvError::Empty) => {}
                }
            }

            let tokens = message.split_whitespace().collect::<Vec<_>>();
            match tokens.as_slice() {
                ["isready"] => {
                    if active_search_finished.is_some() && !stop_requested {
                        // UCI requires isready to remain responsive during an
                        // ordinary search. Once stop is requested, however, it
                        // becomes a completion barrier and must follow the old
                        // bestmove. When idle, forwarding also orders readyok
                        // after queued position/setoption/ucinewgame commands.
                        uci_out!("readyok");
                    } else {
                        if command_tx.send(QueuedMessage::plain(message)).is_err() {
                            break;
                        }
                    }
                }
                ["stop"] => {
                    if active_search_finished.is_some() {
                        stop_requested = true;
                    }
                    shared.status.set(Status::STOPPED);
                }
                ["quit"] => {
                    shared.status.set(Status::STOPPED);
                    let _ = command_tx.send(QueuedMessage::plain("quit".to_string()));
                    break;
                }
                _ => {
                    // UCI says unexpected commands during search are ignored.
                    // This is the same policy as `spawn_listener` above.
                    if active_search_finished.is_some() && !stop_requested {
                        continue;
                    }

                    if tokens.first() == Some(&"go") {
                        let (started_tx, started_rx) = std::sync::mpsc::channel();
                        let (finished_tx, finished_rx) = std::sync::mpsc::channel();
                        if command_tx
                            .send(QueuedMessage {
                                text: message,
                                search_lifecycle: Some(SearchLifecycle { started: started_tx, finished: finished_tx }),
                            })
                            .is_err()
                        {
                            break;
                        }

                        // Do not consume a following stop until `go` has made
                        // its RUNNING transition. Otherwise a host that queues
                        // `go; stop` can have STOPPED set first and immediately
                        // overwritten by ThreadPool startup.
                        loop {
                            match started_rx.recv_timeout(CHANNEL_LISTENER_POLL_INTERVAL) {
                                Ok(()) => {
                                    active_search_finished = Some(finished_rx);
                                    stop_requested = false;
                                    break;
                                }
                                Err(RecvTimeoutError::Timeout) if !listener_shutdown.load(Ordering::Acquire) => {}
                                Err(_) => break 'listen,
                            }
                        }
                    } else if command_tx.send(QueuedMessage::plain(message)).is_err() {
                        break;
                    }
                }
            }
        }
    });

    ChannelListener { receiver, shutdown, handle }
}

fn uci() {
    uci_out!("id name Reckless {}", env!("CARGO_PKG_VERSION"));
    uci_out!("id author Arseniy Surkov, Shahin M. Shahin, and Styx");
    uci_out!("option name Hash type spin default {DEFAULT_TT_SIZE} min 1 max 262144");
    uci_out!("option name Threads type spin default 1 min 1 max {}", ThreadPool::available_threads());
    uci_out!("option name MoveOverhead type spin default 100 min 0 max 2000");
    uci_out!("option name Minimal type check default false");
    uci_out!("option name Clear Hash type button");
    uci_out!("option name UCI_Chess960 type check default false");
    uci_out!("option name MultiPV type spin default 1 min 1 max {MAX_MOVES}");

    #[cfg(feature = "syzygy")]
    uci_out!("option name SyzygyPath type string default");

    #[cfg(feature = "spsa")]
    crate::parameters::print_options();

    uci_out!("uciok");
}

fn compiler() {
    println!("Compiler Version: {}", env!("COMPILER_VERSION"));
    println!("Compiler Target: {}", env!("COMPILER_TARGET"));
    println!("Compiler Features: {}", env!("COMPILER_FEATURES"));
}

fn reset(threads: &mut ThreadPool, shared: &Arc<SharedContext>) {
    threads.clear();
    shared.tt.clear(threads.len());

    for corrhist in unsafe { shared.replicator.get_all() } {
        corrhist.pawn.clear();
        corrhist.minor.clear();
        corrhist.non_pawn[Color::White].clear();
        corrhist.non_pawn[Color::Black].clear();
    }
}

fn go(
    threads: &mut ThreadPool, settings: &Settings, shared: &Arc<SharedContext>, tokens: &[&str],
    search_lifecycle: Option<SearchLifecycle>,
) {
    let (search_started, search_finished) = match search_lifecycle {
        Some(lifecycle) => (Some(lifecycle.started), Some(lifecycle.finished)),
        None => (None, None),
    };
    let _search_completion = SearchCompletion(search_finished);
    let board = &threads.main_thread().board;
    let limits = parse_limits(board.side_to_move(), tokens);
    let time_manager = TimeManager::new(limits, board.fullmove_number(), settings.move_overhead);

    threads.main_thread().multi_pv = settings.multi_pv;
    threads.execute_searches_with_start_hook(time_manager, settings.report, shared, move || {
        if let Some(started) = search_started {
            let _ = started.send(());
        }
    });

    // Terminal position (no legal moves): every thread's root_moves is empty
    // (search::start returned immediately). Emit a null bestmove instead of
    // indexing root_moves[0] below. (terminal-position guard)
    if threads.main_thread().root_moves.is_empty() {
        uci_out!("bestmove (none)");
        crate::misc::dbg_print();
        return;
    }

    let min_score = threads.iter().map(|v| v.root_moves[0].score).min().unwrap();
    let vote_value = |td: &ThreadData| (td.root_moves[0].score - min_score + 10) * td.completed_depth;

    let mut votes: HashMap<&Move, i32> = HashMap::new();
    for result in threads.iter() {
        *votes.entry(&result.root_moves[0].mv).or_default() += vote_value(result);
    }

    let mut best = 0;

    if !matches!(threads[best].time_manager.limits(), Limits::Depth(_)) && threads[0].multi_pv == 1 {
        for current in 1..threads.len() {
            let is_better_candidate = || -> bool {
                let best = &threads[best];
                let current = &threads[current];

                if is_win(best.root_moves[0].score) {
                    return current.root_moves[0].score > best.root_moves[0].score;
                }

                if current.root_moves[0].score != -Score::INFINITE
                    && best.root_moves[0].score != -Score::INFINITE
                    && is_loss(best.root_moves[0].score)
                {
                    return current.root_moves[0].score < best.root_moves[0].score;
                }

                if current.root_moves[0].score != -Score::INFINITE && is_decisive(current.root_moves[0].score) {
                    return true;
                }

                let best_vote = votes[&best.root_moves[0].mv];
                let current_vote = votes[&current.root_moves[0].mv];

                !is_loss(current.root_moves[0].score)
                    && (current_vote > best_vote
                        || (current_vote == best_vote && vote_value(current) > vote_value(best)))
            };

            if is_better_candidate() {
                best = current;
            }
        }
    }

    if best != 0 {
        threads[best].print_uci_info(threads[best].completed_depth);
    }

    uci_out!("bestmove {}", threads[best].root_moves[0].mv.to_uci(&threads.main_thread().board));
    crate::misc::dbg_print();
}

fn position(threads: &mut ThreadPool, settings: &Settings, mut tokens: &[&str]) {
    let mut board = Board::default();

    while !tokens.is_empty() {
        match tokens {
            ["startpos", rest @ ..] => {
                board = Board::starting_position();
                tokens = rest;
            }
            ["fen", rest @ ..] => {
                match Board::from_fen(&rest.join(" ")) {
                    Ok(b) => board = b,
                    Err(e) => eprintln!("Invalid FEN: {e:?}"),
                }
                board.set_frc(settings.frc);
                tokens = rest;
            }
            ["moves", rest @ ..] => {
                for uci_move in rest {
                    make_uci_move(&mut board, uci_move);
                }
                break;
            }
            _ => {
                tokens = &tokens[1..];
                continue;
            }
        }
    }

    for thread in threads.iter_mut() {
        thread.board = board.clone();
    }
}

fn make_uci_move(board: &mut Board, uci_move: &str) {
    let moves = board.generate_all_moves();
    if let Some(mv) = moves.iter().map(|entry| entry.mv).find(|mv| mv.to_uci(board) == uci_move) {
        board.make_move(mv, &mut NullBoardObserver {});
        board.advance_fullmove_counter();
    }
}

fn set_option(threads: &mut ThreadPool, settings: &mut Settings, shared: &Arc<SharedContext>, tokens: &[&str]) {
    match tokens {
        ["name", "Minimal", "value", v] => match *v {
            "true" => settings.report = Report::Minimal,
            "false" => settings.report = Report::Full,
            _ => eprintln!("Invalid value: '{v}'"),
        },
        ["name", "Clear", "Hash"] => {
            shared.tt.clear(threads.len());
            uci_out!("info string Hash cleared");
        }
        ["name", "Hash", "value", v] => {
            shared.tt.resize(threads.len(), v.parse().unwrap());
            uci_out!("info string set Hash to {v} MB");
        }
        ["name", "Threads", "value", v] => {
            threads.set_count(v.parse().unwrap());
            uci_out!("info string set Threads to {v}");
        }
        ["name", "MoveOverhead", "value", v] => {
            settings.move_overhead = v.parse().unwrap();
            uci_out!("info string set MoveOverhead to {v} ms");
        }
        #[cfg(feature = "syzygy")]
        ["name", "SyzygyPath", "value", v] => match crate::tb::initialize(v) {
            Some(size) => uci_out!("info string Loaded Syzygy tablebases with {size} pieces"),
            None => eprintln!("Failed to load Syzygy tablebases"),
        },
        ["name", "UCI_Chess960", "value", v] => {
            settings.frc = v.parse().unwrap_or_default();
            uci_out!("info string set UCI_Chess960 to {v}");
        }
        ["name", "MultiPV", "value", v] => {
            settings.multi_pv = v.parse().unwrap_or_default();
            uci_out!("info string set MultiPV to {v}");
        }
        #[cfg(feature = "spsa")]
        ["name", name, "value", v] => {
            crate::parameters::set_parameter(name, v);
            uci_out!("info string set {name} to {v}");
        }
        _ => eprintln!("Unknown option: '{}'", tokens.join(" ").trim_end()),
    }
}

fn eval(td: &mut ThreadData) {
    td.nnue.full_refresh(&td.board);
    let eval = match td.board.side_to_move() {
        Color::White => td.nnue.evaluate(&td.board),
        Color::Black => -td.nnue.evaluate(&td.board),
    };
    uci_out!("{eval}");
}

fn parse_limits(color: Color, tokens: &[&str]) -> Limits {
    if let ["infinite"] = tokens {
        return Limits::Infinite;
    }

    let mut main = None;
    let mut inc = None;
    let mut moves = None;

    for chunk in tokens.chunks(2) {
        if let [name, value] = *chunk {
            let Ok(value) = value.parse() else {
                continue;
            };

            match name {
                "depth" if value > 0 => return Limits::Depth(value),
                "movetime" if value > 0 => return Limits::Time(value as u64),
                "nodes" if value > 0 => return Limits::Nodes(value as u64),

                "wtime" if Color::White == color => main = Some(value),
                "btime" if Color::Black == color => main = Some(value),
                "winc" if Color::White == color => inc = Some(value),
                "binc" if Color::Black == color => inc = Some(value),
                "movestogo" => moves = Some(value as u64),

                _ => continue,
            }
        }
    }

    if main.is_none() && inc.is_none() {
        return Limits::Infinite;
    }

    let main = main.unwrap_or_default().max(0) as u64;
    let inc = inc.unwrap_or_default().max(0) as u64;

    match moves {
        Some(moves) => Limits::Cyclic(main, inc, moves),
        None => Limits::Fischer(main, inc),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::Ordering;
    use std::time::{Duration, Instant};

    use super::*;

    const CONTROL_LATENCY_LIMIT: Duration = Duration::from_millis(500);

    struct OutputSinkReset;

    impl Drop for OutputSinkReset {
        fn drop(&mut self) {
            crate::set_output_sink(None);
        }
    }

    #[test]
    fn injected_channel_controls_stop_search_and_order_ready_barrier() {
        let shared = Arc::new(SharedContext::default());
        let (host_tx, host_rx) = std::sync::mpsc::channel();
        let ChannelListener { receiver, shutdown, handle } = spawn_channel_listener(host_rx, shared.clone());

        // Queue stop immediately behind go. The listener must not consume it
        // until the synthetic search publishes RUNNING and acknowledges start;
        // otherwise startup could overwrite STOPPED and lose cancellation.
        host_tx.send("go infinite".to_string()).unwrap();
        let QueuedMessage { text, search_lifecycle } = receiver.recv_timeout(CONTROL_LATENCY_LIMIT).unwrap();
        assert_eq!(text, "go infinite");
        let SearchLifecycle { started: search_started, finished: search_finished } =
            search_lifecycle.expect("injected go must carry lifecycle acknowledgements");
        host_tx.send("stop".to_string()).unwrap();

        shared.status.set(Status::RUNNING);
        let stop_started = Instant::now();
        search_started.send(()).unwrap();
        while shared.status.get() != Status::STOPPED {
            assert!(stop_started.elapsed() < CONTROL_LATENCY_LIMIT, "stop was not observed promptly");
            std::thread::sleep(Duration::from_millis(1));
        }

        // Once stop is accepted, isready must be forwarded behind the active
        // synchronous go rather than emitted by the listener. Simulate the old
        // bestmove, completion, and queued run-loop reply in their required
        // order; the real-NNUE integration test pins the same end to end.
        let (ready_tx, ready_rx) = std::sync::mpsc::channel();
        let ready_shared = shared.clone();
        crate::set_output_sink(Some(Box::new(move |line| {
            let _ = ready_tx.send((line.to_string(), ready_shared.status.get()));
        })));
        let _sink_reset = OutputSinkReset;
        host_tx.send("isready".to_string()).unwrap();
        let QueuedMessage { text, search_lifecycle } = receiver.recv_timeout(CONTROL_LATENCY_LIMIT).unwrap();
        assert_eq!(text, "isready");
        assert!(search_lifecycle.is_none());
        assert!(matches!(ready_rx.try_recv(), Err(TryRecvError::Empty)));

        uci_out!("bestmove e2e4");
        search_finished.send(()).unwrap();
        uci_out!("readyok");
        let (bestmove, status_at_bestmove) = ready_rx.recv_timeout(CONTROL_LATENCY_LIMIT).unwrap();
        let (ready, status_at_ready) = ready_rx.recv_timeout(CONTROL_LATENCY_LIMIT).unwrap();
        assert_eq!(bestmove, "bestmove e2e4");
        assert_eq!(ready, "readyok");
        assert_eq!(status_at_bestmove, Status::STOPPED);
        assert_eq!(status_at_ready, Status::STOPPED);

        // Completion can land while the listener is blocked in the host
        // receive. The waking command must refresh lifecycle state and be
        // forwarded instead of being dropped as an in-search command.
        host_tx.send("position startpos".to_string()).unwrap();
        let QueuedMessage { text, search_lifecycle } = receiver.recv_timeout(CONTROL_LATENCY_LIMIT).unwrap();
        assert_eq!(text, "position startpos");
        assert!(search_lifecycle.is_none());

        // Quit is also a listener-level control: it must stop a running search
        // and enqueue termination without waiting for synchronous go() to end.
        shared.status.set(Status::RUNNING);
        let quit_started = Instant::now();
        host_tx.send("quit".to_string()).unwrap();
        let QueuedMessage { text, search_lifecycle } = receiver.recv_timeout(CONTROL_LATENCY_LIMIT).unwrap();
        assert_eq!(text, "quit");
        assert!(search_lifecycle.is_none());
        assert_eq!(shared.status.get(), Status::STOPPED);
        assert!(quit_started.elapsed() < CONTROL_LATENCY_LIMIT);

        shutdown.store(true, Ordering::Release);
        handle.join().unwrap();
    }
}
