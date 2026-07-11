use std::collections::VecDeque;
use std::sync::mpsc::{Receiver, RecvTimeoutError};
use std::time::{Duration, Instant};

const STOP_LATENCY_LIMIT: Duration = Duration::from_secs(3);
const CONTROL_TIMEOUT: Duration = Duration::from_secs(10);

fn receive_matching(
    receiver: &Receiver<String>, observed: &mut Vec<String>, timeout: Duration, predicate: impl Fn(&str) -> bool,
) -> Option<String> {
    let deadline = Instant::now() + timeout;
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return None;
        }
        match receiver.recv_timeout(remaining) {
            Ok(line) => {
                let matched = predicate(&line);
                observed.push(line.clone());
                if matched {
                    return Some(line);
                }
            }
            Err(RecvTimeoutError::Timeout | RecvTimeoutError::Disconnected) => return None,
        }
    }
}

/// End-to-end regression for the injected channel path used by SwiftReckless.
/// The 60 MB NNUE is intentionally not committed, so CI opts in by setting
/// `RECKLESS_TEST_NET` to a staged `v54-5478683c.nnue` file.
#[test]
fn infinite_search_stops_and_reaches_ready_barrier() {
    let Ok(network_path) = std::env::var("RECKLESS_TEST_NET") else {
        eprintln!("SKIP: set RECKLESS_TEST_NET to run the injected-channel search regression");
        return;
    };
    let network = std::fs::read(&network_path).expect("read staged Reckless NNUE");
    reckless::nnue::load_network(&network).expect("load staged Reckless NNUE");

    let (command_tx, command_rx) = std::sync::mpsc::channel();
    let (output_tx, output_rx) = std::sync::mpsc::channel();
    let engine = std::thread::spawn(move || {
        reckless::run_io(
            VecDeque::new(),
            command_rx,
            Box::new(move |line| {
                let _ = output_tx.send(line.to_string());
            }),
        );
    });
    let mut observed = Vec::new();

    command_tx.send("isready".to_string()).unwrap();
    assert_eq!(
        receive_matching(&output_rx, &mut observed, CONTROL_TIMEOUT, |line| line == "readyok").as_deref(),
        Some("readyok")
    );

    command_tx.send("position startpos".to_string()).unwrap();
    command_tx.send("go infinite".to_string()).unwrap();
    let stop_started = Instant::now();
    command_tx.send("stop".to_string()).unwrap();
    command_tx.send("isready".to_string()).unwrap();
    let old_search_start = observed.len();
    let bestmove = receive_matching(&output_rx, &mut observed, STOP_LATENCY_LIMIT, |line| line.starts_with("bestmove"));
    assert!(bestmove.is_some(), "infinite search did not stop within {STOP_LATENCY_LIMIT:?}");
    assert!(stop_started.elapsed() < STOP_LATENCY_LIMIT);
    assert_eq!(
        receive_matching(&output_rx, &mut observed, CONTROL_TIMEOUT, |line| line == "readyok").as_deref(),
        Some("readyok"),
        "post-stop readyok did not follow the old bestmove"
    );

    // Reset only after the stopped search's bestmove/ready barrier. A second
    // readyok proves ucinewgame has executed before the engine is handed to the
    // next borrower.
    command_tx.send("ucinewgame".to_string()).unwrap();
    command_tx.send("isready".to_string()).unwrap();
    assert_eq!(
        receive_matching(&output_rx, &mut observed, CONTROL_TIMEOUT, |line| line == "readyok").as_deref(),
        Some("readyok")
    );

    command_tx.send("position startpos".to_string()).unwrap();
    command_tx.send("go depth 1".to_string()).unwrap();
    assert!(
        receive_matching(&output_rx, &mut observed, CONTROL_TIMEOUT, |line| line.starts_with("bestmove")).is_some(),
        "new search did not produce bestmove after the reset barrier"
    );

    let ordered = &observed[old_search_start..];
    let old_bestmove = ordered.iter().position(|line| line.starts_with("bestmove")).unwrap();
    let post_stop_ready = ordered.iter().position(|line| line == "readyok").unwrap();
    let reset_ready =
        ordered.iter().enumerate().skip(post_stop_ready + 1).find(|(_, line)| *line == "readyok").unwrap().0;
    let new_bestmove =
        ordered.iter().enumerate().skip(reset_ready + 1).find(|(_, line)| line.starts_with("bestmove")).unwrap().0;
    assert!(old_bestmove < post_stop_ready && post_stop_ready < reset_ready && reset_ready < new_bestmove);

    command_tx.send("quit".to_string()).unwrap();
    let quit_deadline = Instant::now() + CONTROL_TIMEOUT;
    while !engine.is_finished() && Instant::now() < quit_deadline {
        std::thread::sleep(Duration::from_millis(1));
    }
    assert!(engine.is_finished(), "injected engine did not quit promptly");
    engine.join().unwrap();
}
