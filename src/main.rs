fn main() {
    let buffer: std::collections::VecDeque<String> = std::env::args().skip(1).collect();
    reckless::run(buffer);
}
