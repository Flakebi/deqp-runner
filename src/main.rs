use clap::Clap;

use deqp_runner::*;
use slog::{info, o, Drain};

fn main() {
    let logger = {
        let decorator = slog_term::TermDecorator::new().build();
        let drain = slog_term::CompactFormat::new(decorator).build().fuse();
        let drain = slog_async::Async::new(drain).build().fuse();

        slog::Logger::root(drain, o!())
    };

    let options: Options = Options::parse();
    info!(logger, "Running"; "command" => ?options.run_command);
}
