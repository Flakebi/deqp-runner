use clap::Clap;

use anyhow::Result;
use deqp_runner::*;
use rand::seq::SliceRandom;
use rand::thread_rng;
use slog::{info, o, Drain};
use tokio::stream::StreamExt;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    real_main().await
}

// TODO Add executable that generates mapping between cts test and pipelines

async fn real_main() -> Result<()> {
    if std::env::var("RUST_LOG").is_err() {
        std::env::set_var("RUST_LOG", "info");
    }

    let logger = {
        let decorator = slog_term::TermDecorator::new().build();
        let drain = slog_term::CompactFormat::new(decorator).build().fuse();
        let drain = slog_envlogger::new(drain).fuse();
        let drain = slog_async::Async::new(drain).build().fuse();

        slog::Logger::root(drain, o!())
    };

    let mut options: Options = Options::parse();

    // Read test file
    let test_file = tokio::fs::read_to_string(&options.tests).await?;
    let mut tests = parse_test_file(&test_file);

    if options.shuffle {
        let mut rng = thread_rng();
        tests.shuffle(&mut rng);
    }

    if options.run_command.is_empty() {
        // Try to read run command from options in test list file
        if let Some(cmd) = test_file.strip_prefix("#!").and_then(|l| l.lines().next()) {
            let cmd = cmd.trim().trim_start_matches("/usr/bin/env -S ");
            // Split by spaces, we do not want to implement a whole parser
            options.run_command = cmd.split(' ').map(|s| s.to_string()).collect();
        }
    }

    info!(logger, "Running"; "command" => ?options.run_command);

    let run_options = RunOptions {
        args: options.run_command,
        capture_dumps: true,
        timeout: std::time::Duration::from_secs(options.timeout.into()),
        fail_dir: options.failures,
    };

    let job_count = options.jobs.unwrap_or_else(num_cpus::get);
    let mut running = run_tests_parallel(
        &logger,
        &tests,
        &run_options,
        options.log.as_deref(),
        options.output.as_deref(),
        job_count,
    );

    // TODO Handle ctrl+c
    // TODO Show progress bar

    while let Some(res) = running.next().await {
        slog::debug!(logger, "Result"; "res" => ?res);
    }

    Ok(())
}
