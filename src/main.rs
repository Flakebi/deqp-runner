use clap::Clap;

use anyhow::Result;
use deqp_runner::*;
use slog::{info, o, Drain};
use time::Duration;
use tokio::stream::StreamExt;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    real_main().await
}

async fn real_main() -> Result<()> {
    let logger = {
        let decorator = slog_term::TermDecorator::new().build();
        let drain = slog_term::CompactFormat::new(decorator).build().fuse();
        let drain = slog_envlogger::new(drain).fuse();
        let drain = slog_async::Async::new(drain).build().fuse();

        slog::Logger::root(drain, o!())
    };

    let options: Options = Options::parse();

    // Read test file
    let test_file = tokio::fs::read_to_string(&options.tests).await?;
    let tests = parse_test_file(&test_file);

    // TODO Read run command from options
    info!(logger, "Running"; "command" => ?options.run_command);

    let run_options = RunOptions {
        args: options.run_command,
        capture_dumps: true,
        timeout: Duration::seconds(10),
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

    while let Some(res) = running.next().await {
        slog::debug!(logger, "Result"; "res" => ?res);
    }

    Ok(())
}
