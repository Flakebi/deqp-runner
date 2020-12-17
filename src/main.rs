use anyhow::Result;
use clap::Clap;
use deqp_runner::*;
use rand::seq::SliceRandom;
use rand::thread_rng;
use slog::{info, o, Drain};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    real_main().await
}

// TODO Add executable that generates mapping between cts test and pipelines
// TODO Add executable that generates summary from run log

async fn real_main() -> Result<()> {
    let mut options: Options = Options::parse();

    if std::env::var("RUST_LOG").is_err() {
        std::env::set_var("RUST_LOG", "info");
    }

    if PROGRESS_BAR.is_hidden() {
        options.no_progress = true;
    }

    let logger = if !options.no_progress {
        let drain = slog_term::FullFormat::new(deqp_runner::slog_pg::ProgressBarDecorator)
            .build()
            .fuse();
        let drain = slog_envlogger::new(drain).fuse();
        let drain = slog_async::Async::new(drain).build().fuse();

        slog::Logger::root(drain, o!())
    } else {
        let decorator = slog_term::TermDecorator::new().build();
        let drain = slog_term::CompactFormat::new(decorator).build().fuse();
        let drain = slog_envlogger::new(drain).fuse();
        let drain = slog_async::Async::new(drain).build().fuse();

        slog::Logger::root(drain, o!())
    };

    // Read test file
    let test_file = tokio::fs::read_to_string(&options.tests).await?;
    let mut tests = parse_test_file(&test_file);

    if let Some(end) = options.end {
        tests.truncate(end);
    }
    if let Some(start) = options.start {
        tests.drain(..std::cmp::min(start, tests.len()));
    }

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
        max_failures: options.max_failures,
        fail_dir: Some(options.failures),
    };

    let progress_bar = if !options.no_progress {
        Some(&*PROGRESS_BAR)
    } else {
        None
    };

    let job_count = options.jobs.unwrap_or_else(num_cpus::get);
    let mut summary = Summary::default();
    tokio::select! {
        _ = run_tests_parallel(
            &logger,
            &tests,
            &mut summary,
            &run_options,
            Some(&options.log),
            job_count,
            progress_bar,
        ) => {}
        _ = tokio::signal::ctrl_c() => {
            info!(logger, "Killed by sigint");
        }
    }

    summary::write_summary(
        &logger,
        &tests,
        &summary,
        run_options.fail_dir.as_deref(),
        Some(&options.csv_summary),
        Some(&options.xml_summary),
    )?;

    Ok(())
}
