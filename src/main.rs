use std::collections::HashSet;

use anyhow::{bail, format_err, Result};
use clap::Clap;
use deqp_runner::*;
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
    let test_file = match tokio::fs::read_to_string(&options.tests).await {
        Ok(r) => r,
        Err(e) => bail!("Failed to read test list file {:?}: {}", options.tests, e),
    };
    let mut tests = parse_test_file(&test_file);

    if let Some(end) = options.end {
        tests.truncate(end);
    }
    if let Some(start) = options.start {
        tests.drain(..std::cmp::min(start, tests.len()));
    }

    let sorted_list;
    let missing: Vec<_>;
    if !options.no_sort {
        // Run through deqp to sort
        sorted_list = sort_with_deqp(&logger, &options.run_command, &tests)
            .await
            .map_err(|e| format_err!("Failed to sort test list: {}", e))?;
        // Search missing tests
        let mut orig = tests
            .iter()
            .copied()
            .filter(|t| !t.contains('*'))
            .collect::<HashSet<_>>();
        for t in &sorted_list {
            orig.remove(t.as_str());
        }
        missing = orig.into_iter().collect();
        tests = sorted_list.iter().map(|t| t.as_str()).collect();
    } else {
        missing = Vec::new();
    }

    if options.shuffle {
        shuffle_in_batches(&mut tests);
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
        fail_dir: Some(options.output.join(FAIL_DIR)),
    };

    let progress_bar = if !options.no_progress {
        Some(&*PROGRESS_BAR)
    } else {
        None
    };

    let job_count = options.jobs.unwrap_or_else(num_cpus::get);
    let log_file = options.output.join(LOG_FILE);
    let mut summary = Summary::default();
    tokio::select! {
        _ = run_tests_parallel(
            &logger,
            &tests,
            &mut summary,
            &run_options,
            Some(&log_file),
            job_count,
            progress_bar,
        ) => {}
        _ = tokio::signal::ctrl_c() => {
            info!(logger, "Killed by sigint");
        }
    }

    // Add filtered out missing tests
    for t in missing {
        summary.0.insert(
            t,
            (
                summary::SummaryEntry {
                    name: t,
                    result: TestResultType::Missing,
                    run_id: None,
                },
                None,
            ),
        );
    }

    summary::write_summary(
        &logger,
        &tests,
        &summary,
        run_options.fail_dir.as_deref(),
        Some(&options.output.join(CSV_SUMMARY)),
        Some(&options.output.join(XML_SUMMARY)),
    )?;

    // Print stats
    let mut total = 0;
    let mut success = 0;
    let mut not_supported = 0;
    let mut fail = 0;
    let mut crash = 0;
    let mut timeout = 0;
    let mut missing = 0;
    let mut not_run = 0;
    let mut flake = 0;
    for s in summary.0.values() {
        total += 1;
        let r = &s.0.result;
        match r {
            TestResultType::NotSupported => not_supported += 1,
            TestResultType::Crash => crash += 1,
            TestResultType::Timeout => timeout += 1,
            TestResultType::Missing => missing += 1,
            TestResultType::NotRun => not_run += 1,
            TestResultType::Flake(_) => flake += 1,
            _ if r.is_failure() => fail += 1,
            _ => success += 1,
        }
    }
    info!(logger, "Tests finished"; "total" => total, "success" => success,
        "not_supported" => not_supported, "fail" => fail, "crash" => crash, "timeout" => timeout,
        "missing" => missing, "not_run" => not_run, "flake" => flake);

    Ok(())
}
