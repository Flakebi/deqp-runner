use std::borrow::Cow;
use std::collections::HashSet;

use anyhow::{Result, bail, format_err};
use clap::Parser;
use deqp_runner::*;
use indicatif::ProgressBar;
use tracing::info;
use tracing_subscriber::EnvFilter;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    real_main().await
}

// TODO Add executable that generates mapping between cts test and pipelines
// TODO Add executable that generates summary from run log

async fn real_main() -> Result<()> {
    let mut options: Options = Options::parse();

    let progress_bar = if !options.no_progress {
        let bar = ProgressBar::new(1);
        bar.set_style(
            indicatif::ProgressStyle::with_template("{wide_bar} test {pos}/{len}{msg} ({eta})")
                .unwrap(),
        );
        bar.enable_steady_tick(std::time::Duration::from_secs(1));
        bar
    } else {
        ProgressBar::hidden()
    };

    if !progress_bar.is_hidden() {
        tracing_subscriber::fmt()
            .with_env_filter(EnvFilter::from_default_env())
            .with_writer(tracing_pg::ProgressBarWriter(progress_bar.clone()))
            .init();
    } else {
        tracing_subscriber::fmt()
            .with_env_filter(EnvFilter::from_default_env())
            .init();
    };

    // Read test file
    let test_file = match tokio::fs::read_to_string(&options.tests).await {
        Ok(r) => r,
        Err(e) => bail!("Failed to read test list file {:?}: {e}", options.tests),
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
        sorted_list = sort_with_deqp(&options.run_command, &tests)
            .await
            .map_err(|e| format_err!("Failed to sort test list: {e}"))?;
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
        shuffle_in_batches(&mut tests, BATCH_SIZE);
    }

    if options.run_command.is_empty() {
        // Try to read run command from options in test list file
        if let Some(cmd) = test_file.strip_prefix("#!").and_then(|l| l.lines().next()) {
            let cmd = cmd.trim().trim_start_matches("/usr/bin/env -S ");
            // Split by spaces, we do not want to implement a whole parser
            options.run_command = cmd.split(' ').map(|s| s.to_string()).collect();
        }
    }

    info!(command = ?options.run_command, "Running");

    let run_options = RunOptions {
        args: options.run_command,
        capture_dumps: true,
        timeout: std::time::Duration::from_secs(options.timeout.into()),
        max_failures: options.max_failures,
        fail_dir: Some(options.output.join(FAIL_DIR)),
        retry: !options.no_retry,
        batch_size: BATCH_SIZE,
    };

    let job_count = options.jobs.unwrap_or_else(num_cpus::get);
    let log_file = options.output.join(LOG_FILE);
    let mut summary = Summary::default();
    tokio::select! {
        _ = run_tests_parallel(
            &tests,
            &mut summary,
            &run_options,
            Some(&log_file),
            job_count,
            &progress_bar,
        ) => {}
        _ = tokio::signal::ctrl_c() => {
            info!("Killed by sigint");
        }
    }

    // Add filtered out missing tests
    for t in &missing {
        summary.0.insert(
            t,
            (
                summary::SummaryEntry {
                    name: Cow::Borrowed(t),
                    result: TestResultType::Missing,
                    run_id: None,
                },
                None,
            ),
        );
    }

    summary::write_summary(
        &tests,
        &summary,
        run_options.fail_dir.as_deref(),
        Some(&options.output.join(CSV_SUMMARY)),
        Some(&options.output.join(XML_SUMMARY)),
    )?;

    // Print stats
    let mut success = 0;
    let mut not_supported = 0;
    let mut fail = 0;
    let mut crash = 0;
    let mut timeout = 0;
    let mut missing_count = 0;
    let mut not_run = 0;
    let mut flake = 0;
    for t in &tests {
        if let Some(s) = summary.0.get(t) {
            let r = &s.0.result;
            match r {
                TestResultType::NotSupported => not_supported += 1,
                TestResultType::Crash => crash += 1,
                TestResultType::Timeout => timeout += 1,
                TestResultType::Missing => missing_count += 1,
                TestResultType::NotRun => not_run += 1,
                TestResultType::Flake(_) => flake += 1,
                _ if r.is_failure() => fail += 1,
                _ => success += 1,
            }
        } else {
            not_run += 1;
        }
    }
    info!(
        total = tests.len() + missing.len(),
        success,
        not_supported,
        fail,
        crash,
        timeout,
        missing = missing_count,
        not_found = missing.len(),
        not_run,
        flake,
        "Tests finished"
    );

    Ok(())
}
