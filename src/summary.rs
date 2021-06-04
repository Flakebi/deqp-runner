use std::borrow::Cow;
use std::collections::{HashMap, VecDeque};
use std::convert::TryInto;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;

use serde::{Deserialize, Serialize};
use slog::{warn, Logger};
use thiserror::Error;

use crate::{TestResultData, TestResultType};

#[derive(Clone, Debug, Default)]
pub struct Summary<'a>(pub HashMap<&'a str, (SummaryEntry<'a>, Option<TestResultData<'a>>)>);

/// Failure when writing the summary file
#[derive(Debug, Error)]
pub enum WriteSummaryError {
    #[error("Failed to write csv summary file: {0}")]
    WriteCsvFile(#[source] csv::Error),
    #[error("Failed to open csv summary file: {0}")]
    OpenCsvFile(#[source] csv::Error),
    #[error("Failed to write xml summary file: {0}")]
    WriteXmlFile(String),
    #[error("Failed to open xml summary file: {0}")]
    OpenFile(#[source] std::io::Error),
}

/// Lines of the `summary.csv` file.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct SummaryEntry<'a> {
    /// Name of the deqp test.
    pub name: Cow<'a, str>,
    pub result: TestResultType,
    /// Reference into the run log.
    ///
    /// References a [`TestResultEntry`], the reference is `None` if the test was not executed.
    pub run_id: Option<u64>,
}

/// Write summary csv and xml file.
pub fn write_summary<'a>(
    logger: &Logger,
    tests: &[&'a str],
    summary: &Summary,
    fail_dir: Option<&Path>,
    csv_file: Option<&Path>,
    xml_file: Option<&Path>,
) -> Result<(), WriteSummaryError> {
    // Write csv
    if let Some(file) = csv_file {
        let mut writer = csv::Writer::from_path(file).map_err(WriteSummaryError::OpenCsvFile)?;
        for t in tests {
            let r = summary
                .0
                .get(t)
                .map(|r| Cow::Borrowed(&r.0))
                .unwrap_or_else(|| {
                    Cow::Owned(SummaryEntry {
                        name: Cow::Borrowed(t),
                        result: TestResultType::NotRun,
                        run_id: None,
                    })
                });
            writer
                .serialize(r)
                .map_err(WriteSummaryError::WriteCsvFile)?;
        }
    }

    // Write xml
    if let Some(file) = xml_file {
        let report = create_xml_summary(logger, tests, summary, fail_dir)?;
        let mut file = std::fs::File::create(file).map_err(WriteSummaryError::OpenFile)?;
        report
            .write_xml(&mut file)
            .map_err(|e| WriteSummaryError::WriteXmlFile(e.to_string()))?;
    }
    Ok(())
}

pub fn create_xml_summary<'a>(
    logger: &Logger,
    tests: &[&'a str],
    summary: &Summary,
    fail_dir: Option<&Path>,
) -> Result<junit_report::Report, WriteSummaryError> {
    use junit_report::TestCase;

    // Write only failures
    let mut has_not_run = 0;
    let mut ts = junit_report::TestSuite::new("CTS").add_testcases(tests.iter().filter_map(|t| {
        if let Some(entry) = summary.0.get(t) {
            if entry.0.result.is_failure() || matches!(entry.0.result, TestResultType::Flake(_)) {
                // Count flakes as success but report anyway
                if let Some(run) = &entry.1 {
                    let mut test = if let TestResultType::Flake(res) = &entry.0.result {
                        TestCase::success(
                            t,
                            junit_report::Duration::from_std(run.duration.try_into().unwrap())
                                .unwrap(),
                        )
                        .set_system_out(&format!("{}\nFlake({:?})", run.result.stdout, res))
                    } else {
                        TestCase::failure(
                            t,
                            junit_report::Duration::from_std(run.duration.try_into().unwrap())
                                .unwrap(),
                            &format!("{:?}", entry.0.result),
                            "",
                        )
                        .set_system_out(&run.result.stdout)
                    };

                    // Read stderr from failure dir
                    if let (Some(fail_dir), Some(run_dir)) = (fail_dir, &run.fail_dir) {
                        let path = fail_dir.join(run_dir).join(crate::STDERR_FILE);
                        match File::open(&path) {
                            Ok(f) => {
                                let mut last_lines =
                                    VecDeque::with_capacity(crate::LAST_STDERR_LINES);
                                for l in BufReader::new(f).lines() {
                                    let l = match l {
                                        Ok(l) => l,
                                        Err(e) => {
                                            warn!(logger, "Failed to read stderr file";
                                                "path" => ?path, "error" => %e);
                                            break;
                                        }
                                    };
                                    if last_lines.len() >= crate::LAST_STDERR_LINES {
                                        last_lines.pop_front();
                                    }
                                    last_lines.push_back(l);
                                }

                                let mut last_lines_str = String::new();
                                for l in last_lines {
                                    if !last_lines_str.is_empty() {
                                        last_lines_str.push('\n');
                                    }
                                    last_lines_str.push_str(&l);
                                }
                                test = test.set_system_err(&last_lines_str);
                            }
                            Err(e) => {
                                warn!(logger, "Failed to open stderr file"; "path" => ?path,
                                    "error" => %e);
                            }
                        }
                    }

                    Some(test)
                } else {
                    Some(TestCase::failure(
                        t,
                        junit_report::Duration::seconds(0),
                        &format!("{:?}", entry.0.result),
                        "",
                    ))
                }
            } else {
                None
            }
        } else {
            has_not_run += 1;
            None
        }
    }));

    if has_not_run > 0 {
        ts = ts.add_testcase(TestCase::error(
            "aborted",
            junit_report::Duration::seconds(0),
            "NotRun",
            &format!("{} test cases were not run", has_not_run),
        ));
    }

    Ok(junit_report::Report::new().add_testsuite(ts))
}

#[cfg(test)]
mod tests {
    use anyhow::Result;
    use junit_report::Report;

    use super::*;
    use crate::*;

    async fn check_tests(args: &[&str]) -> Result<Report> {
        let logger = crate::tests::create_logger();
        let run_options = RunOptions {
            args: args.iter().map(|s| s.to_string()).collect(),
            capture_dumps: true,
            timeout: std::time::Duration::from_secs(2),
            max_failures: 0,
            fail_dir: None,
            retry: true,
        };

        // Read test file
        let test_file = tokio::fs::read_to_string("logs/in").await?;
        let tests = parse_test_file(&test_file);
        assert_eq!(tests.len(), 18, "Test size does not match");

        let mut summary = Summary::default();
        run_tests_parallel(
            &logger,
            &tests,
            &mut summary,
            &run_options,
            None,
            num_cpus::get(),
            None,
        )
        .await;

        Ok(create_xml_summary(
            &logger,
            &tests,
            &summary,
            run_options.fail_dir.as_deref(),
        )?)
    }

    #[tokio::test]
    async fn test_a() -> Result<()> {
        let report = check_tests(&["test/test-runner.sh", "logs/a", "/dev/null", "0"]).await?;
        let count: usize = report.testsuites().iter().map(|s| s.testcases.len()).sum();
        assert_eq!(count, 0, "Test case count does not match");

        let report = check_tests(&["test/test-runner.sh", "logs/c", "logs/c-err", "1"]).await?;
        let count: usize = report.testsuites().iter().map(|s| s.testcases.len()).sum();
        assert_eq!(count, 0, "Test case count does not match");

        Ok(())
    }

    #[tokio::test]
    async fn test_b() -> Result<()> {
        let report = check_tests(&["test/test-runner.sh", "logs/b", "logs/b-err", "1"]).await?;
        let count: usize = report.testsuites().iter().map(|s| s.testcases.len()).sum();
        println!("{:?}", report);
        assert_eq!(count, 1, "Test case count does not match");
        let test = &report.testsuites()[0].testcases[0];
        assert_eq!(test.name, "aborted");
        assert!(test.is_error());

        Ok(())
    }

    #[tokio::test]
    async fn test_d() -> Result<()> {
        let report = check_tests(&["test/test-runner.sh", "logs/d", "/dev/null", "1"]).await?;
        let count: usize = report.testsuites().iter().map(|s| s.testcases.len()).sum();
        println!("{:?}", report);
        assert_eq!(count, 2, "Test case count does not match");
        let test = &report.testsuites()[0].testcases[0];
        assert_eq!(test.name, "dEQP-VK.tessellation.primitive_discard.triangles_fractional_even_spacing_cw_point_mode");
        assert!(test.is_failure());

        Ok(())
    }

    #[tokio::test]
    async fn test_d_fatal() -> Result<()> {
        let report = check_tests(&["test/test-runner.sh", "logs/d", "logs/d-err", "1"]).await?;
        let count: usize = report.testsuites().iter().map(|s| s.testcases.len()).sum();
        println!("{:?}", report);
        assert_eq!(count, 2, "Test case count does not match");
        let test = &report.testsuites()[0].testcases[0];
        assert_eq!(test.name, "dEQP-VK.tessellation.primitive_discard.triangles_fractional_even_spacing_cw_point_mode");
        assert!(test.is_failure());

        Ok(())
    }

    #[tokio::test]
    async fn test_timeout() -> Result<()> {
        let report = check_tests(&["test/test-timeout.sh", "logs/d", "/dev/null", "1"]).await?;
        let count: usize = report.testsuites().iter().map(|s| s.testcases.len()).sum();
        println!("{:?}", report);
        assert_eq!(count, 2, "Test case count does not match");
        let test = &report.testsuites()[0].testcases[0];
        assert_eq!(test.name, "dEQP-VK.tessellation.primitive_discard.triangles_fractional_even_spacing_cw_point_mode");
        assert!(test.is_failure());

        Ok(())
    }

    #[tokio::test]
    async fn test_bisect() -> Result<()> {
        let report = check_tests(
            &["test/bisect-test-runner.sh", "dEQP-VK.tessellation.primitive_discard.triangles_fractional_odd_spacing_ccw_valid_levels", "logs/d", "/dev/null", "1", "logs/a", "dev/null", "0"],
        ).await?;
        let count: usize = report.testsuites().iter().map(|s| s.testcases.len()).sum();
        println!("{:?}", report);
        assert_eq!(count, 1, "Test case count does not match");
        let test = &report.testsuites()[0].testcases[0];
        assert_eq!(test.name, "dEQP-VK.tessellation.primitive_discard.triangles_fractional_even_spacing_cw_point_mode");
        assert!(test.is_failure());

        Ok(())
    }
}
