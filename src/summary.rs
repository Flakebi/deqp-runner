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
    pub name: &'a str,
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
                        name: t,
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
        use junit_report::TestCase;

        // Write only failures
        let ts = junit_report::TestSuite::new("CTS").add_testcases(tests.iter().filter_map(|t| {
            if let Some(entry) = summary.0.get(t) {
                if entry.0.result.is_failure() || matches!(entry.0.result, TestResultType::Flake(_))
                {
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
                Some(TestCase::error(
                    t,
                    junit_report::Duration::seconds(0),
                    "NotRun",
                    "",
                ))
            }
        }));

        let r = junit_report::Report::new().add_testsuite(ts);
        let mut file = std::fs::File::create(file).map_err(WriteSummaryError::OpenFile)?;
        r.write_xml(&mut file)
            .map_err(|e| WriteSummaryError::WriteXmlFile(e.to_string()))?;
    }
    Ok(())
}
