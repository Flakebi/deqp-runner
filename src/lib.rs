use std::borrow::Cow;
use std::collections::hash_map::Entry;
use std::collections::{HashMap, VecDeque};
use std::ffi::OsStr;
use std::io::Write;
use std::mem;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Instant;

use futures::future::Either;
use futures::prelude::*;
use genawaiter::sync::r#gen;
use genawaiter::yield_;
use indicatif::ProgressBar;
use rand::rng;
use rand::seq::SliceRandom;
use serde::{Deserialize, Serialize};
use tempfile::NamedTempFile;
use thiserror::Error;
use time::{Duration, OffsetDateTime};
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::Command;
use tracing::{Instrument, debug, error, info, info_span, trace, warn};

pub mod deqp;
pub mod summary;
pub mod tracing_pg;

use deqp::*;
pub use summary::Summary;

/// This many tests will be executed with a single deqp run.
pub const BATCH_SIZE: usize = 1000;
/// Name of the file where stderr is saved.
const STDERR_FILE: &str = "stderr.txt";
/// Name of the file where the test list is saved.
const TEST_LIST_FILE: &str = "reproduce-list.txt";
/// Directory name where failure folders are stored.
pub const LOG_FILE: &str = "log.json";
/// CSV summary file with one test per line.
pub const CSV_SUMMARY: &str = "summary.csv";
/// XML junit summary file.
pub const XML_SUMMARY: &str = "summary.xml";
/// Directory name where failure folders are stored.
pub const FAIL_DIR: &str = "fails";
/// Dummy name if a failure cannot be attributed to a test.
pub const UNKNOWN_TEST_NAME: &str = "unknown";
/// These many lines from stderr will be saved in the junit xml result file.
const LAST_STDERR_LINES: usize = 5;
/// How often to print progress messages when no progress bar is displayed.
const UPDATE_PROGRESS_INTERVAL: std::time::Duration = std::time::Duration::from_secs(10);

#[derive(Clone, Debug)]
#[cfg_attr(feature = "bin", derive(clap::Parser))]
#[cfg_attr(feature = "bin", clap(version = clap::crate_version!(), author = clap::crate_authors!(),
    about = clap::crate_description!()))]
pub struct Options {
    /// Instances of deqp to run, defaults to cpu count.
    #[cfg_attr(feature = "bin", clap(short, long))]
    pub jobs: Option<usize>,
    /// Shuffle tests before running.
    ///
    /// This can uncover bugs that are not detected normally.
    #[cfg_attr(feature = "bin", clap(long))]
    pub shuffle: bool,
    /// Do not retry failing tests to find flakes.
    #[cfg_attr(feature = "bin", clap(long))]
    pub no_retry: bool,
    /// Hide progress bar.
    #[cfg_attr(feature = "bin", clap(short = 'p', long))]
    pub no_progress: bool,
    /// Do not sort before running.
    ///
    /// Sorting also expands wildcards.
    #[cfg_attr(feature = "bin", clap(long))]
    pub no_sort: bool,
    /// Start of test range from test list.
    #[cfg_attr(feature = "bin", clap(long))]
    pub start: Option<usize>,
    /// End of test range from test list.
    #[cfg_attr(feature = "bin", clap(long))]
    pub end: Option<usize>,
    /// Path for the output folder.
    ///
    /// Various files are written into the output folder: The summary in csv and xml format,
    /// the log and fail directories
    #[cfg_attr(feature = "bin", clap(short, long, default_value = "."))]
    pub output: PathBuf,
    /// A file with tests to run.
    #[cfg_attr(feature = "bin", clap(short, long))]
    pub tests: PathBuf,
    /// Timout for a single test in seconds.
    ///
    /// A test that runs this long is considered failing.
    #[cfg_attr(feature = "bin", clap(long, default_value = "900"))]
    pub timeout: u32,
    /// Abort after this amount of failures. 0 means disabled.
    ///
    /// This is not necessarily counted accurately.
    #[cfg_attr(feature = "bin", clap(long, default_value = "100"))]
    pub max_failures: usize,
    /// The deqp command to run. E.g. `./deqp-vk --deqp-caselist-file`
    ///
    /// A filename with the tests cases that should be run is appended to the command.
    pub run_command: Vec<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct TestResultData<'a> {
    /// Name of the deqp test.
    pub name: &'a str,
    pub result: TestResult,
    pub start: OffsetDateTime,
    pub duration: Duration,
    /// PID of the deqp process that ran this test.
    pub pid: Option<u32>,
    /// If a directory with data about the failure was created, the name of the directory.
    pub fail_dir: Option<String>,
}

/// Same as [`TestResultData`] but with information on how to reproduce the run.
#[derive(Debug)]
pub struct ReproducibleTestResultData<'a, 'list> {
    pub data: TestResultData<'a>,
    /// The last test in this list is the one the test result is about.
    pub run_list: &'list [&'a str],
    pub args: &'a [String],
}

#[derive(Debug, Error)]
pub enum DeqpSortError {
    #[error("Failed to create temporary file : {0}")]
    TempFile(#[source] std::io::Error),
    #[error("Failed to write into file: {0}")]
    WriteFailed(#[source] std::io::Error),
    #[error("Failed to spawn process: {0}")]
    SpawnFailed(#[source] std::io::Error),
    #[error("Failed to wait for process to end: {0}")]
    WaitFailed(#[source] std::io::Error),
    #[error("Failed to read output from process: {0}")]
    ReadFailed(#[source] std::io::Error),
    #[error("deqp exit code {0:?}; stdout: {1}; stderr: {2}")]
    SortFailed(Option<i32>, String, String),
}

#[derive(Debug, Deserialize, Serialize)]
pub struct DeqpErrorWithOutput {
    error: DeqpError,
    stdout: String,
}

#[derive(Clone, Debug)]
pub struct RunOptions {
    pub args: Vec<String>,
    pub capture_dumps: bool,
    pub timeout: std::time::Duration,
    pub max_failures: usize,
    /// Directory where failure dumps should be created.
    pub fail_dir: Option<PathBuf>,
    pub retry: bool,
    pub batch_size: usize,
}

#[derive(Debug)]
pub enum RunTestListEvent<'a, 'list> {
    TestResult(ReproducibleTestResultData<'a, 'list>),
    DeqpError(DeqpErrorWithOutput),
}

/// Struct for the `run_log.json` file. Every line in this file is such an entry.
#[derive(Debug, Deserialize, Serialize)]
pub enum RunLogEntry<'a> {
    TestResult(#[serde(borrow)] TestResultEntry<'a>),
    /// Error that happened independant of a test.
    DeqpError(DeqpErrorWithOutput),
}

/// Entry for a test result in the run log.
#[derive(Debug, Deserialize, Serialize)]
pub struct TestResultEntry<'a> {
    /// Globally unique id for a test result entry in the run log.
    pub id: u64,
    #[serde(borrow)]
    pub data: TestResultData<'a>,
}

#[derive(Debug, Eq, PartialEq)]
enum BisectState {
    /// Default state
    Unknown,
    /// Running the test with only the last half of the tests succeeds.
    SucceedingWithLastHalf,
}

#[derive(Debug)]
enum JobEvent<'a> {
    RunLogEntry(RunLogEntry<'a>),
    NewJob(Job<'a>),
}

#[derive(Debug)]
enum Job<'a> {
    /// Run all tests in the list.
    FirstRun { list: &'a [&'a str] },
    /// Run only the last test in the list, which is the one that failed.
    SecondRun { list: &'a [&'a str] },
    /// Run all tests again, the last one is the one we are looking at.
    ThirdRun { list: &'a [&'a str] },
    /// Bisect tests run before to see if some interaction causes the problem.
    Bisect {
        list: Vec<&'a str>,
        state: BisectState,
    },
}

struct RunTestListState<'a, 'list> {
    /// Input to the process that was last started.
    tests: &'list [&'a str],
    options: &'a RunOptions,
    running: Option<RunDeqpState>,
    /// Index into current `tests` and start time.
    cur_test: Option<(usize, OffsetDateTime)>,
    last_finished: Option<usize>,

    /// Temporary file that contains the test list and is passed to deqp.
    test_list_file: Option<NamedTempFile>,
    /// If the current run had a failure and we already created a failure dir, this is the
    /// directory.
    fail_dir: Option<String>,
}

impl DeqpError {
    pub fn is_fatal(&self) -> bool {
        matches!(
            self,
            Self::SpawnFailed(_) | Self::StartError(_) | Self::NoTestsRun | Self::NoProcessResult
        )
    }
}

impl<'a> From<TestResultData<'a>> for JobEvent<'a> {
    /// Do not forget to fill out the run id afterwards.
    fn from(data: TestResultData<'a>) -> Self {
        Self::RunLogEntry(RunLogEntry::TestResult(TestResultEntry { id: 0, data }))
    }
}

impl<'a> From<DeqpErrorWithOutput> for JobEvent<'a> {
    fn from(err: DeqpErrorWithOutput) -> Self {
        Self::RunLogEntry(RunLogEntry::DeqpError(err))
    }
}

impl<'a, 'list> RunTestListState<'a, 'list> {
    fn new(tests: &'list [&'a str], options: &'a RunOptions) -> Self {
        RunTestListState {
            tests,
            options,
            running: None,
            cur_test: None,
            last_finished: None,

            test_list_file: None,
            fail_dir: None,
        }
    }

    /// Start a new deqp process.
    ///
    /// Returns the arguments for starting the process.
    /// We cannot start the process here because we cannot name the type of [`run_deqp`].
    fn start(&mut self) -> Result<Vec<String>, DeqpErrorWithOutput> {
        // TODO pipeline dumps
        // Create a temporary file for the test list
        let mut temp_file = NamedTempFile::new().map_err(|e| DeqpErrorWithOutput {
            error: DeqpError::StartError(format!(
                "Failed to create temporary file for test list: {e}"
            )),
            stdout: String::new(),
        })?;
        for t in self.tests {
            writeln!(&mut temp_file, "{t}").map_err(|e| DeqpErrorWithOutput {
                error: DeqpError::StartError(format!(
                    "Failed to write temporary file for test list: {e}"
                )),
                stdout: String::new(),
            })?;
        }

        let mut args = self.options.args.clone();
        args.push(
            temp_file
                .path()
                .as_os_str()
                .to_str()
                .ok_or_else(|| DeqpErrorWithOutput {
                    error: DeqpError::StartError(format!(
                        "Failed to get name of temporary file for test list (path: {:?})",
                        temp_file.path()
                    )),
                    stdout: String::new(),
                })?
                .into(),
        );
        self.cur_test = None;
        self.last_finished = None;
        self.fail_dir = None;
        self.test_list_file = Some(temp_file);
        Ok(args)
    }

    fn create_fail_dir(&mut self, failed_test: &str) {
        if self.fail_dir.is_none()
            && let Some(dir) = &self.options.fail_dir
        {
            for i in 0.. {
                let dir_name = if i == 0 {
                    failed_test.to_string()
                } else {
                    format!("{failed_test}-{i}")
                };
                let new_dir = dir.join(&dir_name);
                if !new_dir.exists() {
                    if let Err(error) = std::fs::create_dir_all(&new_dir) {
                        error!(%error, "Failed to create failure directory");
                        return;
                    }
                    self.fail_dir = Some(dir_name);
                    // Write reproduce-list.txt
                    match std::fs::File::create(new_dir.join(TEST_LIST_FILE)) {
                        Ok(mut f) => {
                            if let Err(error) = (|| -> Result<(), std::io::Error> {
                                // Write options
                                write!(&mut f, "#!")?;
                                if self
                                    .options
                                    .args
                                    .first()
                                    .map(|a| !a.starts_with('/'))
                                    .unwrap_or_default()
                                {
                                    write!(&mut f, "/usr/bin/env -S ")?;
                                }
                                writeln!(
                                    &mut f,
                                    "{}",
                                    self.options
                                        .args
                                        .iter()
                                        .map(|a| a.replace('\n', "\\n"))
                                        .collect::<Vec<_>>()
                                        .join(" ")
                                )?;

                                // Write tests
                                for t in self.tests {
                                    writeln!(&mut f, "{t}")?;
                                }
                                Ok(())
                            })() {
                                error!(%error, "Failed to write reproduce list");
                            }
                        }
                        Err(error) => {
                            error!(%error, "Failed to create reproduce list file");
                        }
                    }
                    break;
                }
            }
        }

        if let Some(running) = &mut self.running
            && !running.stderr.is_empty()
        {
            // Save current stderr
            let stderr = mem::take(&mut running.stderr);
            self.save_fail_dir_stderr(&stderr);
        }
    }

    /// Save stderr to the current `fail_dir` if there is one.
    fn save_fail_dir_stderr(&self, stderr: &str) {
        if let Some(dir_name) = &self.fail_dir {
            let fail_dir = self.options.fail_dir.as_ref().unwrap().join(dir_name);
            // Save stderr
            match std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(fail_dir.join(STDERR_FILE))
            {
                Ok(mut f) => {
                    if let Err(error) = f.write_all(stderr.as_bytes()) {
                        error!(%error, "Failed to write stderr file");
                    }
                }
                Err(error) => {
                    error!(%error, "Failed to create stderr file");
                }
            }
        } else {
            warn!("Tried to save stderr without a fail dir set, ignoring");
        }
    }

    fn get_missing(&self, count: usize) -> Vec<RunTestListEvent<'a, 'list>> {
        let start = self.last_finished.map(|i| i + 1).unwrap_or_default();
        let pid = self.running.as_ref().map(|r| r.pid);
        (0..count)
            .map(|i| {
                RunTestListEvent::TestResult(ReproducibleTestResultData {
                    data: TestResultData {
                        name: self.tests[start + i],
                        result: TestResult {
                            stdout: String::new(),
                            variant: TestResultType::Missing,
                        },
                        start: OffsetDateTime::now_utc(),
                        duration: Duration::new(0, 0),
                        pid,
                        fail_dir: None,
                    },
                    run_list: &self.tests[..start + i + 1],
                    args: &self.options.args,
                })
            })
            .collect()
    }

    fn handle_test_start(&mut self, test: &str) -> Vec<RunTestListEvent<'a, 'list>> {
        trace!(test, "Test started");
        let next_test = self.last_finished.map(|i| i + 1).unwrap_or_default();
        if let Some(i) = self.tests[next_test..].iter().position(|t| t == &test) {
            self.cur_test = Some((next_test + i, OffsetDateTime::now_utc()));
            self.get_missing(i)
        } else {
            warn!(test, "Executing unknown test");
            Vec::new()
        }
    }

    fn handle_test_end(&mut self, result: TestResult) -> Option<RunTestListEvent<'a, 'list>> {
        trace!(?result, cur_test = ?self.cur_test, "Test end");
        if let Some(cur_test) = self.cur_test.take() {
            self.last_finished = Some(cur_test.0);
            let duration = OffsetDateTime::now_utc() - cur_test.1;
            let is_failure = result.variant.is_failure();
            if is_failure {
                self.create_fail_dir(self.tests[cur_test.0]);
            }

            Some(RunTestListEvent::TestResult(ReproducibleTestResultData {
                data: TestResultData {
                    name: self.tests[cur_test.0],
                    result,
                    start: cur_test.1,
                    duration,
                    pid: self.running.as_ref().map(|r| r.pid),
                    fail_dir: if is_failure {
                        self.fail_dir.clone()
                    } else {
                        None
                    },
                },
                run_list: &self.tests[..cur_test.0 + 1],
                args: &self.options.args,
            }))
        } else {
            warn!(last_finished = ?self.last_finished.map(|i| self.tests[i]), "Finished test without starting a test");
            None
        }
    }

    fn handle_finished(&mut self, state: RunDeqpState) -> Vec<RunTestListEvent<'a, 'list>> {
        trace!(result = ?state.finished_result, stdout = state.stdout, stderr = state.stderr, "Finished");

        let mut is_failure = true;
        let pid = state.pid;
        let res = if let Some(cur_test) = self.cur_test {
            let duration = OffsetDateTime::now_utc() - cur_test.1;
            self.create_fail_dir(self.tests[cur_test.0]);

            if state
                .finished_result
                .as_ref()
                .map(Result::is_ok)
                .unwrap_or_default()
            {
                warn!(cur_test = self.tests[cur_test.0], started = %cur_test.1, "test not finished but deqp exited successful, count as failure");
            }

            // Continue testing
            let run_list = &self.tests[..cur_test.0 + 1];
            self.tests = &self.tests[cur_test.0 + 1..];

            let result_data = RunTestListEvent::TestResult(ReproducibleTestResultData {
                data: TestResultData {
                    name: run_list[cur_test.0],
                    result: TestResult {
                        stdout: state.stdout,
                        variant: if matches!(state.finished_result, Some(Err(DeqpError::Timeout))) {
                            TestResultType::Timeout
                        } else {
                            TestResultType::Crash
                        },
                    },
                    start: cur_test.1,
                    duration,
                    pid: Some(state.pid),
                    fail_dir: self.fail_dir.clone(),
                },
                run_list,
                args: &self.options.args,
            });

            let mut result = Vec::new();
            if let Some(Err(e)) = state.finished_result
                && e.is_fatal()
            {
                result.push(RunTestListEvent::DeqpError(DeqpErrorWithOutput {
                    error: e,
                    stdout: String::new(),
                }));
            }
            result.push(result_data);

            trace!(?result, "Test finish returns");
            result
        } else if let Some(error) = state.finished_result.map(Result::err).unwrap_or_else(|| {
            error!(pid, "Process result is not set, aborting");
            Some(DeqpError::NoProcessResult)
        }) {
            if let Some(last_finished) = self.last_finished {
                self.create_fail_dir(self.tests[last_finished]);
                // No current test executed, so probably some tests are failing, therefore the exit
                // status is not 0
                // Mark rest of tests as missing
                let mut r = self.get_missing(self.tests.len() - last_finished - 1);
                self.tests = &[];
                if error.is_fatal() {
                    r.push(RunTestListEvent::DeqpError(DeqpErrorWithOutput {
                        error,
                        stdout: state.stdout,
                    }));
                }
                r
            } else {
                self.create_fail_dir(UNKNOWN_TEST_NAME);
                // No test executed, counts as fatal error
                self.tests = &[];
                warn!(?error, "Deqp exited without running tests, aborting");
                vec![RunTestListEvent::DeqpError(DeqpErrorWithOutput {
                    error: DeqpError::NoTestsRun,
                    stdout: state.stdout,
                })]
            }
        } else {
            is_failure = false;
            let r = if let Some(last) = self.last_finished {
                // Mark rest of tests as missing
                debug!(tests = ?self.tests, last, "not all missing");
                self.get_missing(self.tests.len() - last - 1)
            } else {
                // No test executed, counts all tests as missing
                debug!(tests = ?self.tests, "all missing");
                self.get_missing(self.tests.len())
            };
            self.tests = &[];
            r
        };

        if is_failure && self.fail_dir.is_some() {
            // Create a fail dir if there is none yet
            self.create_fail_dir(UNKNOWN_TEST_NAME);
            // Save rest of stderr
            self.save_fail_dir_stderr(&state.stderr);
        }
        res
    }
}

impl<'a> Job<'a> {
    /// Returns new jobs for failed tests
    fn run(self, options: &'a RunOptions) -> impl Stream<Item = JobEvent<'a>> {
        match self {
            Self::FirstRun { list } => {
                let res: Box<dyn Stream<Item = _> + Send + Unpin> =
                    Box::new(run_test_list(list, options).flat_map(move |r| {
                        trace!("First run test result");
                        match r {
                            RunTestListEvent::TestResult(res) => {
                                let is_failure = res.data.result.variant.is_failure();
                                let entry = res.data.into();
                                // Start second run for failed tests
                                if is_failure && options.retry {
                                    let new_job =
                                        JobEvent::NewJob(Job::SecondRun { list: res.run_list });
                                    Either::Left(stream::iter(vec![entry, new_job]))
                                } else {
                                    // Return either so we do not need to allocate for non-failures
                                    Either::Right(stream::iter(Some(entry)))
                                }
                            }
                            RunTestListEvent::DeqpError(e) => {
                                Either::Right(stream::iter(Some(e.into())))
                            }
                        }
                    }));
                res
            }
            Self::SecondRun { list } => {
                // Run only the failing test (which is the last in the list)
                Box::new(
                    run_test_list(&list[list.len() - 1..], options).flat_map(move |r| {
                        trace!("Second run test result");
                        match r {
                            RunTestListEvent::TestResult(res) => {
                                let is_failure = res.data.result.variant.is_failure();
                                // Start third run if the test succeeded when run in isolation
                                let entry = res.data.into();
                                if !is_failure {
                                    let new_job = JobEvent::NewJob(Job::ThirdRun { list });
                                    Either::Left(stream::iter(vec![entry, new_job]))
                                } else {
                                    Either::Right(stream::iter(Some(entry)))
                                }
                            }
                            RunTestListEvent::DeqpError(e) => {
                                Either::Right(stream::iter(Some(e.into())))
                            }
                        }
                    }),
                )
            }
            Self::ThirdRun { list } => {
                // Run the whole list again
                let last_test = list[list.len() - 1];
                debug!(len = list.len(), "Third run test");
                Box::new(run_test_list(list, options).flat_map(move |r| {
                    trace!("Third run test result");
                    match r {
                        RunTestListEvent::TestResult(res) => {
                            if res.data.name != last_test {
                                // Ignore if there is not the test we are testing
                                return Either::Right(stream::iter(None));
                            }
                            let is_failure = res.data.result.variant.is_failure();
                            // If this run failed again, start to bisect.
                            let entry = res.data.into();
                            if is_failure {
                                // Don't care if only a subset was run, we got the failure anyway
                                let new_job = JobEvent::NewJob(Job::Bisect {
                                    list: res.run_list.to_vec(),
                                    state: BisectState::Unknown,
                                });
                                Either::Left(stream::iter(vec![entry, new_job]))
                            } else {
                                if res.run_list != list {
                                    // We only ran a subset (a test in-between probably crashed)
                                    info!(
                                        last_failing_test = list[list.len() - res.run_list.len()],
                                        "Reproducing failure in third run failed because not the \
                                         whole test list was run, this can happen because of \
                                         intermediate failures"
                                    );
                                }

                                Either::Right(stream::iter(Some(entry)))
                            }
                        }
                        RunTestListEvent::DeqpError(e) => {
                            Either::Right(stream::iter(Some(e.into())))
                        }
                    }
                }))
            }
            Self::Bisect { list, state } => {
                let split_i = (list.len() - 1) / 2;
                let last_test = list[list.len() - 1];
                Box::new(r#gen!({
                    if list.len() <= 2 {
                        trace!("Bisect succeeded, two tests or less left");
                        return;
                    }

                    let test_list = match state {
                        BisectState::Unknown => {
                            // Test with last half
                            trace!("Bisect run with last half");
                            Cow::Borrowed(&list[split_i..])
                        }
                        BisectState::SucceedingWithLastHalf => {
                            // Test with first half
                            let mut tests = list[..split_i].to_vec();
                            tests.push(last_test);
                            trace!(?tests, "Bisect run with first half");
                            Cow::Owned(tests)
                        }
                    };

                    let mut test_list_stream = run_test_list(test_list.as_ref(), options);
                    while let Some(r) = test_list_stream.next().await {
                        trace!("Bisect run test result");
                        match r {
                            RunTestListEvent::TestResult(res) => {
                                if res.data.name != last_test {
                                    // Ignore if there is not the test we are testing
                                    continue;
                                }
                                let is_failure = res.data.result.variant.is_failure();
                                // If this run failed again, start to bisect.
                                yield_!(res.data.into());
                                if is_failure {
                                    // Don't care if only a subset was run, we got the failure
                                    // anyway
                                    let new_job = JobEvent::NewJob(Job::Bisect {
                                        list: res.run_list.to_vec(),
                                        state: BisectState::Unknown,
                                    });
                                    yield_!(new_job);
                                } else {
                                    if state == BisectState::SucceedingWithLastHalf {
                                        debug!("Unable to reproduce failure with either half");
                                    } else {
                                        // The error can be in the other half
                                        let new_job = JobEvent::NewJob(Job::Bisect {
                                            list: list.clone(),
                                            state: BisectState::SucceedingWithLastHalf,
                                        });
                                        yield_!(new_job);
                                    }
                                }
                            }
                            RunTestListEvent::DeqpError(e) => {
                                yield_!(e.into());
                            }
                        }
                    }
                }))
            }
        }
    }
}

/// Parses every line of the file as a test name.
///
/// Empty lines and lines starting with `#` will be skipped.
pub fn parse_test_file(content: &str) -> Vec<&str> {
    content
        .lines()
        .filter_map(|s| {
            let s = s.trim();
            if s.is_empty() || s.starts_with('#') {
                None
            } else {
                Some(s)
            }
        })
        .collect()
}

/// Shuffle the list while retaining order inside a batch.
pub fn shuffle_in_batches(tests: &mut [&str], batch_size: usize) {
    // Tests within a batch should be in the same order as before
    // Map test name to previous index
    let name_to_index = tests
        .iter()
        .enumerate()
        .map(|(i, n)| (*n, i))
        .collect::<HashMap<_, _>>();
    let mut rng = rng();
    tests.shuffle(&mut rng);
    for c in tests.chunks_mut(batch_size) {
        c.sort_by_key(|n| name_to_index.get(n).unwrap());
    }
}

/// Sort a list of tests into the order that deqp will run them in by running deqp with
/// `--deqp-runmode=stdout-caselist`.
///
/// Deqp walks a trie and filters out all tests that match the given test expressions. The result
/// list may be longer or shorter than the original list if *-expressions were used or names do not
/// exist.
pub async fn sort_with_deqp<S: AsRef<OsStr>>(
    args: &[S],
    tests: &[&str],
) -> Result<Vec<String>, DeqpSortError> {
    // Create a temporary file for the input test list
    let mut temp_file = NamedTempFile::new().map_err(DeqpSortError::TempFile)?;
    for t in tests {
        writeln!(&mut temp_file, "{t}").map_err(DeqpSortError::WriteFailed)?;
    }

    let mut args = args.iter().map(|s| s.as_ref()).collect::<Vec<_>>();
    args.push(temp_file.path().as_os_str());
    args.push("--deqp-runmode=stdout-caselist".as_ref());
    trace!(?args, "Run deqp for sorting");
    let mut cmd = Command::new(args[0]);
    cmd.args(&args[1..])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    let mut child = cmd.spawn().map_err(DeqpSortError::SpawnFailed)?;
    let mut stdout = BufReader::new(child.stdout.take().unwrap()).lines();

    // Continue reading stdout and stderr even when the process exited
    let mut stdout_finished = false;
    let mut res = Vec::new();
    let mut exit_code = None;
    loop {
        if stdout_finished && exit_code.is_some() {
            break;
        }
        tokio::select! {
            l = stdout.next_line(), if !stdout_finished => {
                let l = l.map_err(DeqpSortError::ReadFailed)?;
                if let Some(l) = l {
                    if let Some(t) = l.strip_prefix("TEST: ") {
                        res.push(t.to_string());
                    }
                } else {
                    stdout_finished = true;
                }
            }
            r = child.wait(), if exit_code.is_none() => {
                exit_code = Some(r.map_err(DeqpSortError::WaitFailed)?);
            }
        }
    }
    let exit_code = exit_code.unwrap();
    if !exit_code.success() {
        let r = child
            .wait_with_output()
            .await
            .map_err(DeqpSortError::WaitFailed)?;
        return Err(DeqpSortError::SortFailed(
            exit_code.code(),
            String::from_utf8_lossy(&r.stdout).into(),
            String::from_utf8_lossy(&r.stderr).into(),
        ));
    }
    Ok(res)
}

/// Run a list of tests and restart the process with remaining tests if it crashed.
///
/// Also record missing tests and the test lists when failures occured.
pub fn run_test_list<'a, 'list>(
    tests: &'list [&'a str],
    options: &'a RunOptions,
) -> impl Stream<Item = RunTestListEvent<'a, 'list>> + Send + Unpin {
    let mut state = RunTestListState::new(tests, options);

    r#gen!({
        loop {
            if state.tests.is_empty() {
                return;
            }
            if state.running.is_none() {
                let args = match state.start() {
                    Ok(r) => r,
                    Err(e) => {
                        // Cannot create test list, fatal error
                        state.tests = &[];
                        yield_!(RunTestListEvent::DeqpError(e));
                        return;
                    }
                };
                match run_deqp(options.timeout, &args, &[]) {
                    Ok(r) => state.running = Some(r),
                    Err(error) => {
                        yield_!(RunTestListEvent::DeqpError(DeqpErrorWithOutput {
                            error,
                            stdout: "Failed to start deqp process".into(),
                        }));
                        return;
                    }
                }
            }

            let running = state.running.as_mut().unwrap();
            match running.next().await {
                None => {
                    let running = state.running.take().unwrap();
                    for r in state.handle_finished(running) {
                        yield_!(r);
                    }
                }
                Some(e) => match e {
                    DeqpEvent::TestStart { name } => {
                        for r in state.handle_test_start(&name) {
                            yield_!(r);
                        }
                    }
                    DeqpEvent::TestEnd { result } => {
                        if let Some(r) = state.handle_test_end(result) {
                            yield_!(r);
                        }
                    }
                },
            }
        }
    })
}

fn print_progress(
    start: Instant,
    last_progress_print: &mut Instant,
    finished_tests: u64,
    total_tests: u64,
) {
    let now = Instant::now();
    if now.duration_since(*last_progress_print) > UPDATE_PROGRESS_INTERVAL {
        let eta_secs = now.duration_since(start).as_secs_f32() / finished_tests as f32
            * (total_tests - finished_tests) as f32;
        let eta = std::time::Duration::from_secs_f32(eta_secs);
        info!(finished_tests, total_tests, ?eta, "Progress update");
        *last_progress_print = now;
    }
}

pub async fn run_tests_parallel<'a>(
    tests: &'a [&'a str],
    // Map test names to summary entries
    summary: &mut Summary<'a>,
    options: &'a RunOptions,
    log_file: Option<&'a Path>,
    job_count: usize,
    progress_bar: &ProgressBar,
) {
    let mut pending_jobs: VecDeque<Job<'a>> = tests
        .chunks(options.batch_size)
        .map(|list| Job::FirstRun { list })
        .collect();

    // New jobs can be added to this list
    let mut job_executor = stream::FuturesUnordered::new();
    // The total number of jobs added to the executor
    let mut job_id: u64 = 0;
    let mut log_entry_id: u64 = 0;
    progress_bar.set_length(tests.len() as u64);
    // For logging progress when there is no progress bar
    let start_instant = Instant::now();
    let mut last_progress_print = Instant::now();

    let mut fails = 0;
    let mut crashes = 0;

    let mut log = if let Some(log_file) = log_file {
        match std::fs::File::create(log_file) {
            Ok(r) => Some(r),
            Err(error) => {
                error!(%error, "Failed to create log file");
                None
            }
        }
    } else {
        None
    };

    loop {
        if options.max_failures != 0
            && fails + crashes >= options.max_failures
            && !pending_jobs.is_empty()
        {
            warn!(
                max_failures = options.max_failures,
                failures = fails + crashes,
                "The number of failures is high, skip remaining jobs"
            );
            // Do not start new jobs when we have our max number of failures
            pending_jobs.clear();
        }

        while job_executor.len() < job_count {
            if let Some(job) = pending_jobs.pop_front() {
                let span = info_span!("job", job = job_id);
                job_id += 1;
                span.in_scope(|| debug!("Adding job to queue"));
                job_executor.push(job.run(options).into_future().instrument(span).boxed());
            } else {
                break;
            }
        }

        match job_executor.next().await {
            None => break,
            Some((None, _)) => {
                debug!("Job finished");
            }
            Some((Some(event), job_stream)) => {
                let mut fatal_error = false;
                match event {
                    JobEvent::RunLogEntry(mut entry) => {
                        match &mut entry {
                            RunLogEntry::TestResult(res) => {
                                res.id = log_entry_id;
                                log_entry_id += 1;
                                progress_bar.inc(1);
                                debug_assert_eq!(progress_bar.position(), log_entry_id);
                                if progress_bar.is_hidden() {
                                    print_progress(
                                        start_instant,
                                        &mut last_progress_print,
                                        progress_bar.position(),
                                        progress_bar.length().unwrap(),
                                    );
                                }
                                match summary.0.entry(res.data.name) {
                                    Entry::Occupied(mut entry) => {
                                        let old_id = entry.get().0.run_id;
                                        // Merge result variants
                                        let old = entry.get().0.result.clone();
                                        let new = res.data.result.variant.clone();
                                        let (result, take_new) = old.merge(new);
                                        entry.get_mut().0 = summary::SummaryEntry {
                                            name: Cow::Borrowed(res.data.name),
                                            result,
                                            run_id: if take_new { Some(res.id) } else { old_id },
                                        };
                                        if take_new {
                                            entry.get_mut().1 = Some(res.data.clone());
                                        }
                                    }
                                    Entry::Vacant(entry) => {
                                        if res.data.result.variant.is_failure() {
                                            if res.data.result.variant == TestResultType::Crash {
                                                crashes += 1;
                                            } else {
                                                fails += 1;
                                            }
                                            progress_bar.println(format!(
                                                "{}: {:?}",
                                                res.data.name, res.data.result.variant
                                            ));
                                            // Show fails and crashes on progress bar
                                            progress_bar.set_message(format!(
                                                "; fails: {fails}, crashes: {crashes}"
                                            ));
                                            progress_bar.tick();
                                            if progress_bar.is_hidden() {
                                                info!(test = res.data.name,
                                                    result = ?res.data.result.variant, "Test failed");
                                            }
                                        }
                                        entry.insert((
                                            summary::SummaryEntry {
                                                name: Cow::Borrowed(res.data.name),
                                                result: res.data.result.variant.clone(),
                                                run_id: Some(res.id),
                                            },
                                            Some(res.data.clone()),
                                        ));
                                    }
                                }
                            }
                            RunLogEntry::DeqpError(e) => {
                                if e.error.is_fatal() {
                                    fatal_error = true;
                                }
                            }
                        }

                        if let Some(f) = &mut log {
                            if let Err(error) = serde_json::to_writer(&mut *f, &entry) {
                                error!(%error, ?entry, "Failed to write entry into log file");
                            }
                            if let Err(error) = f.write_all(b"\n") {
                                error!(%error, "Failed to write into log file");
                            }
                        } else {
                            trace!(?entry, "Log");
                        }
                    }
                    JobEvent::NewJob(job) => {
                        let mut bisect_finished = false;
                        if let Job::Bisect { list, .. } = &job
                            && list.len() <= 2
                        {
                            trace!("Bisect succeeded, two tests or less left");
                            bisect_finished = true;
                        }
                        if !bisect_finished {
                            // all new jobs return exactly one test result
                            assert!(!matches!(job, Job::FirstRun { .. }), "Unexpected FirstRun");
                            pending_jobs.push_back(job);
                            progress_bar.inc_length(1);
                            if progress_bar.is_hidden() {
                                print_progress(
                                    start_instant,
                                    &mut last_progress_print,
                                    progress_bar.position(),
                                    progress_bar.length().unwrap(),
                                );
                            }
                        }
                    }
                }

                if fatal_error {
                    info!("A fatal error occured, aborting all pending jobs");
                    progress_bar.finish_and_clear();
                    pending_jobs.clear();
                    job_executor = stream::FuturesUnordered::new();
                } else {
                    job_executor.push(job_stream.into_future().boxed());
                }
            }
        }
    }

    debug_assert!(progress_bar.position() == progress_bar.length().unwrap());
    progress_bar.finish_and_clear();
}

#[cfg(test)]
mod tests {
    use anyhow::Result;
    use once_cell::sync::Lazy;

    use super::*;

    pub(crate) static TRACING: Lazy<()> =
        Lazy::new(|| tracing_subscriber::fmt().with_test_writer().init());

    async fn check_tests(args: &[&str], expected: &[(&str, TestResultType)]) -> Result<()> {
        check_tests_with_summary(args, expected, |_| {}).await
    }

    async fn check_tests_with_summary<F: for<'a> FnOnce(Summary<'a>)>(
        args: &[&str],
        expected: &[(&str, TestResultType)],
        check: F,
    ) -> Result<()> {
        Lazy::force(&TRACING);
        // Read test file
        let test_file = tokio::fs::read_to_string("logs/in").await?;
        let tests = parse_test_file(&test_file);
        assert_eq!(tests.len(), 18, "Test size does not match");

        check_tests_intern(args, expected, check, &tests, true, BATCH_SIZE).await
    }

    async fn check_tests_intern<F: for<'a> FnOnce(Summary<'a>)>(
        args: &[&str],
        expected: &[(&str, TestResultType)],
        check: F,
        tests: &[&str],
        retry: bool,
        batch_size: usize,
    ) -> Result<()> {
        let run_options = RunOptions {
            args: args.iter().map(|s| s.to_string()).collect(),
            capture_dumps: true,
            timeout: std::time::Duration::from_secs(2),
            max_failures: 0,
            fail_dir: None,
            retry,
            batch_size,
        };

        let mut summary = Summary::default();
        let pb = ProgressBar::hidden();
        run_tests_parallel(
            tests,
            &mut summary,
            &run_options,
            None,
            1, // Run only one job in parallel, to get deterministic behavior
            &pb,
        )
        .await;

        assert_eq!(
            summary.0.len(),
            expected.len(),
            "Result length does not match"
        );
        for (t, r) in expected {
            if let Some(r2) = summary.0.get(t) {
                assert_eq!(r2.0.result, *r, "Test result does not match for test {t}");
            } else {
                panic!("Test {} has no result but expected {:?}", t, r);
            }
        }

        check(summary);

        Ok(())
    }

    #[tokio::test]
    async fn test_a() -> Result<()> {
        let expected = vec![
            (
                "dEQP-VK.tessellation.primitive_discard.triangles_equal_spacing_ccw",
                TestResultType::Pass,
            ),
            (
                "dEQP-VK.tessellation.primitive_discard.triangles_equal_spacing_ccw_point_mode",
                TestResultType::Pass,
            ),
            (
                "dEQP-VK.tessellation.primitive_discard.triangles_equal_spacing_cw",
                TestResultType::Pass,
            ),
            (
                "dEQP-VK.tessellation.primitive_discard.triangles_equal_spacing_cw_point_mode",
                TestResultType::Pass,
            ),
            (
                "dEQP-VK.tessellation.primitive_discard.\
                 triangles_fractional_odd_spacing_ccw_valid_levels",
                TestResultType::Pass,
            ),
            (
                "dEQP-VK.tessellation.primitive_discard.triangles_fractional_odd_spacing_ccw",
                TestResultType::Pass,
            ),
            (
                "dEQP-VK.tessellation.primitive_discard.\
                 triangles_fractional_odd_spacing_ccw_point_mode_valid_levels",
                TestResultType::Pass,
            ),
            (
                "dEQP-VK.tessellation.primitive_discard.\
                 triangles_fractional_odd_spacing_ccw_point_mode",
                TestResultType::Pass,
            ),
            (
                "dEQP-VK.tessellation.primitive_discard.\
                 triangles_fractional_odd_spacing_cw_valid_levels",
                TestResultType::Pass,
            ),
            (
                "dEQP-VK.tessellation.primitive_discard.triangles_fractional_odd_spacing_cw",
                TestResultType::Pass,
            ),
            (
                "dEQP-VK.tessellation.primitive_discard.\
                 triangles_fractional_odd_spacing_cw_point_mode_valid_levels",
                TestResultType::Pass,
            ),
            (
                "dEQP-VK.tessellation.primitive_discard.\
                 triangles_fractional_odd_spacing_cw_point_mode",
                TestResultType::Pass,
            ),
            (
                "dEQP-VK.tessellation.primitive_discard.triangles_fractional_even_spacing_ccw",
                TestResultType::Pass,
            ),
            (
                "dEQP-VK.tessellation.primitive_discard.\
                 triangles_fractional_even_spacing_ccw_point_mode",
                TestResultType::Pass,
            ),
            (
                "dEQP-VK.tessellation.primitive_discard.triangles_fractional_even_spacing_cw",
                TestResultType::Pass,
            ),
            (
                "dEQP-VK.tessellation.primitive_discard.\
                 triangles_fractional_even_spacing_cw_point_mode",
                TestResultType::Pass,
            ),
            (
                "dEQP-VK.fragment_shader_interlock.basic.discard.ssbo.shading_rate_unordered.4xaa.\
                 sample_shading.512x512",
                TestResultType::NotSupported,
            ),
            (
                "dEQP-VK.fragment_shader_interlock.basic.discard.ssbo.shading_rate_unordered.4xaa.\
                 sample_shading.1024x1024",
                TestResultType::NotSupported,
            ),
        ];

        check_tests(
            &["test/test-runner.sh", "logs/a", "/dev/null", "0"],
            &expected,
        )
        .await?;

        check_tests(
            &["test/test-runner.sh", "logs/c", "logs/c-err", "1"],
            &expected,
        )
        .await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_b() -> Result<()> {
        check_tests(&["test/test-runner.sh", "logs/b", "logs/b-err", "1"], &[]).await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_d() -> Result<()> {
        let expected = vec![
            (
                "dEQP-VK.tessellation.primitive_discard.triangles_equal_spacing_ccw",
                TestResultType::Pass,
            ),
            (
                "dEQP-VK.tessellation.primitive_discard.triangles_equal_spacing_ccw_point_mode",
                TestResultType::Pass,
            ),
            (
                "dEQP-VK.tessellation.primitive_discard.triangles_equal_spacing_cw",
                TestResultType::Pass,
            ),
            (
                "dEQP-VK.tessellation.primitive_discard.triangles_equal_spacing_cw_point_mode",
                TestResultType::Pass,
            ),
            (
                "dEQP-VK.tessellation.primitive_discard.\
                 triangles_fractional_odd_spacing_ccw_valid_levels",
                TestResultType::Pass,
            ),
            (
                "dEQP-VK.tessellation.primitive_discard.triangles_fractional_odd_spacing_ccw",
                TestResultType::Pass,
            ),
            (
                "dEQP-VK.tessellation.primitive_discard.\
                 triangles_fractional_odd_spacing_ccw_point_mode_valid_levels",
                TestResultType::Pass,
            ),
            (
                "dEQP-VK.tessellation.primitive_discard.\
                 triangles_fractional_odd_spacing_ccw_point_mode",
                TestResultType::Pass,
            ),
            (
                "dEQP-VK.tessellation.primitive_discard.\
                 triangles_fractional_odd_spacing_cw_valid_levels",
                TestResultType::Pass,
            ),
            (
                "dEQP-VK.tessellation.primitive_discard.triangles_fractional_odd_spacing_cw",
                TestResultType::Pass,
            ),
            (
                "dEQP-VK.tessellation.primitive_discard.\
                 triangles_fractional_odd_spacing_cw_point_mode_valid_levels",
                TestResultType::Pass,
            ),
            (
                "dEQP-VK.tessellation.primitive_discard.\
                 triangles_fractional_odd_spacing_cw_point_mode",
                TestResultType::Pass,
            ),
            (
                "dEQP-VK.tessellation.primitive_discard.triangles_fractional_even_spacing_ccw",
                TestResultType::Pass,
            ),
            (
                "dEQP-VK.tessellation.primitive_discard.\
                 triangles_fractional_even_spacing_ccw_point_mode",
                TestResultType::Pass,
            ),
            (
                "dEQP-VK.tessellation.primitive_discard.triangles_fractional_even_spacing_cw",
                TestResultType::Pass,
            ),
            (
                "dEQP-VK.tessellation.primitive_discard.\
                 triangles_fractional_even_spacing_cw_point_mode",
                TestResultType::Crash,
            ),
            (
                "dEQP-VK.fragment_shader_interlock.basic.discard.ssbo.shading_rate_unordered.4xaa.\
                 sample_shading.512x512",
                TestResultType::Missing,
            ),
            (
                "dEQP-VK.fragment_shader_interlock.basic.discard.ssbo.shading_rate_unordered.4xaa.\
                 sample_shading.1024x1024",
                TestResultType::Missing,
            ),
        ];

        check_tests(
            &["test/test-runner.sh", "logs/d", "/dev/null", "0"],
            &expected,
        )
        .await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_d_fatal() -> Result<()> {
        let expected = vec![
            (
                "dEQP-VK.tessellation.primitive_discard.triangles_equal_spacing_ccw",
                TestResultType::Pass,
            ),
            (
                "dEQP-VK.tessellation.primitive_discard.triangles_equal_spacing_ccw_point_mode",
                TestResultType::Pass,
            ),
            (
                "dEQP-VK.tessellation.primitive_discard.triangles_equal_spacing_cw",
                TestResultType::Pass,
            ),
            (
                "dEQP-VK.tessellation.primitive_discard.triangles_equal_spacing_cw_point_mode",
                TestResultType::Pass,
            ),
            (
                "dEQP-VK.tessellation.primitive_discard.\
                 triangles_fractional_odd_spacing_ccw_valid_levels",
                TestResultType::Pass,
            ),
            (
                "dEQP-VK.tessellation.primitive_discard.triangles_fractional_odd_spacing_ccw",
                TestResultType::Pass,
            ),
            (
                "dEQP-VK.tessellation.primitive_discard.\
                 triangles_fractional_odd_spacing_ccw_point_mode_valid_levels",
                TestResultType::Pass,
            ),
            (
                "dEQP-VK.tessellation.primitive_discard.\
                 triangles_fractional_odd_spacing_ccw_point_mode",
                TestResultType::Pass,
            ),
            (
                "dEQP-VK.tessellation.primitive_discard.\
                 triangles_fractional_odd_spacing_cw_valid_levels",
                TestResultType::Pass,
            ),
            (
                "dEQP-VK.tessellation.primitive_discard.triangles_fractional_odd_spacing_cw",
                TestResultType::Pass,
            ),
            (
                "dEQP-VK.tessellation.primitive_discard.\
                 triangles_fractional_odd_spacing_cw_point_mode_valid_levels",
                TestResultType::Pass,
            ),
            (
                "dEQP-VK.tessellation.primitive_discard.\
                 triangles_fractional_odd_spacing_cw_point_mode",
                TestResultType::Pass,
            ),
            (
                "dEQP-VK.tessellation.primitive_discard.triangles_fractional_even_spacing_ccw",
                TestResultType::Pass,
            ),
            (
                "dEQP-VK.tessellation.primitive_discard.\
                 triangles_fractional_even_spacing_ccw_point_mode",
                TestResultType::Pass,
            ),
            (
                "dEQP-VK.tessellation.primitive_discard.triangles_fractional_even_spacing_cw",
                TestResultType::Pass,
            ),
            (
                "dEQP-VK.tessellation.primitive_discard.\
                 triangles_fractional_even_spacing_cw_point_mode",
                TestResultType::Crash,
            ),
        ];

        // TODO Check that we retest after a fatal error
        check_tests(
            &["test/test-runner.sh", "logs/d", "logs/d-err", "1"],
            &expected,
        )
        .await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_timeout() -> Result<()> {
        let expected = vec![
            (
                "dEQP-VK.tessellation.primitive_discard.triangles_equal_spacing_ccw",
                TestResultType::Pass,
            ),
            (
                "dEQP-VK.tessellation.primitive_discard.triangles_equal_spacing_ccw_point_mode",
                TestResultType::Pass,
            ),
            (
                "dEQP-VK.tessellation.primitive_discard.triangles_equal_spacing_cw",
                TestResultType::Pass,
            ),
            (
                "dEQP-VK.tessellation.primitive_discard.triangles_equal_spacing_cw_point_mode",
                TestResultType::Pass,
            ),
            (
                "dEQP-VK.tessellation.primitive_discard.\
                 triangles_fractional_odd_spacing_ccw_valid_levels",
                TestResultType::Pass,
            ),
            (
                "dEQP-VK.tessellation.primitive_discard.triangles_fractional_odd_spacing_ccw",
                TestResultType::Pass,
            ),
            (
                "dEQP-VK.tessellation.primitive_discard.\
                 triangles_fractional_odd_spacing_ccw_point_mode_valid_levels",
                TestResultType::Pass,
            ),
            (
                "dEQP-VK.tessellation.primitive_discard.\
                 triangles_fractional_odd_spacing_ccw_point_mode",
                TestResultType::Pass,
            ),
            (
                "dEQP-VK.tessellation.primitive_discard.\
                 triangles_fractional_odd_spacing_cw_valid_levels",
                TestResultType::Pass,
            ),
            (
                "dEQP-VK.tessellation.primitive_discard.triangles_fractional_odd_spacing_cw",
                TestResultType::Pass,
            ),
            (
                "dEQP-VK.tessellation.primitive_discard.\
                 triangles_fractional_odd_spacing_cw_point_mode_valid_levels",
                TestResultType::Pass,
            ),
            (
                "dEQP-VK.tessellation.primitive_discard.\
                 triangles_fractional_odd_spacing_cw_point_mode",
                TestResultType::Pass,
            ),
            (
                "dEQP-VK.tessellation.primitive_discard.triangles_fractional_even_spacing_ccw",
                TestResultType::Pass,
            ),
            (
                "dEQP-VK.tessellation.primitive_discard.\
                 triangles_fractional_even_spacing_ccw_point_mode",
                TestResultType::Pass,
            ),
            (
                "dEQP-VK.tessellation.primitive_discard.triangles_fractional_even_spacing_cw",
                TestResultType::Pass,
            ),
            (
                "dEQP-VK.tessellation.primitive_discard.\
                 triangles_fractional_even_spacing_cw_point_mode",
                TestResultType::Timeout,
            ),
        ];

        check_tests(
            &["test/test-timeout.sh", "logs/d", "/dev/null", "1"],
            &expected,
        )
        .await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_timeout_fatal_error() -> Result<()> {
        let expected = vec![
            (
                "dEQP-VK.tessellation.primitive_discard.triangles_equal_spacing_ccw",
                TestResultType::Pass,
            ),
            (
                "dEQP-VK.tessellation.primitive_discard.triangles_equal_spacing_ccw_point_mode",
                TestResultType::Pass,
            ),
            (
                "dEQP-VK.tessellation.primitive_discard.triangles_equal_spacing_cw",
                TestResultType::Pass,
            ),
            (
                "dEQP-VK.tessellation.primitive_discard.triangles_equal_spacing_cw_point_mode",
                TestResultType::Pass,
            ),
            (
                "dEQP-VK.tessellation.primitive_discard.\
                 triangles_fractional_odd_spacing_ccw_valid_levels",
                TestResultType::Pass,
            ),
            (
                "dEQP-VK.tessellation.primitive_discard.triangles_fractional_odd_spacing_ccw",
                TestResultType::Pass,
            ),
            (
                "dEQP-VK.tessellation.primitive_discard.\
                 triangles_fractional_odd_spacing_ccw_point_mode_valid_levels",
                TestResultType::Pass,
            ),
            (
                "dEQP-VK.tessellation.primitive_discard.\
                 triangles_fractional_odd_spacing_ccw_point_mode",
                TestResultType::Pass,
            ),
            (
                "dEQP-VK.tessellation.primitive_discard.\
                 triangles_fractional_odd_spacing_cw_valid_levels",
                TestResultType::Pass,
            ),
            (
                "dEQP-VK.tessellation.primitive_discard.triangles_fractional_odd_spacing_cw",
                TestResultType::Pass,
            ),
            (
                "dEQP-VK.tessellation.primitive_discard.\
                 triangles_fractional_odd_spacing_cw_point_mode_valid_levels",
                TestResultType::Pass,
            ),
            (
                "dEQP-VK.tessellation.primitive_discard.\
                 triangles_fractional_odd_spacing_cw_point_mode",
                TestResultType::Pass,
            ),
            (
                "dEQP-VK.tessellation.primitive_discard.triangles_fractional_even_spacing_ccw",
                TestResultType::Pass,
            ),
            (
                "dEQP-VK.tessellation.primitive_discard.\
                 triangles_fractional_even_spacing_ccw_point_mode",
                TestResultType::Pass,
            ),
            (
                "dEQP-VK.tessellation.primitive_discard.triangles_fractional_even_spacing_cw",
                TestResultType::Pass,
            ),
            (
                "dEQP-VK.tessellation.primitive_discard.\
                 triangles_fractional_even_spacing_cw_point_mode",
                TestResultType::Timeout,
            ),
        ];

        check_tests_with_summary(
            &["test/test-timeout.sh", "logs/d", "logs/d-err", "1"],
            &expected,
            |summary| {
                let res = summary
                    .0
                    .get(
                        "dEQP-VK.tessellation.primitive_discard.\
                         triangles_fractional_even_spacing_cw_point_mode",
                    )
                    .unwrap();
                assert_eq!(res.0.run_id, Some(15));
                assert!(res.1.as_ref().unwrap().pid.is_some());
            },
        )
        .await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_bisect() -> Result<()> {
        let expected = vec![
            (
                "dEQP-VK.tessellation.primitive_discard.triangles_equal_spacing_ccw",
                TestResultType::Pass,
            ),
            (
                "dEQP-VK.tessellation.primitive_discard.triangles_equal_spacing_ccw_point_mode",
                TestResultType::Pass,
            ),
            (
                "dEQP-VK.tessellation.primitive_discard.triangles_equal_spacing_cw",
                TestResultType::Pass,
            ),
            (
                "dEQP-VK.tessellation.primitive_discard.triangles_equal_spacing_cw_point_mode",
                TestResultType::Pass,
            ),
            (
                "dEQP-VK.tessellation.primitive_discard.\
                 triangles_fractional_odd_spacing_ccw_valid_levels",
                TestResultType::Pass,
            ),
            (
                "dEQP-VK.tessellation.primitive_discard.triangles_fractional_odd_spacing_ccw",
                TestResultType::Pass,
            ),
            (
                "dEQP-VK.tessellation.primitive_discard.\
                 triangles_fractional_odd_spacing_ccw_point_mode_valid_levels",
                TestResultType::Pass,
            ),
            (
                "dEQP-VK.tessellation.primitive_discard.\
                 triangles_fractional_odd_spacing_ccw_point_mode",
                TestResultType::Pass,
            ),
            (
                "dEQP-VK.tessellation.primitive_discard.\
                 triangles_fractional_odd_spacing_cw_valid_levels",
                TestResultType::Pass,
            ),
            (
                "dEQP-VK.tessellation.primitive_discard.triangles_fractional_odd_spacing_cw",
                TestResultType::Pass,
            ),
            (
                "dEQP-VK.tessellation.primitive_discard.\
                 triangles_fractional_odd_spacing_cw_point_mode_valid_levels",
                TestResultType::Pass,
            ),
            (
                "dEQP-VK.tessellation.primitive_discard.\
                 triangles_fractional_odd_spacing_cw_point_mode",
                TestResultType::Pass,
            ),
            (
                "dEQP-VK.tessellation.primitive_discard.triangles_fractional_even_spacing_ccw",
                TestResultType::Pass,
            ),
            (
                "dEQP-VK.tessellation.primitive_discard.\
                 triangles_fractional_even_spacing_ccw_point_mode",
                TestResultType::Pass,
            ),
            (
                "dEQP-VK.tessellation.primitive_discard.triangles_fractional_even_spacing_cw",
                TestResultType::Pass,
            ),
            (
                "dEQP-VK.tessellation.primitive_discard.\
                 triangles_fractional_even_spacing_cw_point_mode",
                TestResultType::Crash,
            ),
            (
                "dEQP-VK.fragment_shader_interlock.basic.discard.ssbo.shading_rate_unordered.4xaa.\
                 sample_shading.512x512",
                TestResultType::NotSupported,
            ),
            (
                "dEQP-VK.fragment_shader_interlock.basic.discard.ssbo.shading_rate_unordered.4xaa.\
                 sample_shading.1024x1024",
                TestResultType::NotSupported,
            ),
        ];

        check_tests_with_summary(
            &[
                "test/bisect-test-runner.sh",
                "dEQP-VK.tessellation.primitive_discard.\
                 triangles_fractional_odd_spacing_ccw_valid_levels",
                "logs/d",
                "/dev/null",
                "1",
                "logs/a",
                "dev/null",
                "0",
            ],
            &expected,
            |summary| {
                // TODO Check bisection result with get_test_results returned by check_tests
                let res = summary
                    .0
                    .get(
                        "dEQP-VK.tessellation.primitive_discard.\
                         triangles_fractional_even_spacing_cw_point_mode",
                    )
                    .unwrap();
                assert_eq!(res.1.as_ref().unwrap().fail_dir, None);
                assert_eq!(res.0.run_id, Some(25));
            },
        )
        .await?;

        Ok(())
    }

    fn test_sort_list(batch_size: usize) -> Vec<(String, TestResultType)> {
        let mut expected = Vec::new();
        for i in 0..(batch_size * 5 - batch_size / 3) {
            expected.push((i.to_string(), TestResultType::Pass));
        }
        let mut rng = rng();
        expected.shuffle(&mut rng);
        expected
    }

    #[tokio::test]
    #[should_panic(expected = "Test result does not match for test")]
    async fn test_sort_no_shuffle_no_sort() {
        Lazy::force(&TRACING);
        let batch_size = 10;
        let expected = test_sort_list(batch_size);
        let expected = expected
            .iter()
            .map(|(s, r)| (s.as_str(), r.clone()))
            .collect::<Vec<_>>();

        let tests = expected.iter().map(|e| e.0).collect::<Vec<_>>();
        let _ = check_tests_intern(
            &["test/test-sorted.sh"],
            &expected,
            |_| {},
            &tests,
            true,
            batch_size,
        )
        .await;
    }

    #[tokio::test]
    async fn test_sort_no_shuffle_sort() -> Result<()> {
        Lazy::force(&TRACING);
        let batch_size = 10;
        let expected = test_sort_list(batch_size);
        let expected = expected
            .iter()
            .map(|(s, r)| (s.as_str(), r.clone()))
            .collect::<Vec<_>>();

        let tests = expected.iter().map(|e| e.0).collect::<Vec<_>>();
        let sorted_list = sort_with_deqp(&["test/test-sorted.sh"], &tests).await?;
        let sorted_tests = sorted_list.iter().map(|t| t.as_str()).collect::<Vec<_>>();
        check_tests_intern(
            &["test/test-sorted.sh"],
            &expected,
            |_| {},
            &sorted_tests,
            true,
            batch_size,
        )
        .await?;

        Ok(())
    }

    #[tokio::test]
    #[should_panic(expected = "Test result does not match for test")]
    async fn test_sort_shuffle_no_sort() {
        Lazy::force(&TRACING);
        let batch_size = 10;
        let expected = test_sort_list(batch_size);
        let expected = expected
            .iter()
            .map(|(s, r)| (s.as_str(), r.clone()))
            .collect::<Vec<_>>();

        let mut tests = expected.iter().map(|e| e.0).collect::<Vec<_>>();
        shuffle_in_batches(&mut tests, batch_size);
        let _ = check_tests_intern(
            &["test/test-sorted.sh"],
            &expected,
            |_| {},
            &tests,
            true,
            batch_size,
        )
        .await;
    }

    #[tokio::test]
    async fn test_sort_shuffle_sort() -> Result<()> {
        Lazy::force(&TRACING);
        let batch_size = 10;
        let expected = test_sort_list(batch_size);
        let expected = expected
            .iter()
            .map(|(s, r)| (s.as_str(), r.clone()))
            .collect::<Vec<_>>();

        let tests = expected.iter().map(|e| e.0).collect::<Vec<_>>();
        let sorted_list = sort_with_deqp(&["test/test-sorted.sh"], &tests).await?;
        let mut sorted_tests = sorted_list.iter().map(|t| t.as_str()).collect::<Vec<_>>();
        shuffle_in_batches(&mut sorted_tests, batch_size);
        check_tests_intern(
            &["test/test-sorted.sh"],
            &expected,
            |_| {},
            &sorted_tests,
            true,
            batch_size,
        )
        .await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_no_retry() -> Result<()> {
        Lazy::force(&TRACING);
        let test_file = tokio::fs::read_to_string("logs/in").await?;
        let tests = parse_test_file(&test_file);
        assert_eq!(tests.len(), 18, "Test size does not match");

        let expected = vec![
            (
                "dEQP-VK.tessellation.primitive_discard.triangles_equal_spacing_ccw",
                TestResultType::Pass,
            ),
            (
                "dEQP-VK.tessellation.primitive_discard.triangles_equal_spacing_ccw_point_mode",
                TestResultType::Pass,
            ),
            (
                "dEQP-VK.tessellation.primitive_discard.triangles_equal_spacing_cw",
                TestResultType::Pass,
            ),
            (
                "dEQP-VK.tessellation.primitive_discard.triangles_equal_spacing_cw_point_mode",
                TestResultType::Pass,
            ),
            (
                "dEQP-VK.tessellation.primitive_discard.\
                 triangles_fractional_odd_spacing_ccw_valid_levels",
                TestResultType::Pass,
            ),
            (
                "dEQP-VK.tessellation.primitive_discard.triangles_fractional_odd_spacing_ccw",
                TestResultType::Pass,
            ),
            (
                "dEQP-VK.tessellation.primitive_discard.\
                 triangles_fractional_odd_spacing_ccw_point_mode_valid_levels",
                TestResultType::Pass,
            ),
            (
                "dEQP-VK.tessellation.primitive_discard.\
                 triangles_fractional_odd_spacing_ccw_point_mode",
                TestResultType::Pass,
            ),
            (
                "dEQP-VK.tessellation.primitive_discard.\
                 triangles_fractional_odd_spacing_cw_valid_levels",
                TestResultType::Pass,
            ),
            (
                "dEQP-VK.tessellation.primitive_discard.triangles_fractional_odd_spacing_cw",
                TestResultType::Pass,
            ),
            (
                "dEQP-VK.tessellation.primitive_discard.\
                 triangles_fractional_odd_spacing_cw_point_mode_valid_levels",
                TestResultType::Pass,
            ),
            (
                "dEQP-VK.tessellation.primitive_discard.\
                 triangles_fractional_odd_spacing_cw_point_mode",
                TestResultType::Pass,
            ),
            (
                "dEQP-VK.tessellation.primitive_discard.triangles_fractional_even_spacing_ccw",
                TestResultType::Pass,
            ),
            (
                "dEQP-VK.tessellation.primitive_discard.\
                 triangles_fractional_even_spacing_ccw_point_mode",
                TestResultType::Pass,
            ),
            (
                "dEQP-VK.tessellation.primitive_discard.triangles_fractional_even_spacing_cw",
                TestResultType::Pass,
            ),
            (
                "dEQP-VK.tessellation.primitive_discard.\
                 triangles_fractional_even_spacing_cw_point_mode",
                TestResultType::Crash,
            ),
            (
                "dEQP-VK.fragment_shader_interlock.basic.discard.ssbo.shading_rate_unordered.4xaa.\
                 sample_shading.512x512",
                TestResultType::Missing,
            ),
            (
                "dEQP-VK.fragment_shader_interlock.basic.discard.ssbo.shading_rate_unordered.4xaa.\
                 sample_shading.1024x1024",
                TestResultType::Missing,
            ),
        ];

        check_tests_intern(
            &["test/test-runner.sh", "logs/d", "/dev/null", "0"],
            &expected,
            |summary| {
                let res = summary
                    .0
                    .get(
                        "dEQP-VK.tessellation.primitive_discard.\
                         triangles_fractional_even_spacing_cw_point_mode",
                    )
                    .unwrap();
                assert_eq!(res.1.as_ref().unwrap().fail_dir, None);
                assert_eq!(res.0.run_id, Some(15));
            },
            &tests,
            false,
            BATCH_SIZE,
        )
        .await?;

        Ok(())
    }
}
