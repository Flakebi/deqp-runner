use std::borrow::Cow;
use std::collections::hash_map::Entry;
use std::collections::{HashMap, VecDeque};
use std::ffi::OsStr;
use std::io::Write;
use std::mem;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::process::Stdio;
use std::task::{Context, Poll};

use futures::future::Either;
use futures::prelude::*;
use genawaiter::sync::gen;
use genawaiter::yield_;
use indicatif::ProgressBar;
use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};
use slog::{debug, error, info, o, trace, warn, Logger};
use tempfile::NamedTempFile;
use thiserror::Error;
use time::{Duration, OffsetDateTime};
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::prelude::*;
use tokio::process::{Child, ChildStderr, ChildStdout, Command};
use tokio::time::Sleep;

pub mod slog_pg;
pub mod summary;

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

static RESULT_VARIANTS: Lazy<HashMap<&str, TestResultType>> = Lazy::new(|| {
    let mut result_variants = HashMap::new();
    result_variants.insert("Pass", TestResultType::Pass);
    result_variants.insert("CompatibilityWarning", TestResultType::CompatibilityWarning);
    result_variants.insert("QualityWarning", TestResultType::QualityWarning);
    result_variants.insert("NotSupported", TestResultType::NotSupported);
    result_variants.insert("Fail", TestResultType::Fail);
    result_variants.insert("InternalError", TestResultType::InternalError);
    result_variants
});

pub static PROGRESS_BAR: Lazy<ProgressBar> = Lazy::new(|| {
    let bar = ProgressBar::new(1);
    bar.set_style(
        indicatif::ProgressStyle::default_bar().template("{wide_bar} job {pos}/{len}{msg} ({eta})"),
    );
    bar.enable_steady_tick(1000);
    bar
});

#[derive(Clone, Debug)]
#[cfg_attr(feature = "bin", derive(clap::Clap))]
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
    #[cfg_attr(feature = "bin", clap(long, default_value = "300"))]
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

/// Different results from running a deqp test.
#[derive(Clone, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
pub enum TestResultType {
    Pass,
    /// Counts as success
    CompatibilityWarning,
    /// Counts as success
    QualityWarning,
    /// Counts as success
    NotSupported,
    Fail,
    InternalError,
    Crash,
    Timeout,
    /// Test should be executed but was not found in the output.
    Missing,
    /// The test was not executed because a fatal error happened before and aborted the whole run.
    NotRun,
    /// Failed one time but is not reproducible.
    Flake(Box<TestResultType>),
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct TestResult {
    pub stdout: String,
    pub variant: TestResultType,
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
}

/// Failure when running `deqp`.
#[derive(Debug, Deserialize, Error, Serialize)]
pub enum DeqpError {
    /// Fatal error
    #[error("Failed to spawn process: {0}")]
    SpawnFailed(
        #[source]
        #[serde(with = "serde_io_error")]
        std::io::Error,
    ),
    #[error("Failed to wait for process to end: {0}")]
    WaitFailed(
        #[source]
        #[serde(with = "serde_io_error")]
        std::io::Error,
    ),
    #[error("Failed to read output from process: {0}")]
    ReadFailed(
        #[source]
        #[serde(with = "serde_io_error")]
        std::io::Error,
    ),
    #[error("deqp encountered a fatal error")]
    DeqpFatalError,
    /// Fatal error
    #[error("deqp encountered a fatal error")]
    FatalError,
    #[error("deqp timed out")]
    Timeout,
    #[error("deqp exited with {exit_status:?}")]
    Crash { exit_status: Option<i32> },
}

#[derive(Debug, Deserialize, Serialize)]
pub struct DeqpErrorWithOutput {
    error: DeqpError,
    stdout: String,
}

#[derive(Debug)]
pub enum DeqpEvent {
    TestStart { name: String },
    TestEnd { result: TestResult },
}

#[derive(Clone, Debug)]
pub struct RunOptions {
    pub args: Vec<String>,
    pub capture_dumps: bool,
    pub timeout: std::time::Duration,
    pub max_failures: usize,
    /// Directory where failure dumps should be created.
    pub fail_dir: Option<PathBuf>,
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

pub struct RunDeqpState {
    logger: Logger,
    pub pid: u32,
    timeout_duration: std::time::Duration,
    timeout: Sleep,
    stdout_reader: io::Lines<BufReader<ChildStdout>>,
    stderr_reader: io::Lines<BufReader<ChildStderr>>,
    /// Buffer for stdout
    pub stdout: String,
    /// Buffer for stderr
    pub stderr: String,
    stdout_finished: bool,
    stderr_finished: bool,
    /// Process exited
    finished: bool,
    has_timeout: bool,
    tests_done: bool,
    /// deqp reported a fatal error on stderr
    has_fatal_error: bool,
    /// Process exit status
    pub finished_result: Option<Result<(), DeqpError>>,
    child: Option<tokio::process::Child>,
}

struct RunTestListState<'a, 'list> {
    logger: Logger,
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

mod serde_io_error {
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub fn serialize<S: Serializer>(e: &std::io::Error, ser: S) -> Result<S::Ok, S::Error> {
        e.to_string().serialize(ser)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(de: D) -> Result<std::io::Error, D::Error> {
        let s: String = Deserialize::deserialize(de)?;
        Ok(std::io::Error::new(std::io::ErrorKind::Other, s))
    }
}

impl DeqpError {
    pub fn is_fatal(&self) -> bool {
        matches!(self, Self::SpawnFailed(_) | Self::FatalError)
    }
}

impl TestResultType {
    /// If the test result is a failure and the test should be retested.
    pub fn is_failure(&self) -> bool {
        matches!(self, Self::Fail | Self::Crash | Self::Timeout | Self::Flake(_))
    }

    /// Merges the result with a new result and marks it as flake if necessary.
    ///
    /// The returned `bool` is `false` if the new result should be ignored because it is succeeding,
    /// while it was failing before.
    pub fn merge(self, other: Self) -> (Self, bool) {
        if self.is_failure() && !other.is_failure() {
            (Self::Flake(Box::new(self)), false)
        } else {
            (other, true)
        }
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

impl RunDeqpState {
    fn new(
        mut logger: Logger,
        timeout_duration: std::time::Duration,
        mut child: Child,
    ) -> Result<Self, DeqpError> {
        let pid = child.id().ok_or_else(|| {
            DeqpError::SpawnFailed(std::io::Error::new(
                std::io::ErrorKind::Other,
                "Failed to get child pid",
            ))
        })?;
        logger = logger.new(o!("pid" => pid));

        let stdout = child.stdout.take().unwrap();
        let stderr = child.stderr.take().unwrap();
        Ok(Self {
            logger,
            pid,
            timeout_duration,
            timeout: tokio::time::sleep(timeout_duration),
            stdout_reader: BufReader::new(stdout).lines(),
            stderr_reader: BufReader::new(stderr).lines(),
            stdout: String::new(),
            stderr: String::new(),
            stdout_finished: false,
            stderr_finished: false,
            finished: false,
            has_timeout: false,
            tests_done: false,
            has_fatal_error: false,
            finished_result: None,
            child: Some(child),
        })
    }

    fn handle_stdout_line(
        &mut self,
        l: Option<Result<String, std::io::Error>>,
    ) -> Option<DeqpEvent> {
        let l = match l {
            None => {
                self.stdout_finished = true;
                return None;
            }
            Some(Ok(r)) => r,
            Some(Err(e)) => {
                self.stdout_finished = true;
                debug!(self.logger, "Failed to read stdout of process"; "error" => %e);
                return None;
            }
        };
        if self.tests_done {
            if !l.is_empty() {
                self.stdout.push_str(&l);
                self.stdout.push('\n');
            }
            return None;
        }

        if let Some(l) = l.strip_prefix("  ") {
            for (s, res) in &*RESULT_VARIANTS {
                if let Some(l) = l.strip_prefix(s) {
                    let mut l = l.trim();
                    if l.starts_with('(') && l.ends_with(')') {
                        l = &l[1..l.len() - 1];
                    }
                    self.stdout.push_str(l);
                    self.timeout = tokio::time::sleep(self.timeout_duration);
                    return Some(DeqpEvent::TestEnd {
                        result: TestResult {
                            stdout: mem::replace(&mut self.stdout, String::new()),
                            variant: res.clone(),
                        },
                    });
                }
            }
        }

        if let Some(l) = l.strip_prefix("TEST: ") {
            return Some(DeqpEvent::TestStart {
                name: l.to_string(),
            });
        } else if let Some(l) = l.strip_prefix("Test case '") {
            if let Some(l) = l.strip_suffix("'..") {
                self.stdout.clear();
                return Some(DeqpEvent::TestStart { name: l.into() });
            } else {
                self.stdout.push_str(&l);
                self.stdout.push('\n');
            }
        } else if l == "DONE!" {
            self.tests_done = true;
        } else if l.is_empty() {
        } else {
            self.stdout.push_str(&l);
            self.stdout.push('\n');
        }
        None
    }

    fn handle_stderr_line(&mut self, l: Option<Result<String, std::io::Error>>) {
        let l = match l {
            None => {
                self.stderr_finished = true;
                return;
            }
            Some(Ok(r)) => r,
            Some(Err(e)) => {
                self.stderr_finished = true;
                debug!(self.logger, "Failed to read stderr of process"; "error" => %e);
                return;
            }
        };
        if l.contains("FATAL ERROR: ") {
            warn!(self.logger, "Deqp encountered fatal error"; "error" => &l);
            self.has_fatal_error = true;
        }
        self.stderr.push_str(&l);
        self.stderr.push('\n');
    }
}

impl<'a, 'list> RunTestListState<'a, 'list> {
    fn new(logger: Logger, tests: &'list [&'a str], options: &'a RunOptions) -> Self {
        RunTestListState {
            logger,
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
            error: DeqpError::FatalError,
            stdout: format!("Failed to create temporary file for test list: {}", e),
        })?;
        for t in self.tests {
            writeln!(&mut temp_file, "{}", t).map_err(|e| DeqpErrorWithOutput {
                error: DeqpError::FatalError,
                stdout: format!("Failed to write temporary file for test list: {}", e),
            })?;
        }

        let mut args = self.options.args.clone();
        args.push(
            temp_file
                .path()
                .as_os_str()
                .to_str()
                .ok_or_else(|| DeqpErrorWithOutput {
                    error: DeqpError::FatalError,
                    stdout: format!(
                        "Failed to get name of temporary file for test list (path: {:?})",
                        temp_file.path()
                    ),
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
        if self.fail_dir.is_none() {
            if let Some(dir) = &self.options.fail_dir {
                for i in 0.. {
                    let dir_name = if i == 0 {
                        failed_test.to_string()
                    } else {
                        format!("{}-{}", failed_test, i)
                    };
                    let new_dir = dir.join(&dir_name);
                    if !new_dir.exists() {
                        if let Err(e) = std::fs::create_dir_all(&new_dir) {
                            error!(self.logger, "Failed to create failure directory";
                                "error" => %e);
                            return;
                        }
                        self.fail_dir = Some(dir_name);
                        // Write reproduce-list.txt
                        match std::fs::File::create(new_dir.join(TEST_LIST_FILE)) {
                            Ok(mut f) => {
                                if let Err(e) = (|| -> Result<(), std::io::Error> {
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
                                        writeln!(&mut f, "{}", t)?;
                                    }
                                    Ok(())
                                })() {
                                    error!(self.logger, "Failed to write reproduce list";
                                        "error" => %e);
                                }
                            }
                            Err(e) => {
                                error!(self.logger, "Failed to create reproduce list file";
                                    "error" => %e);
                            }
                        }
                        break;
                    }
                }
            }
        }

        if let Some(running) = &mut self.running {
            if !running.stderr.is_empty() {
                // Save current stderr
                let stderr = mem::replace(&mut running.stderr, String::new());
                self.save_fail_dir_stderr(&stderr);
            }
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
                    if let Err(e) = f.write_all(stderr.as_bytes()) {
                        error!(self.logger, "Failed to write stderr file"; "error" => %e);
                    }
                }
                Err(e) => {
                    error!(self.logger, "Failed to create stderr file"; "error" => %e);
                }
            }
        } else {
            warn!(
                self.logger,
                "Tried to save stderr without a fail dir set, ignoring"
            );
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

    fn handle_test_start(&mut self, name: &str) -> Vec<RunTestListEvent<'a, 'list>> {
        trace!(self.logger, "Test started"; "test" => name);
        let next_test = self.last_finished.map(|i| i + 1).unwrap_or_default();
        if let Some(i) = (&self.tests[next_test..]).iter().position(|t| t == &name) {
            self.cur_test = Some((next_test + i, OffsetDateTime::now_utc()));
            self.get_missing(i)
        } else {
            warn!(self.logger, "Executing unknown test"; "test" => name);
            Vec::new()
        }
    }

    fn handle_test_end(&mut self, result: TestResult) -> Option<RunTestListEvent<'a, 'list>> {
        trace!(self.logger, "Test end"; "cur_test" => ?self.cur_test, "result" => ?result);
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
            warn!(self.logger, "Finished test without starting a test";
                "last-finished" => ?self.last_finished.map(|i| self.tests[i]));
            None
        }
    }

    fn handle_finished(&mut self, state: RunDeqpState) -> Vec<RunTestListEvent<'a, 'list>> {
        trace!(self.logger, "Finished"; "result" => ?state.finished_result,
            "stdout" => &state.stdout, "stderr" => &state.stderr);

        let mut is_failure = true;
        let res = if let Some(cur_test) = self.cur_test {
            let duration = OffsetDateTime::now_utc() - cur_test.1;
            self.create_fail_dir(self.tests[cur_test.0]);

            if state
                .finished_result
                .as_ref()
                .map(Result::is_ok)
                .unwrap_or_default()
            {
                warn!(self.logger, "test not finished but deqp exited successful, count as failure";
                    "cur_test" => self.tests[cur_test.0], "started" => %cur_test.1);
            }

            // Continue testing
            let run_list = &self.tests[..cur_test.0 + 1];
            self.tests = &self.tests[cur_test.0 + 1..];

            let mut res = vec![RunTestListEvent::TestResult(ReproducibleTestResultData {
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
            })];

            if let Some(Err(e)) = state.finished_result {
                if e.is_fatal() {
                    res.push(RunTestListEvent::DeqpError(DeqpErrorWithOutput {
                        error: e,
                        stdout: String::new(),
                    }));
                }
            }
            res
        } else if let Some(e) = state
            .finished_result
            .map(Result::err)
            .unwrap_or_else(|| Some(DeqpError::FatalError))
        {
            if let Some(last_finished) = self.last_finished {
                self.create_fail_dir(self.tests[last_finished]);
                // No current test executed, so probably some tests are failing, therefore the exit
                // status is not 0
                // Mark rest of tests as missing
                let mut r = self.get_missing(self.tests.len() - last_finished - 1);
                self.tests = &[];
                if e.is_fatal() {
                    r.push(RunTestListEvent::DeqpError(DeqpErrorWithOutput {
                        error: e,
                        stdout: state.stdout,
                    }));
                }
                r
            } else {
                self.create_fail_dir(UNKNOWN_TEST_NAME);
                // No test executed, counts as fatal error
                self.tests = &[];
                warn!(self.logger, "Deqp exited without running tests, aborting"; "error" => ?e);
                vec![RunTestListEvent::DeqpError(DeqpErrorWithOutput {
                    error: DeqpError::FatalError,
                    stdout: state.stdout,
                })]
            }
        } else {
            is_failure = false;
            let r = if let Some(last_finished) = self.last_finished {
                // Mark rest of tests as missing
                debug!(self.logger, "not all missing"; "tests" => ?self.tests,
                    "last" => last_finished);
                self.get_missing(self.tests.len() - last_finished - 1)
            } else {
                // No test executed, counts all tests as missing
                debug!(self.logger, "all missing"; "tests" => ?self.tests);
                self.get_missing(self.tests.len())
            };
            self.tests = &[];
            r
        };

        if is_failure || self.fail_dir.is_some() {
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
    fn run(self, logger: Logger, options: &'a RunOptions) -> impl Stream<Item = JobEvent<'a>> {
        match self {
            Self::FirstRun { list } => {
                let logger2 = logger.clone();
                let res: Box<dyn Stream<Item = _> + Send + Unpin> =
                    Box::new(run_test_list(logger, list, options).flat_map(move |r| {
                        trace!(logger2, "First run test result");
                        match r {
                            RunTestListEvent::TestResult(res) => {
                                let is_failure = res.data.result.variant.is_failure();
                                let entry = res.data.into();
                                // Start second run for failed tests
                                if is_failure {
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
                let logger2 = logger.clone();
                Box::new(
                    run_test_list(logger, &list[list.len() - 1..], options).flat_map(move |r| {
                        trace!(logger2, "Second run test result");
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
                let logger2 = logger.clone();
                let last_test = list[list.len() - 1];
                debug!(logger2, "Third run test"; "len" => list.len());
                Box::new(run_test_list(logger, list, options).flat_map(move |r| {
                    trace!(logger2, "Third run test result");
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
                                    info!(logger2, "Reproducing failure in third run failed \
                                        because not the whole test list was run, this can happen \
                                        because of intermediate failures";
                                        "last_failing_test" => list[list.len() - res.run_list.len()]
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
                Box::new(gen!({
                    if list.len() <= 2 {
                        trace!(logger, "Bisect succeeded, two tests or less left");
                        return;
                    }

                    let test_list = match state {
                        BisectState::Unknown => {
                            // Test with last half
                            trace!(logger, "Bisect run with last half");
                            Cow::Borrowed(&list[split_i..])
                        }
                        BisectState::SucceedingWithLastHalf => {
                            // Test with first half
                            let mut tests = list[..split_i].to_vec();
                            tests.push(last_test);
                            trace!(logger, "Bisect run with first half"; "tests" => ?tests);
                            Cow::Owned(tests)
                        }
                    };

                    let mut test_list_stream =
                        run_test_list(logger.clone(), test_list.as_ref(), options);
                    while let Some(r) = test_list_stream.next().await {
                        trace!(logger, "Bisect run test result");
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
                                        debug!(
                                            logger,
                                            "Unable to reproduce failure with either half"
                                        );
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

/// Start a deqp process and parse the output.
///
/// The started process gets killed on drop.
///
/// Returns the pid of the started process and a stream of events.
pub fn run_deqp<S: AsRef<OsStr> + std::fmt::Debug>(
    logger: Logger,
    timeout_duration: std::time::Duration,
    args: &[S],
    env: &[(&str, &str)],
) -> Result<RunDeqpState, DeqpError> {
    debug!(logger, "Start deqp"; "args" => ?args);
    let mut cmd = Command::new(&args[0]);
    cmd.args(&args[1..])
        .envs(env.iter().cloned())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);

    trace!(logger, "Run deqp"; "args" => ?args);
    let child = cmd.spawn().map_err(DeqpError::SpawnFailed)?;
    RunDeqpState::new(logger, timeout_duration, child)
}

/// Sort a list of tests into the order that deqp will run them in by running deqp with
/// `--deqp-runmode=stdout-caselist`.
///
/// Deqp walks a trie and filters out all tests that match the given test expressions. The result
/// list may be longer or shorter than the original list if *-expressions were used or names do not
/// exist.
pub async fn sort_with_deqp<S: AsRef<OsStr>>(
    logger: &Logger,
    args: &[S],
    tests: &[&str],
) -> Result<Vec<String>, DeqpSortError> {
    // Create a temporary file for the input test list
    let mut temp_file = NamedTempFile::new().map_err(DeqpSortError::TempFile)?;
    for t in tests {
        writeln!(&mut temp_file, "{}", t).map_err(DeqpSortError::WriteFailed)?;
    }

    let mut args = args.iter().map(|s| s.as_ref()).collect::<Vec<_>>();
    args.push(temp_file.path().as_os_str());
    args.push("--deqp-runmode=stdout-caselist".as_ref());
    trace!(logger, "Run deqp for sorting"; "args" => ?args);
    let mut cmd = Command::new(&args[0]);
    cmd.args(&args[1..])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    let mut child = cmd.spawn().map_err(DeqpSortError::SpawnFailed)?;
    let mut stdout = BufReader::new(child.stdout.take().unwrap()).lines();

    // Continue reading stdout and stderr even when the process exited
    let mut stdout_finished = false;
    let mut finished = false;
    let mut res = Vec::new();
    loop {
        if stdout_finished && finished {
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
            r = child.wait(), if !finished => {
                r.map_err(DeqpSortError::WaitFailed)?;
                finished = true;
            }
        }
    }
    Ok(res)
}

impl Stream for RunDeqpState {
    type Item = DeqpEvent;
    fn poll_next(mut self: Pin<&mut Self>, ctx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // Continue reading stdout and stderr even when the process exited
        loop {
            if !self.stdout_finished {
                if let Poll::Ready(r) = self.stdout_reader.poll_next_unpin(ctx) {
                    if let Some(r) = self.handle_stdout_line(r) {
                        return Poll::Ready(Some(r));
                    } else {
                        continue;
                    }
                }
            }

            if !self.stderr_finished {
                if let Poll::Ready(r) = self.stderr_reader.poll_next_unpin(ctx) {
                    self.handle_stderr_line(r);
                    continue;
                }
            }

            if !self.finished {
                // Wait for process in parallel so it can make progress
                let res = if let Poll::Ready(r) =
                    Pin::new(&mut Box::pin(self.child.as_mut().unwrap().wait())).poll(ctx)
                {
                    Some(match r {
                        Ok(status) if status.success() => Ok(()),
                        Ok(status) => Err(DeqpError::Crash {
                            exit_status: status.code(),
                        }),
                        Err(e) => Err(DeqpError::WaitFailed(e)),
                    })
                } else {
                    None
                };
                if let Some(res) = res {
                    self.finished_result = Some(res);
                    self.finished = true;
                    continue;
                }

                if !self.has_timeout {
                    if let Poll::Ready(_) = self.timeout.poll_unpin(ctx) {
                        debug!(self.logger, "Detected timeout");
                        self.has_timeout = true;
                        if self.has_fatal_error {
                            self.finished_result = Some(Err(DeqpError::FatalError));
                        } else {
                            self.finished_result = Some(Err(DeqpError::Timeout));
                        }
                        // Kill deqp
                        let logger = self.logger.clone();
                        let mut child = self.child.take().unwrap();
                        tokio::spawn(async move {
                            if let Err(e) = child.kill().await {
                                error!(logger, "Failed to kill deqp after timeout"; "error" => %e);
                            }
                        });
                        return Poll::Ready(None);
                    }
                }
            }

            if self.stdout_finished && self.stderr_finished && self.finished {
                if self.has_fatal_error {
                    self.finished_result = Some(Err(DeqpError::DeqpFatalError));
                }
                return Poll::Ready(None);
            }
            break Poll::Pending;
        }
    }
}

/// Run a list of tests and restart the process with remaining tests if it crashed.
///
/// Also record missing tests and the test lists when failures occured.
pub fn run_test_list<'a, 'list>(
    logger: Logger,
    tests: &'list [&'a str],
    options: &'a RunOptions,
) -> impl Stream<Item = RunTestListEvent<'a, 'list>> + Send + Unpin {
    let mut state = RunTestListState::new(logger, tests, options);

    gen!({
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
                match run_deqp(state.logger.clone(), options.timeout, &args, &[]) {
                    Ok(r) => state.running = Some(r),
                    Err(e) => {
                        yield_!(RunTestListEvent::DeqpError(DeqpErrorWithOutput {
                            error: e,
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
                    return;
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

pub async fn run_tests_parallel<'a>(
    logger: &'a Logger,
    tests: &'a [&'a str],
    // Map test names to summary entries
    summary: &mut Summary<'a>,
    options: &'a RunOptions,
    log_file: Option<&'a Path>,
    job_count: usize,
    progress_bar: Option<&ProgressBar>,
) {
    let mut pending_jobs: VecDeque<Job<'a>> = tests
        .chunks(BATCH_SIZE)
        .map(|list| Job::FirstRun { list })
        .collect();

    // New jobs can be added to this list
    let mut job_executor = stream::FuturesUnordered::new();
    // The total number of jobs added to the executor
    let mut job_id: u64 = 0;
    let mut log_entry_id: u64 = 0;
    if let Some(pb) = progress_bar {
        pb.set_length(pending_jobs.len() as u64);
    }

    let mut fails = 0;
    let mut crashes = 0;

    let mut log = if let Some(log_file) = log_file {
        match std::fs::File::create(log_file) {
            Ok(r) => Some(r),
            Err(e) => {
                error!(logger, "Failed to create log file"; "error" => %e);
                None
            }
        }
    } else {
        None
    };

    loop {
        if options.max_failures != 0 && fails + crashes >= options.max_failures {
            // Do not start new jobs when we have our max number of failures
            pending_jobs.clear();
        }

        while job_executor.len() < job_count {
            if let Some(job) = pending_jobs.pop_front() {
                let logger = logger.new(o!("job" => job_id));
                job_id += 1;
                debug!(logger, "Adding job to queue");
                job_executor.push(job.run(logger, options).into_future());
            } else {
                break;
            }
        }

        match job_executor.next().await {
            None => break,
            Some((None, _)) => {
                debug!(logger, "Job finished");
                if let Some(pb) = progress_bar {
                    pb.inc(1);
                    debug_assert_eq!(pb.position(), job_id - job_executor.len() as u64);
                }
            }
            Some((Some(event), job_stream)) => {
                let mut fatal_error = false;
                match event {
                    JobEvent::RunLogEntry(mut entry) => {
                        match &mut entry {
                            RunLogEntry::TestResult(res) => {
                                res.id = log_entry_id;
                                log_entry_id += 1;
                                match summary.0.entry(res.data.name) {
                                    Entry::Occupied(mut entry) => {
                                        let old_id = entry.get().0.run_id;
                                        // Merge result variants
                                        let old = entry.get().0.result.clone();
                                        let new = res.data.result.variant.clone();
                                        let (result, take_new) = old.merge(new);
                                        entry.get_mut().0 = summary::SummaryEntry {
                                            name: res.data.name,
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
                                            if let Some(pb) = progress_bar {
                                                pb.println(format!(
                                                    "{}: {:?}",
                                                    res.data.name, res.data.result.variant
                                                ));
                                                // Show fails and crashes on progress bar
                                                pb.set_message(&format!(
                                                    "; fails: {}, crashes: {}",
                                                    fails, crashes
                                                ));
                                                pb.tick();
                                            }
                                        }
                                        entry.insert((
                                            summary::SummaryEntry {
                                                name: res.data.name,
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
                            if let Err(e) = serde_json::to_writer(&mut *f, &entry) {
                                error!(logger, "Failed to write entry into log file";
                                    "error" => %e, "entry" => ?entry);
                            }
                            if let Err(e) = f.write_all(b"\n") {
                                error!(logger, "Failed to write into log file"; "error" => %e);
                            }
                        } else {
                            trace!(logger, "Log"; "entry" => ?entry);
                        }
                    }
                    JobEvent::NewJob(job) => {
                        pending_jobs.push_back(job);
                        if let Some(pb) = progress_bar {
                            pb.inc_length(1);
                        }
                    }
                }

                if fatal_error {
                    pending_jobs.clear();
                    job_executor = stream::FuturesUnordered::new();
                } else {
                    job_executor.push(job_stream.into_future());
                }
            }
        }
    }

    if let Some(pb) = progress_bar {
        pb.finish_and_clear();
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use anyhow::Result;
    use slog::Drain;

    use super::*;

    pub(crate) fn create_logger() -> Logger {
        let decorator = slog_term::PlainDecorator::new(slog_term::TestStdoutWriter);
        let drain = Mutex::new(slog_term::FullFormat::new(decorator).build()).fuse();

        slog::Logger::root(drain, o!())
    }

    async fn check_tests(args: &[&str], expected: &[(&str, TestResultType)]) -> Result<()> {
        check_tests_with_summary(args, expected, |_| {}).await
    }

    async fn check_tests_with_summary<F: for<'a> FnOnce(Summary<'a>)>(
        args: &[&str],
        expected: &[(&str, TestResultType)],
        check: F,
    ) -> Result<()> {
        let logger = create_logger();
        let run_options = RunOptions {
            args: args.iter().map(|s| s.to_string()).collect(),
            capture_dumps: true,
            timeout: std::time::Duration::from_secs(2),
            max_failures: 0,
            fail_dir: None,
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

        assert_eq!(
            summary.0.len(),
            expected.len(),
            "Result length does not match"
        );
        for (t, r) in expected {
            if let Some(r2) = summary.0.get(t) {
                assert_eq!(r2.0.result, *r, "Test result does not match for test {}", t);
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
            ("dEQP-VK.tessellation.primitive_discard.triangles_equal_spacing_ccw", TestResultType::Pass),
            ("dEQP-VK.tessellation.primitive_discard.triangles_equal_spacing_ccw_point_mode", TestResultType::Pass),
            ("dEQP-VK.tessellation.primitive_discard.triangles_equal_spacing_cw", TestResultType::Pass),
            ("dEQP-VK.tessellation.primitive_discard.triangles_equal_spacing_cw_point_mode", TestResultType::Pass),
            ("dEQP-VK.tessellation.primitive_discard.triangles_fractional_odd_spacing_ccw_valid_levels", TestResultType::Pass),
            ("dEQP-VK.tessellation.primitive_discard.triangles_fractional_odd_spacing_ccw", TestResultType::Pass),
            ("dEQP-VK.tessellation.primitive_discard.triangles_fractional_odd_spacing_ccw_point_mode_valid_levels", TestResultType::Pass),
            ("dEQP-VK.tessellation.primitive_discard.triangles_fractional_odd_spacing_ccw_point_mode", TestResultType::Pass),
            ("dEQP-VK.tessellation.primitive_discard.triangles_fractional_odd_spacing_cw_valid_levels", TestResultType::Pass),
            ("dEQP-VK.tessellation.primitive_discard.triangles_fractional_odd_spacing_cw", TestResultType::Pass),
            ("dEQP-VK.tessellation.primitive_discard.triangles_fractional_odd_spacing_cw_point_mode_valid_levels", TestResultType::Pass),
            ("dEQP-VK.tessellation.primitive_discard.triangles_fractional_odd_spacing_cw_point_mode", TestResultType::Pass),
            ("dEQP-VK.tessellation.primitive_discard.triangles_fractional_even_spacing_ccw", TestResultType::Pass),
            ("dEQP-VK.tessellation.primitive_discard.triangles_fractional_even_spacing_ccw_point_mode", TestResultType::Pass),
            ("dEQP-VK.tessellation.primitive_discard.triangles_fractional_even_spacing_cw", TestResultType::Pass),
            ("dEQP-VK.tessellation.primitive_discard.triangles_fractional_even_spacing_cw_point_mode", TestResultType::Pass),
            ("dEQP-VK.fragment_shader_interlock.basic.discard.ssbo.shading_rate_unordered.4xaa.sample_shading.512x512", TestResultType::NotSupported),
            ("dEQP-VK.fragment_shader_interlock.basic.discard.ssbo.shading_rate_unordered.4xaa.sample_shading.1024x1024", TestResultType::NotSupported),
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
            ("dEQP-VK.tessellation.primitive_discard.triangles_equal_spacing_ccw", TestResultType::Pass),
            ("dEQP-VK.tessellation.primitive_discard.triangles_equal_spacing_ccw_point_mode", TestResultType::Pass),
            ("dEQP-VK.tessellation.primitive_discard.triangles_equal_spacing_cw", TestResultType::Pass),
            ("dEQP-VK.tessellation.primitive_discard.triangles_equal_spacing_cw_point_mode", TestResultType::Pass),
            ("dEQP-VK.tessellation.primitive_discard.triangles_fractional_odd_spacing_ccw_valid_levels", TestResultType::Pass),
            ("dEQP-VK.tessellation.primitive_discard.triangles_fractional_odd_spacing_ccw", TestResultType::Pass),
            ("dEQP-VK.tessellation.primitive_discard.triangles_fractional_odd_spacing_ccw_point_mode_valid_levels", TestResultType::Pass),
            ("dEQP-VK.tessellation.primitive_discard.triangles_fractional_odd_spacing_ccw_point_mode", TestResultType::Pass),
            ("dEQP-VK.tessellation.primitive_discard.triangles_fractional_odd_spacing_cw_valid_levels", TestResultType::Pass),
            ("dEQP-VK.tessellation.primitive_discard.triangles_fractional_odd_spacing_cw", TestResultType::Pass),
            ("dEQP-VK.tessellation.primitive_discard.triangles_fractional_odd_spacing_cw_point_mode_valid_levels", TestResultType::Pass),
            ("dEQP-VK.tessellation.primitive_discard.triangles_fractional_odd_spacing_cw_point_mode", TestResultType::Pass),
            ("dEQP-VK.tessellation.primitive_discard.triangles_fractional_even_spacing_ccw", TestResultType::Pass),
            ("dEQP-VK.tessellation.primitive_discard.triangles_fractional_even_spacing_ccw_point_mode", TestResultType::Pass),
            ("dEQP-VK.tessellation.primitive_discard.triangles_fractional_even_spacing_cw", TestResultType::Pass),
            ("dEQP-VK.tessellation.primitive_discard.triangles_fractional_even_spacing_cw_point_mode", TestResultType::Crash),
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
            ("dEQP-VK.tessellation.primitive_discard.triangles_equal_spacing_ccw", TestResultType::Pass),
            ("dEQP-VK.tessellation.primitive_discard.triangles_equal_spacing_ccw_point_mode", TestResultType::Pass),
            ("dEQP-VK.tessellation.primitive_discard.triangles_equal_spacing_cw", TestResultType::Pass),
            ("dEQP-VK.tessellation.primitive_discard.triangles_equal_spacing_cw_point_mode", TestResultType::Pass),
            ("dEQP-VK.tessellation.primitive_discard.triangles_fractional_odd_spacing_ccw_valid_levels", TestResultType::Pass),
            ("dEQP-VK.tessellation.primitive_discard.triangles_fractional_odd_spacing_ccw", TestResultType::Pass),
            ("dEQP-VK.tessellation.primitive_discard.triangles_fractional_odd_spacing_ccw_point_mode_valid_levels", TestResultType::Pass),
            ("dEQP-VK.tessellation.primitive_discard.triangles_fractional_odd_spacing_ccw_point_mode", TestResultType::Pass),
            ("dEQP-VK.tessellation.primitive_discard.triangles_fractional_odd_spacing_cw_valid_levels", TestResultType::Pass),
            ("dEQP-VK.tessellation.primitive_discard.triangles_fractional_odd_spacing_cw", TestResultType::Pass),
            ("dEQP-VK.tessellation.primitive_discard.triangles_fractional_odd_spacing_cw_point_mode_valid_levels", TestResultType::Pass),
            ("dEQP-VK.tessellation.primitive_discard.triangles_fractional_odd_spacing_cw_point_mode", TestResultType::Pass),
            ("dEQP-VK.tessellation.primitive_discard.triangles_fractional_even_spacing_ccw", TestResultType::Pass),
            ("dEQP-VK.tessellation.primitive_discard.triangles_fractional_even_spacing_ccw_point_mode", TestResultType::Pass),
            ("dEQP-VK.tessellation.primitive_discard.triangles_fractional_even_spacing_cw", TestResultType::Pass),
            ("dEQP-VK.tessellation.primitive_discard.triangles_fractional_even_spacing_cw_point_mode", TestResultType::Crash),
        ];

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
            ("dEQP-VK.tessellation.primitive_discard.triangles_equal_spacing_ccw", TestResultType::Pass),
            ("dEQP-VK.tessellation.primitive_discard.triangles_equal_spacing_ccw_point_mode", TestResultType::Pass),
            ("dEQP-VK.tessellation.primitive_discard.triangles_equal_spacing_cw", TestResultType::Pass),
            ("dEQP-VK.tessellation.primitive_discard.triangles_equal_spacing_cw_point_mode", TestResultType::Pass),
            ("dEQP-VK.tessellation.primitive_discard.triangles_fractional_odd_spacing_ccw_valid_levels", TestResultType::Pass),
            ("dEQP-VK.tessellation.primitive_discard.triangles_fractional_odd_spacing_ccw", TestResultType::Pass),
            ("dEQP-VK.tessellation.primitive_discard.triangles_fractional_odd_spacing_ccw_point_mode_valid_levels", TestResultType::Pass),
            ("dEQP-VK.tessellation.primitive_discard.triangles_fractional_odd_spacing_ccw_point_mode", TestResultType::Pass),
            ("dEQP-VK.tessellation.primitive_discard.triangles_fractional_odd_spacing_cw_valid_levels", TestResultType::Pass),
            ("dEQP-VK.tessellation.primitive_discard.triangles_fractional_odd_spacing_cw", TestResultType::Pass),
            ("dEQP-VK.tessellation.primitive_discard.triangles_fractional_odd_spacing_cw_point_mode_valid_levels", TestResultType::Pass),
            ("dEQP-VK.tessellation.primitive_discard.triangles_fractional_odd_spacing_cw_point_mode", TestResultType::Pass),
            ("dEQP-VK.tessellation.primitive_discard.triangles_fractional_even_spacing_ccw", TestResultType::Pass),
            ("dEQP-VK.tessellation.primitive_discard.triangles_fractional_even_spacing_ccw_point_mode", TestResultType::Pass),
            ("dEQP-VK.tessellation.primitive_discard.triangles_fractional_even_spacing_cw", TestResultType::Pass),
            ("dEQP-VK.tessellation.primitive_discard.triangles_fractional_even_spacing_cw_point_mode", TestResultType::Timeout),
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
            ("dEQP-VK.tessellation.primitive_discard.triangles_equal_spacing_ccw", TestResultType::Pass),
            ("dEQP-VK.tessellation.primitive_discard.triangles_equal_spacing_ccw_point_mode", TestResultType::Pass),
            ("dEQP-VK.tessellation.primitive_discard.triangles_equal_spacing_cw", TestResultType::Pass),
            ("dEQP-VK.tessellation.primitive_discard.triangles_equal_spacing_cw_point_mode", TestResultType::Pass),
            ("dEQP-VK.tessellation.primitive_discard.triangles_fractional_odd_spacing_ccw_valid_levels", TestResultType::Pass),
            ("dEQP-VK.tessellation.primitive_discard.triangles_fractional_odd_spacing_ccw", TestResultType::Pass),
            ("dEQP-VK.tessellation.primitive_discard.triangles_fractional_odd_spacing_ccw_point_mode_valid_levels", TestResultType::Pass),
            ("dEQP-VK.tessellation.primitive_discard.triangles_fractional_odd_spacing_ccw_point_mode", TestResultType::Pass),
            ("dEQP-VK.tessellation.primitive_discard.triangles_fractional_odd_spacing_cw_valid_levels", TestResultType::Pass),
            ("dEQP-VK.tessellation.primitive_discard.triangles_fractional_odd_spacing_cw", TestResultType::Pass),
            ("dEQP-VK.tessellation.primitive_discard.triangles_fractional_odd_spacing_cw_point_mode_valid_levels", TestResultType::Pass),
            ("dEQP-VK.tessellation.primitive_discard.triangles_fractional_odd_spacing_cw_point_mode", TestResultType::Pass),
            ("dEQP-VK.tessellation.primitive_discard.triangles_fractional_even_spacing_ccw", TestResultType::Pass),
            ("dEQP-VK.tessellation.primitive_discard.triangles_fractional_even_spacing_ccw_point_mode", TestResultType::Pass),
            ("dEQP-VK.tessellation.primitive_discard.triangles_fractional_even_spacing_cw", TestResultType::Pass),
            ("dEQP-VK.tessellation.primitive_discard.triangles_fractional_even_spacing_cw_point_mode", TestResultType::Crash),
        ];

        check_tests_with_summary(
            &["test/test-timeout.sh", "logs/d", "logs/d-err", "1"],
            &expected,
            |summary| {
                let res = summary.0.get("dEQP-VK.tessellation.primitive_discard.triangles_fractional_even_spacing_cw_point_mode").unwrap();
                assert_eq!(res.0.run_id, Some(15));
                assert!(res.1.as_ref().unwrap().pid.is_some());
            }
        )
        .await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_bisect() -> Result<()> {
        let expected = vec![
            ("dEQP-VK.tessellation.primitive_discard.triangles_equal_spacing_ccw", TestResultType::Pass),
            ("dEQP-VK.tessellation.primitive_discard.triangles_equal_spacing_ccw_point_mode", TestResultType::Pass),
            ("dEQP-VK.tessellation.primitive_discard.triangles_equal_spacing_cw", TestResultType::Pass),
            ("dEQP-VK.tessellation.primitive_discard.triangles_equal_spacing_cw_point_mode", TestResultType::Pass),
            ("dEQP-VK.tessellation.primitive_discard.triangles_fractional_odd_spacing_ccw_valid_levels", TestResultType::Pass),
            ("dEQP-VK.tessellation.primitive_discard.triangles_fractional_odd_spacing_ccw", TestResultType::Pass),
            ("dEQP-VK.tessellation.primitive_discard.triangles_fractional_odd_spacing_ccw_point_mode_valid_levels", TestResultType::Pass),
            ("dEQP-VK.tessellation.primitive_discard.triangles_fractional_odd_spacing_ccw_point_mode", TestResultType::Pass),
            ("dEQP-VK.tessellation.primitive_discard.triangles_fractional_odd_spacing_cw_valid_levels", TestResultType::Pass),
            ("dEQP-VK.tessellation.primitive_discard.triangles_fractional_odd_spacing_cw", TestResultType::Pass),
            ("dEQP-VK.tessellation.primitive_discard.triangles_fractional_odd_spacing_cw_point_mode_valid_levels", TestResultType::Pass),
            ("dEQP-VK.tessellation.primitive_discard.triangles_fractional_odd_spacing_cw_point_mode", TestResultType::Pass),
            ("dEQP-VK.tessellation.primitive_discard.triangles_fractional_even_spacing_ccw", TestResultType::Pass),
            ("dEQP-VK.tessellation.primitive_discard.triangles_fractional_even_spacing_ccw_point_mode", TestResultType::Pass),
            ("dEQP-VK.tessellation.primitive_discard.triangles_fractional_even_spacing_cw", TestResultType::Pass),
            ("dEQP-VK.tessellation.primitive_discard.triangles_fractional_even_spacing_cw_point_mode", TestResultType::Crash),
        ];

        check_tests_with_summary(
            &["test/bisect-test-runner.sh", "dEQP-VK.tessellation.primitive_discard.triangles_fractional_odd_spacing_ccw_valid_levels", "logs/d", "/dev/null", "1", "logs/a", "dev/null", "0"],
            &expected,
            |summary| {
                // TODO Check bisection result with get_test_results returned by check_tests
                let res = summary.0.get("dEQP-VK.tessellation.primitive_discard.triangles_fractional_even_spacing_cw_point_mode").unwrap();
                assert_eq!(res.1.as_ref().unwrap().fail_dir, None);
                assert_eq!(res.0.run_id, Some(23));
            }
        )
        .await?;

        Ok(())
    }
}
