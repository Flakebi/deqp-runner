use std::borrow::Cow;
use std::collections::hash_map::Entry;
use std::collections::{HashMap, VecDeque};
use std::ffi::OsStr;
use std::io::Write;
use std::mem;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::process::Stdio;

use futures::future::Either;
use futures::prelude::*;
use futures::task::Poll;
use genawaiter::sync::gen;
use genawaiter::yield_;
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

/// This many tests will be executed with a single deqp run.
const BATCH_SIZE: usize = 1000;

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

// TODO Replace all streams with gen!

#[derive(Clone, Debug)]
#[cfg_attr(feature = "bin", derive(clap::Clap))]
#[cfg_attr(feature = "bin", clap(version = clap::crate_version!(), author = clap::crate_authors!(),
    about = clap::crate_description!()))]
pub struct Options {
    #[cfg_attr(feature = "bin", clap(short, long))]
    pub jobs: Option<usize>,
    /// Shuffle tests before running.
    ///
    /// This can uncover bugs that are not detected normally.
    #[cfg_attr(feature = "bin", clap(long))]
    pub shuffle: bool,
    /// Path for the output summary file.
    ///
    /// Writes a csv file with one line for every run test.
    #[cfg_attr(feature = "bin", clap(short, long))]
    pub output: Option<PathBuf>,
    /// Path for the log file.
    ///
    /// Writes a json file with one line for every entry.
    #[cfg_attr(feature = "bin", clap(short, long))]
    pub log: Option<PathBuf>,
    /// Path for the failure data.
    ///
    /// Every failed test gets a folder with stdout, stderr and other data to reproduce.
    #[cfg_attr(feature = "bin", clap(long))]
    pub failures: Option<PathBuf>,
    /// A file with tests to run.
    #[cfg_attr(feature = "bin", clap(short, long))]
    pub tests: PathBuf,
    /// Timout for a single test in seconds.
    ///
    /// A test that runs this long is considered failing.
    #[cfg_attr(feature = "bin", clap(long, default_value = "600"))]
    pub timeout: u32,
    // TODO Is deqp called like this?
    /// The deqp command to run. E.g. `./deqp --caselist-file`
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

#[derive(Debug, Deserialize, Serialize)]
pub struct TestResult {
    pub stdout: String,
    pub variant: TestResultType,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct TestResultData<'a> {
    /// Name of the deqp test.
    pub name: &'a str,
    pub result: TestResult,
    pub start: OffsetDateTime,
    pub duration: Duration,
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
    /// Fatal error
    #[error("deqp encountered a fatal error")]
    FatalError,
    #[error("deqp timed out")]
    Timeout,
    #[error("deqp exited with {exit_status:?}")]
    Crash { exit_status: Option<i32> },
}

#[derive(Debug, Deserialize, Serialize)]
pub struct DeqpErrorWithStdout {
    error: DeqpError,
    stdout: String,
}

#[derive(Debug)]
pub enum DeqpEvent {
    TestStart {
        name: String,
    },
    TestEnd {
        result: TestResult,
    },
    Finished {
        result: Result<(), DeqpError>,
        stdout: String,
        stderr: String,
    },
}

#[derive(Clone, Debug)]
pub struct RunOptions {
    pub args: Vec<String>,
    pub capture_dumps: bool,
    pub timeout: std::time::Duration,
    /// Directory where failure dumps should be created.
    pub fail_dir: Option<PathBuf>,
}

#[derive(Debug)]
pub enum RunTestListEvent<'a, 'list> {
    TestResult(ReproducibleTestResultData<'a, 'list>),
    DeqpError(DeqpErrorWithStdout),
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

/// Struct for the `run_log.json` file. Every line in this file is such an entry.
#[derive(Debug, Deserialize, Serialize)]
pub enum RunLogEntry<'a> {
    TestResult(#[serde(borrow)] TestResultEntry<'a>),
    /// Error that happened independant of a test.
    DeqpError(DeqpErrorWithStdout),
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

struct RunDeqpState {
    logger: Logger,
    timeout_duration: std::time::Duration,
    timeout: Sleep,
    stdout_reader: io::Lines<BufReader<ChildStdout>>,
    stderr_reader: io::Lines<BufReader<ChildStderr>>,
    /// Buffer for stdout
    stdout: String,
    /// Buffer for stderr
    stderr: String,
    stdout_finished: bool,
    stderr_finished: bool,
    /// Process exited
    finished: bool,
    has_timeout: bool,
    tests_done: bool,
    /// deqp reported a fatal error on stderr
    has_fatal_error: bool,
    /// Process exit status
    finished_result: Option<Result<(), DeqpError>>,
    child: tokio::process::Child,
}

struct RunTestListState<'a, 'list, S: Stream<Item = DeqpEvent>> {
    logger: Logger,
    /// Input to the process that was last started.
    tests: &'list [&'a str],
    options: &'a RunOptions,
    running: Option<S>,
    /// Index into current `tests` and start time.
    cur_test: Option<(usize, OffsetDateTime)>,
    last_finished: Option<usize>,

    /// Temporary file that contains the test list and is passed to deqp.
    test_list_file: Option<NamedTempFile>,
    /// If the current run had a failure and we already created a failure dir, this is the
    /// directory.
    fail_dir: Option<String>,
    /// How many tests were skipped.
    skipped: usize,
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

impl<'a> From<DeqpErrorWithStdout> for JobEvent<'a> {
    fn from(err: DeqpErrorWithStdout) -> Self {
        Self::RunLogEntry(RunLogEntry::DeqpError(err))
    }
}

impl RunDeqpState {
    fn new(mut logger: Logger, timeout_duration: std::time::Duration, mut child: Child) -> Self {
        if let Some(id) = child.id() {
            logger = logger.new(o!("pid" => id));
        }

        let stdout = child.stdout.take().unwrap();
        let stderr = child.stderr.take().unwrap();
        Self {
            logger,
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
            child,
        }
    }

    fn handle_stdout_line(
        &mut self,
        l: Result<Option<String>, std::io::Error>,
    ) -> Option<DeqpEvent> {
        let l = match l {
            Ok(r) => r,
            Err(e) => {
                self.stdout_finished = true;
                debug!(self.logger, "Failed to read stdout of process"; "error" => %e);
                return None;
            }
        };
        if let Some(l) = l {
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
        } else {
            self.stdout_finished = true;
        }
        None
    }

    fn handle_stderr_line(&mut self, l: Result<Option<String>, std::io::Error>) {
        let l = match l {
            Ok(r) => r,
            Err(e) => {
                self.stderr_finished = true;
                debug!(self.logger, "Failed to read stderr of process"; "error" => %e);
                return;
            }
        };
        if let Some(l) = l {
            if l.contains("FATAL ERROR: ") {
                self.has_fatal_error = true;
            }
            self.stderr.push_str(&l);
            self.stderr.push('\n');
        } else {
            self.stderr_finished = true;
        }
    }
}

impl<'a, 'list, S: Stream<Item = DeqpEvent>> RunTestListState<'a, 'list, S> {
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
            skipped: 0,
        }
    }

    fn is_finished(&self) -> bool {
        self.tests.is_empty()
            || self
                .cur_test
                .as_ref()
                .map(|t| t.0 >= self.tests.len())
                .unwrap_or_default()
    }

    /// Start a new deqp process.
    ///
    /// Returns the arguments for starting the process.
    /// We cannot start the process here because we cannot name the type of [`run_deqp`].
    fn start(&mut self) -> Result<Vec<String>, DeqpErrorWithStdout> {
        // TODO pipeline dumps
        // Create a temporary file for the test list
        let mut temp_file = NamedTempFile::new().map_err(|e| DeqpErrorWithStdout {
            error: DeqpError::FatalError,
            stdout: format!("Failed to create temporary file for test list: {}", e),
        })?;
        for t in self.tests {
            writeln!(&mut temp_file, "{}", t).map_err(|e| DeqpErrorWithStdout {
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
                .ok_or_else(|| DeqpErrorWithStdout {
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
        if self.fail_dir.is_some() {
            return;
        }
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
                        error!(self.logger, "Failed to create failure directory"; "error" => %e);
                        return;
                    }
                    self.fail_dir = Some(dir_name);
                    // Write reproduce-list.txt
                    match std::fs::File::create(new_dir.join("reproduce-list.txt")) {
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
                                writeln!(&mut f, "{}", self.options.args.iter().map(|a| {
                                    a.replace('\n', "\\n")
                                }).collect::<Vec<_>>().join(" "))?;

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

    /// Save stderr to the current `fail_dir` if there is one.
    fn save_fail_dir_stderr(&self, stderr: &str) {
        if let Some(dir_name) = &self.fail_dir {
            let fail_dir = self.options.fail_dir.as_ref().unwrap().join(dir_name);
            // Save stderr
            match std::fs::File::create(fail_dir.join("stderr.txt")) {
                Ok(mut f) => {
                    if let Err(e) = f.write_all(stderr.as_bytes()) {
                        error!(self.logger, "Failed to write stderr file"; "error" => %e);
                    }
                }
                Err(e) => {
                    error!(self.logger, "Failed to create stderr file"; "error" => %e);
                }
            }
        }
    }

    fn handle_test_start(&mut self, name: &str) {
        trace!(self.logger, "Test started"; "test" => name);
        let next_test = self.last_finished.map(|i| i + 1).unwrap_or_default();
        if let Some(i) = (&self.tests[next_test..]).iter().position(|t| t == &name) {
            self.skipped = i;
            self.cur_test = Some((next_test + i, OffsetDateTime::now_utc()));
        } else {
            warn!(self.logger, "Executing unknown test"; "test" => name);
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

    fn handle_finished(
        &mut self,
        result: Result<(), DeqpError>,
        stdout: String,
        stderr: String,
    ) -> Option<RunTestListEvent<'a, 'list>> {
        trace!(self.logger, "Finished"; "result" => ?result, "stdout" => &stdout,
            "stderr" => &stderr);
        let run_list = self.tests;
        self.running = None;
        self.save_fail_dir_stderr(&stderr);

        if let Err(e) = result {
            if let Some(cur_test) = self.cur_test {
                let duration = OffsetDateTime::now_utc() - cur_test.1;
                if self.fail_dir.is_none() {
                    self.create_fail_dir(run_list[cur_test.0]);
                    self.save_fail_dir_stderr(&stderr);
                }

                // Continue testing
                let run_list = &self.tests[..cur_test.0 + 1];
                self.tests = &run_list[cur_test.0 + 1..];

                return Some(RunTestListEvent::TestResult(ReproducibleTestResultData {
                    data: TestResultData {
                        name: run_list[cur_test.0],
                        result: TestResult {
                            stdout,
                            variant: if matches!(e, DeqpError::Timeout) {
                                TestResultType::Timeout
                            } else {
                                TestResultType::Crash
                            },
                        },
                        start: cur_test.1,
                        duration,
                        fail_dir: self.fail_dir.clone(),
                    },
                    run_list,
                    args: &self.options.args,
                }));
            } else {
                // No test executed or crashed in between tests, counts as fatal error
                self.tests = &[];
                trace!(self.logger, "Deqp exited without running tests, aborting"; "error" => ?e);
                return Some(RunTestListEvent::DeqpError(DeqpErrorWithStdout {
                    error: DeqpError::FatalError,
                    stdout,
                }));
            }
        } else {
            if let Some(cur_test) = self.cur_test {
                warn!(self.logger, "test not finished but deqp exited successful, ignoring";
                    "cur_test" => self.tests[cur_test.0], "started" => %cur_test.1);
            }
            if let Some(last_finished) = self.last_finished {
                self.tests = &run_list[last_finished + 1..];
                if !self.tests.is_empty() {
                    warn!(self.logger, "deqp finished without running all tests, ignoring";
                        "last_finished" => self.tests[last_finished]);
                }
            } else {
                // No test executed, counts all tests as missing
                self.skipped = self.tests.len();
                self.cur_test = Some((self.skipped, OffsetDateTime::now_utc()));
            }
        }
        None
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
pub fn run_deqp<S: AsRef<OsStr> + std::fmt::Debug>(
    logger: Logger,
    timeout_duration: std::time::Duration,
    args: &[S],
    env: &[(&str, &str)],
) -> impl Stream<Item = DeqpEvent> {
    debug!(logger, "Start deqp"; "args" => ?args);
    let mut cmd = Command::new(&args[0]);
    cmd.args(&args[1..])
        .envs(env.iter().cloned())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);

    trace!(logger, "Run deqp"; "args" => ?args);
    let child = match cmd.spawn() {
        Ok(r) => r,
        Err(e) => {
            let r: Pin<Box<dyn Stream<Item = DeqpEvent> + Send>> =
                Box::pin(stream::once(future::ready(DeqpEvent::Finished {
                    result: Err(DeqpError::SpawnFailed(e)),
                    stdout: String::new(),
                    stderr: String::new(),
                })));
            return r;
        }
    };

    let state = RunDeqpState::new(logger, timeout_duration, child);

    Box::pin(stream::unfold(state, |mut state| async {
        // Continue reading stdout and stderr even when the process exited
        loop {
            if state.has_timeout {
                match state.finished_result.take() {
                    Some(mut result) => {
                        if state.has_fatal_error {
                            result = Err(DeqpError::FatalError);
                        }
                        return Some((
                            DeqpEvent::Finished {
                                result,
                                stdout: mem::replace(&mut state.stdout, String::new()),
                                stderr: mem::replace(&mut state.stderr, String::new()),
                            },
                            state,
                        ));
                    }
                    None => {
                        // Kill deqp
                        let logger = state.logger;
                        let mut child = state.child;
                        tokio::spawn(async move {
                            if let Err(e) = child.kill().await {
                                error!(logger, "Failed to kill deqp after timeout"; "error" => %e);
                            }
                        });
                        return None;
                    }
                }
            }

            tokio::select! {
                // Stdout
                l = state.stdout_reader.next_line(), if !state.stdout_finished => {
                    if let Some(r) = state.handle_stdout_line(l) {
                        return Some((r, state));
                    }
                }
                // Stderr
                l = state.stderr_reader.next_line(), if !state.stderr_finished => {
                    state.handle_stderr_line(l);
                }
                // Wait for process in parallel so it can make progress
                r = state.child.wait(), if !state.finished => {
                    state.finished_result = Some(match r {
                        Ok(status) if status.success() => Ok(()),
                        Ok(status) => Err(DeqpError::Crash { exit_status: status.code(), }),
                        Err(e) => Err(DeqpError::WaitFailed(e)),
                    });
                    state.finished = true;
                }
                _ = &mut state.timeout, if !state.has_timeout && !state.finished => {
                    debug!(state.logger, "Detected timeout");
                    state.has_timeout = true;
                    state.finished_result = Some(Err(DeqpError::Timeout));
                }
                // Return finished here as stdout and stderr are completely read
                else => match state.finished_result.take() {
                    Some(mut result) => {
                        if state.has_fatal_error {
                            result = Err(DeqpError::FatalError);
                        }
                        return Some((DeqpEvent::Finished {
                            result,
                            stdout: mem::replace(&mut state.stdout, String::new()),
                            stderr: mem::replace(&mut state.stderr, String::new()),
                        }, state));
                    }
                    None => return None,
                },
            }
        }
    }))
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

    stream::poll_fn(move |ctx| {
        loop {
            if state.skipped > 0 {
                state.skipped -= 1;
                let cur_test = state.cur_test.as_ref().unwrap().0;
                return Poll::Ready(Some(RunTestListEvent::TestResult(
                    ReproducibleTestResultData {
                        data: TestResultData {
                            name: state.tests[cur_test - state.skipped - 1],
                            result: TestResult {
                                stdout: String::new(),
                                variant: TestResultType::Missing,
                            },
                            start: OffsetDateTime::now_utc(),
                            duration: Duration::new(0, 0),
                            fail_dir: None,
                        },
                        run_list: &state.tests[..cur_test - state.skipped],
                        args: &options.args,
                    },
                )));
            }
            if state.is_finished() {
                return Poll::Ready(None);
            }
            if state.running.is_none() {
                let args = match state.start() {
                    Ok(r) => r,
                    Err(e) => {
                        // Cannot create test list, fatal error
                        state.tests = &[];
                        return Poll::Ready(Some(RunTestListEvent::DeqpError(e)));
                    }
                };
                state.running = Some(run_deqp(state.logger.clone(), options.timeout, &args, &[]));
            }

            match state.running.as_mut().unwrap().poll_next_unpin(ctx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Ready(Some(e)) => match e {
                    DeqpEvent::TestStart { name } => state.handle_test_start(&name),
                    DeqpEvent::TestEnd { result } => {
                        if let Some(r) = state.handle_test_end(result) {
                            return Poll::Ready(Some(r));
                        }
                    }
                    DeqpEvent::Finished {
                        result,
                        stdout,
                        stderr,
                    } => {
                        if let Some(r) = state.handle_finished(result, stdout, stderr) {
                            return Poll::Ready(Some(r));
                        }
                    }
                },
            }
        }
    })
}

pub fn run_tests_parallel<'a>(
    logger: &'a Logger,
    tests: &'a [&'a str],
    options: &'a RunOptions,
    log_file: Option<&'a Path>,
    summary_file: Option<&'a Path>,
    job_count: usize,
) -> impl Stream<Item = SummaryEntry<'a>> + 'a {
    let mut pending_jobs: VecDeque<Job<'a>> = tests
        .chunks(BATCH_SIZE)
        .map(|list| Job::FirstRun { list })
        .collect();

    // New jobs can be added to this list
    let mut job_executor = stream::FuturesUnordered::new();
    let mut job_id: u64 = 0;
    let mut log_entry_id: u64 = 0;

    // Map test names to summary entries
    let mut summary = HashMap::<&'a str, SummaryEntry>::new();

    // TODO Use async to write into file?
    let mut log = log_file.and_then(|f| match std::fs::File::create(f) {
        Ok(r) => Some(r),
        Err(e) => {
            error!(logger, "Failed to create log file"; "error" => %e);
            None
        }
    });

    stream::poll_fn(move |ctx| {
        loop {
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

            match job_executor.poll_next_unpin(ctx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(None) => {
                    if let Some(summary_file) = summary_file {
                        // Write summary
                        match csv::Writer::from_path(summary_file) {
                            Ok(mut writer) => {
                                for t in tests {
                                    let r = summary.remove(t).unwrap_or_else(|| SummaryEntry {
                                        name: t,
                                        result: TestResultType::Missing,
                                        run_id: None,
                                    });
                                    if let Err(e) = writer.serialize(r) {
                                        error!(logger, "Failed to write summary file";
                                            "error" => %e);
                                        break;
                                    }
                                }
                            }
                            Err(e) => {
                                error!(logger, "Failed to open summary file"; "error" => %e);
                            }
                        }
                    }
                    return Poll::Ready(None);
                }
                Poll::Ready(Some((None, _))) => {} // Job ended
                Poll::Ready(Some((Some(event), job_stream))) => {
                    let mut fatal_error = false;
                    match event {
                        JobEvent::RunLogEntry(mut entry) => {
                            match &mut entry {
                                RunLogEntry::TestResult(res) => {
                                    res.id = log_entry_id;
                                    log_entry_id += 1;
                                    match summary.entry(res.data.name) {
                                        Entry::Occupied(mut entry) => {
                                            let old_id = entry.get().run_id;
                                            // Merge result variants
                                            let old = entry.get().result.clone();
                                            let new = res.data.result.variant.clone();
                                            let (result, take_new) = old.merge(new);
                                            entry.insert(SummaryEntry {
                                                name: res.data.name,
                                                result,
                                                run_id: if take_new {
                                                    Some(res.id)
                                                } else {
                                                    old_id
                                                },
                                            });
                                        }
                                        Entry::Vacant(entry) => {
                                            entry.insert(SummaryEntry {
                                                name: res.data.name,
                                                result: res.data.result.variant.clone(),
                                                run_id: Some(res.id),
                                            });
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
                        JobEvent::NewJob(job) => pending_jobs.push_back(job),
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
    })
}
