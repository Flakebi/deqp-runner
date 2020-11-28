use std::collections::{HashMap, VecDeque};
use std::ffi::OsStr;
use std::io::Write;
use std::mem;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::process::Stdio;

use futures::prelude::*;
use futures::task::Poll;
use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};
use slog::{debug, error, o, trace, warn, Logger};
use thiserror::Error;
use time::{Duration, OffsetDateTime};
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::prelude::*;
use tokio::process::Command;

/// This many tests will be executed with a single deqp run.
const BATCH_SIZE: usize = 1000;

static RESULT_VARIANTS: Lazy<HashMap<&str, TestResultType>> = Lazy::new(|| {
    let mut result_variants = HashMap::new();
    result_variants.insert("Pass", TestResultType::Pass);
    result_variants.insert("CompatibilityWarning", TestResultType::CompatibilityWarning);
    result_variants.insert("QualityWarning", TestResultType::QualityWarning);
    result_variants.insert("Fail", TestResultType::Fail);
    result_variants.insert("InternalError", TestResultType::InternalError);
    result_variants.insert("NotSupported", TestResultType::NotSupported);
    result_variants
});

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
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
pub enum TestResultType {
    Pass,
    /// Counts as success
    CompatibilityWarning,
    /// Counts as success
    QualityWarning,
    Fail,
    InternalError,
    NotSupported,
    Crash,
    Timeout,
    /// Test should be executed but was not found in the output.
    Missing,
    /// The test was not executed because a fatal error happened before and aborted the whole run.
    NotRun,
    /// Failed one time but is not reproducible.
    Flake,
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
pub struct ReproducibleTestResultData<'a> {
    pub data: TestResultData<'a>,
    /// The last test in this list is the one the test result is about.
    pub run_list: &'a [&'a str],
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
    pub timeout: Duration,
    /// Directory where failure dumps should be created.
    pub fail_dir: Option<PathBuf>,
}

#[derive(Debug)]
pub enum RunTestListEvent<'a> {
    TestResult(ReproducibleTestResultData<'a>),
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

#[derive(Debug)]
enum BisectState {
    /// Default state
    Unknown,
    /// Running the test after only the last half of the tests succeeds.
    SucceedingWithLastHalf,
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
        list: &'a [&'a str],
        state: BisectState,
    },
}

#[derive(Debug)]
enum JobEvent<'a> {
    RunLogEntry(RunLogEntry<'a>),
    NewJob(Job<'a>),
}

struct RunTestListState<'a, S: Stream<Item = DeqpEvent>> {
    logger: Logger,
    /// Input to the process that was last started.
    tests: &'a [&'a str],
    options: &'a RunOptions,
    running: Option<S>,
    /// Index into current `tests` and start time.
    cur_test: Option<(usize, OffsetDateTime)>,
    last_finished: Option<usize>,

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
        matches!(self, Self::SpawnFailed(_))
    }
}

impl<'a, S: Stream<Item = DeqpEvent>> RunTestListState<'a, S> {
    fn new(logger: Logger, tests: &'a [&'a str], options: &'a RunOptions) -> Self {
        RunTestListState {
            logger,
            tests,
            options,
            running: None,
            cur_test: None,
            last_finished: None,

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

    fn handle_test_end(&mut self, result: TestResult) -> Option<RunTestListEvent<'a>> {
        trace!(self.logger, "Test end"; "cur_test" => ?self.cur_test, "result" => ?result);
        if let Some(cur_test) = self.cur_test.take() {
            self.last_finished = Some(cur_test.0);
            let duration = OffsetDateTime::now_utc() - cur_test.1;
            Some(RunTestListEvent::TestResult(ReproducibleTestResultData {
                data: TestResultData {
                    name: self.tests[cur_test.0],
                    result,
                    start: cur_test.1,
                    duration,
                    fail_dir: None, // TODO Create on error
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
    ) -> Option<RunTestListEvent<'a>> {
        trace!(self.logger, "Finished"; "result" => ?result, "stdout" => &stdout,
            "stderr" => &stderr);
        let run_list = self.tests;
        self.running = None;
        if let Err(e) = result {
            if let Some(cur_test) = self.cur_test {
                // Continue testing
                let run_list = &self.tests[..cur_test.0 + 1];
                self.tests = &run_list[cur_test.0 + 1..];
                let duration = OffsetDateTime::now_utc() - cur_test.1;
                return Some(RunTestListEvent::TestResult(ReproducibleTestResultData {
                    data: TestResultData {
                        name: run_list[cur_test.0],
                        result: TestResult {
                            stdout,
                            variant: TestResultType::Crash,
                        },
                        start: cur_test.1,
                        duration,
                        fail_dir: None, // TODO Create
                    },
                    run_list,
                    args: &self.options.args,
                }));
            } else {
                // No test executed or crashed in between tests, counts as fatal
                // error
                self.tests = &[];
                return Some(RunTestListEvent::DeqpError(DeqpErrorWithStdout {
                    error: e,
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
                debug!(logger, "First run");
                let logger2 = logger.clone();
                let res: Box<dyn Stream<Item = _> + Send + Unpin> =
                    Box::new(run_test_list(logger, list, options).map(move |r| {
                        trace!(logger2, "Test result");
                        match r {
                            RunTestListEvent::TestResult(res) => {
                                // TODO Start next job
                                JobEvent::RunLogEntry(RunLogEntry::TestResult(TestResultEntry {
                                    id: 0, // Will be filled in later
                                    data: res.data,
                                }))
                            }
                            RunTestListEvent::DeqpError(e) => {
                                JobEvent::RunLogEntry(RunLogEntry::DeqpError(e))
                            }
                            //=> JobEvent::NewJob(),
                        }
                    }));
                res
            }
            Self::SecondRun { list } => Box::new(stream::empty()),
            Self::ThirdRun { list } => Box::new(stream::empty()),
            Self::Bisect { list, state } => Box::new(stream::empty()),
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
    args: &[S],
    env: &[(&str, &str)],
) -> impl Stream<Item = DeqpEvent> {
    let mut cmd = Command::new(&args[0]);
    cmd.args(&args[1..])
        .envs(env.iter().cloned())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);

    trace!(logger, "Run deqp"; "args" => ?args);
    let mut child = match cmd.spawn() {
        Ok(r) => r,
        Err(e) => {
            let r: Pin<Box<dyn Stream<Item = DeqpEvent> + Send>> =
                Box::pin(stream::once(future::ready(DeqpEvent::Finished {
                    result: Err(DeqpError::SpawnFailed(e)),
                    stdout: "".into(),
                    stderr: "".into(),
                })));
            return r;
        }
    };

    let stdout = child.stdout.take().unwrap();
    let stderr = child.stderr.take().unwrap();

    struct StreamState<R1, R2> {
        logger: Logger,
        stdout_reader: io::Lines<R1>,
        stderr_reader: io::Lines<R2>,
        stdout: String,
        stderr: String,
        stdout_finished: bool,
        stderr_finished: bool,
        finished: bool,
        tests_done: bool,
        has_fatal_error: bool,
        finished_result: Option<Result<(), DeqpError>>,
        child: tokio::process::Child,
    }

    let state = StreamState {
        logger,
        stdout_reader: BufReader::new(stdout).lines(),
        stderr_reader: BufReader::new(stderr).lines(),
        stdout: String::new(),
        stderr: String::new(),
        stdout_finished: false,
        stderr_finished: false,
        finished: false,
        tests_done: false,
        has_fatal_error: false,
        finished_result: None,
        child,
    };

    Box::pin(stream::unfold(state, |mut state| async {
        // Continue reading stdout and stderr even when the process exited
        if state.finished && state.stdout_finished && state.stderr_finished {
            return None;
        }
        loop {
            // TODO Handle timeout
            tokio::select! {
                // Stdout
                l = state.stdout_reader.next_line(), if !state.stdout_finished => {
                    let l = match l {
                        Ok(r) => r,
                        Err(e) => {
                            state.stdout_finished = true;
                            debug!(state.logger, "Failed to read stdout of process"; "error" => %e);
                            continue;
                        }
                    };
                    if let Some(l) = l {
                        if state.tests_done {
                            if !l.is_empty() {
                                state.stdout.push_str(&l);
                                state.stdout.push('\n');
                            }
                            continue;
                        }

                        if let Some(l) = l.strip_prefix("  ") {
                            for (s, res) in &*RESULT_VARIANTS {
                                if let Some(l) = l.strip_prefix(s) {
                                    let mut l = l.trim();
                                    if l.starts_with('(') && l.ends_with(')') {
                                        l = &l[1..l.len() - 1];
                                    }
                                    state.stdout.push_str(l);
                                    return Some((DeqpEvent::TestEnd { result: TestResult {
                                        stdout: mem::replace(&mut state.stdout, String::new()),
                                        variant: *res,
                                    } }, state));
                                }
                            }
                        }

                        if let Some(l) = l.strip_prefix("TEST: ") {
                            return Some((DeqpEvent::TestStart { name: l.to_string() }, state));
                        } else if let Some(l) = l.strip_prefix("Test case '") {
                            if let Some(l) = l.strip_suffix("'..") {
                                state.stdout.clear();
                                return Some((DeqpEvent::TestStart { name: l.into() }, state));
                            } else {
                                state.stdout.push_str(&l);
                                state.stdout.push('\n');
                            }
                        } else if l == "DONE!" {
                            state.tests_done = true;
                        } else if l.is_empty() {
                        } else {
                            state.stdout.push_str(&l);
                            state.stdout.push('\n');
                        }
                    } else {
                        state.stdout_finished = true;
                    }
                }
                // Stderr
                l = state.stderr_reader.next_line(), if !state.stderr_finished => {
                    let l = match l {
                        Ok(r) => r,
                        Err(e) => {
                            state.stderr_finished = true;
                            debug!(state.logger, "Failed to read stderr of process"; "error" => %e);
                            continue;
                        }
                    };
                    if let Some(l) = l {
                        if l.contains("FATAL ERROR: ") {
                            state.has_fatal_error = true;
                        }
                        state.stderr.push_str(&l);
                        state.stderr.push('\n');
                    } else {
                        state.stderr_finished = true;
                    }
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
pub fn run_test_list<'a>(
    logger: Logger,
    tests: &'a [&'a str],
    options: &'a RunOptions,
) -> impl Stream<Item = RunTestListEvent<'a>> {
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
                // Start new process
                // TODO pipeline dumps
                // TODO Create a temporary file for the test list
                let args = options.args.clone();
                state.running = Some(run_deqp(state.logger.clone(), &args, &[]));
                state.cur_test = None;
                state.last_finished = None;
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

    // TODO Write test log
    stream::poll_fn(move |ctx| {
        loop {
            while job_executor.len() < job_count && !pending_jobs.is_empty() {
                if let Some(job) = pending_jobs.pop_front() {
                    let logger = logger.new(o!("job" => job_id));
                    job_id += 1;
                    debug!(logger, "Adding job to queue");
                    job_executor.push(job.run(logger, options).into_future());
                } else {
                    debug!(logger, "No jobs left in queue");
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
                Poll::Ready(Some((Some(r), job_stream))) => {
                    let mut fatal_error = false;
                    match r {
                        JobEvent::RunLogEntry(mut entry) => {
                            match &mut entry {
                                RunLogEntry::TestResult(res) => {
                                    res.id = log_entry_id;
                                    log_entry_id += 1;
                                    summary.insert(res.data.name, SummaryEntry {
                                        name: res.data.name,
                                        result: res.data.result.variant,
                                        run_id: Some(res.id),
                                    });
                                }
                                RunLogEntry::DeqpError(e) => {
                                    if let DeqpError::FatalError = e.error {
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
                        JobEvent::NewJob(j) => pending_jobs.push_back(j),
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
