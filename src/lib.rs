use std::ffi::OsStr;
use std::mem;
use std::path::PathBuf;
use std::pin::Pin;
use std::process::{ExitStatus, Stdio};

use futures::prelude::*;
use futures::task::Poll;
use serde::{Deserialize, Serialize};
use slog::{debug, warn, Logger};
use thiserror::Error;
use time::Duration;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::prelude::*;
use tokio::process::Command;

/// This many tests will be executed with a single deqp run.
const BATCH_SIZE: usize = 1000;

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
    /// Path for the failure data.
    ///
    /// Every failed test gets a folder with stdout, stderr and other data to reproduce.
    #[cfg_attr(feature = "bin", clap(long))]
    pub failures: Option<PathBuf>,
    /// Timout for a single test in seconds.
    ///
    /// A test that runs this long is considered failing.
    #[cfg_attr(feature = "bin", clap(long, default_value = "600"))]
    pub timeout: u32,
    /// Path for temporary data.
    ///
    /// Defaults to the standard temporary directory.
    #[cfg_attr(feature = "bin", clap(long))]
    pub tmp_dir: Option<PathBuf>,
    /// The deqp command to run. E.g. `./deqp --caselist-file`
    ///
    /// TODO Is the deqp called like this?
    ///
    /// A filename with the tests cases that should be run is appended to the command.
    pub run_command: Vec<String>,
}

#[derive(Debug, Error)]
pub enum TestError {
    #[error("Timeout")]
    Timeout,
    #[error("Crash")]
    Crash,
    /// Test should be executed but was not found in the output.
    #[error("Missing")]
    Missing,
    #[error("Fail: {0}")]
    Fail(String),
}

/// Same as [`TestError`] but with information on how to reproduce the run.
#[derive(Debug, Error)]
#[error("{source}")]
pub struct ExtendedTestError<'a> {
    source: TestError,
    run_list: &'a [&'a str],
}

#[derive(Debug, Error)]
pub enum DeqpError {
    /// Fatal error
    #[error("Failed to spawn process: {0}")]
    SpawnFailed(#[source] std::io::Error),
    #[error("Failed to wait for process: {0}")]
    WaitFailed(#[source] std::io::Error),
    #[error("Failed to read output from process: {0}")]
    ReadFailed(#[source] std::io::Error),
    #[error("deqp exited with {exit_status}")]
    Crash { exit_status: ExitStatus },
}

#[derive(Debug)]
pub enum DeqpEvent {
    TestStart {
        name: String,
    },
    TestEnd {
        result: Result<String, TestError>,
    },
    Finished {
        result: Result<(), DeqpError>,
        stderr: String,
    },
}

#[derive(Debug)]
pub struct RunOptions {
    args: Vec<String>,
    tmp_dir: PathBuf,
    capture_dumps: bool,
    timeout: Duration,
}

#[derive(Debug)]
struct Job<'a> {
    chunk: &'a [&'a str],
}

impl DeqpError {
    pub fn is_fatal(&self) -> bool {
        matches!(self, Self::SpawnFailed(_))
    }
}

/// Start a deqp process and parse the output.
///
/// The started process gets killed on drop.
pub fn run_deqp<S: AsRef<OsStr>>(
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

    let mut child = match cmd.spawn() {
        Ok(r) => r,
        Err(e) => {
            let r: Pin<Box<dyn Stream<Item = DeqpEvent> + Send>> =
                Box::pin(stream::once(future::ready(DeqpEvent::Finished {
                    result: Err(DeqpError::SpawnFailed(e)),
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
        stderr: String,
        stdout: String,
        stdout_finished: bool,
        stderr_finished: bool,
        finished: bool,
        child: tokio::process::Child,
    }

    let state = StreamState {
        logger,
        stdout_reader: BufReader::new(stdout).lines(),
        stderr_reader: BufReader::new(stderr).lines(),
        stderr: String::new(),
        stdout: String::new(),
        stdout_finished: false,
        stderr_finished: false,
        finished: false,
        child,
    };

    Box::pin(stream::unfold(state, |mut state| async {
        // Continue reading stdout and stderr even when the process exited
        if state.finished && state.stdout_finished && state.stderr_finished {
            return None;
        }
        loop {
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
                        // TODO Parse
                        state.stdout.push_str(&l);
                        state.stdout.push('\n');
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
                        state.stderr.push_str(&l);
                        state.stderr.push('\n');
                    } else {
                        state.stderr_finished = true;
                    }
                }
                // Wait for process in parallel so it can make progress
                r = state.child.wait(), if !state.finished => {
                    let result = match r {
                        Ok(exit_status) if exit_status.success() => Ok(()),
                        Ok(exit_status) => Err(DeqpError::Crash { exit_status, }),
                        Err(e) => Err(DeqpError::WaitFailed(e)),
                    };
                    state.finished = true;
                    return Some((DeqpEvent::Finished {
                        result,
                        stderr: mem::replace(&mut state.stderr, String::new()),
                    }, state));
                }
                else => return None,
            }
        }
    }))
}

/// Run a list of tests and restart the process with remaining tests if it crashed.
///
/// Also record missing tests and the test lists when failures occured.
pub fn run_test_list<'a>(
    logger: &'a Logger,
    tests: &'a [&'a str],
    options: &'a RunOptions,
) -> impl Stream<Item = Result<String, ExtendedTestError<'a>>> {
    struct StreamState<'a, S: Stream<Item = DeqpEvent>> {
        /// Input to the process that was last started.
        tests: &'a [&'a str],
        running: Option<S>,
        /// Index into current `tests`.
        cur_test: Option<usize>,
        last_finished: Option<usize>,

        // Temporary
        /// How many tests were skipped.
        skipped: usize,
    }

    let mut state = StreamState {
        tests,
        running: None,
        cur_test: None,
        last_finished: None,

        skipped: 0,
    };

    stream::poll_fn(move |ctx| {
        loop {
            if state.skipped > 0 {
                state.skipped -= 1;
                return Poll::Ready(Some(Err(ExtendedTestError {
                    source: TestError::Missing,
                    run_list: state.tests,
                })));
            }
            if state.tests.is_empty() {
                return Poll::Ready(None);
            }
            if state.running.is_none() {
                // Start new process
                // TODO pipeline dumps
                let args = options.args.clone();
                state.running = Some(run_deqp(logger.clone(), &args, &[]));
                state.cur_test = None;
                state.last_finished = None;
            }

            match state.running.as_mut().unwrap().poll_next_unpin(ctx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Ready(Some(e)) => match e {
                    DeqpEvent::TestStart { name } => {
                        let next_test = state.last_finished.map(|i| i + 1).unwrap_or_default();
                        if let Some(i) = (&state.tests[next_test..]).iter().position(|t| t == &name)
                        {
                            state.skipped = i;
                            state.cur_test = Some(next_test + i);
                        } else {
                            warn!(logger, "Executing unknown test"; "test" => name);
                        }
                    }
                    DeqpEvent::TestEnd { result } => {
                        if let Some(cur_test) = state.cur_test.take() {
                            state.last_finished = Some(cur_test);
                            return Poll::Ready(Some(result.map_err(|source| ExtendedTestError {
                                source,
                                run_list: state.tests,
                            })));
                        } else {
                            warn!(logger, "Finished test without starting a test";
                                "last-finished" => ?state.last_finished.map(|i| state.tests[i]));
                        }
                    }
                    DeqpEvent::Finished { result, stderr } => {
                        let run_list = state.tests;
                        state.running = None;
                        // TODO Pass more info (stdout, stderr)
                        if let Err(e) = result {
                            if e.is_fatal() {
                                // Finish early
                                state.tests = &[];
                                return Poll::Ready(Some(Err(ExtendedTestError {
                                    source: TestError::Crash,
                                    run_list,
                                })));
                            }

                            if let Some(cur_test) = state.cur_test {
                                // Continue testing
                                state.tests = &run_list[cur_test + 1..];
                                return Poll::Ready(Some(Err(ExtendedTestError {
                                    source: TestError::Crash,
                                    run_list,
                                })));
                            } else {
                                // No test executed or crashed in between tests, counts as fatal
                                // error
                                state.tests = &[];
                                return Poll::Ready(Some(Err(ExtendedTestError {
                                    source: TestError::Crash,
                                    run_list,
                                })));
                            }
                        } else {
                            if let Some(cur_test) = state.cur_test {
                                state.tests = &run_list[cur_test + 1..];
                                if !state.tests.is_empty() {
                                    warn!(logger, "deqp finished without running all tests, ignoring";
                                        "last-test" => state.tests[cur_test]);
                                }
                            } else {
                                // No test executed, counts all tests as missing
                                state.tests = &[];
                                return Poll::Ready(Some(Err(ExtendedTestError {
                                    source: TestError::Missing,
                                    run_list,
                                })));
                            }
                        }
                    }
                },
            }
        }
    })
}

pub fn run_tests_parallel<'a>(
    tests: &'a [&'a str],
    options: &RunOptions,
    job_count: usize,
) -> impl Stream<Item = (&'a str, Result<String, ExtendedTestError<'a>>)> + 'a {
    // New jobs can be sent to the channel?
    let mut jobs: stream::FuturesOrdered<_> = tests
        .chunks(BATCH_SIZE)
        .map(|chunk| future::ready(Job { chunk }))
        .collect();

    let mut results = Vec::new();

    /*jobs.for_each_concurrent(Some(job_count), |job| async {
        results.push(("test", Ok("".into())));
    });*/

    stream::poll_fn(move |ctx| {
        if let Some(r) = results.pop() {
            return Poll::Ready(Some(r));
        }
        Poll::Pending
    })
}
