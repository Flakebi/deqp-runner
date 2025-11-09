//! A tracing writer that prints log messages to a progress bar.
//!
//! Use with `tracing_subscriber::fmt().with_writer(ProgressBarWriter(progress_bar)).init()`.

use std::io;

use indicatif::ProgressBar;

#[derive(Clone)]
pub struct ProgressBarWriter(pub ProgressBar);

impl io::Write for ProgressBarWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let out_str = String::from_utf8_lossy(buf);
        self.0.println(out_str);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for ProgressBarWriter {
    type Writer = Self;

    fn make_writer(&'a self) -> Self {
        self.clone()
    }
}
