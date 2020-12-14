//! A slog term decorator that prints log messages to a progress bar.

use slog::{OwnedKVList, Record};
use slog_term::{Decorator, RecordDecorator};

pub struct ProgressBarDecorator;

struct ProgressBarRecordDecorator<'a>(&'a mut Vec<u8>);

impl Decorator for ProgressBarDecorator {
    fn with_record<F: FnOnce(&mut dyn RecordDecorator) -> std::io::Result<()>>(
        &self,
        _record: &Record,
        _logger_values: &OwnedKVList,
        f: F,
    ) -> std::io::Result<()> {
        let mut rec = Vec::new();
        f(&mut ProgressBarRecordDecorator(&mut rec))?;
        crate::PROGRESS_BAR.println(String::from_utf8(rec).map_err(|e| {
            std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("Cannot convert log message to string: {}", e),
            )
        })?);
        Ok(())
    }
}

impl std::io::Write for ProgressBarRecordDecorator<'_> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.write(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl RecordDecorator for ProgressBarRecordDecorator<'_> {
    fn reset(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
