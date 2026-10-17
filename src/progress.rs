use anyhow::Result;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use std::{
    future::Future,
    io::{self, IsTerminal, Write},
    sync::OnceLock,
    time::{Duration, Instant},
};

fn output() -> &'static MultiProgress {
    static OUTPUT: OnceLock<MultiProgress> = OnceLock::new();
    OUTPUT.get_or_init(MultiProgress::new)
}

pub fn println(message: &str) {
    output().suspend(|| println!("{message}"));
}

pub(crate) struct LogWriter;

impl Write for LogWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        output().suspend(|| io::stderr().write(bytes))
    }

    fn flush(&mut self) -> io::Result<()> {
        output().suspend(|| io::stderr().flush())
    }
}

struct Indicator(Option<ProgressBar>);

impl Drop for Indicator {
    fn drop(&mut self) {
        if let Some(bar) = &self.0 {
            bar.finish_and_clear();
            output().remove(bar);
        }
    }
}

pub async fn wait<T>(
    enabled: bool,
    message: &'static str,
    future: impl Future<Output = Result<T>>,
) -> Result<T> {
    let started = Instant::now();
    let bar = if enabled
        && io::stderr().is_terminal()
        && std::env::var_os("TERM").is_none_or(|term| term != "dumb")
    {
        let bar = output().add(ProgressBar::new_spinner());
        bar.set_style(
            ProgressStyle::with_template("{spinner:.cyan} {msg} {elapsed}")?
                .tick_strings(&["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"]),
        );
        bar.set_message(message);
        bar.enable_steady_tick(Duration::from_millis(80));
        Some(bar)
    } else {
        None
    };
    let indicator = Indicator(bar);
    tracing::debug!(operation = message, "Operation started");
    let result = future.await;
    drop(indicator);
    tracing::debug!(
        operation = message,
        elapsed_ms = started.elapsed().as_millis() as u64,
        success = result.is_ok(),
        "Operation finished"
    );
    result
}
