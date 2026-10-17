use anyhow::Result;
use std::sync::{Arc, OnceLock};
use tokio::sync::Semaphore;

async fn execute<T: Send + 'static>(
    slots: &Arc<Semaphore>,
    work: impl FnOnce() -> Result<T> + Send + 'static,
) -> Result<T> {
    let permit = slots.clone().acquire_owned().await?;
    let span = tracing::Span::current();
    tokio::task::spawn_blocking(move || {
        let _permit = permit;
        span.in_scope(work)
    })
    .await?
}

pub async fn io<T: Send + 'static>(work: impl FnOnce() -> Result<T> + Send + 'static) -> Result<T> {
    static SLOTS: OnceLock<Arc<Semaphore>> = OnceLock::new();
    execute(SLOTS.get_or_init(|| Arc::new(Semaphore::new(128))), work).await
}

pub async fn cpu<T: Send + 'static>(
    work: impl FnOnce() -> Result<T> + Send + 'static,
) -> Result<T> {
    static SLOTS: OnceLock<Arc<Semaphore>> = OnceLock::new();
    execute(
        SLOTS.get_or_init(|| {
            Arc::new(Semaphore::new(
                std::thread::available_parallelism().map_or(2, usize::from),
            ))
        }),
        work,
    )
    .await
}

pub struct Cleanup<T: Send + 'static>(Option<T>);

impl<T: Send + 'static> Cleanup<T> {
    pub fn new(value: T) -> Self {
        Self(Some(value))
    }
}

impl<T: Send + 'static> std::ops::Deref for Cleanup<T> {
    type Target = T;
    fn deref(&self) -> &T {
        self.0.as_ref().unwrap()
    }
}

impl<T: Send + 'static> Drop for Cleanup<T> {
    fn drop(&mut self) {
        let value = self.0.take();
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            runtime.spawn_blocking(move || drop(value));
        }
    }
}
