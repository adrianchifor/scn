use anyhow::{Context, Result};
use axum::{
    Router,
    extract::Request,
    http::HeaderValue,
    middleware::{self, Next},
    response::Response,
};
use std::{
    future::Future,
    io::IsTerminal,
    sync::{
        OnceLock,
        atomic::{AtomicU64, Ordering},
    },
    time::{Instant, SystemTime, UNIX_EPOCH},
};
use tracing::Instrument;
use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::EnvFilter;

pub const REQUEST_ID: &str = "x-request-id";

tokio::task_local! {
    static CURRENT_REQUEST: String;
}

#[derive(Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub enum Format {
    Text,
    Json,
}

pub fn init(component: &str, format: Format) -> Result<WorkerGuard> {
    let default = if component == "cli" {
        "warn"
    } else {
        "warn,scn=info"
    };
    let filter = EnvFilter::try_new(std::env::var("RUST_LOG").unwrap_or_else(|_| default.into()))
        .context("invalid RUST_LOG filter")?;
    let (writer, guard) = tracing_appender::non_blocking::NonBlockingBuilder::default()
        .buffered_lines_limit(4096)
        .lossy(true)
        .finish(crate::progress::LogWriter);
    let subscriber = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(writer);
    match format {
        Format::Text => subscriber
            .with_ansi(std::io::stderr().is_terminal())
            .try_init(),
        Format::Json => subscriber.json().with_ansi(false).try_init(),
    }
    .map_err(|error| anyhow::anyhow!("could not initialize logging: {error}"))?;
    Ok(guard)
}

pub fn endpoint(value: &str) -> String {
    reqwest::Url::parse(value)
        .map(|url| url.origin().ascii_serialization())
        .unwrap_or_else(|_| "invalid endpoint".into())
}

pub fn new_request_id() -> String {
    static PREFIX: OnceLock<String> = OnceLock::new();
    static SEQUENCE: AtomicU64 = AtomicU64::new(1);
    let prefix = PREFIX.get_or_init(|| {
        format!(
            "{:x}-{:x}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        )
    });
    format!("{prefix}-{:x}", SEQUENCE.fetch_add(1, Ordering::Relaxed))
}

pub fn request_id() -> String {
    CURRENT_REQUEST
        .try_with(Clone::clone)
        .unwrap_or_else(|_| new_request_id())
}

pub async fn scope<T>(id: String, future: impl Future<Output = T>) -> T {
    CURRENT_REQUEST.scope(id, future).await
}

fn incoming_id(value: &HeaderValue) -> Option<String> {
    let id = value.to_str().ok()?;
    (!id.is_empty()
        && id.len() <= 64
        && id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.')))
    .then(|| id.to_owned())
}

pub fn requests(router: Router, component: &'static str) -> Router {
    router.layer(middleware::from_fn(move |mut request: Request, next: Next| async move {
        let id = request.headers().get(REQUEST_ID).and_then(incoming_id).unwrap_or_else(new_request_id);
        let header = HeaderValue::from_str(&id).unwrap();
        request.headers_mut().insert(REQUEST_ID, header.clone());
        let health = request.uri().path() == "/health";
        let head = request.method() == axum::http::Method::HEAD;
        let method = request.method().clone();
        let path = request.uri().path().to_owned();
        let span = tracing::info_span!("http_request", component, request_id = %id, %method, %path);
        scope(id, async move {
            let started = Instant::now();
            tracing::debug!("Request started");
            let mut response: Response = next.run(request).await;
            response.headers_mut().insert(REQUEST_ID, header);
            let status = response.status().as_u16();
            let elapsed_ms = started.elapsed().as_millis() as u64;
            if status >= 500 {
                tracing::error!(component, request_id = %request_id(), %method, %path, status, elapsed_ms, "Request failed");
            } else if status >= 400 && !(head && status == 404) {
                tracing::warn!(component, request_id = %request_id(), %method, %path, status, elapsed_ms, "Request rejected");
            } else if health || head {
                tracing::debug!(status, elapsed_ms, "Request completed");
            } else {
                tracing::info!(status, elapsed_ms, "Request completed");
            }
            response
        }.instrument(span)).await
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_ids_are_bounded_and_replace_untrusted_values() {
        assert_eq!(
            incoming_id(&HeaderValue::from_static("agent-123_abc.7")),
            Some("agent-123_abc.7".into())
        );
        for value in ["", "contains spaces", "invalid:id", &"a".repeat(65)] {
            assert!(incoming_id(&HeaderValue::from_str(value).unwrap()).is_none());
        }
        let first = new_request_id();
        assert_ne!(first, new_request_id());
        assert!(incoming_id(&HeaderValue::from_str(&first).unwrap()).is_some());
    }
}
