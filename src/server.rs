use crate::{
    model::*,
    store::{MAX_BLOB, Store},
};
use anyhow::Result;
use axum::{
    Json, Router,
    body::{Body, Bytes},
    extract::{DefaultBodyLimit, FromRequestParts, Path, Query, Request},
    http::{StatusCode, header, request::Parts},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use futures_util::StreamExt;
use serde::Deserialize;
use std::sync::Arc;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

pub(crate) struct ApiError(pub StatusCode, pub String);

impl From<anyhow::Error> for ApiError {
    fn from(error: anyhow::Error) -> Self {
        let status = match error.downcast_ref::<TagError>() {
            Some(TagError::Exists(_)) => StatusCode::CONFLICT,
            Some(TagError::NotFound(_)) => StatusCode::NOT_FOUND,
            None => StatusCode::BAD_REQUEST,
        };
        Self(status, format!("{error:#}"))
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (self.0, Json(serde_json::json!({"error": self.1}))).into_response()
    }
}

pub(crate) struct ApiState<T> {
    value: T,
    permit: Arc<OwnedSemaphorePermit>,
}

impl<T: Clone + Send + Sync> FromRequestParts<T> for ApiState<T> {
    type Rejection = ApiError;

    async fn from_request_parts(parts: &mut Parts, state: &T) -> Result<Self, ApiError> {
        let permit = parts
            .extensions
            .get::<Arc<OwnedSemaphorePermit>>()
            .cloned()
            .ok_or_else(|| {
                ApiError(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "missing request permit".into(),
                )
            })?;
        Ok(Self {
            value: state.clone(),
            permit,
        })
    }
}

pub(crate) fn limit_requests(router: Router) -> Router {
    with_request_limit(router, Arc::new(Semaphore::new(8)))
}

fn with_request_limit(router: Router, permits: Arc<Semaphore>) -> Router {
    router.layer(middleware::from_fn(
        move |mut request: Request, next: Next| {
            let permits = permits.clone();
            async move {
                if request.uri().path() == "/health" {
                    return next.run(request).await;
                }
                let Ok(permit) = permits.try_acquire_owned() else {
                    return (
                        StatusCode::SERVICE_UNAVAILABLE,
                        [(header::RETRY_AFTER, "1")],
                        Json(serde_json::json!({"error": "server busy; retry later"})),
                    )
                        .into_response();
                };
                let permit = Arc::new(permit);
                request.extensions_mut().insert(permit.clone());
                let (parts, body) = next.run(request).await.into_parts();
                let stream = body.into_data_stream().map(move |chunk| {
                    let _permit = &permit;
                    chunk
                });
                Response::from_parts(parts, Body::from_stream(stream))
            }
        },
    ))
}

pub(crate) async fn blocking<T: Send + 'static, S: Send + 'static>(
    state: ApiState<S>,
    work: impl FnOnce(S) -> Result<T> + Send + 'static,
) -> Result<T, ApiError> {
    tokio::task::spawn_blocking(move || {
        let _permit = state.permit;
        work(state.value)
    })
    .await
    .map_err(|error| ApiError(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?
    .map_err(Into::into)
}

pub fn router(store: Store) -> Router {
    limit_requests(
        Router::new()
            .route("/health", get(|| async { "ok" }))
            .route("/v1/refs/main", get(main_ref).post(update_main))
            .route("/v1/tags", get(tags).post(create_tag))
            .route("/v1/tags/{*name}", get(tag).delete(delete_tag))
            .route("/v1/blobs", post(put_blob))
            .route("/v1/files", post(prepare_file))
            .route("/v1/blobs/{id}", get(get_blob).head(has_blob))
            .route("/v1/commits", post(create_commit))
            .route("/v1/import/commits", post(import_commit))
            .route("/v1/import/tags", post(import_tag))
            .route("/v1/commits/{revision}", get(get_commit))
            .route("/v1/tree/{revision}", get(tree))
            .route("/v1/log/{revision}", get(log))
            .route("/v1/diff/{from}/{to}", get(diff))
            .route("/v1/merge-base/{left}/{right}", get(merge_base))
            .route("/v1/blame/{revision}", get(blame))
            .route("/v1/merge", post(merge))
            .route("/v1/rebase", post(rebase))
            .layer(DefaultBodyLimit::max(MAX_BLOB))
            .with_state(store),
    )
}

pub async fn serve(store: Store, address: &str) -> Result<()> {
    serve_router(router(store), address).await
}

pub async fn serve_router(router: Router, address: &str) -> Result<()> {
    let listener = tokio::net::TcpListener::bind(address).await?;
    eprintln!("scn listening on http://{}", listener.local_addr()?);
    axum::serve(listener, router)
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await?;
    Ok(())
}

async fn main_ref(store: ApiState<Store>) -> Result<Json<RefValue>, ApiError> {
    Ok(Json(RefValue {
        target: blocking(store, |s| s.main_ref()).await?,
    }))
}

async fn tags(store: ApiState<Store>) -> Result<Json<Vec<Tag>>, ApiError> {
    Ok(Json(blocking(store, |s| s.tags()).await?))
}

async fn tag(store: ApiState<Store>, Path(name): Path<String>) -> Result<Json<Tag>, ApiError> {
    Ok(Json(blocking(store, move |s| s.tag(&name)).await?))
}

async fn create_tag(
    store: ApiState<Store>,
    Json(request): Json<TagRequest>,
) -> Result<Json<Tag>, ApiError> {
    Ok(Json(blocking(store, move |s| s.create_tag(request)).await?))
}

async fn import_tag(store: ApiState<Store>, Json(tag): Json<Tag>) -> Result<Json<Tag>, ApiError> {
    Ok(Json(blocking(store, move |s| s.import_tag(tag)).await?))
}

async fn delete_tag(
    store: ApiState<Store>,
    Path(name): Path<String>,
) -> Result<StatusCode, ApiError> {
    blocking(store, move |s| s.delete_tag(&name)).await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn update_main(
    store: ApiState<Store>,
    Json(request): Json<RefUpdate>,
) -> Result<StatusCode, ApiError> {
    blocking(store, |s| s.update_main(request))
        .await
        .map_err(|error| ApiError(StatusCode::CONFLICT, error.1))?;
    Ok(StatusCode::NO_CONTENT)
}

#[derive(Deserialize)]
struct BlobQuery {
    path: Option<String>,
}

async fn put_blob(
    store: ApiState<Store>,
    Query(query): Query<BlobQuery>,
    body: Bytes,
) -> Result<Json<RefValue>, ApiError> {
    Ok(Json(RefValue {
        target: Some(blocking(store, move |s| s.put_blob_at(&body, query.path.as_deref())).await?),
    }))
}

async fn get_blob(store: ApiState<Store>, Path(id): Path<String>) -> Result<Response, ApiError> {
    let bytes = blocking(store, move |s| s.compressed_blob(&id)).await?;
    Ok((
        [
            (header::CONTENT_TYPE, "application/zstd"),
            (header::CACHE_CONTROL, "public, max-age=31536000, immutable"),
        ],
        bytes,
    )
        .into_response())
}

async fn has_blob(store: ApiState<Store>, Path(id): Path<String>) -> Result<StatusCode, ApiError> {
    Ok(if blocking(store, move |s| s.has_blob(&id)).await? {
        StatusCode::NO_CONTENT
    } else {
        StatusCode::NOT_FOUND
    })
}

async fn create_commit(
    store: ApiState<Store>,
    Json(request): Json<CommitRequest>,
) -> Result<Json<Commit>, ApiError> {
    Ok(Json(blocking(store, |s| s.create_commit(request)).await?))
}

async fn prepare_file(
    store: ApiState<Store>,
    Json(request): Json<FileRequest>,
) -> Result<Json<FileEntry>, ApiError> {
    Ok(Json(
        blocking(store, move |s| s.prepare_file(request)).await?,
    ))
}

async fn import_commit(
    store: ApiState<Store>,
    Json(request): Json<ImportCommitRequest>,
) -> Result<Json<Commit>, ApiError> {
    Ok(Json(blocking(store, |s| s.import_commit(request)).await?))
}

async fn get_commit(
    store: ApiState<Store>,
    Path(revision): Path<String>,
) -> Result<Json<Commit>, ApiError> {
    Ok(Json(
        blocking(store, move |s| s.get_commit(&s.resolve(&revision)?)).await?,
    ))
}

#[derive(Deserialize)]
struct TreeQuery {
    prefix: Option<String>,
}

async fn tree(
    store: ApiState<Store>,
    Path(revision): Path<String>,
    Query(query): Query<TreeQuery>,
) -> Result<Json<Tree>, ApiError> {
    Ok(Json(
        blocking(store, move |s| {
            if let Some(prefix) = &query.prefix {
                validate_path(prefix)?;
            }
            let mut tree = s.tree(&s.resolve(&revision)?)?;
            if let Some(prefix) = query.prefix {
                tree.retain(|path, _| selected(path, std::slice::from_ref(&prefix)));
            }
            Ok(tree)
        })
        .await?,
    ))
}

#[derive(Deserialize)]
struct LogQuery {
    limit: Option<usize>,
    path: Option<String>,
}

async fn log(
    store: ApiState<Store>,
    Path(revision): Path<String>,
    Query(query): Query<LogQuery>,
) -> Result<Json<Vec<Commit>>, ApiError> {
    Ok(Json(
        blocking(store, move |s| {
            s.log(
                &s.resolve(&revision)?,
                query.limit.unwrap_or(20).min(1000),
                query.path.as_deref(),
            )
        })
        .await?,
    ))
}

async fn diff(
    store: ApiState<Store>,
    Path((from, to)): Path<(String, String)>,
) -> Result<Json<Vec<DiffEntry>>, ApiError> {
    Ok(Json(
        blocking(store, move |s| s.diff(&s.resolve(&from)?, &s.resolve(&to)?)).await?,
    ))
}

async fn merge_base(
    store: ApiState<Store>,
    Path((left, right)): Path<(String, String)>,
) -> Result<Json<RefValue>, ApiError> {
    Ok(Json(RefValue {
        target: Some(
            blocking(store, move |s| {
                s.merge_base(&s.resolve(&left)?, &s.resolve(&right)?)
            })
            .await?,
        ),
    }))
}

#[derive(Deserialize)]
struct BlameQuery {
    path: String,
}

async fn blame(
    store: ApiState<Store>,
    Path(revision): Path<String>,
    Query(query): Query<BlameQuery>,
) -> Result<Json<Vec<BlameLine>>, ApiError> {
    Ok(Json(
        blocking(store, move |s| s.blame(&s.resolve(&revision)?, &query.path)).await?,
    ))
}

async fn merge(
    store: ApiState<Store>,
    Json(request): Json<IntegrateRequest>,
) -> Result<Json<Commit>, ApiError> {
    Ok(Json(
        blocking(store, move |s| {
            s.merge(
                &s.resolve(&request.head)?,
                &s.resolve(&request.onto)?,
                &request.author,
            )
        })
        .await?,
    ))
}

async fn rebase(
    store: ApiState<Store>,
    Json(request): Json<IntegrateRequest>,
) -> Result<Json<Commit>, ApiError> {
    Ok(Json(
        blocking(store, move |s| {
            s.rebase(
                &s.resolve(&request.head)?,
                &s.resolve(&request.onto)?,
                &request.author,
            )
        })
        .await?,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        io::Write,
        sync::{
            Condvar, Mutex,
            atomic::{AtomicBool, Ordering},
        },
        time::Duration,
    };

    async fn wait_until(mut ready: impl FnMut() -> bool) {
        tokio::time::timeout(Duration::from_secs(5), async {
            while !ready() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
    }

    async fn start(router: Router) -> (String, tokio::task::JoinHandle<()>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let task = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
        (url, task)
    }

    #[tokio::test]
    async fn requests_are_limited_before_reading_bodies_and_health_remains_available() {
        let permits = Arc::new(Semaphore::new(1));
        let router = with_request_limit(
            Router::new()
                .route("/upload", post(|_: Bytes| async { "ok" }))
                .route("/health", get(|| async { "ok" }))
                .layer(DefaultBodyLimit::max(MAX_BLOB)),
            permits.clone(),
        );
        let (url, task) = start(router).await;
        let mut stalled = std::net::TcpStream::connect(url.trim_start_matches("http://")).unwrap();
        stalled
            .write_all(
                b"POST /upload HTTP/1.1\r\nHost: localhost\r\nContent-Length: 33554432\r\n\r\n",
            )
            .unwrap();
        wait_until(|| permits.available_permits() == 0).await;
        let http = reqwest::Client::new();
        let response = http.post(format!("{url}/upload")).send().await.unwrap();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(response.headers()[header::RETRY_AFTER], "1");
        assert_eq!(
            http.get(format!("{url}/health"))
                .send()
                .await
                .unwrap()
                .status(),
            StatusCode::OK
        );
        drop(stalled);
        wait_until(|| permits.available_permits() == 1).await;
        assert_eq!(
            http.post(format!("{url}/upload"))
                .send()
                .await
                .unwrap()
                .text()
                .await
                .unwrap(),
            "ok"
        );
        task.abort();
    }

    #[derive(Default)]
    struct Gate {
        started: AtomicBool,
        released: Mutex<bool>,
        wake: Condvar,
    }

    struct Release(Arc<Gate>);

    impl Drop for Release {
        fn drop(&mut self) {
            *self.0.released.lock().unwrap() = true;
            self.0.wake.notify_all();
        }
    }

    #[tokio::test]
    async fn cancelled_requests_keep_their_permit_until_blocking_work_finishes() {
        let permits = Arc::new(Semaphore::new(1));
        let gate = Arc::new(Gate::default());
        let release = Release(gate.clone());
        let router = with_request_limit(
            Router::new()
                .route(
                    "/work",
                    post(|state: ApiState<Arc<Gate>>| async {
                        blocking(state, |gate| {
                            gate.started.store(true, Ordering::SeqCst);
                            let mut released = gate.released.lock().unwrap();
                            while !*released {
                                released = gate.wake.wait(released).unwrap();
                            }
                            Ok(StatusCode::NO_CONTENT)
                        })
                        .await
                    }),
                )
                .with_state(gate.clone()),
            permits.clone(),
        );
        let (url, task) = start(router).await;
        let mut cancelled =
            std::net::TcpStream::connect(url.trim_start_matches("http://")).unwrap();
        cancelled
            .write_all(b"POST /work HTTP/1.1\r\nHost: localhost\r\nContent-Length: 0\r\n\r\n")
            .unwrap();
        wait_until(|| gate.started.load(Ordering::SeqCst)).await;
        drop(cancelled);
        let response = reqwest::Client::new()
            .post(format!("{url}/work"))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        drop(release);
        wait_until(|| permits.available_permits() == 1).await;
        task.abort();
    }

    #[tokio::test]
    async fn clients_retry_busy_responses_with_the_same_upload_body() {
        use axum::extract::State;
        use std::sync::atomic::AtomicUsize;

        let attempts = Arc::new(AtomicUsize::new(0));
        let router = Router::new()
            .route(
                "/v1/blobs",
                post(
                    |State(attempts): State<Arc<AtomicUsize>>, body: Bytes| async move {
                        assert_eq!(&body[..], b"upload contents");
                        if attempts.fetch_add(1, Ordering::SeqCst) == 0 {
                            StatusCode::SERVICE_UNAVAILABLE.into_response()
                        } else {
                            Json(RefValue {
                                target: Some(hash(&body)),
                            })
                            .into_response()
                        }
                    },
                ),
            )
            .route(
                "/v1/refs/main",
                get(|State(attempts): State<Arc<AtomicUsize>>| async move {
                    attempts.fetch_add(1, Ordering::SeqCst);
                    StatusCode::BAD_REQUEST
                }),
            )
            .with_state(attempts.clone());
        let (url, task) = start(router).await;
        let cache = tempfile::tempdir().unwrap();
        let client = crate::client::Client::new(&url, cache.path()).unwrap();
        assert_eq!(
            client.put_blob(b"upload contents".to_vec()).await.unwrap(),
            hash(b"upload contents")
        );
        assert_eq!(attempts.load(Ordering::SeqCst), 2);
        assert!(client.main_ref().await.is_err());
        assert_eq!(attempts.load(Ordering::SeqCst), 3);
        task.abort();
    }
}
