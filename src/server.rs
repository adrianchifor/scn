use crate::{
    model::*,
    store::{MAX_BLOB, Store},
};
use anyhow::Result;
use axum::{
    Json, Router,
    body::Bytes,
    extract::{DefaultBodyLimit, Path, Query, State},
    http::{StatusCode, header},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use serde::Deserialize;

struct ApiError(StatusCode, String);

impl From<anyhow::Error> for ApiError {
    fn from(error: anyhow::Error) -> Self {
        Self(StatusCode::BAD_REQUEST, format!("{error:#}"))
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (self.0, Json(serde_json::json!({"error": self.1}))).into_response()
    }
}

async fn blocking<T: Send + 'static>(
    store: Store,
    work: impl FnOnce(Store) -> Result<T> + Send + 'static,
) -> Result<T, ApiError> {
    tokio::task::spawn_blocking(move || work(store))
        .await
        .map_err(|error| ApiError(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?
        .map_err(Into::into)
}

pub fn router(store: Store) -> Router {
    Router::new()
        .route("/health", get(|| async { "ok" }))
        .route("/v1/refs/main", get(main_ref).post(update_main))
        .route("/v1/blobs", post(put_blob))
        .route("/v1/blobs/{id}", get(get_blob))
        .route("/v1/commits", post(create_commit))
        .route("/v1/commits/{revision}", get(get_commit))
        .route("/v1/tree/{revision}", get(tree))
        .route("/v1/log/{revision}", get(log))
        .route("/v1/diff/{from}/{to}", get(diff))
        .route("/v1/merge-base/{left}/{right}", get(merge_base))
        .route("/v1/blame/{revision}", get(blame))
        .route("/v1/merge", post(merge))
        .route("/v1/rebase", post(rebase))
        .layer(DefaultBodyLimit::max(MAX_BLOB))
        .with_state(store)
}

pub async fn serve(store: Store, address: &str) -> Result<()> {
    let listener = tokio::net::TcpListener::bind(address).await?;
    eprintln!("scn listening on http://{}", listener.local_addr()?);
    axum::serve(listener, router(store))
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await?;
    Ok(())
}

async fn main_ref(State(store): State<Store>) -> Result<Json<RefValue>, ApiError> {
    Ok(Json(RefValue {
        target: blocking(store, |s| s.main_ref()).await?,
    }))
}

async fn update_main(
    State(store): State<Store>,
    Json(request): Json<RefUpdate>,
) -> Result<StatusCode, ApiError> {
    blocking(store, |s| s.update_main(request))
        .await
        .map_err(|error| ApiError(StatusCode::CONFLICT, error.1))?;
    Ok(StatusCode::NO_CONTENT)
}

async fn put_blob(State(store): State<Store>, body: Bytes) -> Result<Json<RefValue>, ApiError> {
    Ok(Json(RefValue {
        target: Some(blocking(store, move |s| s.put_blob(&body)).await?),
    }))
}

async fn get_blob(
    State(store): State<Store>,
    Path(id): Path<String>,
) -> Result<Response, ApiError> {
    let bytes = blocking(store, move |s| s.compressed_blob(&id)).await?;
    Ok((
        [
            (header::CONTENT_TYPE, "application/zlib"),
            (header::CACHE_CONTROL, "public, max-age=31536000, immutable"),
        ],
        bytes,
    )
        .into_response())
}

async fn create_commit(
    State(store): State<Store>,
    Json(request): Json<CommitRequest>,
) -> Result<Json<Commit>, ApiError> {
    Ok(Json(blocking(store, |s| s.create_commit(request)).await?))
}

async fn get_commit(
    State(store): State<Store>,
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
    State(store): State<Store>,
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
    State(store): State<Store>,
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
    State(store): State<Store>,
    Path((from, to)): Path<(String, String)>,
) -> Result<Json<Vec<DiffEntry>>, ApiError> {
    Ok(Json(
        blocking(store, move |s| s.diff(&s.resolve(&from)?, &s.resolve(&to)?)).await?,
    ))
}

async fn merge_base(
    State(store): State<Store>,
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
    State(store): State<Store>,
    Path(revision): Path<String>,
    Query(query): Query<BlameQuery>,
) -> Result<Json<Vec<BlameLine>>, ApiError> {
    Ok(Json(
        blocking(store, move |s| s.blame(&s.resolve(&revision)?, &query.path)).await?,
    ))
}

async fn merge(
    State(store): State<Store>,
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
    State(store): State<Store>,
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
