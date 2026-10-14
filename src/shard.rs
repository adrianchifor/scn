use crate::{
    model::*,
    server::{ApiError, ApiState, blocking, limit_requests},
    storage::Local,
    store::{MAX_BLOB, decode_blob},
};
use axum::{
    Json, Router,
    body::Bytes,
    extract::{DefaultBodyLimit, Path},
    http::{StatusCode, header},
    routing::get,
};

pub fn router(local: Local) -> Router {
    limit_requests(
        Router::new()
            .route("/health", get(|| async { "ok" }))
            .route(
                "/internal/blobs/{id}",
                get(blob).put(put_blob).head(has_blob),
            )
            .route("/internal/commits/{id}", get(commit).put(put_commit))
            .route("/internal/changes/{id}", get(changes).put(put_changes))
            .layer(DefaultBodyLimit::max(MAX_BLOB + 128 * 1024))
            .with_state(local),
    )
}

async fn blob(
    local: ApiState<Local>,
    Path(id): Path<String>,
) -> Result<([(axum::http::HeaderName, &'static str); 2], Vec<u8>), ApiError> {
    let bytes = blocking(local, move |local| {
        let bytes = local.blobs.get(&id)?;
        decode_blob(&bytes, &id)?;
        Ok(bytes)
    })
    .await?;
    Ok((
        [
            (header::CONTENT_TYPE, "application/zstd"),
            (header::CACHE_CONTROL, "public, max-age=31536000, immutable"),
        ],
        bytes,
    ))
}

async fn put_blob(
    local: ApiState<Local>,
    Path(id): Path<String>,
    body: Bytes,
) -> Result<StatusCode, ApiError> {
    blocking(local, move |local| {
        validate_id(&id)?;
        decode_blob(&body, &id)?;
        local.blobs.put(&id, &body)
    })
    .await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn has_blob(local: ApiState<Local>, Path(id): Path<String>) -> Result<StatusCode, ApiError> {
    Ok(
        if blocking(local, move |local| local.blobs.contains(&id)).await? {
            StatusCode::NO_CONTENT
        } else {
            StatusCode::NOT_FOUND
        },
    )
}

async fn commit(
    local: ApiState<Local>,
    Path(id): Path<String>,
) -> Result<Json<StoredCommit>, ApiError> {
    Ok(Json(blocking(local, move |local| local.header(&id)).await?))
}

async fn put_commit(
    local: ApiState<Local>,
    Path(id): Path<String>,
    Json(header): Json<StoredCommit>,
) -> Result<StatusCode, ApiError> {
    blocking(local, move |local| local.put_header(&id, header)).await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn changes(
    local: ApiState<Local>,
    Path(id): Path<String>,
) -> Result<Json<Changes>, ApiError> {
    Ok(Json(
        blocking(local, move |local| local.changes(&id)).await?,
    ))
}

async fn put_changes(
    local: ApiState<Local>,
    Path(id): Path<String>,
    Json(changes): Json<Changes>,
) -> Result<StatusCode, ApiError> {
    blocking(local, move |local| local.put_changes(&id, changes)).await?;
    Ok(StatusCode::NO_CONTENT)
}
