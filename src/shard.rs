use crate::{
    model::*,
    server::{ApiError, ApiState, RequestLimits, execute, limit_requests},
    storage::{Backend, Local},
    store::{MAX_BLOB, verify_blob},
};
use axum::{
    Json, Router,
    body::Bytes,
    extract::{DefaultBodyLimit, Path},
    http::{StatusCode, header},
    response::{IntoResponse, Response},
    routing::get,
};

pub fn router(local: Local) -> Router {
    router_with_limits(local, RequestLimits::default())
}

pub fn router_with_limits(local: Local, limits: RequestLimits) -> Router {
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
        limits,
        "shard",
    )
}

async fn blob(
    local: ApiState<Local>,
    Path(id): Path<String>,
) -> Result<([(axum::http::HeaderName, &'static str); 2], Bytes), ApiError> {
    let bytes = execute(local, move |local| async move {
        let bytes = local.blobs.get(&id).await?;
        crate::worker::cpu(move || {
            verify_blob(&bytes, &id)?;
            Ok(bytes)
        })
        .await
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
    execute(local, move |local| async move {
        let (id, body, size) = crate::worker::cpu(move || {
            validate_id(&id)?;
            let size = verify_blob(&body, &id)?;
            Ok((id, body, size))
        })
        .await?;
        local.put_blob(&id, body, size, None).await
    })
    .await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn has_blob(local: ApiState<Local>, Path(id): Path<String>) -> Result<Response, ApiError> {
    Ok(
        match execute(
            local,
            move |local| async move { local.blob_size(&id).await },
        )
        .await?
        {
            Some(size) => (
                StatusCode::NO_CONTENT,
                [("x-scn-blob-size", size.to_string())],
            )
                .into_response(),
            None => StatusCode::NOT_FOUND.into_response(),
        },
    )
}

async fn commit(
    local: ApiState<Local>,
    Path(id): Path<String>,
) -> Result<Json<StoredCommit>, ApiError> {
    Ok(Json(
        execute(local, move |local| async move {
            crate::worker::io(move || local.header(&id)).await
        })
        .await?,
    ))
}

async fn put_commit(
    local: ApiState<Local>,
    Path(id): Path<String>,
    Json(header): Json<StoredCommit>,
) -> Result<StatusCode, ApiError> {
    execute(local, move |local| async move {
        crate::worker::io(move || local.put_header(&id, header)).await
    })
    .await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn changes(
    local: ApiState<Local>,
    Path(id): Path<String>,
) -> Result<Json<Changes>, ApiError> {
    Ok(Json(
        execute(local, move |local| async move {
            crate::worker::io(move || local.changes(&id)).await
        })
        .await?,
    ))
}

async fn put_changes(
    local: ApiState<Local>,
    Path(id): Path<String>,
    Json(changes): Json<Changes>,
) -> Result<StatusCode, ApiError> {
    execute(local, move |local| async move {
        crate::worker::io(move || local.put_changes(&id, changes)).await
    })
    .await?;
    Ok(StatusCode::NO_CONTENT)
}
