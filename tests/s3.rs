use axum::{
    Router,
    body::Bytes,
    extract::{Path, State},
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
    routing::get,
};
use scn::{
    client::Client,
    model::*,
    server,
    storage::Blobs,
    store::{Store, is_zstd},
};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};

type Objects = Arc<Mutex<BTreeMap<String, Vec<u8>>>>;

async fn put(
    State(objects): State<Objects>,
    Path((bucket, key)): Path<(String, String)>,
    headers: HeaderMap,
    bytes: Bytes,
) -> StatusCode {
    assert_eq!(bucket, "test-bucket");
    assert!(
        headers[header::AUTHORIZATION]
            .to_str()
            .unwrap()
            .starts_with("AWS4-HMAC-SHA256 Credential=test-access/")
    );
    assert_eq!(headers[header::CONTENT_TYPE], "application/zstd");
    assert_eq!(headers["x-amz-content-sha256"], scn::model::hash(&bytes));
    assert!(is_zstd(&bytes));
    objects.lock().unwrap().insert(key, bytes.to_vec());
    StatusCode::OK
}

async fn fetch(State(objects): State<Objects>, Path((_, key)): Path<(String, String)>) -> Response {
    match objects.lock().unwrap().get(&key) {
        Some(bytes) => {
            ([(header::CONTENT_TYPE, "application/zstd")], bytes.clone()).into_response()
        }
        None => StatusCode::NOT_FOUND.into_response(),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn signed_s3_storage_round_trips_zstandard_blobs_without_local_blob_files() {
    let objects = Objects::default();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let router = Router::new()
        .route("/{bucket}/{*key}", get(fetch).put(put))
        .with_state(objects.clone());
    let mock = tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    let directory = tempfile::tempdir().unwrap();
    let store_path = directory.path().join("store");
    let store = tokio::task::spawn_blocking(move || {
        let credentials =
            s3::creds::Credentials::new(Some("test-access"), Some("test-secret"), None, None, None)
                .unwrap();
        let bucket = s3::Bucket::new(
            "test-bucket",
            s3::Region::Custom {
                region: "us-east-1".into(),
                endpoint,
            },
            credentials,
        )
        .unwrap()
        .with_path_style();
        let blobs = Blobs::S3 {
            bucket: Arc::from(bucket),
            prefix: "repository/blobs".into(),
        };
        assert!(!blobs.contains(&"0".repeat(64)).unwrap());
        Store::with_blobs(&store_path, blobs).unwrap()
    })
    .await
    .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let api = tokio::spawn(async move {
        axum::serve(listener, server::router(store)).await.unwrap();
    });
    let client = Client::new(&url, &directory.path().join("cache")).unwrap();
    let id = client.put_blob(b"S3 content\n".to_vec()).await.unwrap();
    let commit: Commit = client
        .post(
            "/v1/commits",
            &CommitRequest {
                parents: vec![],
                author: "author".into(),
                message: "S3".into(),
                changes: Changes::from([(
                    "file".into(),
                    Some(FileEntry {
                        blob: id.clone(),
                        executable: false,
                    }),
                )]),
            },
        )
        .await
        .unwrap();
    client.land(commit.id).await.unwrap();
    assert_eq!(client.blob(&id).await.unwrap(), b"S3 content\n");
    assert!(
        objects
            .lock()
            .unwrap()
            .contains_key(&format!("repository/blobs/{id}.zst"))
    );
    assert!(!directory.path().join("store/blobs").exists());
    api.abort();
    mock.abort();
}
