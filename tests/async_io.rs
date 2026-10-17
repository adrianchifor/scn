use axum::{Router, body::Bytes, routing::get};
use scn::{client::Client, server, storage::Blobs, store::Store};
use std::{
    io::{self, Read, Write},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    thread::ThreadId,
    time::Duration,
};

struct Reader {
    runtime_thread: ThreadId,
    remaining: u64,
    read: u64,
}

impl Read for Reader {
    fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
        assert_ne!(std::thread::current().id(), self.runtime_thread);
        let length = self.remaining.min(bytes.len() as u64) as usize;
        bytes[..length].fill((self.read / scn::content::CHUNK_SIZE as u64) as u8);
        self.remaining -= length as u64;
        self.read += length as u64;
        Ok(length)
    }
}

struct Writer {
    runtime_thread: ThreadId,
    written: u64,
}

impl Write for Writer {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        assert_ne!(std::thread::current().id(), self.runtime_thread);
        let expected = (self.written / scn::content::CHUNK_SIZE as u64) as u8;
        assert!(bytes.iter().all(|byte| *byte == expected));
        self.written += bytes.len() as u64;
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[tokio::test]
async fn cli_file_io_runs_off_the_async_thread_and_preserves_chunk_order() {
    let runtime_thread = std::thread::current().id();
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().join("store");
    let store = scn::worker::io(move || Store::open(&root)).await.unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        axum::serve(listener, server::router(store)).await.unwrap();
    });
    let client = Client::new(&url, &directory.path().join("cache"))
        .await
        .unwrap();
    let size = scn::store::MAX_BLOB as u64 + 17;
    let source = Reader {
        runtime_thread,
        remaining: size,
        read: 0,
    };
    let (source, entry) = client.put_reader(source, size, false, None).await.unwrap();
    assert_eq!(source.remaining, 0);
    let output = client
        .write_file(
            &entry,
            Writer {
                runtime_thread,
                written: 0,
            },
        )
        .await
        .unwrap();
    assert_eq!(output.written, size);
    server.abort();
}

#[test]
fn s3_requests_progress_with_the_blocking_pool_saturated() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .max_blocking_threads(1)
        .build()
        .unwrap();
    runtime.block_on(async {
        let active = Arc::new(AtomicUsize::new(0));
        let maximum = Arc::new(AtomicUsize::new(0));
        let concurrent = active.clone();
        let peak = maximum.clone();
        let router = Router::new()
            .route(
                "/{bucket}/{*key}",
                get(|| async { Bytes::from_static(b"object") }).put(|| async { "" }),
            )
            .layer(axum::middleware::from_fn(
                move |request: axum::extract::Request, next: axum::middleware::Next| {
                    let concurrent = concurrent.clone();
                    let peak = peak.clone();
                    async move {
                        let count = concurrent.fetch_add(1, Ordering::SeqCst) + 1;
                        peak.fetch_max(count, Ordering::SeqCst);
                        tokio::time::sleep(Duration::from_millis(20)).await;
                        let result = next.run(request).await;
                        concurrent.fetch_sub(1, Ordering::SeqCst);
                        result
                    }
                },
            ));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });
        let bucket = s3::Bucket::new(
            "bucket",
            s3::Region::Custom {
                region: "us-east-1".into(),
                endpoint,
            },
            s3::creds::Credentials::new(Some("access"), Some("secret"), None, None, None).unwrap(),
        )
        .unwrap()
        .with_path_style();
        let blobs = Blobs::S3 {
            bucket: Arc::from(bucket),
            prefix: "test".into(),
        };
        let id = "0".repeat(64);
        blobs.get(&id).await.unwrap();
        let (release, wait) = std::sync::mpsc::channel();
        let (ready, started) = tokio::sync::oneshot::channel();
        let blocker = tokio::task::spawn_blocking(move || {
            ready.send(()).unwrap();
            wait.recv().unwrap();
        });
        started.await.unwrap();
        let result = tokio::time::timeout(
            Duration::from_secs(5),
            futures_util::future::try_join_all((0..16).map(|_| async {
                blobs.put(&id, Bytes::from_static(b"upload")).await?;
                assert!(blobs.contains(&id).await?);
                assert_eq!(blobs.get(&id).await?, b"object"[..]);
                Ok::<_, anyhow::Error>(())
            })),
        )
        .await;
        release.send(()).unwrap();
        blocker.await.unwrap();
        result.unwrap().unwrap();
        assert!(maximum.load(Ordering::SeqCst) > 1);
        server.abort();
    });
}
