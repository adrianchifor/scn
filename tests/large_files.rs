use scn::{
    client::Client,
    content::{CHUNK_SIZE, manifest_entry, read_entry},
    model::*,
    server,
    store::{MAX_BLOB, Store},
    workspace::Workspace,
};
use std::{
    fs,
    io::{self, Read, Seek, SeekFrom, Write},
    path::Path,
    process::Command,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

struct Server {
    directory: tempfile::TempDir,
    url: String,
    task: tokio::task::JoinHandle<()>,
    uploads: Arc<AtomicUsize>,
}

impl Server {
    async fn start() -> Self {
        let directory = tempfile::tempdir().unwrap();
        let store = Store::open(&directory.path().join("store")).unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let uploads = Arc::new(AtomicUsize::new(0));
        let counter = uploads.clone();
        let router = server::router(store).layer(axum::middleware::from_fn(
            move |request: axum::extract::Request, next: axum::middleware::Next| {
                let counter = counter.clone();
                async move {
                    if request.method() == reqwest::Method::POST
                        && request.uri().path() == "/v1/blobs"
                    {
                        counter.fetch_add(1, Ordering::Relaxed);
                    }
                    next.run(request).await
                }
            },
        ));
        let task = tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });
        Self {
            directory,
            url,
            task,
            uploads,
        }
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}

fn cli(root: &Path, args: &[&str]) -> String {
    let output = Command::new(env!("CARGO_BIN_EXE_scn"))
        .current_dir(root)
        .env("SCN_AUTHOR", "tester")
        .args(args)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{args:?}: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).unwrap()
}

fn request(entry: FileEntry) -> CommitRequest {
    CommitRequest {
        parents: vec![],
        author: "tester".into(),
        message: "large file".into(),
        changes: Changes::from([("large".into(), Some(entry))]),
    }
}

#[derive(Default)]
struct Sink {
    size: u64,
    max_write: usize,
    last: Option<u8>,
}

impl Write for Sink {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        assert!(bytes.len() <= CHUNK_SIZE);
        self.size += bytes.len() as u64;
        self.max_write = self.max_write.max(bytes.len());
        self.last = bytes.last().copied();
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[test]
fn file_chunking_has_canonical_boundaries_and_detects_size_changes() {
    for size in [0, 1, MAX_BLOB as u64, MAX_BLOB as u64 + 1] {
        let mut read = 0;
        let entry = read_entry(io::repeat(0).take(size), size, false, |_, bytes| {
            assert!(
                bytes.len()
                    <= if size > MAX_BLOB as u64 {
                        CHUNK_SIZE
                    } else {
                        MAX_BLOB
                    }
            );
            read += bytes.len() as u64;
            Ok(())
        })
        .unwrap();
        assert_eq!(read, size);
        assert_eq!(entry.manifest.is_some(), size > MAX_BLOB as u64);
        scn::content::validate_entry(&entry).unwrap();
    }
    assert!(read_entry(&b"abc"[..], 2, false, |_, _| Ok(())).is_err());
    assert!(read_entry(&b"abc"[..], 4, false, |_, _| Ok(())).is_err());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn api_rejects_oversized_blobs_and_validates_chunk_manifests() {
    let server = Server::start().await;
    let client = Client::new(&server.url, &server.directory.path().join("cache")).unwrap();
    let response = reqwest::Client::new()
        .post(format!("{}/v1/blobs", server.url))
        .body(vec![0; MAX_BLOB + 1])
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 413);
    server.uploads.store(0, Ordering::Relaxed);
    let size = MAX_BLOB as u64 + 17;
    let entry = client
        .put_file(io::repeat(7).take(size), size, false, Some("large"))
        .await
        .unwrap();
    let manifest = entry.manifest.as_ref().unwrap();
    assert_eq!(manifest.chunks.len(), 5);
    assert_eq!(server.uploads.load(Ordering::Relaxed), 2);
    let retried = client
        .put_file(io::repeat(7).take(size), size, false, Some("large"))
        .await
        .unwrap();
    assert_eq!(retried, entry);
    assert_eq!(server.uploads.load(Ordering::Relaxed), 2);
    assert_eq!(
        fs::read_dir(server.directory.path().join("store/blobs"))
            .unwrap()
            .count(),
        2
    );
    let prepared: FileEntry = client
        .post(
            "/v1/files",
            &FileRequest {
                manifest: manifest.clone(),
                executable: false,
            },
        )
        .await
        .unwrap();
    assert_eq!(prepared, entry);
    let commit: Commit = client
        .post("/v1/commits", &request(entry.clone()))
        .await
        .unwrap();
    assert_eq!(client.tree(&commit.id, &[]).await.unwrap()["large"], entry);
    let mut sink = Sink::default();
    client.write_file(&entry, &mut sink).await.unwrap();
    assert_eq!(sink.size, size);
    assert_eq!(sink.last, Some(7));
    let modified = io::repeat(7).take(size - 1).chain(&b"\x08"[..]);
    let changed = client
        .put_file(modified, size, false, Some("large"))
        .await
        .unwrap();
    assert_ne!(changed.blob, entry.blob);
    assert_eq!(server.uploads.load(Ordering::Relaxed), 3);
    let exact = client.put_blob(vec![0; MAX_BLOB]).await.unwrap();
    assert!(client.has_blob(&exact).await.unwrap());
    let mut invalid = entry.clone();
    invalid.blob = "0".repeat(64);
    assert!(
        client
            .post::<Commit>("/v1/commits", &request(invalid))
            .await
            .is_err()
    );
    let mut invalid = manifest.clone();
    invalid.size += 1;
    assert!(
        client
            .post::<FileEntry>(
                "/v1/files",
                &FileRequest {
                    manifest: invalid,
                    executable: false
                }
            )
            .await
            .is_err()
    );
    let mut missing = manifest.clone();
    missing.chunks[0] = "0".repeat(64);
    assert!(
        client
            .post::<Commit>(
                "/v1/commits",
                &request(manifest_entry(missing, false).unwrap())
            )
            .await
            .is_err()
    );
    let mut invalid = manifest.clone();
    invalid.chunks.pop();
    assert!(manifest_entry(invalid, false).is_err());
    assert!(
        client
            .get::<Vec<BlameLine>>(
                &format!("/v1/blame/{}", commit.id),
                &[("path", "large".into())]
            )
            .await
            .is_err()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn large_files_stage_snapshot_checkout_show_and_import_without_whole_file_buffers() {
    let server = Server::start().await;
    let root = server.directory.path();
    cli(root, &["clone", &server.url, "work"]);
    let work = root.join("work");
    let file = work.join("large");
    let size = MAX_BLOB as u64 + 17;
    let mut output = fs::File::create(&file).unwrap();
    output.set_len(size).unwrap();
    output.seek(SeekFrom::End(-1)).unwrap();
    output.write_all(&[7]).unwrap();
    cli(&work, &["add", "large"]);
    assert!(cli(&work, &["diff", "--cached"]).contains("Binary files differ"));
    output.seek(SeekFrom::End(-1)).unwrap();
    output.write_all(&[8]).unwrap();
    cli(&work, &["commit", "-m", "large"]);
    assert!(cli(&work, &["diff"]).contains("Binary files differ"));
    let workspace = Workspace::find(&work).unwrap();
    let first = workspace.state.revision.clone().unwrap();
    let entry = workspace.state.files["large"].clone();
    let client = workspace.client().unwrap();
    let mut sink = Sink::default();
    client.write_file(&entry, &mut sink).await.unwrap();
    assert_eq!((sink.size, sink.last), (size, Some(7)));
    fs::write(&file, "small").unwrap();
    cli(&work, &["commit", "-am", "shrink"]);
    cli(&work, &["checkout", &first]);
    assert_eq!(fs::metadata(&file).unwrap().len(), size);
    assert!(cli(&work, &["status"]).contains("clean"));
    let shown = root.join("shown");
    let status = Command::new(env!("CARGO_BIN_EXE_scn"))
        .current_dir(&work)
        .args(["show", "HEAD", "large"])
        .stdout(fs::File::create(&shown).unwrap())
        .status()
        .unwrap();
    assert!(status.success());
    let mut shown_file = fs::File::open(&shown).unwrap();
    shown_file.seek(SeekFrom::End(-1)).unwrap();
    let mut last = [0];
    shown_file.read_exact(&mut last).unwrap();
    assert_eq!(last, [7]);
    let source = root.join("git");
    fs::create_dir(&source).unwrap();
    fs::copy(&file, source.join("large")).unwrap();
    for args in [
        &["init", "-b", "main"][..],
        &["add", "."],
        &["commit", "-m", "large"],
    ] {
        let output = Command::new("git")
            .current_dir(&source)
            .args([
                "-c",
                "commit.gpgsign=false",
                "-c",
                "user.name=Tester",
                "-c",
                "user.email=tester@example.com",
            ])
            .args(args)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    cli(root, &["import-git", "git", &server.url]);
    let imported = client.tree("main", &[]).await.unwrap();
    assert_eq!(imported["large"], entry);
    let mut bad_request = request(entry.clone());
    bad_request.parents = vec![first.clone()];
    bad_request.changes = Changes::from([("other".into(), Some(entry.clone()))]);
    let bad: Commit = client.post("/v1/commits", &bad_request).await.unwrap();
    let id = entry.manifest.unwrap().chunks[0].clone();
    fs::remove_file(work.join(".scn/cache").join(&id)).unwrap();
    fs::remove_file(root.join("store/blobs").join(&id)).unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_scn"))
        .current_dir(&work)
        .args(["checkout", &bad.id])
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert_eq!(
        Workspace::find(&work).unwrap().state.revision.as_deref(),
        Some(first.as_str())
    );
    assert!(!work.join("other").exists());
    assert_eq!(fs::metadata(&file).unwrap().len(), size);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "streams 5 GiB; run with --release --ignored"]
async fn five_gibibyte_files_stream_with_bounded_buffers() {
    let server = Server::start().await;
    let client = Client::new(&server.url, &server.directory.path().join("cache")).unwrap();
    let size = 5 * 1024 * 1024 * 1024u64;
    let entry = client
        .put_file(io::repeat(0).take(size), size, false, None)
        .await
        .unwrap();
    assert_eq!(entry.manifest.as_ref().unwrap().chunks.len(), 640);
    let commit: Commit = client
        .post("/v1/commits", &request(entry.clone()))
        .await
        .unwrap();
    assert_eq!(client.tree(&commit.id, &[]).await.unwrap()["large"], entry);
    let mut sink = Sink::default();
    client.write_file(&entry, &mut sink).await.unwrap();
    assert_eq!(sink.size, size);
    assert_eq!(sink.max_write, CHUNK_SIZE);
    assert_eq!(
        fs::read_dir(server.directory.path().join("store/blobs"))
            .unwrap()
            .count(),
        1
    );
    eprintln!("Streamed {size} bytes through 640 chunks, deduplicated to one stored blob");
}
