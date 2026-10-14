use redis::Commands;
use scn::{
    client::Client,
    cluster::Cluster,
    model::*,
    server, shard,
    storage::{Blobs, Local},
    store::{Store, decode_blob, is_zstd},
    workspace::Workspace,
};
use std::{
    collections::BTreeMap,
    fs,
    io::{self, Read},
    net::TcpListener,
    path::Path,
    process::{Child, Command, Stdio},
    sync::Arc,
    time::Duration,
};

struct Valkey {
    directory: tempfile::TempDir,
    child: Child,
    url: String,
    port: u16,
}

impl Valkey {
    fn start() -> Self {
        let directory = tempfile::tempdir().unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);
        let child = Self::spawn(directory.path(), port);
        Self {
            directory,
            child,
            url: format!("redis://127.0.0.1:{port}/"),
            port,
        }
    }

    fn spawn(directory: &Path, port: u16) -> Child {
        let binary = std::env::var("SCN_VALKEY_SERVER").unwrap_or_else(|_| "valkey-server".into());
        let mut child = Command::new(env!("CARGO_BIN_EXE_scn"))
            .args(["cache", "--binary", &binary, "--dir"])
            .arg(directory)
            .args([
                "--listen",
                &format!("127.0.0.1:{port}"),
                "--maxmemory",
                "64mb",
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        for _ in 0..100 {
            if redis::Client::open(format!("redis://127.0.0.1:{port}/"))
                .unwrap()
                .get_connection()
                .is_ok()
            {
                return child;
            }
            assert!(
                child.try_wait().unwrap().is_none(),
                "Valkey exited during startup"
            );
            std::thread::sleep(Duration::from_millis(50));
        }
        child.kill().unwrap();
        child.wait().unwrap();
        panic!("Valkey did not start");
    }

    fn restart(mut self) -> Self {
        let mut connection = redis::Client::open(self.url.as_str())
            .unwrap()
            .get_connection()
            .unwrap();
        let _: redis::RedisResult<()> = redis::cmd("SHUTDOWN").arg("SAVE").query(&mut connection);
        assert!(self.child.wait().unwrap().success());
        self.child = Self::spawn(self.directory.path(), self.port);
        self
    }
}

impl Drop for Valkey {
    fn drop(&mut self) {
        if let Ok(mut connection) = redis::Client::open(self.url.as_str())
            .unwrap()
            .get_connection()
        {
            let _: redis::RedisResult<()> =
                redis::cmd("SHUTDOWN").arg("NOSAVE").query(&mut connection);
        }
        let _ = self.child.wait();
    }
}

struct Running {
    url: String,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for Running {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn start(router: axum::Router) -> Running {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let task = tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    Running { url, task }
}

async fn start_shard(root: &Path) -> Running {
    start(shard::router(
        Local::open(root, Blobs::disk(&root.join("blobs")).unwrap()).unwrap(),
    ))
    .await
}

async fn controller(cache: &str, shards: Vec<String>, ttl: u64) -> Running {
    let cache = cache.to_owned();
    let cluster =
        tokio::task::spawn_blocking(move || Cluster::open(&cache, "test", &shards, ttl).unwrap())
            .await
            .unwrap();
    start(server::router(Store::with_backend(Arc::new(cluster)))).await
}

fn index(url: &str) -> redis::Connection {
    redis::Client::open(url).unwrap().get_connection().unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires valkey-server; set SCN_VALKEY_SERVER and run with --ignored"]
async fn controllers_share_shards_and_concurrent_main_updates_are_atomic() {
    let cache = tokio::task::spawn_blocking(Valkey::start).await.unwrap();
    let directory = tempfile::tempdir().unwrap();
    let shard_a = start_shard(&directory.path().join("a")).await;
    let shard_b = start_shard(&directory.path().join("b")).await;
    let first = controller(
        &cache.url,
        vec![shard_a.url.clone(), shard_b.url.clone()],
        60,
    )
    .await;
    let second = controller(&cache.url, vec![], 60).await;
    let mut work = Workspace::clone(&first.url, &directory.path().join("work"), "main", vec![])
        .await
        .unwrap();
    for n in 0..6 {
        fs::write(work.root.join(format!("file{n}")), format!("initial {n}\n")).unwrap();
    }
    let client = work.client().unwrap();
    let root = work
        .commit(&client, "root".into(), "author".into())
        .await
        .unwrap();
    client.land(root.id.clone()).await.unwrap();
    let other = Client::new(&second.url, &directory.path().join("cache")).unwrap();
    let tag = TagRequest {
        name: "release/v1".into(),
        target: root.id.clone(),
        force: false,
        annotation: None,
    };
    let (left_tag, right_tag) = tokio::join!(
        client.post::<Tag>("/v1/tags", &tag),
        other.post::<Tag>("/v1/tags", &tag)
    );
    assert_eq!(left_tag.is_ok() as usize + right_tag.is_ok() as usize, 1);
    assert_eq!(other.commit("release/v1").await.unwrap().id, root.id);
    assert_eq!(other.tags().await.unwrap(), client.tags().await.unwrap());
    assert_eq!(other.tree(&root.id, &[]).await.unwrap(), work.state.files);
    let local_a = Local::open(
        &directory.path().join("a"),
        Blobs::disk(&directory.path().join("a/blobs")).unwrap(),
    )
    .unwrap();
    let local_b = Local::open(
        &directory.path().join("b"),
        Blobs::disk(&directory.path().join("b/blobs")).unwrap(),
    )
    .unwrap();
    assert_eq!(local_a.changes(&root.id).unwrap().len(), 3);
    assert_eq!(local_b.changes(&root.id).unwrap().len(), 3);
    let left_blob = client
        .put_blob_at(b"left\n".to_vec(), Some("file0"))
        .await
        .unwrap();
    let right_blob = other
        .put_blob_at(b"right\n".to_vec(), Some("file1"))
        .await
        .unwrap();
    let create = |path: &str, blob: String| CommitRequest {
        parents: vec![root.id.clone()],
        author: "author".into(),
        message: path.into(),
        changes: Changes::from([(
            path.into(),
            Some(FileEntry {
                blob,
                executable: false,
                manifest: None,
            }),
        )]),
    };
    let left: Commit = client
        .post("/v1/commits", &create("file0", left_blob))
        .await
        .unwrap();
    let right: Commit = other
        .post("/v1/commits", &create("file1", right_blob))
        .await
        .unwrap();
    let (a, b) = tokio::join!(
        client.update_main(Some(root.id.clone()), left.id.clone()),
        other.update_main(Some(root.id.clone()), right.id.clone())
    );
    assert_eq!(a.is_ok() as usize + b.is_ok() as usize, 1);
    let merged: Commit = other
        .post(
            "/v1/merge",
            &IntegrateRequest {
                head: left.id.clone(),
                onto: right.id.clone(),
                author: "merger".into(),
            },
        )
        .await
        .unwrap();
    assert_eq!(merged.parents, [left.id.clone(), right.id.clone()]);
    let tree = client.tree(&merged.id, &[]).await.unwrap();
    assert_eq!(other.blob(&tree["file0"].blob).await.unwrap(), b"left\n");
    assert_eq!(other.blob(&tree["file1"].blob).await.unwrap(), b"right\n");
    let rebased: Commit = client
        .post(
            "/v1/rebase",
            &IntegrateRequest {
                head: right.id,
                onto: left.id,
                author: "author".into(),
            },
        )
        .await
        .unwrap();
    assert_eq!(other.tree(&rebased.id, &[]).await.unwrap(), tree);
    let mut connection = index(&cache.url);
    for key in ["paths", "blobs", "commits", "main", "paths:counts", "tags"] {
        assert_eq!(
            connection
                .ttl::<_, i64>(format!("scn:{{test}}:{key}"))
                .unwrap(),
            -1
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires valkey-server; set SCN_VALKEY_SERVER and run with --ignored"]
async fn new_shards_take_new_paths_without_rebalancing_and_index_survives_restart() {
    let cache = tokio::task::spawn_blocking(Valkey::start).await.unwrap();
    let directory = tempfile::tempdir().unwrap();
    let a = start_shard(&directory.path().join("a")).await;
    let b = start_shard(&directory.path().join("b")).await;
    let first = controller(&cache.url, vec![a.url.clone(), b.url.clone()], 60).await;
    let mut work = Workspace::clone(&first.url, &directory.path().join("work"), "main", vec![])
        .await
        .unwrap();
    let client = work.client().unwrap();
    for n in 0..8 {
        fs::write(work.root.join(format!("file{n}")), format!("file {n}")).unwrap();
    }
    let original = work
        .commit(&client, "root".into(), "a".into())
        .await
        .unwrap();
    client.land(original.id).await.unwrap();
    let before: BTreeMap<String, String> = index(&cache.url).hgetall("scn:{test}:paths").unwrap();
    let c = start_shard(&directory.path().join("c")).await;
    let second = controller(&cache.url, vec![c.url.clone()], 60).await;
    for n in 8..12 {
        fs::write(work.root.join(format!("file{n}")), format!("file {n}")).unwrap();
    }
    let next = work
        .commit(&client, "growth".into(), "a".into())
        .await
        .unwrap();
    client.land(next.id.clone()).await.unwrap();
    let tag: Tag = client
        .post(
            "/v1/tags",
            &TagRequest {
                name: "persisted".into(),
                target: next.id.clone(),
                force: false,
                annotation: Some(TagAnnotationRequest {
                    tagger: "author".into(),
                    message: "release".into(),
                }),
            },
        )
        .await
        .unwrap();
    let after: BTreeMap<String, String> = index(&cache.url).hgetall("scn:{test}:paths").unwrap();
    for (path, shard) in &before {
        assert_eq!(&after[path], shard);
    }
    for n in 8..12 {
        assert_eq!(after[&format!("file{n}")], c.url);
    }
    let counts: Vec<(String, f64)> = index(&cache.url)
        .zrange_withscores("scn:{test}:paths:counts", 0, -1)
        .unwrap();
    assert!(counts.iter().all(|(_, count)| *count == 4.0));
    let cache = tokio::task::spawn_blocking(move || cache.restart())
        .await
        .unwrap();
    let third = controller(&cache.url, vec![], 60).await;
    let other = Client::new(&third.url, &directory.path().join("cache")).unwrap();
    assert_eq!(other.main_ref().await.unwrap(), Some(next.id.clone()));
    assert_eq!(other.tag("persisted").await.unwrap(), tag);
    assert_eq!(other.commit("persisted").await.unwrap().id, next.id);
    assert_eq!(other.tree(&next.id, &[]).await.unwrap(), work.state.files);
    assert!(cache.directory.path().join("dump.rdb").exists());
    assert!(cache.directory.path().join("appendonlydir").exists());
    let peer = Client::new(&second.url, &directory.path().join("peer-cache")).unwrap();
    let (left, right) = tokio::join!(
        client.put_blob_at(b"same path left".to_vec(), Some("concurrent")),
        peer.put_blob_at(b"same path right".to_vec(), Some("concurrent"))
    );
    left.unwrap();
    right.unwrap();
    assert_eq!(
        index(&cache.url)
            .hlen::<_, usize>("scn:{test}:paths")
            .unwrap(),
        13
    );
    let counts: Vec<(String, f64)> = index(&cache.url)
        .zrange_withscores("scn:{test}:paths:counts", 0, -1)
        .unwrap();
    assert_eq!(
        counts
            .iter()
            .map(|(_, count)| *count as usize)
            .sum::<usize>(),
        13
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires valkey-server; set SCN_VALKEY_SERVER and run with --ignored"]
async fn compressed_blob_cache_is_shared_and_ttl_does_not_expire_the_index() {
    let cache = tokio::task::spawn_blocking(Valkey::start).await.unwrap();
    let directory = tempfile::tempdir().unwrap();
    let shard = start_shard(&directory.path().join("shard")).await;
    let first = controller(&cache.url, vec![shard.url.clone()], 3).await;
    let second = controller(&cache.url, vec![], 3).await;
    let client = Client::new(&first.url, &directory.path().join("cache")).unwrap();
    let id = client
        .put_blob_at(b"cached\n".to_vec(), Some("file"))
        .await
        .unwrap();
    let commit: Commit = client
        .post(
            "/v1/commits",
            &CommitRequest {
                parents: vec![],
                author: "a".into(),
                message: "root".into(),
                changes: Changes::from([(
                    "file".into(),
                    Some(FileEntry {
                        blob: id.clone(),
                        executable: false,
                        manifest: None,
                    }),
                )]),
            },
        )
        .await
        .unwrap();
    let tag: Tag = client
        .post(
            "/v1/tags",
            &TagRequest {
                name: "durable".into(),
                target: commit.id,
                force: false,
                annotation: None,
            },
        )
        .await
        .unwrap();
    let key = format!("scn:{{test}}:blob:{id}");
    assert!(index(&cache.url).ttl::<_, i64>(&key).unwrap() > 0);
    shard.task.abort();
    let http = reqwest::Client::new();
    let bytes = http
        .get(format!("{}/v1/blobs/{id}", second.url))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .bytes()
        .await
        .unwrap();
    assert!(is_zstd(&bytes));
    assert_eq!(decode_blob(&bytes, &id).unwrap(), b"cached\n");
    tokio::task::spawn_blocking(|| std::thread::sleep(Duration::from_millis(3200)))
        .await
        .unwrap();
    assert_eq!(index(&cache.url).ttl::<_, i64>(&key).unwrap(), -2);
    assert_eq!(
        index(&cache.url).ttl::<_, i64>("scn:{test}:paths").unwrap(),
        -1
    );
    assert!(
        !http
            .get(format!("{}/v1/blobs/{id}", first.url))
            .send()
            .await
            .unwrap()
            .status()
            .is_success()
    );
    let mut connection = index(&cache.url);
    redis::cmd("CONFIG")
        .arg("SET")
        .arg("maxmemory")
        .arg("4mb")
        .query::<()>(&mut connection)
        .unwrap();
    for n in 0..100 {
        connection
            .set_ex::<_, _, ()>(
                format!("scn:{{test}}:blob:pressure{n}"),
                vec![0u8; 128 * 1024],
                60,
            )
            .unwrap();
    }
    let stats: String = redis::cmd("INFO")
        .arg("stats")
        .query(&mut connection)
        .unwrap();
    let evicted: u64 = stats
        .lines()
        .find_map(|line| line.strip_prefix("evicted_keys:"))
        .unwrap()
        .parse()
        .unwrap();
    assert!(evicted > 0);
    assert_eq!(
        connection
            .hget::<_, _, String>("scn:{test}:paths", "file")
            .unwrap(),
        shard.url
    );
    assert_eq!(connection.ttl::<_, i64>("scn:{test}:paths").unwrap(), -1);
    assert_eq!(connection.ttl::<_, i64>("scn:{test}:tags").unwrap(), -1);
    assert_eq!(client.tag("durable").await.unwrap(), tag);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires valkey-server; set SCN_VALKEY_SERVER and run with --ignored"]
async fn large_file_chunks_span_shards_and_survive_controller_restart() {
    let cache = tokio::task::spawn_blocking(Valkey::start).await.unwrap();
    let directory = tempfile::tempdir().unwrap();
    let first = start_shard(&directory.path().join("first")).await;
    let second = start_shard(&directory.path().join("second")).await;
    let api = controller(&cache.url, vec![first.url.clone(), second.url.clone()], 60).await;
    let client = Client::new(&api.url, &directory.path().join("cache")).unwrap();
    let chunk = scn::content::CHUNK_SIZE as u64;
    let source = io::repeat(1)
        .take(chunk)
        .chain(io::repeat(2).take(chunk))
        .chain(io::repeat(1).take(chunk * 2))
        .chain(io::repeat(3).take(17));
    let entry = client
        .put_file(source, chunk * 4 + 17, false, Some("large"))
        .await
        .unwrap();
    let commit: Commit = client
        .post(
            "/v1/commits",
            &CommitRequest {
                parents: vec![],
                author: "tester".into(),
                message: "large".into(),
                changes: Changes::from([("large".into(), Some(entry.clone()))]),
            },
        )
        .await
        .unwrap();
    let owners: BTreeMap<String, String> = index(&cache.url).hgetall("scn:{test}:blobs").unwrap();
    assert_eq!(
        owners
            .values()
            .collect::<std::collections::BTreeSet<_>>()
            .len(),
        2
    );
    api.task.abort();
    let cache = tokio::task::spawn_blocking(move || cache.restart())
        .await
        .unwrap();
    let restarted = controller(&cache.url, vec![], 60).await;
    let other = Client::new(&restarted.url, &directory.path().join("other-cache")).unwrap();
    let tree = other.tree(&commit.id, &[]).await.unwrap();
    assert_eq!(tree["large"], entry);
    other
        .write_file(&tree["large"], &mut io::sink())
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "streams four concurrent 1 GiB files; requires Valkey; run with --release --ignored"]
async fn concurrent_gibibyte_files_round_trip_across_controllers_and_shards() {
    use sha2::{Digest, Sha256};
    use std::io::Write;

    struct Source {
        remaining: u64,
        state: u64,
        digest: Sha256,
    }

    impl Read for Source {
        fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
            let length = self.remaining.min(buffer.len() as u64) as usize;
            for word in buffer[..length].chunks_mut(8) {
                self.state ^= self.state << 13;
                self.state ^= self.state >> 7;
                self.state ^= self.state << 17;
                word.copy_from_slice(&self.state.to_le_bytes()[..word.len()]);
            }
            self.digest.update(&buffer[..length]);
            self.remaining -= length as u64;
            Ok(length)
        }
    }

    #[derive(Default)]
    struct Sink {
        digest: Sha256,
        written: u64,
    }

    impl Write for Sink {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            assert!(bytes.len() <= scn::content::CHUNK_SIZE);
            self.digest.update(bytes);
            self.written += bytes.len() as u64;
            Ok(bytes.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    let cache = tokio::task::spawn_blocking(Valkey::start).await.unwrap();
    let directory = tempfile::tempdir().unwrap();
    let a = start_shard(&directory.path().join("a")).await;
    let b = start_shard(&directory.path().join("b")).await;
    let first = controller(&cache.url, vec![a.url.clone(), b.url.clone()], 60).await;
    let second = controller(&cache.url, vec![], 60).await;
    let size = 1024 * 1024 * 1024;
    let started = std::time::Instant::now();
    let transfers = (0..4).map(|index| {
        let endpoint = if index % 2 == 0 {
            &first.url
        } else {
            &second.url
        };
        let cache = directory.path().join(format!("client-{index}"));
        async move {
            let client = Client::new(endpoint, &cache)?;
            let mut source = Source {
                remaining: size,
                state: index + 1,
                digest: Sha256::new(),
            };
            let entry = client.put_file(&mut source, size, false, None).await?;
            assert_eq!(entry.manifest.as_ref().unwrap().chunks.len(), 128);
            let prepared: FileEntry = client
                .post(
                    "/v1/files",
                    &FileRequest {
                        manifest: entry.manifest.clone().unwrap(),
                        executable: false,
                    },
                )
                .await?;
            assert_eq!(prepared, entry);
            let commit: Commit = client
                .post(
                    "/v1/commits",
                    &CommitRequest {
                        parents: vec![],
                        author: "tester".into(),
                        message: "concurrent upload".into(),
                        changes: Changes::from([(format!("file-{index}"), Some(entry.clone()))]),
                    },
                )
                .await?;
            assert_eq!(
                client.tree(&commit.id, &[]).await?[&format!("file-{index}")],
                entry
            );
            let mut output = Sink::default();
            client.write_file(&entry, &mut output).await?;
            assert_eq!(output.written, size);
            assert_eq!(output.digest.finalize(), source.digest.finalize());
            Ok::<_, anyhow::Error>(())
        }
    });
    futures_util::future::try_join_all(transfers).await.unwrap();
    eprintln!(
        "Four concurrent 1 GiB uploads, manifests, commits, and verified downloads in {:?}",
        started.elapsed()
    );
}
