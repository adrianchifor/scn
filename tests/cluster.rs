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
    for key in ["paths", "blobs", "commits", "main", "paths:counts"] {
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
}
