use scn::{client::Client, model::*, server, store::Store, workspace::Workspace};
use std::{
    fs,
    path::Path,
    process::{Command, Output},
};

struct Server {
    directory: tempfile::TempDir,
    url: String,
    task: tokio::task::JoinHandle<()>,
}

impl Server {
    async fn start() -> Self {
        let directory = tempfile::tempdir().unwrap();
        let store = Store::open(&directory.path().join("store")).unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let task = tokio::spawn(async move {
            axum::serve(listener, server::router(store)).await.unwrap();
        });
        Self {
            directory,
            url,
            task,
        }
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}

fn run(root: &Path, args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_scn"))
        .current_dir(root)
        .env("SCN_AUTHOR", "tester")
        .args(args)
        .output()
        .unwrap()
}

fn scn(root: &Path, args: &[&str]) -> String {
    let output = run(root, args);
    assert!(
        output.status.success(),
        "{args:?}: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cli_workflow_including_history_binary_files_and_checkout_protection() {
    let server = Server::start().await;
    let root = server.directory.path();
    scn(root, &["clone", &server.url, "work"]);
    let work = root.join("work");
    fs::write(work.join("hello"), "first\nsecond\n").unwrap();
    fs::write(work.join("empty"), "").unwrap();
    fs::write(work.join("binary"), [0, 255, 1]).unwrap();
    fs::write(work.join(".scnignore"), "ignored\n").unwrap();
    fs::write(work.join("ignored"), "keep me").unwrap();
    assert!(scn(&work, &["status"]).contains("A hello"));
    assert!(!scn(&work, &["status"]).contains("A ignored"));
    let first = scn(&work, &["commit", "-m", "initial"])
        .split_whitespace()
        .next()
        .unwrap()
        .to_owned();
    scn(&work, &["land"]);
    assert!(scn(&work, &["status"]).contains("clean"));
    assert!(scn(&work, &["log"]).contains("initial"));
    assert_eq!(scn(&work, &["show", "HEAD", "hello"]), "first\nsecond\n");
    assert!(scn(&work, &["blame", "hello"]).contains("tester"));
    fs::write(work.join("hello"), "first\nchanged\n").unwrap();
    fs::remove_file(work.join("empty")).unwrap();
    assert!(scn(&work, &["diff"]).contains("+changed"));
    assert!(!run(&work, &["checkout", &first]).status.success());
    assert_eq!(
        fs::read_to_string(work.join("hello")).unwrap(),
        "first\nchanged\n"
    );
    let second = scn(&work, &["commit", "-m", "second"])
        .split_whitespace()
        .next()
        .unwrap()
        .to_owned();
    scn(&work, &["land"]);
    assert!(scn(&work, &["diff", &first, &second]).contains("+changed"));
    scn(&work, &["checkout", &first]);
    assert!(work.join("empty").exists());
    assert_eq!(fs::read(work.join("binary")).unwrap(), [0, 255, 1]);
    assert_eq!(fs::read_to_string(work.join("ignored")).unwrap(), "keep me");
    scn(&work, &["checkout", "main"]);
    assert!(!work.join("empty").exists());
    assert!(scn(&work, &["status"]).contains("clean"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sparse_commits_preserve_omitted_files_and_rebase_onto_main() {
    let server = Server::start().await;
    let root = server.directory.path();
    let mut full = Workspace::clone(&server.url, &root.join("full"), "main", vec![])
        .await
        .unwrap();
    let client = full.client().unwrap();
    fs::create_dir(full.root.join("src")).unwrap();
    fs::create_dir(full.root.join("docs")).unwrap();
    fs::write(full.root.join("src/main.rs"), "before").unwrap();
    fs::write(full.root.join("docs/readme"), "docs").unwrap();
    fs::write(full.root.join("src-extra"), "outside").unwrap();
    let initial = full
        .commit(&client, "initial".into(), "author".into())
        .await
        .unwrap();
    client.land(initial.id.clone()).await.unwrap();
    let mut sparse = Workspace::clone(
        &server.url,
        &root.join("sparse"),
        "main",
        vec!["src".into()],
    )
    .await
    .unwrap();
    assert!(!sparse.root.join("docs").exists());
    assert!(!sparse.root.join("src-extra").exists());
    assert_eq!(
        fs::read_dir(sparse.root.join(".scn/cache"))
            .unwrap()
            .count(),
        1
    );
    fs::write(sparse.root.join("src/main.rs"), "local").unwrap();
    let local = sparse
        .commit(&sparse.client().unwrap(), "local".into(), "agent".into())
        .await
        .unwrap();
    let tree = client.tree(&local.id, &[]).await.unwrap();
    assert_eq!(tree.len(), 3);
    assert_eq!(tree["docs/readme"], full.state.files["docs/readme"]);
    fs::write(full.root.join("docs/readme"), "upstream").unwrap();
    let upstream = full
        .commit(&client, "upstream".into(), "author".into())
        .await
        .unwrap();
    client.land(upstream.id).await.unwrap();
    assert!(client.land(local.id).await.is_err());
    scn(&sparse.root, &["rebase", "main"]);
    scn(&sparse.root, &["land"]);
    let main = client.main_ref().await.unwrap().unwrap();
    let tree = client.tree(&main, &[]).await.unwrap();
    assert_eq!(
        client.blob(&tree["src/main.rs"].blob).await.unwrap(),
        b"local"
    );
    assert_eq!(
        client.blob(&tree["docs/readme"].blob).await.unwrap(),
        b"upstream"
    );
    assert!(!sparse.root.join("docs").exists());
    scn(&sparse.root, &["checkout", "main", "--full"]);
    assert_eq!(
        fs::read_to_string(sparse.root.join("docs/readme")).unwrap(),
        "upstream"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn checkout_handles_file_directory_transitions_and_protects_ignored_files() {
    let server = Server::start().await;
    let root = server.directory.path();
    let mut work = Workspace::clone(&server.url, &root.join("work"), "main", vec![])
        .await
        .unwrap();
    let client = work.client().unwrap();
    fs::write(work.root.join("a"), "file").unwrap();
    fs::write(work.root.join(".scnignore"), "ignored\n").unwrap();
    let first = work
        .commit(&client, "file".into(), "a".into())
        .await
        .unwrap();
    fs::remove_file(work.root.join("a")).unwrap();
    fs::create_dir(work.root.join("a")).unwrap();
    fs::write(work.root.join("a/b"), "nested").unwrap();
    let second = work
        .commit(&client, "directory".into(), "a".into())
        .await
        .unwrap();
    work.checkout(&client, &first.id, None).await.unwrap();
    assert!(work.root.join("a").is_file());
    work.checkout(&client, &second.id, None).await.unwrap();
    assert_eq!(fs::read_to_string(work.root.join("a/b")).unwrap(), "nested");
    let blob = client.put_blob(b"server".to_vec()).await.unwrap();
    let target: Commit = client
        .post(
            "/v1/commits",
            &CommitRequest {
                parents: vec![second.id.clone()],
                message: "ignored path".into(),
                author: "a".into(),
                changes: Changes::from([(
                    "ignored".into(),
                    Some(FileEntry {
                        blob,
                        executable: false,
                    }),
                )]),
            },
        )
        .await
        .unwrap();
    fs::write(work.root.join("ignored"), "precious").unwrap();
    assert!(work.checkout(&client, &target.id, None).await.is_err());
    assert_eq!(
        fs::read_to_string(work.root.join("ignored")).unwrap(),
        "precious"
    );
    assert_eq!(work.state.revision.as_deref(), Some(second.id.as_str()));
}

#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn symlinks_are_rejected_and_executable_bits_round_trip() {
    use std::os::unix::{fs::PermissionsExt, fs::symlink};
    let server = Server::start().await;
    let root = server.directory.path();
    let mut work = Workspace::clone(&server.url, &root.join("work"), "main", vec![])
        .await
        .unwrap();
    let client = work.client().unwrap();
    fs::write(work.root.join("script"), "run").unwrap();
    fs::set_permissions(work.root.join("script"), fs::Permissions::from_mode(0o755)).unwrap();
    let first = work
        .commit(&client, "executable".into(), "a".into())
        .await
        .unwrap();
    fs::set_permissions(work.root.join("script"), fs::Permissions::from_mode(0o644)).unwrap();
    work.commit(&client, "plain".into(), "a".into())
        .await
        .unwrap();
    work.checkout(&client, &first.id, None).await.unwrap();
    assert_eq!(
        fs::metadata(work.root.join("script"))
            .unwrap()
            .permissions()
            .mode()
            & 0o111,
        0o111
    );
    fs::write(root.join("outside"), "safe").unwrap();
    fs::remove_file(work.root.join("script")).unwrap();
    symlink(root.join("outside"), work.root.join("script")).unwrap();
    assert!(work.scan().is_err());
    assert!(work.checkout(&client, &first.id, None).await.is_err());
    assert_eq!(fs::read_to_string(root.join("outside")).unwrap(), "safe");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn client_recovers_from_corrupt_cached_blobs() {
    let server = Server::start().await;
    let cache = server.directory.path().join("cache");
    let client = Client::new(&server.url, &cache).unwrap();
    let id = client.put_blob(b"content".to_vec()).await.unwrap();
    fs::write(cache.join(&id), "corrupt").unwrap();
    assert_eq!(client.blob(&id).await.unwrap(), b"content");
    assert_eq!(client.blob(&id).await.unwrap(), b"content");
}
