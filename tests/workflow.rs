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

fn git(root: &Path, args: &[&str]) -> Vec<u8> {
    git_env(root, args, &[])
}

fn git_env(root: &Path, args: &[&str], environment: &[(&str, &str)]) -> Vec<u8> {
    let output = Command::new("git")
        .current_dir(root)
        .args(["-c", "commit.gpgsign=false"])
        .args(args)
        .env("GIT_AUTHOR_NAME", "Original Author")
        .env("GIT_AUTHOR_EMAIL", "author@example.com")
        .env("GIT_COMMITTER_NAME", "Original Committer")
        .env("GIT_COMMITTER_EMAIL", "committer@example.com")
        .env("GIT_AUTHOR_DATE", "1234567890 +0530")
        .env("GIT_COMMITTER_DATE", "1234567900 +0530")
        .envs(environment.iter().copied())
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{args:?}: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    output.stdout
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn git_import_preserves_history_merges_metadata_and_committed_contents() {
    let server = Server::start().await;
    let root = server.directory.path();
    let source = root.join("git");
    fs::create_dir(&source).unwrap();
    git(&source, &["init", "-b", "main"]);
    git(
        &source,
        &["commit", "--allow-empty", "--allow-empty-message", "-m", ""],
    );
    fs::write(source.join("a"), "initial\n").unwrap();
    fs::write(source.join("binary\tfile"), [0, 255, 1]).unwrap();
    fs::write(source.join("empty"), "").unwrap();
    fs::create_dir(source.join("dir")).unwrap();
    fs::write(source.join("dir/file"), "nested").unwrap();
    git(&source, &["add", "."]);
    git(&source, &["commit", "-m", "Add files\n\nOriginal body"]);
    git(&source, &["branch", "side"]);
    fs::write(source.join("a"), "main\n").unwrap();
    fs::remove_file(source.join("dir/file")).unwrap();
    fs::remove_dir(source.join("dir")).unwrap();
    fs::write(source.join("dir"), "now a file").unwrap();
    git(&source, &["add", "."]);
    git(&source, &["commit", "-m", "Main changes"]);
    git(&source, &["checkout", "side"]);
    fs::write(source.join("binary\tfile"), [0, 2, 254]).unwrap();
    git(&source, &["mv", "empty", "renamed"]);
    fs::write(source.join("script"), "run\n").unwrap();
    git(&source, &["add", "."]);
    git(&source, &["update-index", "--chmod=+x", "script"]);
    git(&source, &["commit", "-m", "Side changes"]);
    git(&source, &["checkout", "--", "script"]);
    git(&source, &["checkout", "main"]);
    git(&source, &["merge", "--no-ff", "side", "-m", "Merge side"]);
    git(&source, &["tag", "import-this"]);
    fs::write(source.join("later"), "not in selected revision").unwrap();
    git(&source, &["add", "."]);
    git(&source, &["commit", "-m", "Later change"]);
    fs::write(source.join("a"), "uncommitted").unwrap();
    fs::write(source.join("untracked"), "untracked").unwrap();
    let status = git(&source, &["status", "--porcelain", "-z"]);
    let output = scn(
        root,
        &[
            "import-git",
            source.to_str().unwrap(),
            &server.url,
            "--rev",
            "import-this",
        ],
    );
    assert_eq!(git(&source, &["status", "--porcelain", "-z"]), status);
    let mapping: std::collections::HashMap<_, _> = output
        .lines()
        .filter_map(|line| line.split_once(" -> "))
        .filter(|(left, _)| *left != "main")
        .collect();
    assert_eq!(mapping.len(), 5);
    let client = Client::new(&server.url, &root.join("cache")).unwrap();
    for (original_id, imported_id) in &mapping {
        let original =
            String::from_utf8(git(&source, &["cat-file", "commit", original_id])).unwrap();
        let (headers, message) = original.split_once("\n\n").unwrap();
        let imported = client.commit(imported_id).await.unwrap();
        assert_eq!(imported.git_id.as_deref(), Some(*original_id));
        assert_eq!(imported.message, message);
        assert_eq!(imported.author, "Original Author <author@example.com>");
        assert_eq!(imported.timestamp, 1_234_567_890_000_000_000);
        let parents: Vec<_> = headers
            .lines()
            .filter_map(|line| line.strip_prefix("parent "))
            .map(|id| mapping[id].to_owned())
            .collect();
        assert_eq!(imported.parents, parents);
        let tree = client.tree(imported_id, &[]).await.unwrap();
        let files = git(&source, &["ls-tree", "-r", "-z", original_id]);
        let mut count = 0;
        for record in files.split(|b| *b == 0).filter(|r| !r.is_empty()) {
            let record = std::str::from_utf8(record).unwrap();
            let (metadata, path) = record.split_once('\t').unwrap();
            let fields: Vec<_> = metadata.split_whitespace().collect();
            assert_eq!(
                client.blob(&tree[path].blob).await.unwrap(),
                git(&source, &["cat-file", "blob", fields[2]])
            );
            assert_eq!(tree[path].executable, fields[0] == "100755");
            count += 1;
        }
        assert_eq!(tree.len(), count);
    }
    let main = client.commit("main").await.unwrap();
    assert_eq!(main.parents.len(), 2);
    scn(root, &["clone", &server.url, "imported"]);
    assert_eq!(
        fs::read_to_string(root.join("imported/a")).unwrap(),
        "main\n"
    );
    assert!(!root.join("imported/later").exists());
    assert!(!root.join("imported/untracked").exists());
    let retry = run(root, &["import-git", source.to_str().unwrap(), &server.url]);
    assert!(!retry.status.success());
    assert!(String::from_utf8_lossy(&retry.stderr).contains("empty main ref"));
    assert_eq!(client.main_ref().await.unwrap(), Some(main.id));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn git_import_supports_bare_repositories_and_empty_histories() {
    let server = Server::start().await;
    let root = server.directory.path();
    let source = root.join("source");
    fs::create_dir(&source).unwrap();
    git(&source, &["init", "-b", "main"]);
    git(
        &source,
        &["commit", "--allow-empty", "--allow-empty-message", "-m", ""],
    );
    git(root, &["clone", "--bare", "source", "bare"]);
    scn(root, &["import-git", "bare", &server.url]);
    let client = Client::new(&server.url, &root.join("cache")).unwrap();
    let main = client.commit("main").await.unwrap();
    assert!(main.parents.is_empty());
    assert!(main.message.is_empty());
    assert!(client.tree(&main.id, &[]).await.unwrap().is_empty());
    scn(root, &["clone", &server.url, "work"]);
    assert!(scn(&root.join("work"), &["status"]).contains("clean"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn failed_git_import_keeps_main_empty_and_can_be_retried() {
    let server = Server::start().await;
    let root = server.directory.path();
    let source = root.join("source");
    fs::create_dir(&source).unwrap();
    git(&source, &["init", "-b", "main"]);
    fs::write(source.join("file"), "content").unwrap();
    git(&source, &["add", "."]);
    git(&source, &["commit", "-m", "Root"]);
    let original_root = String::from_utf8(git(&source, &["rev-parse", "HEAD"]))
        .unwrap()
        .trim()
        .to_owned();
    let cacheinfo = format!("160000,{original_root},submodule");
    git(
        &source,
        &["update-index", "--add", "--cacheinfo", &cacheinfo],
    );
    git(&source, &["commit", "-m", "Add submodule"]);
    let client = Client::new(&server.url, &root.join("cache")).unwrap();
    for _ in 0..2 {
        let failed = run(root, &["import-git", "source", &server.url]);
        assert!(!failed.status.success());
        assert!(String::from_utf8_lossy(&failed.stderr).contains("submodules are unsupported"));
        assert!(client.main_ref().await.unwrap().is_none());
    }
    scn(
        root,
        &["import-git", "source", &server.url, "--rev", &original_root],
    );
    assert!(client.main_ref().await.unwrap().is_some());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn git_import_keeps_distinct_commits_with_identical_author_and_file_metadata() {
    let server = Server::start().await;
    let root = server.directory.path();
    let source = root.join("source");
    fs::create_dir(&source).unwrap();
    git(&source, &["init", "-b", "main"]);
    git(&source, &["commit", "--allow-empty", "-m", "Root"]);
    git(&source, &["branch", "side"]);
    git(&source, &["commit", "--allow-empty", "-m", "Same change"]);
    git(&source, &["checkout", "side"]);
    git_env(
        &source,
        &["commit", "--allow-empty", "-m", "Same change"],
        &[("GIT_COMMITTER_NAME", "Different Committer")],
    );
    git(&source, &["checkout", "main"]);
    git(&source, &["merge", "--no-ff", "side", "-m", "Merge"]);
    scn(root, &["import-git", "source", &server.url]);
    let client = Client::new(&server.url, &root.join("cache")).unwrap();
    let tip = client.commit("main").await.unwrap();
    assert_eq!(tip.parents.len(), 2);
    let left = client.commit(&tip.parents[0]).await.unwrap();
    let right = client.commit(&tip.parents[1]).await.unwrap();
    assert_eq!(left.message, right.message);
    assert_eq!(left.author, right.author);
    assert_eq!(left.timestamp, right.timestamp);
    assert_ne!(left.id, right.id);
    assert_ne!(left.git_id, right.git_id);
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
    assert!(scn(&work, &["status"]).contains("?? hello"));
    assert!(!scn(&work, &["status"]).contains("?? ignored"));
    scn(&work, &["add", "."]);
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
    scn(&work, &["add", "."]);
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn git_style_staging_push_fetch_and_pull_preserve_local_and_remote_changes() {
    let server = Server::start().await;
    let root = server.directory.path();
    scn(root, &["clone", &server.url, "first"]);
    scn(root, &["clone", &server.url, "second"]);
    let first = root.join("first");
    let second = root.join("second");
    fs::write(first.join("a"), "staged\n").unwrap();
    assert!(!run(&first, &["commit", "-m", "unstaged"]).status.success());
    scn(&first, &["add", "a"]);
    fs::write(first.join("a"), "later\n").unwrap();
    assert!(scn(&first, &["status"]).contains("AM a"));
    assert!(scn(&first, &["diff", "--cached"]).contains("+staged"));
    assert!(scn(&first, &["diff"]).contains("+later"));
    scn(&first, &["commit", "-m", "initial"]);
    assert_eq!(scn(&first, &["show", "HEAD", "a"]), "staged\n");
    assert_eq!(fs::read_to_string(first.join("a")).unwrap(), "later\n");
    scn(&first, &["push", "origin", "main"]);
    scn(&second, &["fetch"]);
    assert!(!second.join("a").exists());
    assert_eq!(scn(&second, &["show", "origin/main", "a"]), "staged\n");
    scn(&second, &["pull", "--ff-only"]);
    assert_eq!(fs::read_to_string(second.join("a")).unwrap(), "staged\n");
    fs::write(first.join("a"), "staged\n").unwrap();
    fs::write(first.join("local"), "local\n").unwrap();
    scn(&first, &["add", "local"]);
    scn(&first, &["commit", "-m", "local"]);
    fs::write(second.join("a"), "remote\n").unwrap();
    fs::write(second.join("untracked"), "stay untracked").unwrap();
    scn(&second, &["commit", "-am", "remote"]);
    assert!(scn(&second, &["status"]).contains("?? untracked"));
    scn(&second, &["push"]);
    assert!(!run(&first, &["push"]).status.success());
    assert!(!run(&first, &["pull", "--ff-only"]).status.success());
    assert_eq!(fs::read_to_string(first.join("local")).unwrap(), "local\n");
    scn(&first, &["pull", "--rebase"]);
    assert_eq!(fs::read_to_string(first.join("a")).unwrap(), "remote\n");
    assert_eq!(fs::read_to_string(first.join("local")).unwrap(), "local\n");
    scn(&first, &["push"]);
    assert!(scn(&first, &["status"]).contains("clean"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn staging_respects_gitignore_subdirectories_and_deletions() {
    let server = Server::start().await;
    let root = server.directory.path();
    scn(root, &["clone", &server.url, "work"]);
    let work = root.join("work");
    fs::write(work.join(".gitignore"), "ignored\n").unwrap();
    fs::write(work.join("ignored"), "ignored").unwrap();
    fs::create_dir(work.join("src")).unwrap();
    fs::write(work.join("src/file"), "first").unwrap();
    scn(&work, &["add", "-A"]);
    scn(&work, &["commit", "-m", "initial"]);
    assert!(scn(&work, &["status"]).contains("clean"));
    fs::write(work.join("pending"), "staged").unwrap();
    scn(&work, &["add", "pending"]);
    fs::write(work.join("pending"), "updated after staging").unwrap();
    fs::write(work.join(".gitignore"), "ignored\npending\n").unwrap();
    scn(&work, &["commit", "-am", "update staged file"]);
    assert_eq!(
        scn(&work, &["show", "HEAD", "pending"]),
        "updated after staging"
    );
    fs::remove_file(work.join("src/file")).unwrap();
    scn(&work.join("src"), &["add", "."]);
    assert!(scn(&work, &["status"]).contains("D  src/file"));
    scn(&work, &["commit", "-m", "delete"]);
    assert!(scn(&work, &["status"]).contains("clean"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn controller_endpoint_is_saved_and_can_be_changed_without_losing_work() {
    let server = Server::start().await;
    let root = server.directory.path();
    scn(root, &["clone", &server.url, "work"]);
    let work = root.join("work");
    assert_eq!(
        scn(&work, &["remote", "get-url", "origin"]).trim(),
        server.url
    );
    assert!(scn(&work, &["remote", "-v"]).contains(&server.url));
    fs::write(work.join("pending"), "staged").unwrap();
    scn(&work, &["add", "pending"]);
    assert!(
        !run(&work, &["remote", "set-url", "origin", "file:///tmp"])
            .status
            .success()
    );
    assert_eq!(
        scn(&work, &["remote", "get-url", "origin"]).trim(),
        server.url
    );
    scn(
        &work,
        &["remote", "set-url", "origin", "http://127.0.0.1:1/"],
    );
    assert_eq!(
        scn(&work, &["remote", "get-url", "origin"]).trim(),
        "http://127.0.0.1:1"
    );
    assert!(scn(&work, &["status"]).contains("A  pending"));
    scn(&work, &["remote", "set-url", "origin", &server.url]);
    scn(&work, &["commit", "-m", "keep staged work"]);
    scn(&work, &["push"]);
    assert!(scn(&work, &["status"]).contains("clean"));
}
