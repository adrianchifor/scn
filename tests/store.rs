use anyhow::Result;
use scn::{
    model::*,
    storage::{Backend, Blobs, Local},
    store::Store,
};
use std::{
    collections::BTreeMap,
    sync::{
        Arc, Barrier,
        atomic::{AtomicUsize, Ordering},
    },
};

struct CountingBackend {
    local: Local,
    commits: AtomicUsize,
    parents: AtomicUsize,
    blobs: AtomicUsize,
}

impl Backend for CountingBackend {
    fn put_blob(&self, id: &str, bytes: &[u8], size: usize, path: Option<&str>) -> Result<()> {
        self.local.put_blob(id, bytes, size, path)
    }
    fn blob(&self, id: &str) -> Result<Vec<u8>> {
        self.blobs.fetch_add(1, Ordering::Relaxed);
        self.local.blob(id)
    }
    fn has_blob(&self, id: &str) -> Result<bool> {
        self.local.has_blob(id)
    }
    fn blob_size(&self, id: &str) -> Result<Option<usize>> {
        self.local.blob_size(id)
    }
    fn get_commit(&self, id: &str) -> Result<Commit> {
        self.commits.fetch_add(1, Ordering::Relaxed);
        self.local.get_commit(id)
    }
    fn parents(&self, id: &str) -> Result<Vec<String>> {
        self.parents.fetch_add(1, Ordering::Relaxed);
        self.local.parents(id)
    }
    fn save_commit(&self, commit: &Commit) -> Result<()> {
        self.local.save_commit(commit)
    }
    fn main_ref(&self) -> Result<Option<String>> {
        self.local.main_ref()
    }
    fn update_main(&self, update: RefUpdate) -> Result<()> {
        self.local.update_main(update)
    }
    fn tags(&self) -> Result<Vec<Tag>> {
        self.local.tags()
    }
    fn tag(&self, name: &str) -> Result<Option<Tag>> {
        self.local.tag(name)
    }
    fn save_tag(&self, tag: &Tag, force: bool) -> Result<bool> {
        self.local.save_tag(tag, force)
    }
    fn delete_tag(&self, name: &str) -> Result<bool> {
        self.local.delete_tag(name)
    }
}

#[test]
fn tree_cache_avoids_history_replay_and_rebuilds_evicted_revisions() {
    let directory = tempfile::tempdir().unwrap();
    let backend = Arc::new(CountingBackend {
        local: Local::open(
            directory.path(),
            Blobs::disk(&directory.path().join("blobs")).unwrap(),
        )
        .unwrap(),
        commits: AtomicUsize::new(0),
        parents: AtomicUsize::new(0),
        blobs: AtomicUsize::new(0),
    });
    let store = Store::with_backend(backend.clone());
    let mut head = change(&store, &[], "root", &[("a", Some("initial"))]);
    let mut history = vec![head.id.clone()];
    for n in 0..20 {
        head = change(&store, &[&head.id], "next", &[("a", Some(&n.to_string()))]);
        history.push(head.id.clone());
    }
    assert_eq!(backend.commits.load(Ordering::Relaxed), 0);
    let reader = Store::with_backend(backend.clone());
    let expected = store.tree(&head.id).unwrap();
    assert_eq!(reader.tree(&head.id).unwrap(), expected);
    assert_eq!(backend.commits.load(Ordering::Relaxed), history.len());
    reader.clone().tree(&head.id).unwrap().clear();
    assert_eq!(reader.tree(&head.id).unwrap(), expected);
    assert_eq!(backend.commits.load(Ordering::Relaxed), history.len());
    let next = change(
        &store,
        &[&head.id],
        "delete",
        &[("a", None), ("b", Some("new"))],
    );
    assert_eq!(
        reader.tree(&next.id).unwrap(),
        store.tree(&next.id).unwrap()
    );
    assert_eq!(backend.commits.load(Ordering::Relaxed), history.len() + 1);
    for id in history {
        assert_eq!(reader.tree(&id).unwrap(), store.tree(&id).unwrap());
    }
    backend.parents.store(0, Ordering::Relaxed);
    backend.commits.store(0, Ordering::Relaxed);
    assert!(reader.is_ancestor(&head.id, &next.id).unwrap());
    assert_eq!(backend.parents.load(Ordering::Relaxed), 1);
    assert_eq!(backend.commits.load(Ordering::Relaxed), 0);
    let missing = "0".repeat(64);
    assert!(reader.is_ancestor(&missing, &missing).is_err());
}

#[test]
fn large_file_validation_uses_durable_sizes_without_reading_blob_contents() {
    use std::io::{self, Read};
    let directory = tempfile::tempdir().unwrap();
    let local = Local::open(
        directory.path(),
        Blobs::disk(&directory.path().join("blobs")).unwrap(),
    )
    .unwrap();
    let writer = Store::with_backend(Arc::new(local));
    let size = scn::store::MAX_BLOB as u64 + 17;
    let entry = scn::content::read_entry(io::repeat(3).take(size), size, false, |id, bytes| {
        assert_eq!(writer.put_blob(bytes).unwrap(), id);
        Ok(())
    })
    .unwrap();
    let backend = Arc::new(CountingBackend {
        local: Local::open(
            directory.path(),
            Blobs::disk(&directory.path().join("blobs")).unwrap(),
        )
        .unwrap(),
        commits: AtomicUsize::new(0),
        parents: AtomicUsize::new(0),
        blobs: AtomicUsize::new(0),
    });
    let store = Store::with_backend(backend.clone());
    assert_eq!(
        store
            .prepare_file(FileRequest {
                manifest: entry.manifest.clone().unwrap(),
                executable: false
            })
            .unwrap(),
        entry
    );
    store
        .create_commit(CommitRequest {
            parents: vec![],
            author: "tester".into(),
            message: "large file".into(),
            changes: Changes::from([("large".into(), Some(entry.clone()))]),
        })
        .unwrap();
    assert_eq!(backend.blobs.load(Ordering::Relaxed), 0);
    let id = entry.blobs().next().unwrap();
    let size = backend.local.blob_size(id).unwrap().unwrap();
    assert_eq!(size, scn::content::CHUNK_SIZE);
    std::fs::remove_file(directory.path().join("blobs").join(id)).unwrap();
    assert!(
        store
            .prepare_file(FileRequest {
                manifest: entry.manifest.unwrap(),
                executable: false
            })
            .is_err()
    );
}

fn change(
    store: &Store,
    parents: &[&str],
    message: &str,
    files: &[(&str, Option<&str>)],
) -> Commit {
    let changes = files
        .iter()
        .map(|(path, text)| {
            (
                (*path).to_owned(),
                text.map(|text| FileEntry {
                    blob: store.put_blob(text.as_bytes()).unwrap(),
                    executable: false,
                    manifest: None,
                }),
            )
        })
        .collect();
    store
        .create_commit(CommitRequest {
            parents: parents.iter().map(|id| (*id).to_owned()).collect(),
            author: message.to_owned(),
            message: message.to_owned(),
            changes,
        })
        .unwrap()
}

#[test]
fn changesets_store_deltas_and_survive_restart() {
    let directory = tempfile::tempdir().unwrap();
    let store = Store::open(directory.path()).unwrap();
    let root = change(
        &store,
        &[],
        "root",
        &[("a", Some("one\n")), ("dir/b", Some("two\n"))],
    );
    let next = change(&store, &[&root.id], "next", &[("a", Some("changed\n"))]);
    let last = change(&store, &[&next.id], "delete", &[("dir/b", None)]);
    assert_eq!(next.changes.len(), 1);
    assert_eq!(store.tree(&next.id).unwrap().len(), 2);
    assert_eq!(store.tree(&last.id).unwrap().len(), 1);
    let blob = store.put_blob(b"changed\n").unwrap();
    assert_eq!(blob, store.tree(&last.id).unwrap()["a"].blob);
    assert_eq!(store.blob(&blob).unwrap(), b"changed\n");
    let reopened = Store::open(directory.path()).unwrap();
    assert_eq!(
        reopened.tree(&next.id).unwrap(),
        store.tree(&next.id).unwrap()
    );
    let history = reopened.log(&last.id, 10, Some("dir/b")).unwrap();
    assert_eq!(
        history
            .iter()
            .map(|c| c.message.as_str())
            .collect::<Vec<_>>(),
        ["delete", "root"]
    );
    let diff = reopened.diff(&root.id, &last.id).unwrap();
    assert_eq!(diff.len(), 2);
    assert!(diff[0].patch.as_ref().unwrap().contains("+changed"));
}

#[test]
fn compare_and_swap_allows_only_one_concurrent_landing() {
    let directory = tempfile::tempdir().unwrap();
    let store = Store::open(directory.path()).unwrap();
    let root = change(&store, &[], "root", &[("a", Some("one"))]);
    store
        .update_main(RefUpdate {
            expected: None,
            target: root.id.clone(),
        })
        .unwrap();
    let left = change(&store, &[&root.id], "left", &[("b", Some("left"))]);
    let right = change(&store, &[&root.id], "right", &[("c", Some("right"))]);
    let barrier = Arc::new(Barrier::new(2));
    let handles: Vec<_> = [left.id.clone(), right.id.clone()]
        .into_iter()
        .map(|target| {
            let store = store.clone();
            let barrier = barrier.clone();
            let expected = Some(root.id.clone());
            std::thread::spawn(move || {
                barrier.wait();
                store.update_main(RefUpdate { expected, target }).is_ok()
            })
        })
        .collect();
    assert_eq!(
        handles
            .into_iter()
            .map(|h| h.join().unwrap() as usize)
            .sum::<usize>(),
        1
    );
    let winner = store.main_ref().unwrap().unwrap();
    let loser = if winner == left.id {
        &right.id
    } else {
        &left.id
    };
    assert!(
        store
            .update_main(RefUpdate {
                expected: Some(winner.clone()),
                target: loser.clone()
            })
            .is_err()
    );
    let merged = store.merge(&winner, loser, "merger").unwrap();
    assert_eq!(merged.parents.len(), 2);
    assert_eq!(store.tree(&merged.id).unwrap().len(), 3);
    store
        .update_main(RefUpdate {
            expected: Some(winner),
            target: merged.id.clone(),
        })
        .unwrap();
    assert_eq!(store.main_ref().unwrap(), Some(merged.id));
}

#[test]
fn rebase_replays_linear_changes_and_preserves_upstream_files() {
    let directory = tempfile::tempdir().unwrap();
    let store = Store::open(directory.path()).unwrap();
    let root = change(
        &store,
        &[],
        "root",
        &[("a", Some("root")), ("b", Some("root"))],
    );
    let local = change(&store, &[&root.id], "local", &[("a", Some("local"))]);
    let local2 = change(&store, &[&local.id], "delete", &[("b", None)]);
    let upstream = change(&store, &[&root.id], "upstream", &[("c", Some("upstream"))]);
    let rebased = store.rebase(&local2.id, &upstream.id, "author").unwrap();
    assert_ne!(rebased.id, local2.id);
    assert!(store.is_ancestor(&upstream.id, &rebased.id).unwrap());
    assert_eq!(store.merge_base(&local2.id, &upstream.id).unwrap(), root.id);
    let tree = store.tree(&rebased.id).unwrap();
    assert!(!tree.contains_key("b"));
    assert_eq!(store.blob(&tree["a"].blob).unwrap(), b"local");
    assert_eq!(store.blob(&tree["c"].blob).unwrap(), b"upstream");
    assert_eq!(
        store
            .log(&rebased.id, 3, None)
            .unwrap()
            .iter()
            .map(|c| c.message.as_str())
            .collect::<Vec<_>>(),
        ["delete", "local", "upstream"]
    );
}

#[test]
fn conflicts_leave_existing_changesets_unchanged() {
    let directory = tempfile::tempdir().unwrap();
    let store = Store::open(directory.path()).unwrap();
    let root = change(&store, &[], "root", &[("a", Some("root"))]);
    let left = change(&store, &[&root.id], "left", &[("a", Some("left"))]);
    let right = change(&store, &[&root.id], "right", &[("a", Some("right"))]);
    let before = store.tree(&left.id).unwrap();
    assert!(
        store
            .merge(&left.id, &right.id, "author")
            .unwrap_err()
            .to_string()
            .contains("conflicts: a")
    );
    assert!(store.rebase(&left.id, &right.id, "author").is_err());
    assert_eq!(store.tree(&left.id).unwrap(), before);
    let deleted = change(&store, &[&root.id], "deleted", &[("a", None)]);
    assert!(store.merge(&deleted.id, &right.id, "author").is_err());
}

#[test]
fn blame_tracks_unchanged_lines_through_insertions_and_deletions() {
    let directory = tempfile::tempdir().unwrap();
    let store = Store::open(directory.path()).unwrap();
    let root = change(
        &store,
        &[],
        "alice",
        &[("a", Some("first\nsecond\nthird\n"))],
    );
    let next = change(
        &store,
        &[&root.id],
        "bob",
        &[("a", Some("first\nnew\nthird\nlast"))],
    );
    let lines = store.blame(&next.id, "a").unwrap();
    assert_eq!(
        lines.iter().map(|l| l.author.as_str()).collect::<Vec<_>>(),
        ["alice", "bob", "alice", "bob"]
    );
    assert_eq!(lines[3].line, "last");
}

#[test]
fn invalid_objects_and_file_directory_collisions_are_rejected() {
    let directory = tempfile::tempdir().unwrap();
    let store = Store::open(directory.path()).unwrap();
    let root = change(&store, &[], "root", &[("a", Some("root"))]);
    for path in [
        "../escape",
        "/absolute",
        ".scn/state.json",
        "dir/.git/config",
        "a//b",
        "a/./b",
        "a\\b",
        "",
    ] {
        assert!(validate_path(path).is_err(), "{path}");
    }
    assert!(store.compressed_blob("../../etc/passwd").is_err());
    let entry = FileEntry {
        blob: store.put_blob(b"content").unwrap(),
        executable: false,
        manifest: None,
    };
    let request = |changes| CommitRequest {
        parents: vec![root.id.clone()],
        author: "a".into(),
        message: "m".into(),
        changes,
    };
    assert!(
        store
            .create_commit(request(BTreeMap::from([(
                "a/b".into(),
                Some(entry.clone())
            )])))
            .is_err()
    );
    assert!(
        store
            .create_commit(request(BTreeMap::from([("missing".into(), None)])))
            .is_err()
    );
    assert!(
        store
            .create_commit(request(BTreeMap::from([(
                "missing".into(),
                Some(FileEntry {
                    blob: "0".repeat(64),
                    executable: false,
                    manifest: None,
                })
            )])))
            .is_err()
    );
    let replacement = store
        .create_commit(request(BTreeMap::from([
            ("a".into(), None),
            ("a/b".into(), Some(entry)),
        ])))
        .unwrap();
    assert!(store.tree(&replacement.id).unwrap().contains_key("a/b"));
}

#[test]
fn blobs_use_only_zstandard_and_verify_contents() {
    let directory = tempfile::tempdir().unwrap();
    let store = Store::open(directory.path()).unwrap();
    let id = store.put_blob(b"content\n").unwrap();
    assert!(scn::store::is_zstd(&store.compressed_blob(&id).unwrap()));
    assert_eq!(store.blob(&id).unwrap(), b"content\n");
    assert!(scn::store::decode_blob(b"invalid frame", &id).is_err());
    let other = scn::store::encode_blob(b"different contents").unwrap();
    assert!(scn::store::decode_blob(&other, &id).is_err());
    std::fs::write(directory.path().join("blobs").join(&id), other).unwrap();
    assert!(store.blob(&id).is_err());
    assert!(store.compressed_blob(&id).is_err());
}

#[test]
fn tags_are_atomic_persistent_and_require_explicit_replacement() {
    let directory = tempfile::tempdir().unwrap();
    let store = Store::open(directory.path()).unwrap();
    let root = change(&store, &[], "root", &[("a", Some("root"))]);
    let next = change(&store, &[&root.id], "next", &[("a", Some("next"))]);
    let barrier = Arc::new(Barrier::new(2));
    let handles: Vec<_> = [&root.id, &next.id]
        .into_iter()
        .map(|target| {
            let store = store.clone();
            let barrier = barrier.clone();
            let target = target.clone();
            std::thread::spawn(move || {
                barrier.wait();
                store
                    .create_tag(TagRequest {
                        name: "release/v1".into(),
                        target,
                        force: false,
                        annotation: None,
                    })
                    .is_ok()
            })
        })
        .collect();
    assert_eq!(
        handles
            .into_iter()
            .map(|handle| handle.join().unwrap() as usize)
            .sum::<usize>(),
        1
    );
    let tag = store.tag("release/v1").unwrap();
    assert_eq!(
        Store::open(directory.path())
            .unwrap()
            .tag(&tag.name)
            .unwrap(),
        tag
    );
    let annotated = store
        .create_tag(TagRequest {
            name: "release/v1".into(),
            target: next.id.clone(),
            force: true,
            annotation: Some(TagAnnotationRequest {
                tagger: "Adrian".into(),
                message: "Release".into(),
            }),
        })
        .unwrap();
    assert_eq!(store.resolve("release/v1").unwrap(), next.id);
    assert!(annotated.annotation.unwrap().timestamp > 0);
    assert!(store.main_ref().unwrap().is_none());
    store
        .create_tag(TagRequest {
            name: "main".into(),
            target: root.id.clone(),
            force: false,
            annotation: None,
        })
        .unwrap();
    assert_eq!(store.resolve("refs/tags/main").unwrap(), root.id);
    assert!(store.resolve("main").is_err());
    assert_eq!(
        store
            .tags()
            .unwrap()
            .iter()
            .map(|tag| tag.name.as_str())
            .collect::<Vec<_>>(),
        ["main", "release/v1"]
    );
    for name in [
        "", "-bad", "a..b", "a@{b", "a b", "a\n", "a\\b", "a~b", "a^b", "a:b", "a?b", "a*b", "a[b",
        "/a", "a/", "a//b", ".hidden", "a/.b", "a.lock", "a.lock/b", "a.",
    ] {
        assert!(validate_tag_name(name).is_err(), "{name:?}");
    }
    for name in ["v1.0", "releases/α+β#%", "main", "HEAD", &root.id] {
        validate_tag_name(name).unwrap();
    }
    store.delete_tag("release/v1").unwrap();
    assert!(store.tag("release/v1").is_err());
    assert!(store.delete_tag("release/v1").is_err());
}
