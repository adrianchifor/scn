use scn::{model::*, store::Store};
use std::{
    collections::BTreeMap,
    sync::{Arc, Barrier},
};

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
                    executable: false
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
}
