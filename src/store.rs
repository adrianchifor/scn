use crate::model::*;
use anyhow::{Context, Result, bail, ensure};
use flate2::{Compression, read::ZlibDecoder, write::ZlibEncoder};
use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};
use similar::{ChangeTag, TextDiff};
use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    fs,
    io::{Read, Write},
    path::{Path, PathBuf},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

pub const MAX_BLOB: usize = 32 * 1024 * 1024;

#[derive(Clone)]
pub struct Store {
    root: PathBuf,
}

impl Store {
    pub fn open(root: &Path) -> Result<Self> {
        fs::create_dir_all(root.join("blobs"))?;
        let store = Self {
            root: root.to_owned(),
        };
        store.connection()?.execute_batch(
            "PRAGMA journal_mode=WAL;
             CREATE TABLE IF NOT EXISTS commits (
                 sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                 id TEXT NOT NULL UNIQUE,
                 data TEXT NOT NULL
             );
             CREATE TABLE IF NOT EXISTS refs (name TEXT PRIMARY KEY, target TEXT NOT NULL);",
        )?;
        Ok(store)
    }

    fn connection(&self) -> Result<Connection> {
        let connection = Connection::open(self.root.join("scn.db"))?;
        connection.busy_timeout(Duration::from_secs(5))?;
        Ok(connection)
    }

    pub fn put_blob(&self, bytes: &[u8]) -> Result<String> {
        ensure!(bytes.len() <= MAX_BLOB, "blob exceeds 32 MiB limit");
        let id = hash(bytes);
        let path = self.root.join("blobs").join(&id);
        if !path.exists() {
            let mut encoder = ZlibEncoder::new(Vec::new(), Compression::default());
            encoder.write_all(bytes)?;
            let mut temp = tempfile::NamedTempFile::new_in(self.root.join("blobs"))?;
            temp.write_all(&encoder.finish()?)?;
            temp.as_file().sync_all()?;
            temp.persist(&path)?;
        }
        Ok(id)
    }

    pub fn compressed_blob(&self, id: &str) -> Result<Vec<u8>> {
        validate_id(id)?;
        fs::read(self.root.join("blobs").join(id)).context("blob not found")
    }

    pub fn blob(&self, id: &str) -> Result<Vec<u8>> {
        decode_blob(&self.compressed_blob(id)?, id)
    }

    pub fn get_commit(&self, id: &str) -> Result<Commit> {
        validate_id(id)?;
        let data: String = self
            .connection()?
            .query_row("SELECT data FROM commits WHERE id = ?1", [id], |row| {
                row.get(0)
            })
            .context("changeset not found")?;
        Ok(serde_json::from_str(&data)?)
    }

    pub fn main_ref(&self) -> Result<Option<String>> {
        Ok(self
            .connection()?
            .query_row("SELECT target FROM refs WHERE name = 'main'", [], |row| {
                row.get(0)
            })
            .optional()?)
    }

    pub fn resolve(&self, revision: &str) -> Result<String> {
        if revision == "main" {
            self.main_ref()?
                .context("main is empty; commit and land a changeset first")
        } else {
            Ok(self.get_commit(revision)?.id)
        }
    }

    pub fn tree(&self, id: &str) -> Result<Tree> {
        let mut chain = Vec::new();
        let mut current = Some(id.to_owned());
        while let Some(id) = current {
            let commit = self.get_commit(&id)?;
            current = commit.parents.first().cloned();
            chain.push(commit);
        }
        let mut tree = Tree::new();
        for commit in chain.into_iter().rev() {
            apply(&mut tree, &commit.changes);
        }
        Ok(tree)
    }

    pub fn create_commit(&self, request: CommitRequest) -> Result<Commit> {
        ensure!(
            request.parents.len() <= 2,
            "at most two parents are supported"
        );
        ensure!(
            request.parents.len() != 2 || request.parents[0] != request.parents[1],
            "duplicate parents"
        );
        ensure!(
            !request.message.trim().is_empty(),
            "message must not be empty"
        );
        ensure!(
            !request.author.trim().is_empty(),
            "author must not be empty"
        );
        for parent in &request.parents {
            self.get_commit(parent)?;
        }
        let mut tree = match request.parents.first() {
            Some(parent) => self.tree(parent)?,
            None => Tree::new(),
        };
        for (path, entry) in &request.changes {
            validate_path(path)?;
            if let Some(entry) = entry {
                validate_id(&entry.blob)?;
                ensure!(
                    self.root.join("blobs").join(&entry.blob).is_file(),
                    "missing blob: {}",
                    entry.blob
                );
            } else {
                ensure!(
                    tree.contains_key(path),
                    "cannot delete missing path: {path}"
                );
            }
        }
        let before = tree.clone();
        apply(&mut tree, &request.changes);
        validate_tree(&tree)?;
        let changes = changes_between(&before, &tree);
        ensure!(
            !changes.is_empty() || request.parents.len() == 2,
            "nothing to commit"
        );
        let mut commit = Commit {
            id: String::new(),
            parents: request.parents,
            author: request.author,
            message: request.message,
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)?
                .as_nanos()
                .try_into()?,
            changes,
        };
        commit.id = hash(&serde_json::to_vec(&commit)?);
        self.connection()?.execute(
            "INSERT INTO commits (id, data) VALUES (?1, ?2)",
            params![commit.id, serde_json::to_string(&commit)?],
        )?;
        Ok(commit)
    }

    pub fn update_main(&self, update: RefUpdate) -> Result<()> {
        self.get_commit(&update.target)?;
        if let Some(expected) = &update.expected {
            ensure!(
                self.is_ancestor(expected, &update.target)?,
                "main updates must be fast-forward; merge or rebase first"
            );
        }
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let actual: Option<String> = transaction
            .query_row("SELECT target FROM refs WHERE name = 'main'", [], |row| {
                row.get(0)
            })
            .optional()?;
        ensure!(
            actual == update.expected,
            "main moved; merge or rebase onto main and retry"
        );
        transaction.execute(
            "INSERT INTO refs (name, target) VALUES ('main', ?1)
             ON CONFLICT(name) DO UPDATE SET target = excluded.target",
            [&update.target],
        )?;
        transaction.commit()?;
        Ok(())
    }

    fn ancestors(&self, head: &str) -> Result<BTreeMap<String, usize>> {
        let mut result = BTreeMap::new();
        let mut queue = VecDeque::from([(head.to_owned(), 0)]);
        while let Some((id, distance)) = queue.pop_front() {
            if result.contains_key(&id) {
                continue;
            }
            queue.extend(
                self.get_commit(&id)?
                    .parents
                    .into_iter()
                    .map(|parent| (parent, distance + 1)),
            );
            result.insert(id, distance);
        }
        Ok(result)
    }

    pub fn is_ancestor(&self, ancestor: &str, head: &str) -> Result<bool> {
        Ok(self.ancestors(head)?.contains_key(ancestor))
    }

    pub fn merge_base(&self, left: &str, right: &str) -> Result<String> {
        let left = self.ancestors(left)?;
        let right = self.ancestors(right)?;
        let common: BTreeSet<_> = left
            .keys()
            .filter(|id| right.contains_key(*id))
            .cloned()
            .collect();
        let mut best = common.clone();
        for id in &common {
            for parent in self.get_commit(id)?.parents {
                best.remove(&parent);
            }
        }
        ensure!(
            best.len() <= 1,
            "multiple merge bases; criss-cross merges are unsupported"
        );
        best.into_iter()
            .next()
            .context("changesets have no common ancestor")
    }

    pub fn log(&self, head: &str, limit: usize, path: Option<&str>) -> Result<Vec<Commit>> {
        if let Some(path) = path {
            validate_path(path)?;
        }
        let mut commits = Vec::new();
        let mut current = Some(head.to_owned());
        while let Some(id) = current {
            if commits.len() >= limit {
                break;
            }
            let commit = self.get_commit(&id)?;
            current = commit.parents.first().cloned();
            if path.is_none_or(|path| commit.changes.contains_key(path)) {
                commits.push(commit);
            }
        }
        Ok(commits)
    }

    pub fn diff(&self, from: &str, to: &str) -> Result<Vec<DiffEntry>> {
        let before = self.tree(from)?;
        let after = self.tree(to)?;
        changes_between(&before, &after)
            .into_iter()
            .map(|(path, entry)| {
                let old = before.get(&path).cloned();
                let old_bytes = old
                    .as_ref()
                    .map(|entry| self.blob(&entry.blob))
                    .transpose()?
                    .unwrap_or_default();
                let new_bytes = entry
                    .as_ref()
                    .map(|entry| self.blob(&entry.blob))
                    .transpose()?
                    .unwrap_or_default();
                Ok(DiffEntry {
                    patch: patch(&path, &old_bytes, &new_bytes),
                    path,
                    before: old,
                    after: entry,
                })
            })
            .collect()
    }

    pub fn merge(&self, head: &str, other: &str, author: &str) -> Result<Commit> {
        if self.is_ancestor(other, head)? {
            return self.get_commit(head);
        }
        if self.is_ancestor(head, other)? {
            return self.get_commit(other);
        }
        let base = self.merge_base(head, other)?;
        let ours = self.tree(head)?;
        let merged = merge_trees(&self.tree(&base)?, &ours, &self.tree(other)?)?;
        self.create_commit(CommitRequest {
            parents: vec![head.to_owned(), other.to_owned()],
            author: author.to_owned(),
            message: format!("Merge {} into {}", &other[..12], &head[..12]),
            changes: changes_between(&ours, &merged),
        })
    }

    pub fn rebase(&self, head: &str, onto: &str, author: &str) -> Result<Commit> {
        if self.is_ancestor(head, onto)? {
            return self.get_commit(onto);
        }
        if self.is_ancestor(onto, head)? {
            return self.get_commit(head);
        }
        let base = self.merge_base(head, onto)?;
        let mut chain = Vec::new();
        let mut current = head.to_owned();
        while current != base {
            let commit = self.get_commit(&current)?;
            ensure!(
                commit.parents.len() == 1,
                "rebase requires a linear first-parent history; use merge"
            );
            current = commit.parents[0].clone();
            chain.push(commit);
        }
        let mut target = self.get_commit(onto)?;
        for original in chain.into_iter().rev() {
            let before = self.tree(&target.id)?;
            let merged = merge_trees(
                &self.tree(&original.parents[0])?,
                &before,
                &self.tree(&original.id)?,
            )?;
            let changes = changes_between(&before, &merged);
            if !changes.is_empty() {
                target = self.create_commit(CommitRequest {
                    parents: vec![target.id],
                    author: author.to_owned(),
                    message: original.message,
                    changes,
                })?;
            }
        }
        Ok(target)
    }

    pub fn blame(&self, head: &str, path: &str) -> Result<Vec<BlameLine>> {
        validate_path(path)?;
        ensure!(
            self.tree(head)?.contains_key(path),
            "path not found: {path}"
        );
        let mut history = self.log(head, usize::MAX, Some(path))?;
        history.reverse();
        let mut text = String::new();
        let mut lines: Vec<BlameLine> = Vec::new();
        for commit in history {
            let bytes = commit.changes[path]
                .as_ref()
                .map(|entry| self.blob(&entry.blob))
                .transpose()?
                .unwrap_or_default();
            let next = String::from_utf8(bytes).context("blame requires UTF-8 text")?;
            ensure!(!next.contains('\0'), "blame requires text");
            let mut updated = Vec::new();
            for change in TextDiff::from_lines(&text, &next).iter_all_changes() {
                match change.tag() {
                    ChangeTag::Equal => updated.push(lines[change.old_index().unwrap()].clone()),
                    ChangeTag::Insert => updated.push(BlameLine {
                        commit: commit.id.clone(),
                        author: commit.author.clone(),
                        line: change.value().to_owned(),
                    }),
                    ChangeTag::Delete => {}
                }
            }
            text = next;
            lines = updated;
        }
        Ok(lines)
    }
}

pub fn decode_blob(compressed: &[u8], id: &str) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    ZlibDecoder::new(compressed)
        .take((MAX_BLOB + 1) as u64)
        .read_to_end(&mut bytes)?;
    ensure!(
        bytes.len() <= MAX_BLOB && hash(&bytes) == id,
        "blob integrity check failed: {id}"
    );
    Ok(bytes)
}

pub fn patch(path: &str, before: &[u8], after: &[u8]) -> Option<String> {
    let (Ok(before), Ok(after)) = (std::str::from_utf8(before), std::str::from_utf8(after)) else {
        return None;
    };
    if before.contains('\0') || after.contains('\0') {
        return None;
    }
    Some(
        TextDiff::from_lines(before, after)
            .unified_diff()
            .header(&format!("a/{path}"), &format!("b/{path}"))
            .to_string(),
    )
}

fn merge_trees(base: &Tree, ours: &Tree, theirs: &Tree) -> Result<Tree> {
    let mut result = ours.clone();
    let mut conflicts = Vec::new();
    for (path, entry) in changes_between(base, theirs) {
        if ours.get(&path) == base.get(&path) || ours.get(&path) == entry.as_ref() {
            apply(&mut result, &Changes::from([(path, entry)]));
        } else {
            conflicts.push(path);
        }
    }
    if !conflicts.is_empty() {
        bail!("conflicts: {}", conflicts.join(", "));
    }
    validate_tree(&result)?;
    Ok(result)
}
