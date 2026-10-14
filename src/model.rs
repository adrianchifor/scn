use anyhow::{Result, bail, ensure};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;

pub type Tree = BTreeMap<String, FileEntry>;
pub type Changes = BTreeMap<String, Option<FileEntry>>;

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct FileEntry {
    pub blob: String,
    pub executable: bool,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Commit {
    pub id: String,
    pub parents: Vec<String>,
    pub author: String,
    pub message: String,
    pub timestamp: u64,
    pub changes: Changes,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct CommitRequest {
    pub parents: Vec<String>,
    pub author: String,
    pub message: String,
    pub changes: Changes,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct RefValue {
    pub target: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct RefUpdate {
    pub expected: Option<String>,
    pub target: String,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct IntegrateRequest {
    pub head: String,
    pub onto: String,
    pub author: String,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct DiffEntry {
    pub path: String,
    pub before: Option<FileEntry>,
    pub after: Option<FileEntry>,
    pub patch: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct BlameLine {
    pub commit: String,
    pub author: String,
    pub line: String,
}

pub fn hash(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

pub fn validate_id(id: &str) -> Result<()> {
    ensure!(
        id.len() == 64
            && id
                .bytes()
                .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase()),
        "invalid object id: {id}"
    );
    Ok(())
}

pub fn validate_path(path: &str) -> Result<()> {
    ensure!(
        !path.is_empty() && !path.contains(['\\', '\0', '\n', '\r']),
        "invalid path: {path:?}"
    );
    for part in path.split('/') {
        ensure!(
            !matches!(part, "" | "." | ".." | ".scn" | ".git"),
            "invalid path: {path:?}"
        );
    }
    Ok(())
}

pub fn validate_tree(tree: &Tree) -> Result<()> {
    for path in tree.keys() {
        validate_path(path)?;
        for (index, _) in path.match_indices('/') {
            if tree.contains_key(&path[..index]) {
                bail!("file/directory conflict: {path}");
            }
        }
    }
    Ok(())
}

pub fn selected(path: &str, sparse: &[String]) -> bool {
    sparse.is_empty()
        || sparse.iter().any(|prefix| {
            path == prefix
                || path
                    .strip_prefix(prefix)
                    .is_some_and(|rest| rest.starts_with('/'))
        })
}

pub fn changes_between(before: &Tree, after: &Tree) -> Changes {
    let mut changes = Changes::new();
    for path in before.keys().chain(after.keys()) {
        if before.get(path) != after.get(path) {
            changes.insert(path.clone(), after.get(path).cloned());
        }
    }
    changes
}

pub fn apply(tree: &mut Tree, changes: &Changes) {
    for (path, entry) in changes {
        if let Some(entry) = entry {
            tree.insert(path.clone(), entry.clone());
        } else {
            tree.remove(path);
        }
    }
}
