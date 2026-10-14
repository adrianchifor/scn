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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub manifest: Option<FileManifest>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct FileManifest {
    pub size: u64,
    pub chunks: Vec<String>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct FileRequest {
    #[serde(flatten)]
    pub manifest: FileManifest,
    #[serde(default)]
    pub executable: bool,
}

impl FileEntry {
    pub fn blobs(&self) -> impl Iterator<Item = &str> {
        self.manifest
            .as_ref()
            .map(|manifest| manifest.chunks.as_slice())
            .unwrap_or(std::slice::from_ref(&self.blob))
            .iter()
            .map(String::as_str)
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Commit {
    pub id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub git_id: Option<String>,
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
pub struct ImportCommitRequest {
    #[serde(flatten)]
    pub commit: CommitRequest,
    pub timestamp: u64,
    pub git_id: String,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct StoredCommit {
    pub commit: Commit,
    pub paths: Vec<String>,
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

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct Tag {
    pub name: String,
    pub target: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub annotation: Option<TagAnnotation>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub git_id: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct TagAnnotation {
    pub tagger: String,
    pub message: String,
    pub timestamp: u64,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct TagRequest {
    pub name: String,
    pub target: String,
    #[serde(default)]
    pub force: bool,
    #[serde(default)]
    pub annotation: Option<TagAnnotationRequest>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct TagAnnotationRequest {
    pub tagger: String,
    pub message: String,
}

#[derive(Debug)]
pub enum TagError {
    Exists(String),
    NotFound(String),
}

impl std::fmt::Display for TagError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Exists(name) => write!(
                formatter,
                "tag already exists: {name}; use --force to replace it"
            ),
            Self::NotFound(name) => write!(formatter, "tag not found: {name}"),
        }
    }
}

impl std::error::Error for TagError {}

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
    let mut result = String::with_capacity(64);
    for byte in Sha256::digest(bytes) {
        result.push(b"0123456789abcdef"[(byte >> 4) as usize] as char);
        result.push(b"0123456789abcdef"[(byte & 15) as usize] as char);
    }
    result
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

pub fn validate_git_id(id: &str) -> Result<()> {
    ensure!(
        matches!(id.len(), 40 | 64)
            && id
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase()),
        "invalid Git object id"
    );
    Ok(())
}

pub fn validate_tag_name(name: &str) -> Result<()> {
    ensure!(
        !name.is_empty()
            && !name.starts_with('-')
            && !name.ends_with('.')
            && !name.contains("..")
            && !name.contains("@{")
            && !name
                .bytes()
                .any(|byte| byte <= b' ' || byte == 127 || b"~^:?*[\\".contains(&byte))
            && name
                .split('/')
                .all(|part| !part.is_empty() && !part.starts_with('.') && !part.ends_with(".lock")),
        "invalid tag name: {name:?}"
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
    for (path, entry) in tree {
        crate::content::validate_entry(entry)?;
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
