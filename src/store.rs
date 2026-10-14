use crate::model::*;
use crate::storage::{Backend, Blobs, Local};
use anyhow::{Context, Result, bail, ensure};
use similar::{ChangeTag, TextDiff};
use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    io::Read,
    path::Path,
    sync::{Arc, Mutex},
    time::{SystemTime, UNIX_EPOCH},
};

pub const MAX_BLOB: usize = 32 * 1024 * 1024;
const TREE_CACHE_LIMIT: usize = 32 * 1024 * 1024;

#[derive(Default)]
struct TreeCache {
    entries: VecDeque<(String, Arc<Tree>, usize)>,
    size: usize,
}

impl TreeCache {
    fn get(&mut self, id: &str) -> Option<Arc<Tree>> {
        let index = self.entries.iter().position(|(key, _, _)| key == id)?;
        let entry = self.entries.remove(index)?;
        let tree = Arc::clone(&entry.1);
        self.entries.push_back(entry);
        Some(tree)
    }

    fn insert(&mut self, id: &str, tree: Tree) {
        if self.get(id).is_some() {
            return;
        }
        let size = id.len()
            + tree
                .iter()
                .map(|(path, entry)| {
                    path.capacity()
                        + entry.blob.capacity()
                        + entry.manifest.as_ref().map_or(0, |manifest| {
                            manifest.chunks.capacity() * std::mem::size_of::<String>()
                                + manifest.chunks.iter().map(String::capacity).sum::<usize>()
                        })
                        + std::mem::size_of::<(String, FileEntry)>()
                        + 32
                })
                .sum::<usize>();
        if size > TREE_CACHE_LIMIT {
            return;
        }
        while self.entries.len() >= 8 || self.size + size > TREE_CACHE_LIMIT {
            if let Some((_, _, size)) = self.entries.pop_front() {
                self.size -= size;
            }
        }
        self.size += size;
        self.entries
            .push_back((id.to_owned(), Arc::new(tree), size));
    }
}

#[derive(Clone)]
pub struct Store {
    backend: Arc<dyn Backend>,
    trees: Arc<Mutex<TreeCache>>,
}

impl Store {
    pub fn open(root: &Path) -> Result<Self> {
        Self::with_blobs(root, Blobs::disk(&root.join("blobs"))?)
    }

    pub fn with_blobs(root: &Path, blobs: Blobs) -> Result<Self> {
        Ok(Self::with_backend(Arc::new(Local::open(root, blobs)?)))
    }

    pub fn with_backend(backend: Arc<dyn Backend>) -> Self {
        Self {
            backend,
            trees: Arc::default(),
        }
    }

    pub fn put_blob(&self, bytes: &[u8]) -> Result<String> {
        self.put_blob_at(bytes, None)
    }

    pub fn put_blob_at(&self, bytes: &[u8], path: Option<&str>) -> Result<String> {
        ensure!(bytes.len() <= MAX_BLOB, "blob exceeds 32 MiB limit");
        if let Some(path) = path {
            validate_path(path)?;
        }
        let id = hash(bytes);
        self.backend
            .put_blob(&id, &encode_blob(bytes)?, bytes.len(), path)?;
        Ok(id)
    }

    pub fn compressed_blob(&self, id: &str) -> Result<Vec<u8>> {
        validate_id(id)?;
        let bytes = self.backend.blob(id)?;
        decode_blob(&bytes, id)?;
        Ok(bytes)
    }

    pub fn has_blob(&self, id: &str) -> Result<bool> {
        validate_id(id)?;
        self.backend.has_blob(id)
    }

    pub fn blob(&self, id: &str) -> Result<Vec<u8>> {
        validate_id(id)?;
        decode_blob(&self.backend.blob(id)?, id)
    }

    pub fn prepare_file(&self, request: FileRequest) -> Result<FileEntry> {
        let entry = crate::content::manifest_entry(request.manifest, request.executable)?;
        self.check_file(&entry, &mut BTreeMap::new())?;
        Ok(entry)
    }

    fn check_file<'a>(
        &self,
        entry: &'a FileEntry,
        checked: &mut BTreeMap<&'a str, usize>,
    ) -> Result<()> {
        crate::content::validate_entry(entry)?;
        if let Some(manifest) = &entry.manifest {
            for (index, id) in manifest.chunks.iter().enumerate() {
                let length = match checked.get(id.as_str()).copied() {
                    Some(length) => length,
                    None => {
                        let length = self
                            .backend
                            .blob_size(id)?
                            .with_context(|| format!("missing blob: {id}"))?;
                        checked.insert(id, length);
                        length
                    }
                };
                let expected = (manifest.size - index as u64 * crate::content::CHUNK_SIZE as u64)
                    .min(crate::content::CHUNK_SIZE as u64);
                ensure!(length as u64 == expected, "file chunk size mismatch: {id}");
            }
        } else if !checked.contains_key(entry.blob.as_str()) {
            let size = self
                .backend
                .blob_size(&entry.blob)?
                .with_context(|| format!("missing blob: {}", entry.blob))?;
            checked.insert(&entry.blob, size);
        }
        Ok(())
    }

    pub fn get_commit(&self, id: &str) -> Result<Commit> {
        validate_id(id)?;
        self.backend.get_commit(id)
    }

    pub fn main_ref(&self) -> Result<Option<String>> {
        self.backend.main_ref()
    }

    pub fn resolve(&self, revision: &str) -> Result<String> {
        if revision == "main" {
            self.main_ref()?
                .context("main is empty; commit and land a changeset first")
        } else if let Some(name) = revision.strip_prefix("refs/tags/") {
            Ok(self.tag(name)?.target)
        } else if validate_id(revision).is_ok() {
            Ok(self.get_commit(revision)?.id)
        } else {
            Ok(self.tag(revision)?.target)
        }
    }

    pub fn tags(&self) -> Result<Vec<Tag>> {
        let mut tags = self.backend.tags()?;
        tags.sort_by(|left, right| left.name.cmp(&right.name));
        Ok(tags)
    }

    pub fn tag(&self, name: &str) -> Result<Tag> {
        validate_tag_name(name)?;
        self.backend
            .tag(name)?
            .ok_or_else(|| TagError::NotFound(name.to_owned()).into())
    }

    pub fn create_tag(&self, request: TagRequest) -> Result<Tag> {
        validate_tag_name(&request.name)?;
        let target = self.resolve(&request.target)?;
        let annotation = request
            .annotation
            .map(|annotation| {
                ensure!(
                    !annotation.message.trim().is_empty(),
                    "tag message must not be empty"
                );
                Ok::<_, anyhow::Error>(TagAnnotation {
                    tagger: annotation.tagger,
                    message: annotation.message,
                    timestamp: SystemTime::now()
                        .duration_since(UNIX_EPOCH)?
                        .as_nanos()
                        .try_into()?,
                })
            })
            .transpose()?;
        let tag = Tag {
            name: request.name,
            target,
            annotation,
            git_id: None,
        };
        self.write_tag(tag, request.force, false)
    }

    pub fn import_tag(&self, tag: Tag) -> Result<Tag> {
        self.write_tag(tag, false, true)
    }

    fn write_tag(&self, tag: Tag, force: bool, imported: bool) -> Result<Tag> {
        validate_tag_name(&tag.name)?;
        self.parents(&tag.target)?;
        if let Some(annotation) = &tag.annotation {
            ensure!(
                !annotation.tagger.trim().is_empty(),
                "tagger must not be empty"
            );
        }
        if let Some(id) = &tag.git_id {
            validate_git_id(id)?;
        }
        if !self.backend.save_tag(&tag, force)?
            && !(imported && self.backend.tag(&tag.name)?.as_ref() == Some(&tag))
        {
            return Err(TagError::Exists(tag.name).into());
        }
        Ok(tag)
    }

    pub fn delete_tag(&self, name: &str) -> Result<()> {
        validate_tag_name(name)?;
        ensure!(
            self.backend.delete_tag(name)?,
            TagError::NotFound(name.to_owned())
        );
        Ok(())
    }

    pub fn tree(&self, id: &str) -> Result<Tree> {
        let mut chain = Vec::new();
        let mut current = Some(id.to_owned());
        let mut tree = Tree::new();
        while let Some(id) = current {
            let cached = self.trees.lock().unwrap().get(&id);
            if let Some(cached) = cached {
                tree = (*cached).clone();
                break;
            }
            let commit = self.get_commit(&id)?;
            current = commit.parents.first().cloned();
            chain.push(commit);
        }
        if chain.is_empty() {
            return Ok(tree);
        }
        for commit in chain.into_iter().rev() {
            apply(&mut tree, &commit.changes);
        }
        self.trees.lock().unwrap().insert(id, tree.clone());
        Ok(tree)
    }

    pub fn create_commit(&self, request: CommitRequest) -> Result<Commit> {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)?
            .as_nanos()
            .try_into()?;
        self.write_commit(request, timestamp, None)
    }

    pub fn import_commit(&self, request: ImportCommitRequest) -> Result<Commit> {
        validate_git_id(&request.git_id)?;
        self.write_commit(request.commit, request.timestamp, Some(request.git_id))
    }

    fn write_commit(
        &self,
        request: CommitRequest,
        timestamp: u64,
        git_id: Option<String>,
    ) -> Result<Commit> {
        let imported = git_id.is_some();
        ensure!(
            request.parents.len() <= 2,
            "at most two parents are supported"
        );
        ensure!(
            request.parents.len() != 2 || request.parents[0] != request.parents[1],
            "duplicate parents"
        );
        ensure!(
            imported || !request.message.trim().is_empty(),
            "message must not be empty"
        );
        ensure!(
            !request.author.trim().is_empty(),
            "author must not be empty"
        );
        for parent in request.parents.iter().skip(1) {
            self.parents(parent)?;
        }
        let mut tree = match request.parents.first() {
            Some(parent) => self.tree(parent)?,
            None => Tree::new(),
        };
        let mut checked_blobs = BTreeMap::new();
        for (path, entry) in &request.changes {
            validate_path(path)?;
            if let Some(entry) = entry {
                if tree.get(path) == Some(entry) {
                    continue;
                }
                self.check_file(entry, &mut checked_blobs)?;
            } else {
                ensure!(
                    tree.contains_key(path),
                    "cannot delete missing path: {path}"
                );
            }
        }
        let changes: Changes = request
            .changes
            .into_iter()
            .filter(|(path, entry)| tree.get(path) != entry.as_ref())
            .collect();
        apply(&mut tree, &changes);
        validate_tree(&tree)?;
        ensure!(
            imported || !changes.is_empty() || request.parents.len() == 2,
            "nothing to commit"
        );
        let mut commit = Commit {
            id: String::new(),
            git_id,
            parents: request.parents,
            author: request.author,
            message: request.message,
            timestamp,
            changes,
        };
        commit.id = hash(&serde_json::to_vec(&commit)?);
        self.backend.save_commit(&commit)?;
        self.trees.lock().unwrap().insert(&commit.id, tree);
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
        self.backend.update_main(update)
    }

    fn parents(&self, id: &str) -> Result<Vec<String>> {
        validate_id(id)?;
        self.backend.parents(id)
    }

    fn ancestors(&self, head: &str) -> Result<BTreeMap<String, Vec<String>>> {
        let mut result = BTreeMap::new();
        let mut queue = VecDeque::from([head.to_owned()]);
        while let Some(id) = queue.pop_front() {
            if result.contains_key(&id) {
                continue;
            }
            let parents = self.parents(&id)?;
            queue.extend(parents.iter().cloned());
            result.insert(id, parents);
        }
        Ok(result)
    }

    pub fn is_ancestor(&self, ancestor: &str, head: &str) -> Result<bool> {
        validate_id(ancestor)?;
        let parents = self.parents(head)?;
        if head == ancestor {
            return Ok(true);
        }
        let mut seen = BTreeSet::from([head.to_owned()]);
        let mut queue = VecDeque::from(parents);
        while let Some(id) = queue.pop_front() {
            if id == ancestor {
                return Ok(true);
            }
            if seen.insert(id.clone()) {
                queue.extend(self.parents(&id)?);
            }
        }
        Ok(false)
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
            for parent in &left[id] {
                best.remove(parent);
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
                if old.as_ref().is_some_and(|entry| entry.manifest.is_some())
                    || entry.as_ref().is_some_and(|entry| entry.manifest.is_some())
                {
                    return Ok(DiffEntry {
                        path,
                        before: old,
                        after: entry,
                        patch: None,
                    });
                }
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
        let tree = self.tree(head)?;
        let entry = tree
            .get(path)
            .with_context(|| format!("path not found: {path}"))?;
        ensure!(
            entry.manifest.is_none(),
            "blame is unavailable for files over 32 MiB"
        );
        let mut history = self.log(head, usize::MAX, Some(path))?;
        history.reverse();
        let mut text = String::new();
        let mut lines: Vec<BlameLine> = Vec::new();
        for commit in history {
            ensure!(
                commit.changes[path]
                    .as_ref()
                    .is_none_or(|entry| entry.manifest.is_none()),
                "blame is unavailable for files over 32 MiB"
            );
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

pub fn is_zstd(bytes: &[u8]) -> bool {
    bytes.starts_with(&[0x28, 0xb5, 0x2f, 0xfd])
}

pub fn encode_blob(bytes: &[u8]) -> Result<Vec<u8>> {
    Ok(zstd::stream::encode_all(bytes, 3)?)
}

pub fn decode_blob(compressed: &[u8], id: &str) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    zstd::stream::read::Decoder::new(compressed)?
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
