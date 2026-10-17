use crate::storage::{Backend, Blobs, Local};
use crate::{model::*, worker};
use anyhow::{Context, Result, bail, ensure};
use axum::body::Bytes;
use sha2::{Digest, Sha256};
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

    pub async fn put_blob(&self, bytes: &[u8]) -> Result<String> {
        self.put_blob_at(Bytes::copy_from_slice(bytes), None).await
    }

    pub async fn put_blob_at(&self, bytes: Bytes, path: Option<&str>) -> Result<String> {
        ensure!(bytes.len() <= MAX_BLOB, "blob exceeds 32 MiB limit");
        if let Some(path) = path {
            validate_path(path)?;
        }
        let (id, compressed, size) =
            worker::cpu(move || Ok((hash(&bytes), Bytes::from(encode_blob(&bytes)?), bytes.len())))
                .await?;
        self.backend.put_blob(&id, compressed, size, path).await?;
        Ok(id)
    }

    pub async fn compressed_blob(&self, id: &str) -> Result<Bytes> {
        validate_id(id)?;
        let bytes = self.backend.blob(id).await?;
        let id = id.to_owned();
        worker::cpu(move || {
            verify_blob(&bytes, &id)?;
            Ok(bytes)
        })
        .await
    }

    pub async fn has_blob(&self, id: &str) -> Result<bool> {
        validate_id(id)?;
        self.backend.has_blob(id).await
    }

    pub async fn blob(&self, id: &str) -> Result<Vec<u8>> {
        validate_id(id)?;
        let bytes = self.backend.blob(id).await?;
        let id = id.to_owned();
        worker::cpu(move || decode_blob(&bytes, &id)).await
    }

    pub async fn prepare_file(&self, request: FileRequest) -> Result<FileEntry> {
        let entry = crate::content::manifest_entry(request.manifest, request.executable)?;
        self.check_file(&entry, &mut BTreeMap::new()).await?;
        Ok(entry)
    }

    async fn check_file(
        &self,
        entry: &FileEntry,
        checked: &mut BTreeMap<String, usize>,
    ) -> Result<()> {
        crate::content::validate_entry(entry)?;
        use futures_util::{StreamExt, TryStreamExt, stream};
        let missing: BTreeSet<_> = entry
            .blobs()
            .filter(|id| !checked.contains_key(*id))
            .map(str::to_owned)
            .collect();
        let backend = self.backend.clone();
        let sizes: Vec<_> = stream::iter(missing)
            .map(|id: String| {
                let backend = backend.clone();
                async move {
                    let size = backend
                        .blob_size(&id)
                        .await?
                        .with_context(|| format!("missing blob: {id}"))?;
                    Ok::<_, anyhow::Error>((id, size))
                }
            })
            .buffer_unordered(16)
            .try_collect()
            .await?;
        checked.extend(sizes);
        if let Some(manifest) = &entry.manifest {
            for (index, id) in manifest.chunks.iter().enumerate() {
                let expected = (manifest.size - index as u64 * crate::content::CHUNK_SIZE as u64)
                    .min(crate::content::CHUNK_SIZE as u64);
                ensure!(
                    checked[id.as_str()] as u64 == expected,
                    "file chunk size mismatch: {id}"
                );
            }
        }

        Ok(())
    }

    pub async fn get_commit(&self, id: &str) -> Result<Commit> {
        validate_id(id)?;
        self.backend.get_commit(id).await
    }

    pub async fn main_ref(&self) -> Result<Option<String>> {
        self.backend.main_ref().await
    }

    pub async fn resolve(&self, revision: &str) -> Result<String> {
        if revision == "main" {
            self.main_ref()
                .await?
                .context("main is empty; commit and land a changeset first")
        } else if let Some(name) = revision.strip_prefix("refs/tags/") {
            Ok(self.tag(name).await?.target)
        } else if validate_id(revision).is_ok() {
            Ok(self.get_commit(revision).await?.id)
        } else {
            Ok(self.tag(revision).await?.target)
        }
    }

    pub async fn tags(&self) -> Result<Vec<Tag>> {
        let mut tags = self.backend.tags().await?;
        tags.sort_by(|left, right| left.name.cmp(&right.name));
        Ok(tags)
    }

    pub async fn tag(&self, name: &str) -> Result<Tag> {
        validate_tag_name(name)?;
        self.backend
            .tag(name)
            .await?
            .ok_or_else(|| TagError::NotFound(name.to_owned()).into())
    }

    pub async fn create_tag(&self, request: TagRequest) -> Result<Tag> {
        validate_tag_name(&request.name)?;
        let target = self.resolve(&request.target).await?;
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
        self.write_tag(tag, request.force, false).await
    }

    pub async fn import_tag(&self, tag: Tag) -> Result<Tag> {
        self.write_tag(tag, false, true).await
    }

    async fn write_tag(&self, tag: Tag, force: bool, imported: bool) -> Result<Tag> {
        validate_tag_name(&tag.name)?;
        self.parents(&tag.target).await?;
        if let Some(annotation) = &tag.annotation {
            ensure!(
                !annotation.tagger.trim().is_empty(),
                "tagger must not be empty"
            );
        }
        if let Some(id) = &tag.git_id {
            validate_git_id(id)?;
        }
        if !self.backend.save_tag(&tag, force).await?
            && !(imported && self.backend.tag(&tag.name).await?.as_ref() == Some(&tag))
        {
            return Err(TagError::Exists(tag.name).into());
        }
        tracing::info!(tag = %tag.name, target = %tag.target, force, imported, "Tag stored");
        Ok(tag)
    }

    pub async fn delete_tag(&self, name: &str) -> Result<()> {
        validate_tag_name(name)?;
        ensure!(
            self.backend.delete_tag(name).await?,
            TagError::NotFound(name.to_owned())
        );
        tracing::info!(tag = name, "Tag deleted");
        Ok(())
    }

    pub async fn tree(&self, id: &str) -> Result<Tree> {
        let mut chain = Vec::new();
        let mut current = Some(id.to_owned());
        let mut tree = Tree::new();
        while let Some(id) = current {
            let cached = self.trees.lock().unwrap().get(&id);
            if let Some(cached) = cached {
                tracing::debug!(commit_id = %id, files = cached.len(), "Tree cache hit");
                tree = worker::cpu(move || Ok((*cached).clone())).await?;
                break;
            }
            let commit = self.get_commit(&id).await?;
            current = commit.parents.first().cloned();
            chain.push(commit);
        }
        if chain.is_empty() {
            return Ok(tree);
        }
        let trees = self.trees.clone();
        let id = id.to_owned();
        worker::cpu(move || {
            tracing::debug!(commit_id = %id, commits = chain.len(), "Reconstructing tree");
            for commit in chain.into_iter().rev() {
                apply(&mut tree, &commit.changes);
            }
            trees.lock().unwrap().insert(&id, tree.clone());
            Ok(tree)
        })
        .await
    }

    pub async fn create_commit(&self, request: CommitRequest) -> Result<Commit> {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)?
            .as_nanos()
            .try_into()?;
        self.write_commit(request, timestamp, None).await
    }

    pub async fn import_commit(&self, request: ImportCommitRequest) -> Result<Commit> {
        validate_git_id(&request.git_id)?;
        self.write_commit(request.commit, request.timestamp, Some(request.git_id))
            .await
    }

    async fn write_commit(
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
            self.parents(parent).await?;
        }
        let mut tree = match request.parents.first() {
            Some(parent) => self.tree(parent).await?,
            None => Tree::new(),
        };
        let mut checked_blobs = BTreeMap::new();
        for (path, entry) in &request.changes {
            validate_path(path)?;
            if let Some(entry) = entry {
                if tree.get(path) == Some(entry) {
                    continue;
                }
                self.check_file(entry, &mut checked_blobs).await?;
            } else {
                ensure!(
                    tree.contains_key(path),
                    "cannot delete missing path: {path}"
                );
            }
        }
        let (commit, tree) = worker::cpu(move || {
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
            Ok((commit, tree))
        })
        .await?;
        self.backend.save_commit(&commit).await?;
        let trees = self.trees.clone();
        let id = commit.id.clone();
        worker::cpu(move || {
            trees.lock().unwrap().insert(&id, tree);
            Ok(())
        })
        .await?;
        tracing::info!(commit_id = %commit.id, parents = commit.parents.len(), paths = commit.changes.len(), imported, "Commit stored");
        Ok(commit)
    }

    pub async fn update_main(&self, update: RefUpdate) -> Result<()> {
        self.get_commit(&update.target).await?;
        if let Some(expected) = &update.expected {
            ensure!(
                self.is_ancestor(expected, &update.target).await?,
                "main updates must be fast-forward; merge or rebase first"
            );
        }
        let expected = update.expected.clone();
        let target = update.target.clone();
        self.backend.update_main(update).await?;
        tracing::info!(?expected, %target, "Main advanced");
        Ok(())
    }

    async fn parents(&self, id: &str) -> Result<Vec<String>> {
        validate_id(id)?;
        self.backend.parents(id).await
    }

    async fn ancestors(&self, head: &str) -> Result<BTreeMap<String, Vec<String>>> {
        let mut result = BTreeMap::new();
        let mut queue = VecDeque::from([head.to_owned()]);
        while let Some(id) = queue.pop_front() {
            if result.contains_key(&id) {
                continue;
            }
            let parents = self.parents(&id).await?;
            queue.extend(parents.iter().cloned());
            result.insert(id, parents);
        }
        Ok(result)
    }

    pub async fn is_ancestor(&self, ancestor: &str, head: &str) -> Result<bool> {
        validate_id(ancestor)?;
        let parents = self.parents(head).await?;
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
                queue.extend(self.parents(&id).await?);
            }
        }
        Ok(false)
    }

    pub async fn merge_base(&self, left: &str, right: &str) -> Result<String> {
        let left = self.ancestors(left).await?;
        let right = self.ancestors(right).await?;
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

    pub async fn log(&self, head: &str, limit: usize, path: Option<&str>) -> Result<Vec<Commit>> {
        if let Some(path) = path {
            validate_path(path)?;
        }
        let mut commits = Vec::new();
        let mut current = Some(head.to_owned());
        while let Some(id) = current {
            if commits.len() >= limit {
                break;
            }
            let commit = self.get_commit(&id).await?;
            current = commit.parents.first().cloned();
            if path.is_none_or(|path| commit.changes.contains_key(path)) {
                commits.push(commit);
            }
        }
        Ok(commits)
    }

    pub async fn diff(&self, from: &str, to: &str) -> Result<Vec<DiffEntry>> {
        let before = self.tree(from).await?;
        let after = self.tree(to).await?;
        let mut result = Vec::new();
        for (path, entry) in changes_between(&before, &after) {
            let old = before.get(&path).cloned();
            let patch = if old.as_ref().is_some_and(|e| e.manifest.is_some())
                || entry.as_ref().is_some_and(|e| e.manifest.is_some())
            {
                None
            } else {
                let old_bytes = match &old {
                    Some(e) => self.blob(&e.blob).await?,
                    None => Vec::new(),
                };
                let new_bytes = match &entry {
                    Some(e) => self.blob(&e.blob).await?,
                    None => Vec::new(),
                };
                let path = path.clone();
                worker::cpu(move || Ok(patch(&path, &old_bytes, &new_bytes))).await?
            };
            result.push(DiffEntry {
                path,
                before: old,
                after: entry,
                patch,
            });
        }
        Ok(result)
    }

    #[tracing::instrument(level = "debug", skip(self, author))]
    pub async fn merge(&self, head: &str, other: &str, author: &str) -> Result<Commit> {
        if self.is_ancestor(other, head).await? {
            return self.get_commit(head).await;
        }
        if self.is_ancestor(head, other).await? {
            return self.get_commit(other).await;
        }
        let base = self.merge_base(head, other).await?;
        let ours = self.tree(head).await?;
        let base_tree = self.tree(&base).await?;
        let theirs = self.tree(other).await?;
        let changes = worker::cpu(move || {
            let merged = merge_trees(&base_tree, &ours, &theirs)?;
            Ok(changes_between(&ours, &merged))
        })
        .await?;
        self.create_commit(CommitRequest {
            parents: vec![head.to_owned(), other.to_owned()],
            author: author.to_owned(),
            message: format!("Merge {} into {}", &other[..12], &head[..12]),
            changes,
        })
        .await
    }

    #[tracing::instrument(level = "debug", skip(self, author))]
    pub async fn rebase(&self, head: &str, onto: &str, author: &str) -> Result<Commit> {
        if self.is_ancestor(head, onto).await? {
            return self.get_commit(onto).await;
        }
        if self.is_ancestor(onto, head).await? {
            return self.get_commit(head).await;
        }
        let base = self.merge_base(head, onto).await?;
        let mut chain = Vec::new();
        let mut current = head.to_owned();
        while current != base {
            let commit = self.get_commit(&current).await?;
            ensure!(
                commit.parents.len() == 1,
                "rebase requires a linear first-parent history; use merge"
            );
            current = commit.parents[0].clone();
            chain.push(commit);
        }
        let mut target = self.get_commit(onto).await?;
        for original in chain.into_iter().rev() {
            let before = self.tree(&target.id).await?;
            let base_tree = self.tree(&original.parents[0]).await?;
            let after = self.tree(&original.id).await?;
            let changes = worker::cpu(move || {
                let merged = merge_trees(&base_tree, &before, &after)?;
                Ok(changes_between(&before, &merged))
            })
            .await?;
            if !changes.is_empty() {
                target = self
                    .create_commit(CommitRequest {
                        parents: vec![target.id],
                        author: author.to_owned(),
                        message: original.message,
                        changes,
                    })
                    .await?;
            }
        }
        Ok(target)
    }

    pub async fn blame(&self, head: &str, path: &str) -> Result<Vec<BlameLine>> {
        validate_path(path)?;
        let tree = self.tree(head).await?;
        let entry = tree
            .get(path)
            .with_context(|| format!("path not found: {path}"))?;
        ensure!(
            entry.manifest.is_none(),
            "blame is unavailable for files over 32 MiB"
        );
        let mut history = self.log(head, usize::MAX, Some(path)).await?;
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
            let bytes = match &commit.changes[path] {
                Some(entry) => self.blob(&entry.blob).await?,
                None => Vec::new(),
            };
            (text, lines) = worker::cpu(move || {
                let next = String::from_utf8(bytes).context("blame requires UTF-8 text")?;
                ensure!(!next.contains('\0'), "blame requires text");
                let mut updated = Vec::new();
                for change in TextDiff::from_lines(&text, &next).iter_all_changes() {
                    match change.tag() {
                        ChangeTag::Equal => {
                            updated.push(lines[change.old_index().unwrap()].clone())
                        }
                        ChangeTag::Insert => updated.push(BlameLine {
                            commit: commit.id.clone(),
                            author: commit.author.clone(),
                            line: change.value().to_owned(),
                        }),
                        ChangeTag::Delete => {}
                    }
                }
                Ok((next, updated))
            })
            .await?;
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
    let mut decoder = zstd::stream::read::Decoder::new(compressed)?;
    decoder.window_log_max(25)?;
    decoder
        .take((MAX_BLOB + 1) as u64)
        .read_to_end(&mut bytes)?;
    ensure!(
        bytes.len() <= MAX_BLOB && hash(&bytes) == id,
        "blob integrity check failed: {id}"
    );
    Ok(bytes)
}

pub fn verify_blob(compressed: &[u8], id: &str) -> Result<usize> {
    let mut decoder = zstd::stream::read::Decoder::new(compressed)?;
    decoder.window_log_max(25)?;
    let mut digest = Sha256::new();
    let mut length = 0;
    let mut buffer = [0; 64 * 1024];
    loop {
        let read = decoder.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        length += read;
        ensure!(length <= MAX_BLOB, "blob exceeds 32 MiB limit");
        digest.update(&buffer[..read]);
    }
    ensure!(
        digest_hex(&digest.finalize()) == id,
        "blob integrity check failed: {id}"
    );
    Ok(length)
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
