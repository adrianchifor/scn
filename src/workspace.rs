use crate::{
    client::Client,
    model::*,
    store::{MAX_BLOB, decode_blob, encode_blob},
};
use anyhow::{Context, Result, bail, ensure};
use futures_util::{StreamExt, TryStreamExt, stream};
use ignore::WalkBuilder;
use serde::{Deserialize, Serialize};
use std::{
    fs,
    io::Write,
    path::{Path, PathBuf},
};

#[derive(Clone, Deserialize, Serialize)]
pub struct WorkspaceState {
    pub server: String,
    pub revision: Option<String>,
    pub sparse: Vec<String>,
    pub files: Tree,
    #[serde(default)]
    pub index: Changes,
    #[serde(default)]
    pub remote_main: Option<String>,
}

pub struct Workspace {
    pub root: PathBuf,
    pub state: WorkspaceState,
}

pub struct WorkspaceLock(PathBuf);

impl Drop for WorkspaceLock {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.0);
    }
}

impl Workspace {
    pub fn find(start: &Path) -> Result<Self> {
        for root in start.ancestors() {
            let metadata = root.join(".scn");
            if metadata.exists() {
                ensure!(
                    !fs::symlink_metadata(&metadata)?.file_type().is_symlink(),
                    ".scn must not be a symlink"
                );
                let state: WorkspaceState =
                    serde_json::from_slice(&fs::read(metadata.join("state.json"))?)?;
                validate_tree(&state.files)?;
                for prefix in &state.sparse {
                    validate_path(prefix)?;
                }
                return Ok(Self {
                    root: root.to_owned(),
                    state,
                });
            }
        }
        bail!("not in an scn working copy; run scn clone first")
    }

    pub async fn clone(
        url: &str,
        destination: &Path,
        revision: &str,
        sparse: Vec<String>,
    ) -> Result<Self> {
        for prefix in &sparse {
            validate_path(prefix)?;
        }
        fs::create_dir_all(destination)?;
        ensure!(
            fs::read_dir(destination)?.next().is_none(),
            "clone destination must be empty"
        );
        let root = destination.canonicalize()?;
        fs::create_dir(root.join(".scn"))?;
        let mut workspace = Self {
            root,
            state: WorkspaceState {
                server: url.trim_end_matches('/').to_owned(),
                revision: None,
                sparse: vec![],
                files: Tree::new(),
                index: Changes::new(),
                remote_main: None,
            },
        };
        let _lock = workspace.lock()?;
        workspace.save()?;
        let client = workspace.client()?;
        let target = if revision == "main" {
            client.main_ref().await?
        } else {
            Some(client.commit(revision).await?.id)
        };
        workspace.state.remote_main = client.main_ref().await?;
        workspace.materialize(&client, target, sparse).await?;
        Ok(workspace)
    }

    pub fn lock(&self) -> Result<WorkspaceLock> {
        let path = self.root.join(".scn/lock");
        fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
            .context("working copy is locked; if no scn process is running, remove .scn/lock")?;
        Ok(WorkspaceLock(path))
    }

    pub fn client(&self) -> Result<Client> {
        Client::new(&self.state.server, &self.root.join(".scn/cache"))
    }

    pub fn save(&self) -> Result<()> {
        let mut temp = tempfile::NamedTempFile::new_in(self.root.join(".scn"))?;
        temp.write_all(&serde_json::to_vec_pretty(&self.state)?)?;
        temp.as_file().sync_all()?;
        temp.persist(self.root.join(".scn/state.json"))?;
        Ok(())
    }

    pub fn revision(&self, revision: &str) -> Result<String> {
        if revision == "origin/main" {
            return self
                .state
                .remote_main
                .clone()
                .context("origin/main is empty; run scn fetch");
        }
        if revision == "HEAD" {
            self.state
                .revision
                .clone()
                .context("working copy has no changesets")
        } else {
            crate::client::validate_revision(revision)?;
            Ok(revision.to_owned())
        }
    }

    pub fn path(&self, path: &str) -> Result<PathBuf> {
        validate_path(path)?;
        let mut full = self.root.clone();
        for part in path.split('/') {
            full.push(part);
            match fs::symlink_metadata(&full) {
                Ok(metadata) => ensure!(
                    !metadata.file_type().is_symlink(),
                    "symlinks are unsupported: {path}"
                ),
                Err(error) if missing(&error) => {}
                Err(error) => return Err(error.into()),
            }
        }
        Ok(full)
    }

    pub fn scan(&self) -> Result<Tree> {
        let mut files = Tree::new();
        let root = self.root.clone();
        let sparse = self.state.sparse.clone();
        let mut builder = WalkBuilder::new(&root);
        builder
            .hidden(false)
            .ignore(false)
            .git_ignore(true)
            .require_git(false)
            .git_global(false)
            .git_exclude(false)
            .parents(false)
            .add_custom_ignore_filename(".scnignore")
            .follow_links(false)
            .filter_entry(move |entry| {
                if entry.depth() == 0 {
                    return true;
                }
                let relative = entry.path().strip_prefix(&root).unwrap();
                if relative
                    .components()
                    .any(|part| matches!(part.as_os_str().to_str(), Some(".scn" | ".git")))
                {
                    return false;
                }
                let Some(path) = relative.to_str() else {
                    return true;
                };
                selected(path, &sparse)
                    || entry.file_type().is_some_and(|kind| kind.is_dir())
                        && sparse
                            .iter()
                            .any(|prefix| prefix.starts_with(&format!("{path}/")))
            });
        for entry in builder.build() {
            let entry = entry?;
            if entry.depth() == 0 {
                continue;
            }
            let path = entry
                .path()
                .strip_prefix(&self.root)?
                .to_str()
                .context("non-UTF-8 paths are unsupported")?;
            if entry.file_type().is_some_and(|kind| kind.is_dir()) {
                continue;
            }
            ensure!(
                entry.file_type().is_some_and(|kind| kind.is_file()),
                "only regular files are supported: {path}"
            );
            files.insert(path.to_owned(), self.read_entry(path)?);
        }
        for path in self.state.files.keys().chain(
            self.state
                .index
                .iter()
                .filter_map(|(path, entry)| entry.as_ref().map(|_| path)),
        ) {
            if files.contains_key(path) {
                continue;
            }
            let full = self.path(path)?;
            match fs::metadata(&full) {
                Ok(metadata) if metadata.is_file() => {
                    files.insert(path.clone(), self.read_entry(path)?);
                }
                Ok(metadata) if metadata.is_dir() => {}
                Ok(_) => bail!("only regular files are supported: {path}"),
                Err(error) if missing(&error) => {}
                Err(error) => return Err(error.into()),
            }
        }
        Ok(files)
    }

    fn read_entry(&self, path: &str) -> Result<FileEntry> {
        let full = self.path(path)?;
        let metadata = fs::metadata(&full)?;
        ensure!(
            metadata.is_file(),
            "only regular files are supported: {path}"
        );
        ensure!(
            metadata.len() <= MAX_BLOB as u64,
            "file exceeds 32 MiB limit: {path}"
        );
        let bytes = fs::read(full)?;
        Ok(FileEntry {
            blob: hash(&bytes),
            executable: executable(&metadata),
        })
    }

    pub fn ensure_clean(&self) -> Result<()> {
        ensure!(
            self.state.index.is_empty()
                && changes_between(&self.state.files, &self.scan()?).is_empty(),
            "working copy has changes; commit or move them before checkout, merge, or rebase"
        );
        Ok(())
    }

    pub async fn commit(
        &mut self,
        client: &Client,
        message: String,
        author: String,
    ) -> Result<Commit> {
        self.stage(&[".".into()], &self.root.clone(), false)?;
        self.commit_staged(client, message, author).await
    }

    pub fn staged_tree(&self) -> Tree {
        let mut tree = self.state.files.clone();
        apply(&mut tree, &self.state.index);
        tree
    }

    pub fn stage(&mut self, paths: &[String], cwd: &Path, tracked_only: bool) -> Result<()> {
        let files = self.scan()?;
        let directory = self.root.join(".scn/staged");
        fs::create_dir_all(&directory)?;
        let mut index = self.state.index.clone();
        for input in paths {
            let mut absolute = PathBuf::new();
            for component in cwd.join(input).components() {
                match component {
                    std::path::Component::CurDir => {}
                    std::path::Component::ParentDir => {
                        absolute.pop();
                    }
                    component => absolute.push(component.as_os_str()),
                }
            }
            let prefix = absolute
                .strip_prefix(&self.root)
                .context("path is outside the working copy")?
                .to_str()
                .context("non-UTF-8 path")?;
            if !prefix.is_empty() {
                validate_path(prefix)?;
            }
            let candidates: std::collections::BTreeSet<_> = self
                .state
                .files
                .keys()
                .chain(files.keys())
                .chain(index.keys())
                .cloned()
                .collect();
            let mut matched = false;
            for path in candidates {
                if !prefix.is_empty() && !selected(&path, &[prefix.to_owned()]) {
                    continue;
                }
                if tracked_only
                    && !self.state.files.contains_key(&path)
                    && !self.state.index.contains_key(&path)
                {
                    continue;
                }
                matched = true;
                if files.get(&path) == self.state.files.get(&path) {
                    index.remove(&path);
                    continue;
                }
                let entry = files.get(&path).cloned();
                if let Some(entry) = &entry {
                    let bytes = fs::read(self.path(&path)?)?;
                    ensure!(
                        hash(&bytes) == entry.blob,
                        "file changed while staging; retry: {path}"
                    );
                    let mut temp = tempfile::NamedTempFile::new_in(&directory)?;
                    temp.write_all(&encode_blob(&bytes)?)?;
                    temp.persist(directory.join(&entry.blob))?;
                }
                index.insert(path, entry);
            }
            ensure!(
                matched || prefix.is_empty(),
                "path matched no files: {input}"
            );
        }
        let mut tree = self.state.files.clone();
        apply(&mut tree, &index);
        validate_tree(&tree)?;
        self.state.index = index;
        self.save()?;
        self.prune_staged()?;
        Ok(())
    }

    fn prune_staged(&self) -> Result<()> {
        let directory = self.root.join(".scn/staged");
        if !directory.exists() {
            return Ok(());
        }
        let keep: std::collections::BTreeSet<_> = self
            .state
            .index
            .values()
            .flatten()
            .map(|entry| entry.blob.as_str())
            .collect();
        for entry in fs::read_dir(directory)? {
            let entry = entry?;
            if !keep.contains(entry.file_name().to_string_lossy().as_ref()) {
                fs::remove_file(entry.path())?;
            }
        }
        Ok(())
    }

    pub fn staged_blob(&self, id: &str) -> Result<Vec<u8>> {
        validate_id(id)?;
        decode_blob(&fs::read(self.root.join(".scn/staged").join(id))?, id)
    }

    pub async fn commit_staged(
        &mut self,
        client: &Client,
        message: String,
        author: String,
    ) -> Result<Commit> {
        let files = self.staged_tree();
        let changes = self.state.index.clone();
        ensure!(!changes.is_empty(), "nothing to commit; use scn add first");
        for (path, entry) in &changes {
            if let Some(entry) = entry {
                client
                    .put_blob_at(self.staged_blob(&entry.blob)?, Some(path))
                    .await?;
            }
        }
        let commit: Commit = client
            .post(
                "/v1/commits",
                &CommitRequest {
                    parents: self.state.revision.iter().cloned().collect(),
                    author,
                    message,
                    changes,
                },
            )
            .await?;
        self.state.revision = Some(commit.id.clone());
        self.state.files = files;
        self.state.index.clear();
        self.save()?;
        self.prune_staged()?;
        Ok(commit)
    }

    pub async fn checkout(
        &mut self,
        client: &Client,
        revision: &str,
        sparse: Option<Vec<String>>,
    ) -> Result<()> {
        self.ensure_clean()?;
        let target = client.commit(&self.revision(revision)?).await?.id;
        self.materialize(
            client,
            Some(target),
            sparse.unwrap_or_else(|| self.state.sparse.clone()),
        )
        .await
    }

    pub async fn fetch(&mut self, client: &Client) -> Result<Option<String>> {
        let target = client.main_ref().await?;
        self.state.remote_main = target.clone();
        self.save()?;
        Ok(target)
    }

    pub async fn pull(
        &mut self,
        client: &Client,
        rebase: bool,
        ff_only: bool,
        author: String,
    ) -> Result<()> {
        self.ensure_clean()?;
        let Some(target) = self.fetch(client).await? else {
            return Ok(());
        };
        let Some(head) = self.state.revision.clone() else {
            return self
                .materialize(client, Some(target), self.state.sparse.clone())
                .await;
        };
        if ff_only {
            let base: RefValue = client
                .get(&format!("/v1/merge-base/{head}/{target}"), &[])
                .await?;
            ensure!(
                base.target.as_deref() == Some(&head) || base.target.as_deref() == Some(&target),
                "pull would diverge; use --rebase or merge"
            );
        }
        let route = if rebase { "/v1/rebase" } else { "/v1/merge" };
        let commit: Commit = client
            .post(
                route,
                &IntegrateRequest {
                    head,
                    onto: target,
                    author,
                },
            )
            .await?;
        self.materialize(client, Some(commit.id), self.state.sparse.clone())
            .await
    }

    pub async fn materialize(
        &mut self,
        client: &Client,
        revision: Option<String>,
        sparse: Vec<String>,
    ) -> Result<()> {
        self.ensure_clean()?;
        for prefix in &sparse {
            validate_path(prefix)?;
        }
        let tree = if let Some(revision) = &revision {
            client.tree(revision, &sparse).await?
        } else {
            Tree::new()
        };
        validate_tree(&tree)?;
        let changes = changes_between(&self.state.files, &tree);
        for path in changes.keys().filter(|path| tree.contains_key(*path)) {
            self.check_target(path)?;
        }
        let stage = tempfile::tempdir_in(self.root.join(".scn"))?;
        let mut downloads = std::collections::BTreeMap::<_, Vec<_>>::new();
        for (path, entry) in &changes {
            if let Some(entry) = entry {
                downloads
                    .entry(&entry.blob)
                    .or_default()
                    .push((path, entry.executable));
            }
        }
        stream::iter(downloads)
            .map(|(blob, paths)| {
                let stage = stage.path();
                async move {
                    let bytes = client.blob(blob).await?;
                    for (path, executable) in paths {
                        let staged = stage.join(path);
                        fs::create_dir_all(staged.parent().unwrap())?;
                        fs::write(&staged, &bytes)?;
                        set_executable(&staged, executable)?;
                    }
                    Ok::<_, anyhow::Error>(())
                }
            })
            .buffer_unordered(8)
            .try_collect::<Vec<_>>()
            .await?;
        self.ensure_clean()?;
        for path in changes.keys().filter(|path| tree.contains_key(*path)) {
            self.check_target(path)?;
        }
        for path in changes
            .keys()
            .filter(|path| self.state.files.contains_key(*path))
        {
            let full = self.path(path)?;
            fs::remove_file(&full)?;
            let mut parent = full.parent();
            while let Some(directory) = parent {
                if directory == self.root || fs::remove_dir(directory).is_err() {
                    break;
                }
                parent = directory.parent();
            }
        }
        for path in changes.keys().filter(|path| tree.contains_key(*path)) {
            let full = self.path(path)?;
            if full.is_dir() {
                fs::remove_dir_all(&full)?;
            }
            fs::create_dir_all(full.parent().unwrap())?;
            fs::rename(stage.path().join(path), &full)?;
        }
        self.state.revision = revision;
        self.state.sparse = sparse;
        self.state.files = tree;
        self.save()?;
        Ok(())
    }

    fn check_target(&self, path: &str) -> Result<()> {
        self.path(path)?;
        let mut relative = String::new();
        for part in path.split('/') {
            if !relative.is_empty() {
                relative.push('/');
            }
            relative.push_str(part);
            let full = self.root.join(&relative);
            if full.is_file() {
                ensure!(
                    self.state.files.contains_key(&relative),
                    "checkout would overwrite untracked file: {relative}"
                );
            }
            if relative == path && full.is_dir() {
                self.check_directory(&full)?;
            }
        }
        Ok(())
    }

    fn check_directory(&self, directory: &Path) -> Result<()> {
        for entry in fs::read_dir(directory)? {
            let entry = entry?;
            if entry.file_type()?.is_dir() {
                self.check_directory(&entry.path())?;
            } else {
                let path = entry.path();
                let relative = path
                    .strip_prefix(&self.root)?
                    .to_str()
                    .context("non-UTF-8 path")?;
                ensure!(
                    entry.file_type()?.is_file() && self.state.files.contains_key(relative),
                    "checkout would overwrite untracked file: {relative}"
                );
            }
        }
        Ok(())
    }
}

fn missing(error: &std::io::Error) -> bool {
    matches!(
        error.kind(),
        std::io::ErrorKind::NotFound | std::io::ErrorKind::NotADirectory
    )
}

#[cfg(unix)]
fn executable(metadata: &fs::Metadata) -> bool {
    use std::os::unix::fs::PermissionsExt;
    metadata.permissions().mode() & 0o111 != 0
}

#[cfg(not(unix))]
fn executable(_: &fs::Metadata) -> bool {
    false
}

#[cfg(unix)]
fn set_executable(path: &Path, executable: bool) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(
        path,
        fs::Permissions::from_mode(if executable { 0o755 } else { 0o644 }),
    )?;
    Ok(())
}

#[cfg(not(unix))]
fn set_executable(_: &Path, _: bool) -> Result<()> {
    Ok(())
}
