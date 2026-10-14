use crate::{client::Client, model::*, store::MAX_BLOB};
use anyhow::{Context, Result, bail, ensure};
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
            .git_ignore(false)
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
        for path in self.state.files.keys() {
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
            changes_between(&self.state.files, &self.scan()?).is_empty(),
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
        let files = self.scan()?;
        let changes = changes_between(&self.state.files, &files);
        ensure!(!changes.is_empty(), "nothing to commit");
        for (path, entry) in &changes {
            if let Some(entry) = entry {
                let bytes = fs::read(self.path(path)?)?;
                ensure!(
                    hash(&bytes) == entry.blob,
                    "file changed during commit; retry: {path}"
                );
                client.put_blob(bytes).await?;
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
        self.save()?;
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
        for path in tree.keys() {
            self.check_target(path)?;
        }
        let stage = tempfile::tempdir_in(self.root.join(".scn"))?;
        for (path, entry) in &tree {
            let bytes = client.blob(&entry.blob).await?;
            let staged = stage.path().join(path);
            fs::create_dir_all(staged.parent().unwrap())?;
            fs::write(&staged, bytes)?;
            set_executable(&staged, entry.executable)?;
        }
        self.ensure_clean()?;
        for path in tree.keys() {
            self.check_target(path)?;
        }
        for path in self.state.files.keys() {
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
        for path in tree.keys() {
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
