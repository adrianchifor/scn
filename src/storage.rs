use crate::model::*;
use anyhow::{Context, Result, ensure};
use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};
use s3::{Bucket, Region, creds::Credentials};
use std::{
    fs,
    io::Write,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

pub trait Backend: Send + Sync {
    fn put_blob(&self, id: &str, bytes: &[u8], path: Option<&str>) -> Result<()>;
    fn blob(&self, id: &str) -> Result<Vec<u8>>;
    fn has_blob(&self, id: &str) -> Result<bool>;
    fn get_commit(&self, id: &str) -> Result<Commit>;
    fn save_commit(&self, commit: &Commit) -> Result<()>;
    fn main_ref(&self) -> Result<Option<String>>;
    fn update_main(&self, update: RefUpdate) -> Result<()>;
}

#[derive(Clone)]
pub enum Blobs {
    Disk(PathBuf),
    S3 { bucket: Arc<Bucket>, prefix: String },
}

impl Blobs {
    pub fn disk(path: &Path) -> Result<Self> {
        fs::create_dir_all(path)?;
        Ok(Self::Disk(path.to_owned()))
    }

    pub fn s3(bucket: &str, region: &str, endpoint: Option<&str>, prefix: &str) -> Result<Self> {
        let region = match endpoint {
            Some(endpoint) => Region::Custom {
                region: region.to_owned(),
                endpoint: endpoint.to_owned(),
            },
            None => region.parse()?,
        };
        let mut bucket = Bucket::new(bucket, region, Credentials::default()?)?
            .with_request_timeout(Duration::from_secs(120))?;
        if endpoint.is_some() {
            bucket = bucket.with_path_style();
        }
        Ok(Self::S3 {
            bucket: Arc::from(bucket),
            prefix: prefix.trim_matches('/').to_owned(),
        })
    }

    fn key(prefix: &str, id: &str) -> String {
        if prefix.is_empty() {
            format!("{id}.zst")
        } else {
            format!("{prefix}/{id}.zst")
        }
    }

    pub fn put(&self, id: &str, bytes: &[u8]) -> Result<()> {
        validate_id(id)?;
        match self {
            Self::Disk(directory) => {
                let mut temp = tempfile::NamedTempFile::new_in(directory)?;
                temp.write_all(bytes)?;
                temp.as_file().sync_all()?;
                temp.persist(directory.join(id))?;
            }
            Self::S3 { bucket, prefix } => {
                bucket.put_object_with_content_type(
                    Self::key(prefix, id),
                    bytes,
                    "application/zstd",
                )?;
            }
        }
        Ok(())
    }

    pub fn get(&self, id: &str) -> Result<Vec<u8>> {
        validate_id(id)?;
        match self {
            Self::Disk(directory) => fs::read(directory.join(id)).context("blob not found"),
            Self::S3 { bucket, prefix } => Ok(bucket.get_object(Self::key(prefix, id))?.to_vec()),
        }
    }

    pub fn contains(&self, id: &str) -> Result<bool> {
        validate_id(id)?;
        match self {
            Self::Disk(directory) => Ok(directory.join(id).is_file()),
            Self::S3 { bucket, prefix } => Ok(bucket.object_exists(Self::key(prefix, id))?),
        }
    }
}

#[derive(Clone)]
pub struct Local {
    root: PathBuf,
    pub blobs: Blobs,
}

impl Local {
    pub fn open(root: &Path, blobs: Blobs) -> Result<Self> {
        fs::create_dir_all(root)?;
        let local = Self {
            root: root.to_owned(),
            blobs,
        };
        local.connection()?.execute_batch(
            "PRAGMA journal_mode=WAL;
             CREATE TABLE IF NOT EXISTS commits (sequence INTEGER PRIMARY KEY AUTOINCREMENT, id TEXT NOT NULL UNIQUE, data TEXT NOT NULL);
             CREATE TABLE IF NOT EXISTS refs (name TEXT PRIMARY KEY, target TEXT NOT NULL);
             CREATE TABLE IF NOT EXISTS headers (id TEXT PRIMARY KEY, data TEXT NOT NULL);
             CREATE TABLE IF NOT EXISTS changes (commit_id TEXT NOT NULL, path TEXT NOT NULL, data TEXT NOT NULL, PRIMARY KEY(commit_id, path));"
        )?;
        Ok(local)
    }

    pub fn connection(&self) -> Result<Connection> {
        let connection = Connection::open(self.root.join("scn.db"))?;
        connection.busy_timeout(Duration::from_secs(10))?;
        Ok(connection)
    }

    pub fn header(&self, id: &str) -> Result<StoredCommit> {
        validate_id(id)?;
        let data: String = self
            .connection()?
            .query_row("SELECT data FROM headers WHERE id = ?1", [id], |row| {
                row.get(0)
            })
            .context("changeset not found")?;
        Ok(serde_json::from_str(&data)?)
    }

    pub fn put_header(&self, id: &str, header: StoredCommit) -> Result<()> {
        validate_id(id)?;
        ensure!(
            header.commit.id == id && header.commit.changes.is_empty(),
            "invalid changeset header"
        );
        for path in &header.paths {
            validate_path(path)?;
        }
        let data = serde_json::to_string(&header)?;
        let connection = self.connection()?;
        connection.execute(
            "INSERT OR IGNORE INTO headers (id, data) VALUES (?1, ?2)",
            params![id, data],
        )?;
        ensure!(
            serde_json::to_string(&self.header(id)?)? == data,
            "immutable changeset header mismatch"
        );
        Ok(())
    }

    pub fn changes(&self, id: &str) -> Result<Changes> {
        validate_id(id)?;
        let connection = self.connection()?;
        let mut statement = connection
            .prepare("SELECT path, data FROM changes WHERE commit_id = ?1 ORDER BY path")?;
        let rows = statement.query_map([id], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })?;
        let mut changes = Changes::new();
        for row in rows {
            let (path, data) = row?;
            changes.insert(path, serde_json::from_str(&data)?);
        }
        Ok(changes)
    }

    pub fn put_changes(&self, id: &str, changes: Changes) -> Result<()> {
        validate_id(id)?;
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        for (path, entry) in changes {
            validate_path(&path)?;
            if let Some(entry) = &entry {
                validate_id(&entry.blob)?;
            }
            let data = serde_json::to_string(&entry)?;
            transaction.execute(
                "INSERT OR IGNORE INTO changes (commit_id, path, data) VALUES (?1, ?2, ?3)",
                params![id, path, data],
            )?;
            let existing: String = transaction.query_row(
                "SELECT data FROM changes WHERE commit_id = ?1 AND path = ?2",
                params![id, path],
                |row| row.get(0),
            )?;
            ensure!(existing == data, "immutable file delta mismatch");
        }
        transaction.commit()?;
        Ok(())
    }
}

impl Backend for Local {
    fn put_blob(&self, id: &str, bytes: &[u8], _: Option<&str>) -> Result<()> {
        self.blobs.put(id, bytes)
    }
    fn blob(&self, id: &str) -> Result<Vec<u8>> {
        self.blobs.get(id)
    }
    fn has_blob(&self, id: &str) -> Result<bool> {
        self.blobs.contains(id)
    }

    fn get_commit(&self, id: &str) -> Result<Commit> {
        let data: String = self
            .connection()?
            .query_row("SELECT data FROM commits WHERE id = ?1", [id], |row| {
                row.get(0)
            })
            .context("changeset not found")?;
        Ok(serde_json::from_str(&data)?)
    }

    fn save_commit(&self, commit: &Commit) -> Result<()> {
        self.connection()?.execute(
            "INSERT OR IGNORE INTO commits (id, data) VALUES (?1, ?2)",
            params![commit.id, serde_json::to_string(commit)?],
        )?;
        Ok(())
    }

    fn main_ref(&self) -> Result<Option<String>> {
        Ok(self
            .connection()?
            .query_row("SELECT target FROM refs WHERE name = 'main'", [], |row| {
                row.get(0)
            })
            .optional()?)
    }

    fn update_main(&self, update: RefUpdate) -> Result<()> {
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let actual: Option<String> = transaction
            .query_row("SELECT target FROM refs WHERE name = 'main'", [], |row| {
                row.get(0)
            })
            .optional()?;
        ensure!(actual == update.expected, "main moved; pull and retry push");
        transaction.execute("INSERT INTO refs (name, target) VALUES ('main', ?1) ON CONFLICT(name) DO UPDATE SET target = excluded.target", [&update.target])?;
        transaction.commit()?;
        Ok(())
    }
}
