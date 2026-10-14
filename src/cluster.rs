use crate::{model::*, storage::Backend, store::decode_blob};
use anyhow::{Context, Result, ensure};
use redis::Commands;
use reqwest::blocking::{Client, Response};
use serde::de::DeserializeOwned;
use std::{
    collections::{BTreeMap, BTreeSet},
    time::Duration,
};

pub struct Cluster {
    cache: redis::Client,
    prefix: String,
    http: Client,
    blob_ttl: u64,
}

impl Cluster {
    pub fn open(cache: &str, namespace: &str, shards: &[String], blob_ttl: u64) -> Result<Self> {
        ensure!(
            !namespace.is_empty()
                && namespace
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_')),
            "invalid cluster namespace"
        );
        ensure!(blob_ttl > 0, "blob TTL must be positive");
        let cluster = Self {
            cache: redis::Client::open(cache)?,
            prefix: format!("scn:{{{namespace}}}"),
            http: Client::builder()
                .timeout(Duration::from_secs(120))
                .build()?,
            blob_ttl,
        };
        let mut connection = cluster.connection()?;
        for shard in shards {
            let shard = shard.trim_end_matches('/');
            let url = reqwest::Url::parse(shard)?;
            ensure!(
                matches!(url.scheme(), "http" | "https")
                    && url.host_str().is_some()
                    && url.path() == "/"
                    && url.query().is_none()
                    && url.fragment().is_none(),
                "expected a shard origin URL"
            );
            Self::response(cluster.http.get(format!("{shard}/health")).send()?)?;
            for kind in ["paths", "blobs", "commits"] {
                redis::cmd("ZADD")
                    .arg(cluster.key(&format!("{kind}:counts")))
                    .arg("NX")
                    .arg(0)
                    .arg(shard)
                    .query::<()>(&mut connection)?;
            }
        }
        ensure!(
            connection.zcard::<_, usize>(cluster.key("paths:counts"))? > 0,
            "register at least one shard with --shard"
        );
        Ok(cluster)
    }

    fn connection(&self) -> Result<redis::Connection> {
        let connection = self
            .cache
            .get_connection_with_timeout(Duration::from_secs(5))?;
        connection.set_read_timeout(Some(Duration::from_secs(10)))?;
        connection.set_write_timeout(Some(Duration::from_secs(10)))?;
        Ok(connection)
    }

    fn key(&self, key: &str) -> String {
        format!("{}:{key}", self.prefix)
    }

    fn owner(&self, kind: &str, item: &str) -> Result<Option<String>> {
        Ok(self.connection()?.hget(self.key(kind), item)?)
    }

    fn allocate(&self, kind: &str, item: &str, preferred: Option<&str>) -> Result<String> {
        let script = redis::Script::new(
            "local old = redis.call('HGET', KEYS[1], ARGV[1]); if old then return old end; local shard = ARGV[2]; if shard == '' then shard = redis.call('ZRANGE', KEYS[2], 0, 0)[1] end; if not shard then return redis.error_reply('no shards registered') end; redis.call('HSET', KEYS[1], ARGV[1], shard); redis.call('ZINCRBY', KEYS[2], 1, shard); return shard",
        );
        Ok(script
            .key(self.key(kind))
            .key(self.key(&format!("{kind}:counts")))
            .arg(item)
            .arg(preferred.unwrap_or(""))
            .invoke(&mut self.connection()?)?)
    }

    fn response(response: Response) -> Result<Response> {
        ensure!(
            response.status().is_success(),
            "shard returned {}: {}",
            response.status(),
            response.text()?
        );
        Ok(response)
    }

    fn get<T: DeserializeOwned>(&self, shard: &str, route: &str) -> Result<T> {
        Ok(Self::response(self.http.get(format!("{shard}{route}")).send()?)?.json()?)
    }

    fn blob_shard(&self, id: &str) -> Result<String> {
        self.owner("blobs", id)?.context("blob not found")
    }

    pub fn allocation(&self, path: &str) -> Result<Option<String>> {
        self.owner("paths", path)
    }
}

impl Backend for Cluster {
    fn put_blob(&self, id: &str, bytes: &[u8], path: Option<&str>) -> Result<()> {
        let preferred = path
            .map(|path| self.allocate("paths", path, None))
            .transpose()?;
        let shard = self.allocate("blobs", id, preferred.as_deref())?;
        Self::response(
            self.http
                .put(format!("{shard}/internal/blobs/{id}"))
                .body(bytes.to_owned())
                .send()?,
        )?;
        let _: redis::RedisResult<()> =
            self.connection()?
                .set_ex(self.key(&format!("blob:{id}")), bytes, self.blob_ttl);
        Ok(())
    }

    fn blob(&self, id: &str) -> Result<Vec<u8>> {
        let key = self.key(&format!("blob:{id}"));
        let mut connection = self.connection()?;
        if let Some(bytes) = connection.get::<_, Option<Vec<u8>>>(&key)? {
            if decode_blob(&bytes, id).is_ok() {
                return Ok(bytes);
            }
            connection.del::<_, ()>(&key)?;
        }
        let shard = self.blob_shard(id)?;
        let bytes = Self::response(
            self.http
                .get(format!("{shard}/internal/blobs/{id}"))
                .send()?,
        )?
        .bytes()?
        .to_vec();
        decode_blob(&bytes, id)?;
        let _: redis::RedisResult<()> = connection.set_ex(key, &bytes, self.blob_ttl);
        Ok(bytes)
    }

    fn has_blob(&self, id: &str) -> Result<bool> {
        let Some(shard) = self.owner("blobs", id)? else {
            return Ok(false);
        };
        let response = self
            .http
            .head(format!("{shard}/internal/blobs/{id}"))
            .send()?;
        if response.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok(false);
        }
        Self::response(response)?;
        Ok(true)
    }

    fn get_commit(&self, id: &str) -> Result<Commit> {
        let shard = self.owner("commits", id)?.context("changeset not found")?;
        let stored: StoredCommit = self.get(&shard, &format!("/internal/commits/{id}"))?;
        let mut shards = BTreeSet::new();
        if !stored.paths.is_empty() {
            let owners: Vec<Option<String>> = redis::cmd("HMGET")
                .arg(self.key("paths"))
                .arg(&stored.paths)
                .query(&mut self.connection()?)?;
            for owner in owners {
                shards.insert(owner.context("missing path allocation")?);
            }
        }
        let mut commit = stored.commit;
        for shard in shards {
            commit
                .changes
                .extend(self.get::<Changes>(&shard, &format!("/internal/changes/{id}"))?);
        }
        ensure!(
            commit.changes.keys().eq(stored.paths.iter()),
            "incomplete changeset deltas"
        );
        let expected = commit.id.clone();
        commit.id.clear();
        ensure!(
            hash(&serde_json::to_vec(&commit)?) == expected && expected == id,
            "changeset integrity check failed"
        );
        commit.id = expected;
        Ok(commit)
    }

    fn save_commit(&self, commit: &Commit) -> Result<()> {
        let mut groups: BTreeMap<String, Changes> = BTreeMap::new();
        for (path, entry) in &commit.changes {
            let shard = self.allocate("paths", path, None)?;
            groups
                .entry(shard)
                .or_default()
                .insert(path.clone(), entry.clone());
        }
        for (shard, changes) in groups {
            Self::response(
                self.http
                    .put(format!("{shard}/internal/changes/{}", commit.id))
                    .json(&changes)
                    .send()?,
            )?;
        }
        let shard = self.allocate("commits", &commit.id, None)?;
        let mut header = commit.clone();
        header.changes.clear();
        let stored = StoredCommit {
            commit: header,
            paths: commit.changes.keys().cloned().collect(),
        };
        Self::response(
            self.http
                .put(format!("{shard}/internal/commits/{}", commit.id))
                .json(&stored)
                .send()?,
        )?;
        Ok(())
    }

    fn main_ref(&self) -> Result<Option<String>> {
        Ok(self.connection()?.get(self.key("main"))?)
    }

    fn update_main(&self, update: RefUpdate) -> Result<()> {
        let changed: bool = redis::Script::new("local current = redis.call('GET', KEYS[1]) or ''; if current ~= ARGV[1] then return 0 end; redis.call('SET', KEYS[1], ARGV[2]); return 1")
            .key(self.key("main")).arg(update.expected.unwrap_or_default()).arg(update.target).invoke(&mut self.connection()?)?;
        ensure!(changed, "main moved; pull and retry push");
        Ok(())
    }
}
