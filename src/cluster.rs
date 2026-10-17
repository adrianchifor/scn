use crate::worker;
use crate::{client::retry_delay, model::*, storage::Backend, store::verify_blob};
use anyhow::{Context, Result, ensure};
use async_trait::async_trait;
use axum::body::Bytes;
use futures_util::{StreamExt, TryStreamExt, stream};
use redis::{
    AsyncCommands,
    aio::{ConnectionManager, ConnectionManagerConfig},
};
use reqwest::{Client, Response};
use serde::de::DeserializeOwned;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::{
    collections::{BTreeMap, BTreeSet},
    time::Duration,
};

pub struct Cluster {
    cache: ConnectionManager,
    blob_cache: Vec<ConnectionManager>,
    next_cache: AtomicUsize,
    cache_slots: tokio::sync::Semaphore,
    prefix: String,
    http: Client,
    blob_ttl: u64,
}

impl Cluster {
    pub async fn open(
        cache: &str,
        namespace: &str,
        shards: &[String],
        blob_ttl: u64,
    ) -> Result<Self> {
        ensure!(
            !namespace.is_empty()
                && namespace
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_')),
            "invalid cluster namespace"
        );
        ensure!(blob_ttl > 0, "blob TTL must be positive");
        let client = redis::Client::open(cache)?;
        let config = ConnectionManagerConfig::new()
            .set_connection_timeout(Some(Duration::from_secs(5)))
            .set_response_timeout(Some(Duration::from_secs(10)));
        let cache = ConnectionManager::new_with_config(client.clone(), config.clone()).await?;
        let blob_cache = futures_util::future::try_join_all(
            (0..4).map(|_| ConnectionManager::new_with_config(client.clone(), config.clone())),
        )
        .await?;
        let cluster = Self {
            cache,
            blob_cache,
            next_cache: AtomicUsize::new(0),
            cache_slots: tokio::sync::Semaphore::new(2),
            prefix: format!("scn:{{{namespace}}}"),
            http: Client::builder()
                .timeout(Duration::from_secs(120))
                .build()?,
            blob_ttl,
        };
        let mut connection = cluster.connection();
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
            Self::response(cluster.http.get(format!("{shard}/health")).send().await?).await?;
            for kind in ["paths", "blobs", "commits"] {
                redis::cmd("ZADD")
                    .arg(cluster.key(&format!("{kind}:counts")))
                    .arg("NX")
                    .arg(0)
                    .arg(shard)
                    .query_async::<()>(&mut connection)
                    .await?;
            }
        }
        ensure!(
            connection
                .zcard::<_, usize>(cluster.key("paths:counts"))
                .await?
                > 0,
            "register at least one shard with --shard"
        );
        Ok(cluster)
    }

    fn connection(&self) -> ConnectionManager {
        self.cache.clone()
    }

    fn blob_connection(&self) -> ConnectionManager {
        self.blob_cache[self.next_cache.fetch_add(1, Ordering::Relaxed) % self.blob_cache.len()]
            .clone()
    }

    fn key(&self, key: &str) -> String {
        format!("{}:{key}", self.prefix)
    }

    async fn read<T: redis::FromRedisValue>(&self, command: &mut redis::Cmd) -> Result<T> {
        Self::read_connection(self.connection(), command).await
    }

    async fn read_connection<T: redis::FromRedisValue>(
        mut connection: ConnectionManager,
        command: &mut redis::Cmd,
    ) -> Result<T> {
        for attempt in 0..2 {
            match command.query_async(&mut connection).await {
                Err(error) if attempt == 0 && error.is_connection_dropped() => continue,
                result => return Ok(result?),
            }
        }
        unreachable!()
    }

    async fn owner(&self, kind: &str, item: &str) -> Result<Option<String>> {
        self.read(redis::cmd("HGET").arg(self.key(kind)).arg(item))
            .await
    }

    async fn allocate(&self, kind: &str, item: &str, preferred: Option<&str>) -> Result<String> {
        let script = redis::Script::new(
            "local old = redis.call('HGET', KEYS[1], ARGV[1]); if old then return old end; local shard = ARGV[2]; if shard == '' then shard = redis.call('ZRANGE', KEYS[2], 0, 0)[1] end; if not shard then return redis.error_reply('no shards registered') end; redis.call('HSET', KEYS[1], ARGV[1], shard); redis.call('ZINCRBY', KEYS[2], 1, shard); return shard",
        );
        for attempt in 0..6 {
            let result = script
                .key(self.key(kind))
                .key(self.key(&format!("{kind}:counts")))
                .arg(item)
                .arg(preferred.unwrap_or(""))
                .invoke_async(&mut self.connection())
                .await;
            match result {
                Err(error)
                    if attempt < 5
                        && (error.is_connection_dropped() || error.code() == Some("OOM")) =>
                {
                    tokio::time::sleep(Duration::from_millis(25 << attempt)).await;
                }
                result => return Ok(result?),
            }
        }
        unreachable!()
    }

    async fn response(response: Response) -> Result<Response> {
        ensure!(
            response.status().is_success(),
            "shard returned {}: {}",
            response.status(),
            response.text().await?
        );
        Ok(response)
    }

    async fn send(&self, request: reqwest::RequestBuilder) -> Result<Response> {
        for attempt in 0..6 {
            let response = request
                .try_clone()
                .context("request body cannot be retried")?
                .send()
                .await?;
            if response.status() != reqwest::StatusCode::SERVICE_UNAVAILABLE || attempt == 5 {
                return Ok(response);
            }
            let delay = retry_delay(response.headers(), attempt);
            drop(response);
            tokio::time::sleep(delay).await;
        }
        unreachable!()
    }

    async fn get<T: DeserializeOwned>(&self, shard: &str, route: &str) -> Result<T> {
        Ok(
            Self::response(self.send(self.http.get(format!("{shard}{route}"))).await?)
                .await?
                .json()
                .await?,
        )
    }

    async fn blob_shard(&self, id: &str) -> Result<String> {
        self.owner("blobs", id).await?.context("blob not found")
    }

    pub async fn allocation(&self, path: &str) -> Result<Option<String>> {
        self.owner("paths", path).await
    }
}

#[async_trait]
impl Backend for Cluster {
    async fn put_blob(&self, id: &str, bytes: Bytes, _: usize, path: Option<&str>) -> Result<()> {
        let preferred = match path {
            Some(path) => Some(self.allocate("paths", path, None).await?),
            None => None,
        };
        let shard = self.allocate("blobs", id, preferred.as_deref()).await?;
        Self::response(
            self.send(
                self.http
                    .put(format!("{shard}/internal/blobs/{id}"))
                    .body(bytes.clone()),
            )
            .await?,
        )
        .await?;
        if let Ok(_slot) = self.cache_slots.try_acquire() {
            let _: redis::RedisResult<()> = self
                .blob_connection()
                .set_ex(
                    self.key(&format!("blob:{id}")),
                    bytes.as_ref(),
                    self.blob_ttl,
                )
                .await;
        }
        Ok(())
    }

    async fn blob(&self, id: &str) -> Result<Bytes> {
        let key = self.key(&format!("blob:{id}"));
        if let Ok(_slot) = self.cache_slots.try_acquire() {
            let mut connection = self.blob_connection();
            if let Ok(Some(bytes)) = Self::read_connection::<Option<Vec<u8>>>(
                connection.clone(),
                redis::cmd("GET").arg(&key),
            )
            .await
            {
                let owned = id.to_owned();
                let (bytes, valid) = worker::cpu(move || {
                    let valid = verify_blob(&bytes, &owned).is_ok();
                    Ok((Bytes::from(bytes), valid))
                })
                .await?;
                if valid {
                    return Ok(bytes);
                }
                let _: redis::RedisResult<()> = connection.del(&key).await;
            }
        }
        let shard = self.blob_shard(id).await?;
        let bytes = Self::response(
            self.send(self.http.get(format!("{shard}/internal/blobs/{id}")))
                .await?,
        )
        .await?
        .bytes()
        .await?;
        let owned = id.to_owned();
        let bytes = worker::cpu(move || {
            verify_blob(&bytes, &owned)?;
            Ok(bytes)
        })
        .await?;
        if let Ok(_slot) = self.cache_slots.try_acquire() {
            let _: redis::RedisResult<()> = self
                .blob_connection()
                .set_ex(key, bytes.as_ref(), self.blob_ttl)
                .await;
        }
        Ok(bytes)
    }

    async fn has_blob(&self, id: &str) -> Result<bool> {
        Ok(self.blob_size(id).await?.is_some())
    }

    async fn blob_size(&self, id: &str) -> Result<Option<usize>> {
        let Some(shard) = self.owner("blobs", id).await? else {
            return Ok(None);
        };
        let response = self
            .send(self.http.head(format!("{shard}/internal/blobs/{id}")))
            .await?;
        if response.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok(None);
        }
        let response = Self::response(response).await?;
        let size: usize = response
            .headers()
            .get("x-scn-blob-size")
            .context("missing blob size")?
            .to_str()?
            .parse()?;
        ensure!(size <= crate::store::MAX_BLOB, "invalid blob size");
        Ok(Some(size))
    }

    async fn get_commit(&self, id: &str) -> Result<Commit> {
        let shard = self
            .owner("commits", id)
            .await?
            .context("changeset not found")?;
        let stored: StoredCommit = self.get(&shard, &format!("/internal/commits/{id}")).await?;
        let mut shards = BTreeSet::new();
        if !stored.paths.is_empty() {
            let owners: Vec<Option<String>> = self
                .read(
                    redis::cmd("HMGET")
                        .arg(self.key("paths"))
                        .arg(&stored.paths),
                )
                .await?;
            for owner in owners {
                shards.insert(owner.context("missing path allocation")?);
            }
        }
        let mut commit = stored.commit;
        let deltas: Vec<Changes> = stream::iter(shards)
            .map(|shard| async move { self.get(&shard, &format!("/internal/changes/{id}")).await })
            .buffer_unordered(8)
            .try_collect()
            .await?;
        for changes in deltas {
            commit.changes.extend(changes);
        }
        ensure!(
            commit.changes.keys().eq(stored.paths.iter()),
            "incomplete changeset deltas"
        );
        let id = id.to_owned();
        worker::cpu(move || {
            let expected = std::mem::take(&mut commit.id);
            ensure!(
                hash(&serde_json::to_vec(&commit)?) == expected && expected == id,
                "changeset integrity check failed"
            );
            commit.id = expected;
            Ok(commit)
        })
        .await
    }

    async fn parents(&self, id: &str) -> Result<Vec<String>> {
        let shard = self
            .owner("commits", id)
            .await?
            .context("changeset not found")?;
        let stored: StoredCommit = self.get(&shard, &format!("/internal/commits/{id}")).await?;
        ensure!(
            stored.commit.id == id && stored.commit.changes.is_empty(),
            "invalid changeset header"
        );
        Ok(stored.commit.parents)
    }

    async fn save_commit(&self, commit: &Commit) -> Result<()> {
        let mut groups: BTreeMap<String, Changes> = BTreeMap::new();
        let allocations: Vec<_> = stream::iter(commit.changes.clone())
            .map(|(path, entry): (String, Option<FileEntry>)| async move {
                Ok::<_, anyhow::Error>((self.allocate("paths", &path, None).await?, path, entry))
            })
            .buffer_unordered(16)
            .try_collect()
            .await?;
        for (shard, path, entry) in allocations {
            groups.entry(shard).or_default().insert(path, entry);
        }
        stream::iter(groups)
            .map(|(shard, changes)| async move {
                Self::response(
                    self.send(
                        self.http
                            .put(format!("{shard}/internal/changes/{}", commit.id))
                            .json(&changes),
                    )
                    .await?,
                )
                .await?;
                Ok::<_, anyhow::Error>(())
            })
            .buffer_unordered(8)
            .try_collect::<Vec<_>>()
            .await?;
        let shard = self.allocate("commits", &commit.id, None).await?;
        let mut header = commit.clone();
        header.changes.clear();
        let stored = StoredCommit {
            commit: header,
            paths: commit.changes.keys().cloned().collect(),
        };
        Self::response(
            self.send(
                self.http
                    .put(format!("{shard}/internal/commits/{}", commit.id))
                    .json(&stored),
            )
            .await?,
        )
        .await?;
        Ok(())
    }

    async fn main_ref(&self) -> Result<Option<String>> {
        self.read(redis::cmd("GET").arg(self.key("main"))).await
    }

    async fn tags(&self) -> Result<Vec<Tag>> {
        let values: Vec<String> = self.read(redis::cmd("HVALS").arg(self.key("tags"))).await?;
        values
            .into_iter()
            .map(|value| Ok(serde_json::from_str(&value)?))
            .collect()
    }

    async fn tag(&self, name: &str) -> Result<Option<Tag>> {
        let data: Option<String> = self
            .read(redis::cmd("HGET").arg(self.key("tags")).arg(name))
            .await?;
        data.map(|data| Ok(serde_json::from_str(&data)?))
            .transpose()
    }

    async fn save_tag(&self, tag: &Tag, force: bool) -> Result<bool> {
        let mut connection = self.connection();
        let data = serde_json::to_string(tag)?;
        if force {
            connection
                .hset::<_, _, _, ()>(self.key("tags"), &tag.name, data)
                .await?;
            Ok(true)
        } else {
            Ok(connection
                .hset_nx(self.key("tags"), &tag.name, data)
                .await?)
        }
    }

    async fn delete_tag(&self, name: &str) -> Result<bool> {
        Ok(self.connection().hdel(self.key("tags"), name).await?)
    }

    async fn update_main(&self, update: RefUpdate) -> Result<()> {
        let changed: bool = redis::Script::new("local current = redis.call('GET', KEYS[1]) or ''; if current ~= ARGV[1] then return 0 end; redis.call('SET', KEYS[1], ARGV[2]); return 1")
            .key(self.key("main")).arg(update.expected.unwrap_or_default()).arg(update.target).invoke_async(&mut self.connection()).await?;
        ensure!(changed, "main moved; pull and retry push");
        Ok(())
    }
}
