use crate::{
    content::{CHUNK_SIZE, FileReader, validate_entry},
    model::*,
    store::{MAX_BLOB, decode_blob},
    worker,
};
use anyhow::{Context, Result, ensure};
use reqwest::{Client as HttpClient, Response};
use serde::{Serialize, de::DeserializeOwned};
use std::{
    fs,
    io::{Read, Write},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::{Duration, SystemTime},
};

const CACHE_LIMIT: u64 = 64 * 1024 * 1024;

#[derive(Clone)]
pub struct Client {
    pub url: String,
    http: HttpClient,
    cache: PathBuf,
    cache_size: Arc<Mutex<Option<u64>>>,
}

impl Client {
    pub async fn new(url: &str, cache: &Path) -> Result<Self> {
        let parsed = reqwest::Url::parse(url).context("invalid server URL")?;
        ensure!(
            matches!(parsed.scheme(), "http" | "https")
                && parsed.host_str().is_some()
                && parsed.query().is_none()
                && parsed.fragment().is_none(),
            "expected an http(s) server URL"
        );
        let directory = cache.to_owned();
        worker::io(move || {
            fs::create_dir_all(directory)?;
            Ok(())
        })
        .await?;
        Ok(Self {
            url: url.trim_end_matches('/').to_owned(),
            http: HttpClient::builder()
                .timeout(Duration::from_secs(120))
                .build()?,
            cache: cache.to_owned(),
            cache_size: Arc::new(Mutex::new(None)),
        })
    }

    async fn response(response: Response) -> Result<Response> {
        if !response.status().is_success() {
            let status = response.status();
            let request_id = response
                .headers()
                .get(crate::logging::REQUEST_ID)
                .and_then(|id| id.to_str().ok())
                .map(str::to_owned);
            let body = response.text().await?;
            let message = serde_json::from_str::<serde_json::Value>(&body)
                .ok()
                .and_then(|value| value["error"].as_str().map(str::to_owned))
                .unwrap_or(body);
            if let Some(request_id) = request_id {
                anyhow::bail!("server {status} (request {request_id}): {message}");
            }
            anyhow::bail!("server {status}: {message}");
        }
        Ok(response)
    }

    async fn send(&self, request: reqwest::RequestBuilder) -> Result<Response> {
        let request_id = crate::logging::request_id();
        let request = request
            .header(crate::logging::REQUEST_ID, &request_id)
            .build()?;
        for attempt in 0..6 {
            let started = std::time::Instant::now();
            tracing::debug!(component = "cli", %request_id, method = %request.method(), endpoint = %request.url().origin().ascii_serialization(), path = request.url().path(), attempt = attempt + 1, "Sending HTTP request");
            let response = self
                .http
                .execute(
                    request
                        .try_clone()
                        .context("request body cannot be retried")?,
                )
                .await?;
            tracing::debug!(component = "cli", %request_id, status = response.status().as_u16(), elapsed_ms = started.elapsed().as_millis() as u64, "HTTP response received");
            if response.status() != reqwest::StatusCode::SERVICE_UNAVAILABLE || attempt == 5 {
                return Ok(response);
            }
            let delay = retry_delay(response.headers(), attempt);
            tracing::warn!(component = "cli", %request_id, attempt = attempt + 1, delay_ms = delay.as_millis() as u64, "Server busy; retrying request");
            drop(response);
            tokio::time::sleep(delay).await;
        }
        unreachable!()
    }

    pub async fn get<T: DeserializeOwned>(
        &self,
        route: &str,
        query: &[(&str, String)],
    ) -> Result<T> {
        let response = self
            .send(self.http.get(format!("{}{route}", self.url)).query(query))
            .await
            .context("could not reach scn server")?;
        Ok(Self::response(response).await?.json().await?)
    }

    pub async fn post<T: DeserializeOwned>(&self, route: &str, body: &impl Serialize) -> Result<T> {
        let response = self
            .send(self.http.post(format!("{}{route}", self.url)).json(body))
            .await
            .context("could not reach scn server")?;
        Ok(Self::response(response).await?.json().await?)
    }

    pub async fn main_ref(&self) -> Result<Option<String>> {
        Ok(self.get::<RefValue>("/v1/refs/main", &[]).await?.target)
    }

    pub async fn commit(&self, revision: &str) -> Result<Commit> {
        let revision = revision_segment(revision)?;
        self.get(&format!("/v1/commits/{revision}"), &[]).await
    }

    pub async fn tree(&self, revision: &str, sparse: &[String]) -> Result<Tree> {
        let revision = revision_segment(revision)?;
        let route = format!("/v1/tree/{revision}");
        if sparse.is_empty() {
            return self.get(&route, &[]).await;
        }
        let mut tree = Tree::new();
        for prefix in sparse {
            validate_path(prefix)?;
            tree.extend(
                self.get::<Tree>(&route, &[("prefix", prefix.clone())])
                    .await?,
            );
        }
        Ok(tree)
    }

    pub async fn put_blob(&self, bytes: Vec<u8>) -> Result<String> {
        self.put_blob_at(bytes, None).await
    }

    pub async fn put_file<R: Read + Send + 'static>(
        &self,
        reader: R,
        size: u64,
        executable: bool,
        path: Option<&str>,
    ) -> Result<FileEntry> {
        Ok(self.put_reader(reader, size, executable, path).await?.1)
    }

    pub async fn put_reader<R: Read + Send + 'static>(
        &self,
        reader: R,
        size: u64,
        executable: bool,
        path: Option<&str>,
    ) -> Result<(R, FileEntry)> {
        let (sender, receiver) = tokio::sync::mpsc::channel(2);
        let producer = tokio::spawn(async move {
            let mut reader = FileReader::new(reader, size);
            let mut uploaded = std::collections::BTreeSet::new();
            loop {
                let (next, blob) = worker::io(move || {
                    let blob = reader.next_blob()?;
                    Ok((reader, blob))
                })
                .await?;
                reader = next;
                let Some((id, bytes)) = blob else {
                    break;
                };
                if uploaded.insert(id.clone()) {
                    sender.send((id, bytes)).await?;
                }
            }
            worker::io(move || reader.finish_with_reader(executable)).await
        });
        use futures_util::{StreamExt, TryStreamExt, stream};
        let uploads = stream::unfold(receiver, |mut receiver| async {
            receiver.recv().await.map(|item| (item, receiver))
        })
        .map(|(id, bytes)| async move {
            if size <= MAX_BLOB as u64 || !self.has_blob(&id).await? {
                self.put_blob_at(bytes, if size > MAX_BLOB as u64 { None } else { path })
                    .await?;
            }
            Ok::<_, anyhow::Error>(())
        })
        .buffer_unordered(4)
        .try_collect::<Vec<_>>()
        .await;
        if let Err(error) = uploads {
            producer.abort();
            return Err(error);
        }
        producer.await?
    }

    pub async fn has_blob(&self, id: &str) -> Result<bool> {
        validate_id(id)?;
        let response = self
            .send(self.http.head(format!("{}/v1/blobs/{id}", self.url)))
            .await?;
        if response.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok(false);
        }
        Self::response(response).await?;
        Ok(true)
    }

    pub async fn write_file<W: Write + Send + 'static>(
        &self,
        entry: &FileEntry,
        mut output: W,
    ) -> Result<W> {
        validate_entry(entry)?;
        let mut written = 0;
        use futures_util::{StreamExt, stream};
        let mut downloads = stream::iter(entry.blobs())
            .map(|id| self.blob(id))
            .buffered(4);
        while let Some(bytes) = downloads.next().await {
            let bytes = bytes?;
            if let Some(manifest) = &entry.manifest {
                let expected = (manifest.size - written).min(CHUNK_SIZE as u64);
                ensure!(bytes.len() as u64 == expected, "file chunk size mismatch");
            }
            let length = bytes.len() as u64;
            output = worker::io(move || {
                output.write_all(&bytes)?;
                Ok(output)
            })
            .await?;
            written += length;
        }
        Ok(output)
    }

    pub async fn put_blob_at(&self, bytes: Vec<u8>, path: Option<&str>) -> Result<String> {
        let (bytes, expected) = worker::cpu(move || {
            let expected = hash(&bytes);
            Ok((bytes, expected))
        })
        .await?;
        let mut request = self.http.post(format!("{}/v1/blobs", self.url));
        if let Some(path) = path {
            request = request.query(&[("path", path)]);
        }
        let response = self.send(request.body(bytes)).await?;
        let id = Self::response(response)
            .await?
            .json::<RefValue>()
            .await?
            .target
            .context("missing blob id")?;
        ensure!(id == expected, "server returned incorrect blob hash");
        Ok(id)
    }

    pub async fn blob(&self, id: &str) -> Result<Vec<u8>> {
        validate_id(id)?;
        let path = self.cache.join(id);
        let cached = path.clone();
        if let Some(compressed) = worker::io(move || Ok(fs::read(cached).ok())).await? {
            let owned = id.to_owned();
            if let Ok(bytes) = worker::cpu(move || decode_blob(&compressed, &owned)).await {
                worker::io(move || {
                    if let Ok(file) = fs::File::open(path) {
                        let _ = file.set_modified(SystemTime::now());
                    }
                    Ok(())
                })
                .await?;
                tracing::debug!(blob_id = id, bytes = bytes.len(), "Local blob cache hit");
                return Ok(bytes);
            }
            tracing::warn!(blob_id = id, "Ignoring corrupt local blob cache entry");
        }
        let response = self
            .send(self.http.get(format!("{}/v1/blobs/{id}", self.url)))
            .await?;
        let compressed = Self::response(response).await?.bytes().await?;
        let owned = id.to_owned();
        let (compressed, bytes) = worker::cpu(move || {
            let bytes = decode_blob(&compressed, &owned)?;
            Ok((compressed, bytes))
        })
        .await?;
        let client = self.clone();
        worker::io(move || {
            let mut temp = tempfile::NamedTempFile::new_in(&client.cache)?;
            temp.write_all(&compressed)?;
            let mut size = client.cache_size.lock().unwrap();
            if size.is_none() {
                *size = Some(
                    client
                        .cache_entries()?
                        .iter()
                        .map(|(_, size, _)| size)
                        .sum(),
                );
            }
            let size = size.as_mut().unwrap();
            let previous = fs::metadata(&path).map_or(0, |metadata| metadata.len());
            temp.persist(&path)?;
            *size = size.saturating_sub(previous) + compressed.len() as u64;
            if *size > CACHE_LIMIT {
                *size = client.trim_cache()?;
            }
            Ok(())
        })
        .await?;
        Ok(bytes)
    }

    fn cache_entries(&self) -> Result<Vec<(SystemTime, u64, PathBuf)>> {
        let mut entries = Vec::new();
        for entry in fs::read_dir(&self.cache)? {
            let entry = entry?;
            if validate_id(&entry.file_name().to_string_lossy()).is_err() {
                continue;
            }
            let metadata = entry.metadata()?;
            if !metadata.is_file() {
                continue;
            }
            entries.push((metadata.modified()?, metadata.len(), entry.path()));
        }
        Ok(entries)
    }

    fn trim_cache(&self) -> Result<u64> {
        let mut entries = self.cache_entries()?;
        let mut size: u64 = entries.iter().map(|(_, size, _)| size).sum();
        entries.sort();
        for (_, length, path) in entries {
            if size <= CACHE_LIMIT * 3 / 4 {
                break;
            }
            fs::remove_file(path)?;
            size -= length;
        }
        Ok(size)
    }

    pub async fn land(&self, target: String) -> Result<()> {
        let expected = self.main_ref().await?;
        self.update_main(expected, target).await
    }

    pub async fn tags(&self) -> Result<Vec<Tag>> {
        self.get("/v1/tags", &[]).await
    }

    pub async fn tag(&self, name: &str) -> Result<Tag> {
        validate_tag_name(name)?;
        self.get(&format!("/v1/tags/{}", revision_segment(name)?), &[])
            .await
    }

    pub async fn delete_tag(&self, name: &str) -> Result<()> {
        validate_tag_name(name)?;
        let response = self
            .send(
                self.http
                    .delete(format!("{}/v1/tags/{}", self.url, revision_segment(name)?)),
            )
            .await?;
        Self::response(response).await?;
        Ok(())
    }

    pub async fn update_main(&self, expected: Option<String>, target: String) -> Result<()> {
        let response = self
            .send(
                self.http
                    .post(format!("{}/v1/refs/main", self.url))
                    .json(&RefUpdate { expected, target }),
            )
            .await?;
        Self::response(response).await?;
        Ok(())
    }
}

pub(crate) fn retry_delay(headers: &reqwest::header::HeaderMap, attempt: u32) -> Duration {
    let seconds = headers
        .get(reqwest::header::RETRY_AFTER)
        .and_then(|value| value.to_str().ok()?.parse::<u64>().ok())
        .unwrap_or(1)
        .clamp(1, 30)
        .max(1 << attempt.min(2));
    let jitter = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default()
        .subsec_nanos() as u64
        % 250;
    Duration::from_millis(seconds * 1000 + jitter)
}

pub fn validate_revision(revision: &str) -> Result<()> {
    validate_tag_name(revision.strip_prefix("refs/tags/").unwrap_or(revision))
}

pub fn revision_segment(revision: &str) -> Result<String> {
    validate_revision(revision)?;
    Ok(
        percent_encoding::utf8_percent_encode(revision, percent_encoding::NON_ALPHANUMERIC)
            .to_string(),
    )
}
