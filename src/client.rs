use crate::{model::*, store::decode_blob};
use anyhow::{Context, Result, ensure};
use reqwest::{Client as HttpClient, Response};
use serde::{Serialize, de::DeserializeOwned};
use std::{
    fs,
    io::Write,
    path::{Path, PathBuf},
    sync::Mutex,
    time::{Duration, SystemTime},
};

const CACHE_LIMIT: u64 = 64 * 1024 * 1024;

pub struct Client {
    pub url: String,
    http: HttpClient,
    cache: PathBuf,
    cache_size: Mutex<Option<u64>>,
}

impl Client {
    pub fn new(url: &str, cache: &Path) -> Result<Self> {
        let parsed = reqwest::Url::parse(url).context("invalid server URL")?;
        ensure!(
            matches!(parsed.scheme(), "http" | "https")
                && parsed.host_str().is_some()
                && parsed.query().is_none()
                && parsed.fragment().is_none(),
            "expected an http(s) server URL"
        );
        fs::create_dir_all(cache)?;
        Ok(Self {
            url: url.trim_end_matches('/').to_owned(),
            http: HttpClient::builder()
                .timeout(Duration::from_secs(120))
                .build()?,
            cache: cache.to_owned(),
            cache_size: Mutex::new(None),
        })
    }

    async fn response(response: Response) -> Result<Response> {
        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await?;
            let message = serde_json::from_str::<serde_json::Value>(&body)
                .ok()
                .and_then(|value| value["error"].as_str().map(str::to_owned))
                .unwrap_or(body);
            anyhow::bail!("server {status}: {message}");
        }
        Ok(response)
    }

    pub async fn get<T: DeserializeOwned>(
        &self,
        route: &str,
        query: &[(&str, String)],
    ) -> Result<T> {
        let response = self
            .http
            .get(format!("{}{route}", self.url))
            .query(query)
            .send()
            .await
            .context("could not reach scn server")?;
        Ok(Self::response(response).await?.json().await?)
    }

    pub async fn post<T: DeserializeOwned>(&self, route: &str, body: &impl Serialize) -> Result<T> {
        let response = self
            .http
            .post(format!("{}{route}", self.url))
            .json(body)
            .send()
            .await
            .context("could not reach scn server")?;
        Ok(Self::response(response).await?.json().await?)
    }

    pub async fn main_ref(&self) -> Result<Option<String>> {
        Ok(self.get::<RefValue>("/v1/refs/main", &[]).await?.target)
    }

    pub async fn commit(&self, revision: &str) -> Result<Commit> {
        validate_revision(revision)?;
        self.get(&format!("/v1/commits/{revision}"), &[]).await
    }

    pub async fn tree(&self, revision: &str, sparse: &[String]) -> Result<Tree> {
        validate_revision(revision)?;
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

    pub async fn put_blob_at(&self, bytes: Vec<u8>, path: Option<&str>) -> Result<String> {
        let expected = hash(&bytes);
        let mut request = self.http.post(format!("{}/v1/blobs", self.url));
        if let Some(path) = path {
            request = request.query(&[("path", path)]);
        }
        let response = request.body(bytes).send().await?;
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
        if let Ok(compressed) = fs::read(&path) {
            if let Ok(bytes) = decode_blob(&compressed, id) {
                if let Ok(file) = fs::File::open(&path) {
                    let _ = file.set_modified(std::time::SystemTime::now());
                }
                return Ok(bytes);
            }
            let mut size = self.cache_size.lock().unwrap();
            if let Ok(metadata) = fs::metadata(&path) {
                fs::remove_file(&path)?;
                if let Some(size) = size.as_mut() {
                    *size = size.saturating_sub(metadata.len());
                }
            }
        }
        let response = self
            .http
            .get(format!("{}/v1/blobs/{id}", self.url))
            .send()
            .await?;
        let compressed = Self::response(response).await?.bytes().await?;
        let bytes = decode_blob(&compressed, id)?;
        let mut temp = tempfile::NamedTempFile::new_in(&self.cache)?;
        temp.write_all(&compressed)?;
        let mut size = self.cache_size.lock().unwrap();
        if size.is_none() {
            *size = Some(self.cache_entries()?.iter().map(|(_, size, _)| size).sum());
        }
        let size = size.as_mut().unwrap();
        let previous = fs::metadata(&path).map_or(0, |metadata| metadata.len());
        temp.persist(&path)?;
        *size = size.saturating_sub(previous) + compressed.len() as u64;
        if *size > CACHE_LIMIT {
            *size = self.trim_cache()?;
        }
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

    pub async fn update_main(&self, expected: Option<String>, target: String) -> Result<()> {
        let response = self
            .http
            .post(format!("{}/v1/refs/main", self.url))
            .json(&RefUpdate { expected, target })
            .send()
            .await?;
        Self::response(response).await?;
        Ok(())
    }
}

pub fn validate_revision(revision: &str) -> Result<()> {
    if revision == "main" {
        Ok(())
    } else {
        validate_id(revision)
    }
}
