use crate::{model::*, store::decode_blob};
use anyhow::{Context, Result, ensure};
use reqwest::{Client as HttpClient, Response};
use serde::{Serialize, de::DeserializeOwned};
use std::{
    fs,
    io::Write,
    path::{Path, PathBuf},
    time::Duration,
};

const CACHE_LIMIT: u64 = 64 * 1024 * 1024;

pub struct Client {
    pub url: String,
    http: HttpClient,
    cache: PathBuf,
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
        let expected = hash(&bytes);
        let response = self
            .http
            .post(format!("{}/v1/blobs", self.url))
            .body(bytes)
            .send()
            .await?;
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
                fs::File::open(&path)?.set_modified(std::time::SystemTime::now())?;
                return Ok(bytes);
            }
            fs::remove_file(&path)?;
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
        temp.persist(&path)?;
        self.trim_cache()?;
        Ok(bytes)
    }

    fn trim_cache(&self) -> Result<()> {
        let mut entries = Vec::new();
        let mut size = 0;
        for entry in fs::read_dir(&self.cache)? {
            let entry = entry?;
            let metadata = entry.metadata()?;
            if !metadata.is_file() {
                continue;
            }
            size += metadata.len();
            entries.push((metadata.modified()?, metadata.len(), entry.path()));
        }
        entries.sort();
        for (_, length, path) in entries {
            if size <= CACHE_LIMIT {
                break;
            }
            fs::remove_file(path)?;
            size -= length;
        }
        Ok(())
    }

    pub async fn land(&self, target: String) -> Result<()> {
        let expected = self.main_ref().await?;
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
