use crate::{model::*, store::MAX_BLOB};
use anyhow::{Result, ensure};
use std::io::Read;

pub const CHUNK_SIZE: usize = 8 * 1024 * 1024;

pub struct FileReader<R> {
    reader: R,
    size: u64,
    remaining: u64,
    chunks: Vec<String>,
}

impl<R: Read> FileReader<R> {
    pub fn new(reader: R, size: u64) -> Self {
        Self {
            reader,
            size,
            remaining: size,
            chunks: Vec::new(),
        }
    }

    pub fn next_blob(&mut self) -> Result<Option<(String, Vec<u8>)>> {
        if self.remaining == 0 && !self.chunks.is_empty() {
            return Ok(None);
        }
        let limit = if self.size <= MAX_BLOB as u64 {
            MAX_BLOB
        } else {
            CHUNK_SIZE
        };
        let mut bytes = vec![0; self.remaining.min(limit as u64) as usize];
        self.reader.read_exact(&mut bytes)?;
        self.remaining -= bytes.len() as u64;
        let id = hash(&bytes);
        self.chunks.push(id.clone());
        Ok(Some((id, bytes)))
    }

    pub fn finish(mut self, executable: bool) -> Result<FileEntry> {
        ensure!(
            self.remaining == 0 && !self.chunks.is_empty(),
            "file was not fully read"
        );
        ensure!(
            self.reader.read(&mut [0])? == 0,
            "file changed while reading; retry"
        );
        if self.size > MAX_BLOB as u64 {
            return manifest_entry(
                FileManifest {
                    size: self.size,
                    chunks: self.chunks,
                },
                executable,
            );
        }
        Ok(FileEntry {
            blob: self.chunks.remove(0),
            executable,
            manifest: None,
        })
    }
}

pub fn manifest_entry(manifest: FileManifest, executable: bool) -> Result<FileEntry> {
    let entry = FileEntry {
        blob: hash(&serde_json::to_vec(&manifest)?),
        executable,
        manifest: Some(manifest),
    };
    validate_entry(&entry)?;
    Ok(entry)
}

pub fn read_entry(
    reader: impl Read,
    size: u64,
    executable: bool,
    mut blob: impl FnMut(&str, &[u8]) -> Result<()>,
) -> Result<FileEntry> {
    let mut reader = FileReader::new(reader, size);
    while let Some((id, bytes)) = reader.next_blob()? {
        blob(&id, &bytes)?;
    }
    reader.finish(executable)
}

pub fn validate_entry(entry: &FileEntry) -> Result<()> {
    validate_id(&entry.blob)?;
    if let Some(manifest) = &entry.manifest {
        ensure!(
            manifest.size > MAX_BLOB as u64,
            "only files over 32 MiB use a manifest"
        );
        ensure!(
            manifest.size.div_ceil(CHUNK_SIZE as u64) == manifest.chunks.len() as u64,
            "invalid file chunk count"
        );
        for id in &manifest.chunks {
            validate_id(id)?;
        }
        ensure!(
            hash(&serde_json::to_vec(manifest)?) == entry.blob,
            "file manifest integrity check failed"
        );
    }
    Ok(())
}
