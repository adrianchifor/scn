use anyhow::{Context, Result, ensure};
use std::{fs, net::SocketAddr, path::Path, process::Command};

pub fn serve(
    binary: &str,
    directory: &Path,
    listen: SocketAddr,
    maxmemory: &str,
    fsync: &str,
) -> Result<()> {
    ensure!(
        matches!(fsync, "always" | "everysec"),
        "appendfsync must be always or everysec"
    );
    ensure!(
        !maxmemory.is_empty() && maxmemory != "0",
        "maxmemory must be a positive size"
    );
    fs::create_dir_all(directory)?;
    tracing::info!(%listen, maxmemory, appendfsync = fsync, "Starting Valkey");
    let status = Command::new(binary)
        .args([
            "--bind",
            &listen.ip().to_string(),
            "--port",
            &listen.port().to_string(),
            "--dir",
        ])
        .arg(directory.canonicalize()?)
        .args([
            "--appendonly",
            "yes",
            "--appendfsync",
            fsync,
            "--save",
            "900 1 300 10 60 10000",
            "--maxmemory",
            maxmemory,
            "--maxmemory-policy",
            "volatile-lru",
            "--daemonize",
            "no",
        ])
        .status()
        .context("could not start valkey-server; install Valkey or set --binary")?;
    ensure!(status.success(), "Valkey exited with {status}");
    Ok(())
}
