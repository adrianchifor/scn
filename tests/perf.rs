use anyhow::Result;
use scn::{model::*, store::Store};
use std::{hint::black_box, time::Instant};

#[tokio::test]
#[ignore = "local performance measurements; run with --release --ignored --nocapture"]
async fn local_performance() -> Result<()> {
    let count: usize = std::env::var("SCN_PERF_COMMITS")
        .ok()
        .unwrap_or_else(|| "500".into())
        .parse()?;
    anyhow::ensure!(count >= 2, "at least two commits are required");
    let directory = tempfile::tempdir()?;
    let store = Store::open(directory.path())?;
    let blob = store.put_blob(b"initial\n").await?;
    let changes = (0..64)
        .map(|n| {
            (
                format!("src/file{n}"),
                Some(FileEntry {
                    blob: blob.clone(),
                    executable: false,
                    manifest: None,
                }),
            )
        })
        .collect();
    let root = store
        .create_commit(CommitRequest {
            parents: vec![],
            author: "benchmark".into(),
            message: "root".into(),
            changes,
        })
        .await?;
    let mut head = root.id;
    let mut parent = String::new();
    let start = Instant::now();
    for n in 1..count {
        let blob = store.put_blob(format!("contents {n}\n").as_bytes()).await?;
        let commit = store
            .create_commit(CommitRequest {
                parents: vec![head.clone()],
                author: "benchmark".into(),
                message: format!("change {n}"),
                changes: Changes::from([(
                    format!("src/file{}", n % 64),
                    Some(FileEntry {
                        blob,
                        executable: false,
                        manifest: None,
                    }),
                )]),
            })
            .await?;
        parent = head;
        head = commit.id;
    }
    println!(
        "append_{}_commits_ms={:.3}",
        count - 1,
        start.elapsed().as_secs_f64() * 1000.0
    );
    let start = Instant::now();
    for _ in 0..100 {
        black_box(store.tree(&head).await?);
    }
    println!(
        "tree_100_reads_ms={:.3}",
        start.elapsed().as_secs_f64() * 1000.0
    );
    let start = Instant::now();
    for _ in 0..100 {
        assert!(store.is_ancestor(&parent, &head).await?);
    }
    println!(
        "parent_100_checks_ms={:.3}",
        start.elapsed().as_secs_f64() * 1000.0
    );
    let mut seed = 1u64;
    let data: Vec<u8> = (0..1024 * 1024)
        .map(|_| {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed as u8
        })
        .collect();
    let blob = store.put_blob(&data).await?;
    let start = Instant::now();
    for _ in 0..50 {
        assert_eq!(store.blob(&blob).await?, data);
    }
    println!(
        "blob_50_reads_1mib_ms={:.3}",
        start.elapsed().as_secs_f64() * 1000.0
    );
    Ok(())
}
