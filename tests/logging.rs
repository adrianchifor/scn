use scn::logging::REQUEST_ID;
use serde_json::{Value, json};
use std::{
    fs,
    path::Path,
    process::{Child, Command, Stdio},
    time::Duration,
};

struct Process(Child);

impl Drop for Process {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn records(bytes: &[u8]) -> Vec<Value> {
    std::str::from_utf8(bytes)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect()
}

fn cli(root: &Path, args: &[&str]) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_scn"))
        .current_dir(root)
        .env("RUST_LOG", "warn,scn=debug")
        .env_remove("SCN_LOG_FORMAT")
        .args(args)
        .output()
        .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn structured_logs_correlate_requests_and_preserve_cli_output() {
    let directory = tempfile::tempdir().unwrap();
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    drop(listener);
    let log = directory.path().join("server.jsonl");
    let stdout = directory.path().join("server.stdout");
    let mut server = Process(
        Command::new(env!("CARGO_BIN_EXE_scn"))
            .args([
                "--log-format",
                "json",
                "serve",
                "--listen",
                &address.to_string(),
                "--store",
            ])
            .arg(directory.path().join("store"))
            .env("RUST_LOG", "warn,scn=debug")
            .stdout(Stdio::from(fs::File::create(&stdout).unwrap()))
            .stderr(Stdio::from(fs::File::create(&log).unwrap()))
            .spawn()
            .unwrap(),
    );
    let endpoint = format!("http://{address}");
    let http = reqwest::Client::new();
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if http.get(format!("{endpoint}/health")).send().await.is_ok() {
                break;
            }
            assert!(server.0.try_wait().unwrap().is_none());
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    let bytes = b"binary\0contents\xff\n";
    let response = http
        .post(format!("{endpoint}/v1/blobs"))
        .header(REQUEST_ID, "agent-write-42")
        .body(bytes.to_vec())
        .send()
        .await
        .unwrap();
    assert_eq!(response.headers()[REQUEST_ID], "agent-write-42");
    let blob = response
        .error_for_status()
        .unwrap()
        .json::<Value>()
        .await
        .unwrap()["target"]
        .as_str()
        .unwrap()
        .to_owned();
    let commit = http.post(format!("{endpoint}/v1/commits")).header(REQUEST_ID, "agent-write-42")
        .json(&json!({"parents":[], "author":"tester", "message":"secret-commit-message", "changes":{"file.bin":{"blob":blob,"executable":false}}}))
        .send().await.unwrap().error_for_status().unwrap().json::<Value>().await.unwrap();
    http.post(format!("{endpoint}/v1/refs/main"))
        .header(REQUEST_ID, "agent-write-42")
        .json(&json!({"expected":null,"target":commit["id"]}))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();
    let rejected = http
        .post(format!("{endpoint}/v1/tags"))
        .header(REQUEST_ID, "rejected-tag")
        .json(&json!({"name":"invalid..tag","target":commit["id"]}))
        .send()
        .await
        .unwrap();
    assert_eq!(rejected.status(), 400);
    assert_eq!(rejected.headers()[REQUEST_ID], "rejected-tag");
    let health = http
        .get(format!("{endpoint}/health?token=secret-query-token"))
        .header(REQUEST_ID, "invalid id")
        .bearer_auth("secret-auth-token")
        .send()
        .await
        .unwrap();
    assert_ne!(health.headers()[REQUEST_ID], "invalid id");
    assert!(health.headers()[REQUEST_ID].to_str().unwrap().len() <= 64);
    let cloned = cli(
        directory.path(),
        &["--log-format", "json", "clone", &endpoint, "work"],
    );
    assert!(
        cloned.status.success(),
        "{}",
        String::from_utf8_lossy(&cloned.stderr)
    );
    assert!(!records(&cloned.stderr).is_empty());
    assert!(!cloned.stdout.contains(&0x1b));
    let work = directory.path().join("work");
    let shown = cli(&work, &["show", "HEAD", "file.bin", "--log-format", "json"]);
    assert!(shown.status.success());
    assert_eq!(shown.stdout, bytes);
    assert!(!records(&shown.stderr).is_empty());
    let failed = cli(&work, &["--log-format", "json", "show", "HEAD", "missing"]);
    assert!(!failed.status.success());
    assert!(failed.stdout.is_empty());
    assert!(
        records(&failed.stderr).iter().any(
            |event| event["level"] == "ERROR" && event["fields"]["message"] == "Command failed"
        )
    );
    let plain = cli(&work, &["--no-progress", "status"]);
    assert!(plain.status.success());
    assert_eq!(plain.stdout, b"Working copy clean\n");
    assert!(!plain.stderr.contains(&0x1b));
    assert!(
        Command::new("kill")
            .args(["-INT", &server.0.id().to_string()])
            .status()
            .unwrap()
            .success()
    );
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if let Some(status) = server.0.try_wait().unwrap() {
                assert!(status.success());
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    let payload = fs::read(&log).unwrap();
    let events = records(&payload);
    assert!(fs::read(stdout).unwrap().is_empty());
    for secret in [
        "secret-commit-message",
        "secret-query-token",
        "secret-auth-token",
    ] {
        assert!(!std::str::from_utf8(&payload).unwrap().contains(secret));
    }
    for message in [
        "Server listening",
        "Commit stored",
        "Main advanced",
        "Shutdown requested; draining requests",
        "Server stopped",
    ] {
        assert!(
            events
                .iter()
                .any(|event| event["fields"]["message"] == message),
            "{message}"
        );
    }
    assert!(events.iter().any(|event| {
        event["fields"]["message"] == "Blob stored"
            && event["spans"]
                .as_array()
                .unwrap()
                .iter()
                .any(|span| span["request_id"] == "agent-write-42")
    }));
    assert!(events.iter().any(|event| {
        event["fields"]["status"] == 400
            && event["spans"]
                .as_array()
                .unwrap()
                .iter()
                .any(|span| span["request_id"] == "rejected-tag")
    }));
}
