use crate::{client::Client, model::*, store::MAX_BLOB};
use anyhow::{Context, Result, ensure};
use std::{
    collections::HashMap,
    io::{BufRead, BufReader, Read, Write},
    path::Path,
    process::{Child, ChildStdin, ChildStdout, Command, Stdio},
};

fn git(repository: &Path) -> Command {
    let mut command = Command::new("git");
    command.args(["--no-replace-objects", "-C"]).arg(repository);
    command
}

fn run(repository: &Path, args: &[&str]) -> Result<Vec<u8>> {
    let output = git(repository)
        .args(args)
        .output()
        .context("could not run git; install Git to import a repository")?;
    ensure!(
        output.status.success(),
        "git {}: {}",
        args[0],
        String::from_utf8_lossy(&output.stderr).trim()
    );
    Ok(output.stdout)
}

struct Objects {
    child: Child,
    input: ChildStdin,
    output: BufReader<ChildStdout>,
}

impl Objects {
    fn open(repository: &Path) -> Result<Self> {
        let mut child = git(repository)
            .args(["cat-file", "--batch"])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()?;
        let input = child.stdin.take().unwrap();
        let output = BufReader::new(child.stdout.take().unwrap());
        Ok(Self {
            child,
            input,
            output,
        })
    }

    fn read(&mut self, id: &str, kind: &str) -> Result<Vec<u8>> {
        writeln!(self.input, "{id}")?;
        self.input.flush()?;
        let mut header = String::new();
        self.output.read_line(&mut header)?;
        let fields: Vec<_> = header.split_whitespace().collect();
        ensure!(
            fields.len() == 3 && fields[0] == id && fields[1] == kind,
            "invalid or missing Git object: {id}"
        );
        let size: usize = fields[2].parse()?;
        ensure!(size <= MAX_BLOB, "Git object exceeds 32 MiB limit: {id}");
        let mut bytes = vec![0; size];
        self.output.read_exact(&mut bytes)?;
        let mut newline = [0];
        self.output.read_exact(&mut newline)?;
        ensure!(newline == *b"\n", "invalid Git batch response");
        Ok(bytes)
    }
}

impl Drop for Objects {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

struct GitCommit {
    parents: Vec<String>,
    author: String,
    timestamp: u64,
    message: String,
}

fn parse_commit(bytes: &[u8]) -> Result<GitCommit> {
    let text = std::str::from_utf8(bytes).context("Git commit metadata must be UTF-8")?;
    let (headers, message) = text.split_once("\n\n").context("invalid Git commit")?;
    let parents: Vec<_> = headers
        .lines()
        .filter_map(|line| line.strip_prefix("parent ").map(str::to_owned))
        .collect();
    ensure!(
        parents.len() <= 2,
        "octopus merges with more than two parents are unsupported"
    );
    let author = headers
        .lines()
        .find_map(|line| line.strip_prefix("author "))
        .context("missing Git author")?;
    let mut fields = author.rsplitn(3, ' ');
    fields.next().context("missing Git author timezone")?;
    let seconds: u64 = fields
        .next()
        .context("missing Git author timestamp")?
        .parse()
        .context("Git author timestamp must be non-negative")?;
    let timestamp = seconds
        .checked_mul(1_000_000_000)
        .context("Git author timestamp is out of range")?;
    let author = fields
        .next()
        .context("missing Git author identity")?
        .to_owned();
    Ok(GitCommit {
        parents,
        author,
        timestamp,
        message: message.to_owned(),
    })
}

pub async fn import_git(client: &Client, repository: &Path, revision: &str) -> Result<Commit> {
    ensure!(
        client.main_ref().await?.is_none(),
        "Git import requires an empty main ref"
    );
    ensure!(
        run(repository, &["rev-parse", "--is-shallow-repository"])? == b"false\n",
        "shallow Git repositories are unsupported; fetch the full history first"
    );
    let revision = String::from_utf8(run(
        repository,
        &[
            "rev-parse",
            "--verify",
            "--end-of-options",
            &format!("{revision}^{{commit}}"),
        ],
    )?)?
    .trim()
    .to_owned();
    let history = String::from_utf8(run(
        repository,
        &["rev-list", "--reverse", "--topo-order", &revision, "--"],
    )?)?;
    let mut objects = Objects::open(repository)?;
    let mut commits = HashMap::<String, String>::new();
    let mut blobs = HashMap::<String, String>::new();
    let mut tip = None;
    for id in history.lines() {
        let original = parse_commit(&objects.read(id, "commit")?)
            .with_context(|| format!("cannot import Git commit {id}"))?;
        let mut args = vec![
            "diff-tree",
            "--root",
            "--no-commit-id",
            "--no-renames",
            "--no-abbrev",
            "-r",
            "--raw",
            "-z",
        ];
        if let Some(parent) = original.parents.first() {
            args.push(parent);
        }
        args.extend([id, "--"]);
        let diff = run(repository, &args)?;
        let mut records = diff.split(|byte| *byte == 0);
        let mut changes = Changes::new();
        while let Some(header) = records.next().filter(|header| !header.is_empty()) {
            let fields: Vec<_> = std::str::from_utf8(header)?.split_whitespace().collect();
            ensure!(fields.len() == 5, "invalid Git diff record");
            let path = std::str::from_utf8(records.next().context("missing Git diff path")?)
                .context("Git paths must be UTF-8")?;
            validate_path(path)?;
            let entry = match fields[1] {
                "000000" => None,
                "100644" | "100755" => {
                    let git_blob = fields[3];
                    let blob = match blobs.get(git_blob) {
                        Some(blob) => blob.clone(),
                        None => {
                            let bytes = objects.read(git_blob, "blob")?;
                            let blob = client.put_blob(bytes).await?;
                            blobs.insert(git_blob.to_owned(), blob.clone());
                            blob
                        }
                    };
                    Some(FileEntry {
                        blob,
                        executable: fields[1] == "100755",
                    })
                }
                mode => anyhow::bail!(
                    "unsupported Git file mode {mode} at {path} in {id}; symlinks and submodules are unsupported"
                ),
            };
            changes.insert(path.to_owned(), entry);
        }
        let parents = original
            .parents
            .iter()
            .map(|parent| {
                commits
                    .get(parent)
                    .cloned()
                    .with_context(|| format!("missing imported parent: {parent}"))
            })
            .collect::<Result<_>>()?;
        let commit: Commit = client
            .post(
                "/v1/import/commits",
                &ImportCommitRequest {
                    commit: CommitRequest {
                        parents,
                        author: original.author,
                        message: original.message,
                        changes,
                    },
                    timestamp: original.timestamp,
                    git_id: id.to_owned(),
                },
            )
            .await?;
        println!("{id} -> {}", commit.id);
        commits.insert(id.to_owned(), commit.id.clone());
        tip = Some(commit);
    }
    let tip = tip.context("Git revision has no commits")?;
    client.update_main(None, tip.id.clone()).await?;
    Ok(tip)
}
