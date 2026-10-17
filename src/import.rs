use crate::{client::Client, model::*, store::MAX_BLOB, worker};
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
    child: Option<Child>,
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
            child: Some(child),
            input,
            output,
        })
    }

    fn header(&mut self, id: &str, kind: &str) -> Result<u64> {
        writeln!(self.input, "{id}")?;
        self.input.flush()?;
        let mut header = String::new();
        self.output.read_line(&mut header)?;
        let fields: Vec<_> = header.split_whitespace().collect();
        ensure!(
            fields.len() == 3 && fields[0] == id && fields[1] == kind,
            "invalid or missing Git object: {id}"
        );
        Ok(fields[2].parse()?)
    }

    fn newline(&mut self) -> Result<()> {
        let mut newline = [0];
        self.output.read_exact(&mut newline)?;
        ensure!(newline == *b"\n", "invalid Git batch response");
        Ok(())
    }

    fn read(&mut self, id: &str, kind: &str) -> Result<Vec<u8>> {
        let size = self.header(id, kind)?;
        ensure!(
            size <= MAX_BLOB as u64,
            "Git metadata exceeds 32 MiB limit: {id}"
        );
        let size = size as usize;
        let mut bytes = vec![0; size];
        self.output.read_exact(&mut bytes)?;
        self.newline()?;
        Ok(bytes)
    }

    async fn upload_blob(
        mut self,
        client: &Client,
        id: &str,
        path: &str,
    ) -> Result<(Self, FileEntry)> {
        let id = id.to_owned();
        let (objects, size) = worker::io(move || {
            let size = self.header(&id, "blob")?;
            Ok((self, size))
        })
        .await?;
        let (reader, entry) = client
            .put_reader(objects.take(size), size, false, Some(path))
            .await?;
        let mut objects = reader.into_inner();
        worker::io(move || {
            objects.newline()?;
            Ok((objects, entry))
        })
        .await
    }
}

impl Read for Objects {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        self.output.read(buffer)
    }
}

impl Drop for Objects {
    fn drop(&mut self) {
        if let Some(mut child) = self.child.take() {
            if let Ok(runtime) = tokio::runtime::Handle::try_current() {
                runtime.spawn_blocking(move || {
                    let _ = child.kill();
                    let _ = child.wait();
                });
            } else {
                let _ = child.kill();
                let _ = child.wait();
            }
        }
    }
}

struct GitCommit {
    parents: Vec<String>,
    author: String,
    timestamp: u64,
    message: String,
}

fn parse_identity(value: &str) -> Result<(String, u64)> {
    let mut fields = value.rsplitn(3, ' ');
    fields.next().context("missing Git identity timezone")?;
    let seconds: u64 = fields
        .next()
        .context("missing Git identity timestamp")?
        .parse()
        .context("Git timestamp must be non-negative")?;
    let timestamp = seconds
        .checked_mul(1_000_000_000)
        .context("Git timestamp is out of range")?;
    let identity = fields.next().context("missing Git identity")?.to_owned();
    Ok((identity, timestamp))
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
    let (author, timestamp) = parse_identity(author)?;
    Ok(GitCommit {
        parents,
        author,
        timestamp,
        message: message.to_owned(),
    })
}

fn git_tags(repository: &Path, objects: &mut Objects) -> Result<Vec<Tag>> {
    let refs = String::from_utf8(run(
        repository,
        &[
            "for-each-ref",
            "--format=%(refname:strip=2)%00%(objectname)%00%(objecttype)",
            "refs/tags/",
        ],
    )?)
    .context("Git tag names must be UTF-8")?;
    let mut tags = Vec::new();
    for record in refs.lines() {
        let fields: Vec<_> = record.split('\0').collect();
        ensure!(fields.len() == 3, "invalid Git tag ref");
        let (name, id, object_type) = (fields[0], fields[1], fields[2]);
        validate_tag_name(name)?;
        let mut target = id.to_owned();
        let mut kind = object_type.to_owned();
        let mut annotation = None;
        while kind == "tag" {
            let bytes = objects.read(&target, "tag")?;
            let text = std::str::from_utf8(&bytes).context("Git tag metadata must be UTF-8")?;
            let (headers, message) = text.split_once("\n\n").context("invalid Git tag object")?;
            let field = |prefix: &str| {
                headers
                    .lines()
                    .find_map(|line| line.strip_prefix(prefix))
                    .with_context(|| format!("missing Git tag {prefix}"))
            };
            target = field("object ")?.to_owned();
            validate_git_id(&target)?;
            kind = field("type ")?.to_owned();
            if annotation.is_none() {
                let (tagger, timestamp) = parse_identity(field("tagger ")?)?;
                annotation = Some(TagAnnotation {
                    tagger,
                    timestamp,
                    message: message.to_owned(),
                });
            }
        }
        ensure!(kind == "commit", "tag {name} must point to a commit");
        tags.push(Tag {
            name: name.to_owned(),
            target,
            annotation,
            git_id: (object_type == "tag").then(|| id.to_owned()),
        });
    }
    Ok(tags)
}

fn git_history(repository: &Path, revision: &str, tags: &[Tag]) -> Result<String> {
    let mut child = git(repository)
        .args(["rev-list", "--reverse", "--topo-order", "--stdin"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;
    {
        let mut input = child.stdin.take().unwrap();
        writeln!(input, "{revision}")?;
        for tag in tags {
            writeln!(input, "{}", tag.target)?;
        }
    }
    let output = child.wait_with_output()?;
    ensure!(
        output.status.success(),
        "git rev-list: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    Ok(String::from_utf8(output.stdout)?)
}

pub async fn import_git(client: &Client, repository: &Path, revision: &str) -> Result<Commit> {
    ensure!(
        client.main_ref().await?.is_none(),
        "Git import requires an empty main ref"
    );
    let repository = repository.to_owned();
    let requested = revision.to_owned();
    let directory = repository.clone();
    let (mut objects, revision, tags, history) = worker::io(move || {
        let repository = directory;
        let revision = requested;
        ensure!(
            run(&repository, &["rev-parse", "--is-shallow-repository"])? == b"false\n",
            "shallow Git repositories are unsupported; fetch the full history first"
        );
        let revision = String::from_utf8(run(
            &repository,
            &[
                "rev-parse",
                "--verify",
                "--end-of-options",
                &format!("{revision}^{{commit}}"),
            ],
        )?)?
        .trim()
        .to_owned();
        let mut objects = Objects::open(&repository)?;
        let tags = git_tags(&repository, &mut objects)?;
        let history = git_history(&repository, &revision, &tags)?;
        Ok((objects, revision, tags, history))
    })
    .await?;
    let mut commits = HashMap::<String, String>::new();
    let mut blobs = HashMap::<String, FileEntry>::new();
    let mut tip = None;
    for id in history.lines() {
        let owned = id.to_owned();
        let (next, original) = worker::io(move || {
            let original = parse_commit(&objects.read(&owned, "commit")?)
                .with_context(|| format!("cannot import Git commit {owned}"))?;
            Ok((objects, original))
        })
        .await?;
        objects = next;
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
        let args: Vec<String> = args.into_iter().map(str::to_owned).collect();
        let directory = repository.clone();
        let diff = worker::io(move || {
            run(
                &directory,
                &args.iter().map(String::as_str).collect::<Vec<_>>(),
            )
        })
        .await?;
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
                    let mut entry = match blobs.get(git_blob) {
                        Some(entry) => entry.clone(),
                        None => {
                            let (next, entry) = objects.upload_blob(client, git_blob, path).await?;
                            objects = next;
                            blobs.insert(git_blob.to_owned(), entry.clone());
                            entry
                        }
                    };
                    entry.executable = fields[1] == "100755";
                    Some(entry)
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
        if id == revision {
            tip = Some(commit);
        }
    }
    let tip = tip.context("Git revision has no commits")?;
    for mut tag in tags {
        tag.target = commits
            .get(&tag.target)
            .context("missing imported tag target")?
            .clone();
        client.post::<Tag>("/v1/import/tags", &tag).await?;
    }
    client.update_main(None, tip.id.clone()).await?;
    Ok(tip)
}
