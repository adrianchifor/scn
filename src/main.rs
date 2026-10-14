use anyhow::{Context, Result, bail};
use clap::{Parser, Subcommand};
use scn::{
    model::*,
    server,
    store::{Store, patch},
    workspace::Workspace,
};
use std::{
    fs,
    io::{self, Write},
    path::PathBuf,
};

#[derive(Parser)]
#[command(name = "scn", version, about = "Small, centralized source control")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    #[command(about = "Import a Git revision and its history into an empty scn server")]
    ImportGit {
        repository: PathBuf,
        url: String,
        #[arg(long, default_value = "HEAD")]
        rev: String,
    },
    #[command(about = "Run a source control server")]
    Serve {
        #[arg(long, default_value = ".scn-store")]
        store: PathBuf,
        #[arg(long, default_value = "127.0.0.1:7878")]
        listen: String,
    },
    #[command(about = "Create a working copy without downloading history")]
    Clone {
        url: String,
        directory: PathBuf,
        #[arg(long, default_value = "main")]
        rev: String,
        #[arg(long, value_name = "PATH")]
        sparse: Vec<String>,
    },
    #[command(about = "Show added, modified, and deleted files")]
    Status,
    #[command(about = "Create a server-side changeset from all local changes")]
    Commit {
        #[arg(short, long)]
        message: String,
        #[arg(long)]
        author: Option<String>,
    },
    #[command(about = "Check out a revision, keeping the current sparse selection")]
    Checkout {
        revision: String,
        #[arg(long, value_name = "PATH", conflicts_with = "full")]
        sparse: Vec<String>,
        #[arg(long)]
        full: bool,
    },
    #[command(about = "Show local changes, or compare two server revisions")]
    Diff {
        #[arg(requires = "to")]
        from: Option<String>,
        #[arg(requires = "from")]
        to: Option<String>,
    },
    #[command(about = "Show first-parent changeset history")]
    Log {
        #[arg(default_value = "HEAD")]
        revision: String,
        #[arg(short = 'n', long, default_value_t = 20)]
        limit: usize,
        #[arg(long)]
        path: Option<String>,
    },
    #[command(about = "Show changeset JSON, or raw file contents at a revision")]
    Show {
        #[arg(default_value = "HEAD")]
        revision: String,
        path: Option<String>,
    },
    #[command(about = "Show first-parent line attribution")]
    Blame {
        path: String,
        #[arg(long, default_value = "HEAD")]
        rev: String,
    },
    #[command(about = "Merge a revision into the current changeset on the server")]
    Merge {
        revision: String,
        #[arg(long)]
        author: Option<String>,
    },
    #[command(about = "Replay local changesets onto a revision on the server")]
    Rebase {
        #[arg(default_value = "main")]
        revision: String,
        #[arg(long)]
        author: Option<String>,
    },
    #[command(about = "Fast-forward main to the current changeset using compare-and-swap")]
    Land,
}

fn author(value: Option<String>) -> String {
    value
        .or_else(|| std::env::var("SCN_AUTHOR").ok())
        .or_else(|| std::env::var("USER").ok())
        .unwrap_or_else(|| "unknown".to_owned())
}

fn print_diff(
    path: &str,
    before: Option<&FileEntry>,
    after: Option<&FileEntry>,
    patch: Option<String>,
) {
    println!("diff {path}");
    if before.map(|e| e.executable) != after.map(|e| e.executable) {
        println!("mode {} -> {}", mode(before), mode(after));
    }
    match patch {
        Some(patch) => print!("{patch}"),
        None if before.map(|entry| &entry.blob) != after.map(|entry| &entry.blob) => {
            println!("Binary files differ")
        }
        None => {}
    }
}

fn mode(entry: Option<&FileEntry>) -> &str {
    match entry {
        Some(entry) if entry.executable => "100755",
        Some(_) => "100644",
        None => "absent",
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let command = Cli::parse().command;
    match command {
        Command::ImportGit {
            repository,
            url,
            rev,
        } => {
            let cache = tempfile::tempdir()?;
            let client = scn::client::Client::new(&url, cache.path())?;
            let commit = scn::import::import_git(&client, &repository, &rev).await?;
            println!("main -> {}", commit.id);
            return Ok(());
        }
        Command::Serve { store, listen } => {
            return server::serve(Store::open(&store)?, &listen).await;
        }
        Command::Clone {
            url,
            directory,
            rev,
            sparse,
        } => {
            let workspace = Workspace::clone(&url, &directory, &rev, sparse).await?;
            println!("Working copy: {}", workspace.root.display());
            return Ok(());
        }
        _ => {}
    }
    let mut workspace = Workspace::find(&std::env::current_dir()?)?;
    let _lock = workspace.lock()?;
    let client = workspace.client()?;
    let rebase = matches!(&command, Command::Rebase { .. });
    match command {
        Command::Status => {
            let files = workspace.scan()?;
            let changes = changes_between(&workspace.state.files, &files);
            for (path, entry) in &changes {
                let status = if entry.is_none() {
                    'D'
                } else if workspace.state.files.contains_key(path) {
                    'M'
                } else {
                    'A'
                };
                println!("{status} {path}");
            }
            if changes.is_empty() {
                println!("Working copy clean");
            }
        }
        Command::Commit {
            message,
            author: name,
        } => {
            let commit = workspace.commit(&client, message, author(name)).await?;
            println!("{} {}", commit.id, commit.message);
        }
        Command::Checkout {
            revision,
            sparse,
            full,
        } => {
            let sparse = if full {
                Some(vec![])
            } else if sparse.is_empty() {
                None
            } else {
                Some(sparse)
            };
            workspace.checkout(&client, &revision, sparse).await?;
            println!("Checked out {}", workspace.state.revision.unwrap());
        }
        Command::Diff { from, to } => {
            if let (Some(from), Some(to)) = (from, to) {
                let from = workspace.revision(&from)?;
                let to = workspace.revision(&to)?;
                let diff: Vec<DiffEntry> =
                    client.get(&format!("/v1/diff/{from}/{to}"), &[]).await?;
                for entry in diff {
                    print_diff(
                        &entry.path,
                        entry.before.as_ref(),
                        entry.after.as_ref(),
                        entry.patch,
                    );
                }
            } else {
                let files = workspace.scan()?;
                for (path, entry) in changes_between(&workspace.state.files, &files) {
                    let before = workspace.state.files.get(&path);
                    let old = if let Some(before) = before {
                        client.blob(&before.blob).await?
                    } else {
                        vec![]
                    };
                    let new = if entry.is_some() {
                        fs::read(workspace.path(&path)?)?
                    } else {
                        vec![]
                    };
                    print_diff(&path, before, entry.as_ref(), patch(&path, &old, &new));
                }
            }
        }
        Command::Log {
            revision,
            limit,
            path,
        } => {
            if revision == "HEAD" && workspace.state.revision.is_none() {
                println!("No changesets");
                return Ok(());
            }
            let revision = workspace.revision(&revision)?;
            let mut query = vec![("limit", limit.to_string())];
            if let Some(path) = path {
                validate_path(&path)?;
                query.push(("path", path));
            }
            let commits: Vec<Commit> = client.get(&format!("/v1/log/{revision}"), &query).await?;
            for commit in commits {
                println!(
                    "{} {} {}\n    {}",
                    commit.id,
                    commit.author,
                    commit.timestamp / 1_000_000_000,
                    commit.message
                );
            }
        }
        Command::Show { revision, path } => {
            let revision = workspace.revision(&revision)?;
            let commit = client.commit(&revision).await?;
            if let Some(path) = path {
                validate_path(&path)?;
                let tree = client.tree(&commit.id, std::slice::from_ref(&path)).await?;
                let entry = tree.get(&path).context("file not found in this revision")?;
                io::stdout()
                    .lock()
                    .write_all(&client.blob(&entry.blob).await?)?;
            } else {
                println!("{}", serde_json::to_string_pretty(&commit)?);
            }
        }
        Command::Blame { path, rev } => {
            validate_path(&path)?;
            let revision = workspace.revision(&rev)?;
            let lines: Vec<BlameLine> = client
                .get(&format!("/v1/blame/{revision}"), &[("path", path)])
                .await?;
            for (index, line) in lines.iter().enumerate() {
                print!(
                    "{} {:>4} {} | {}",
                    &line.commit[..12],
                    index + 1,
                    line.author,
                    line.line
                );
                if !line.line.ends_with('\n') {
                    println!();
                }
            }
        }
        Command::Merge {
            revision,
            author: name,
        }
        | Command::Rebase {
            revision,
            author: name,
        } => {
            workspace.ensure_clean()?;
            let route = if rebase { "/v1/rebase" } else { "/v1/merge" };
            let head = workspace.revision("HEAD")?;
            let onto = workspace.revision(&revision)?;
            let commit: Commit = client
                .post(
                    route,
                    &IntegrateRequest {
                        head,
                        onto,
                        author: author(name),
                    },
                )
                .await?;
            workspace
                .materialize(
                    &client,
                    Some(commit.id.clone()),
                    workspace.state.sparse.clone(),
                )
                .await?;
            println!("{} {}", commit.id, commit.message);
        }
        Command::Land => {
            workspace.ensure_clean()?;
            let head = workspace.revision("HEAD")?;
            client.land(head.clone()).await?;
            println!("main -> {head}");
        }
        _ => bail!("unexpected command"),
    }
    Ok(())
}
