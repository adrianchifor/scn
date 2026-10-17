use anyhow::{Context, Result, bail, ensure};
use clap::{Args, Parser, Subcommand};
use scn::{
    client::{Client, revision_segment},
    model::*,
    server,
    store::{Store, patch},
    workspace::Workspace,
};
use std::{fs, io, net::SocketAddr, path::PathBuf, sync::Arc};

#[derive(Parser)]
#[command(name = "scn", version, about)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Args)]
struct BlobArgs {
    #[arg(long)]
    s3_bucket: Option<String>,
    #[arg(long, default_value = "us-east-1")]
    s3_region: String,
    #[arg(long, requires = "s3_bucket")]
    s3_endpoint: Option<String>,
    #[arg(long, default_value = "scn/blobs")]
    s3_prefix: String,
}

impl BlobArgs {
    fn storage(&self, root: &std::path::Path) -> Result<scn::storage::Blobs> {
        match &self.s3_bucket {
            Some(bucket) => scn::storage::Blobs::s3(
                bucket,
                &self.s3_region,
                self.s3_endpoint.as_deref(),
                &self.s3_prefix,
            ),
            None => scn::storage::Blobs::disk(&root.join("blobs")),
        }
    }
}

#[derive(Args)]
struct ShardArgs {
    #[command(subcommand)]
    command: Option<ShardCommand>,
    #[arg(long, default_value = ".scn-shard")]
    store: PathBuf,
    #[arg(long, default_value = "127.0.0.1:7879")]
    listen: String,
    #[command(flatten)]
    blobs: BlobArgs,
    #[command(flatten)]
    limits: server::RequestLimits,
}

#[derive(Subcommand)]
enum ShardCommand {
    #[command(about = "Run a stateless API controller backed by Valkey and SQLite shards")]
    Controller {
        #[arg(long, default_value = "redis://127.0.0.1:6379/")]
        cache: String,
        #[arg(long, default_value = "default")]
        namespace: String,
        #[arg(long)]
        shard: Vec<String>,
        #[arg(long, default_value_t = 3600)]
        blob_ttl: u64,
        #[arg(long, default_value = "127.0.0.1:7878")]
        listen: String,
        #[command(flatten)]
        limits: server::RequestLimits,
    },
}

#[derive(Subcommand)]
enum RemoteCommand {
    GetUrl {
        #[arg(default_value = "origin", value_parser = ["origin"])]
        remote: String,
    },
    SetUrl {
        #[arg(value_parser = ["origin"])]
        remote: String,
        url: String,
    },
}

#[derive(Args)]
struct TagArgs {
    #[arg(value_name = "NAME_OR_REV")]
    names: Vec<String>,
    #[arg(short = 'l', long, conflicts_with_all = ["delete", "force", "annotate", "message", "author"])]
    list: bool,
    #[arg(short = 'd', long, conflicts_with_all = ["force", "annotate", "message", "author"])]
    delete: bool,
    #[arg(short = 'f', long)]
    force: bool,
    #[arg(short = 'a', long, requires = "message")]
    annotate: bool,
    #[arg(short = 'm', long)]
    message: Option<String>,
    #[arg(long, requires = "message")]
    author: Option<String>,
}

#[derive(Subcommand)]
enum Command {
    #[command(about = "Create, list, replace, or delete shared tags")]
    Tag(TagArgs),
    #[command(about = "Run a SQLite storage shard or shard controller")]
    Shard(ShardArgs),
    #[command(about = "Run Valkey with AOF, RDB, and volatile-lru eviction")]
    Cache {
        #[arg(long, default_value = ".scn-cache")]
        dir: PathBuf,
        #[arg(long, default_value = "127.0.0.1:6379")]
        listen: SocketAddr,
        #[arg(long, default_value = "1gb")]
        maxmemory: String,
        #[arg(long, default_value = "always", value_parser = ["always", "everysec"])]
        appendfsync: String,
        #[arg(long, default_value = "valkey-server")]
        binary: String,
    },
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
        #[command(flatten)]
        blobs: BlobArgs,
        #[command(flatten)]
        limits: server::RequestLimits,
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
    #[command(about = "Show or change the working copy's controller endpoint")]
    Remote {
        #[arg(short, long)]
        verbose: bool,
        #[command(subcommand)]
        command: Option<RemoteCommand>,
    },
    #[command(about = "Stage file contents and deletions for the next commit")]
    Add {
        #[arg(required_unless_present = "all")]
        paths: Vec<String>,
        #[arg(short = 'A', long)]
        all: bool,
    },
    #[command(about = "Create a server-side changeset from staged changes")]
    Commit {
        #[arg(short, long)]
        message: String,
        #[arg(long)]
        author: Option<String>,
        #[arg(short = 'a', long)]
        all: bool,
    },
    #[command(
        about = "Check out a revision, keeping the current sparse selection",
        visible_alias = "switch"
    )]
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
        #[arg(long, alias = "staged", conflicts_with = "from")]
        cached: bool,
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
    #[command(name = "push", visible_alias = "land")]
    Push {
        #[arg(default_value = "origin", value_parser = ["origin"])]
        remote: String,
        #[arg(default_value = "main", value_parser = ["main"])]
        branch: String,
    },
    #[command(about = "Update origin/main without changing working files")]
    Fetch {
        #[arg(default_value = "origin", value_parser = ["origin"])]
        remote: String,
    },
    #[command(about = "Fetch main and integrate it into the current changeset")]
    Pull {
        #[arg(default_value = "origin", value_parser = ["origin"])]
        remote: String,
        #[arg(long, conflicts_with = "ff_only")]
        rebase: bool,
        #[arg(long)]
        ff_only: bool,
    },
}

fn author(value: Option<String>) -> String {
    value
        .or_else(|| std::env::var("SCN_AUTHOR").ok())
        .or_else(|| std::env::var("USER").ok())
        .unwrap_or_else(|| "unknown".to_owned())
}

async fn tags(args: TagArgs, workspace: &Workspace, client: &Client) -> Result<()> {
    if args.list || args.names.is_empty() {
        ensure!(
            !args.delete && !args.force && !args.annotate && args.message.is_none(),
            "a tag name is required"
        );
        let mut patterns = globset::GlobSetBuilder::new();
        for pattern in &args.names {
            patterns.add(globset::Glob::new(pattern)?);
        }
        let patterns = patterns.build()?;
        for tag in client.tags().await? {
            if args.names.is_empty() || patterns.is_match(&tag.name) {
                println!("{}", tag.name);
            }
        }
    } else if args.delete {
        for name in args.names {
            client.delete_tag(&name).await?;
            println!("Deleted tag '{name}'");
        }
    } else {
        ensure!(
            args.names.len() <= 2,
            "usage: scn tag [-a -m MESSAGE] [-f] NAME [REV]"
        );
        let target = workspace.revision(args.names.get(1).map(String::as_str).unwrap_or("HEAD"))?;
        let annotation = args.message.map(|message| TagAnnotationRequest {
            tagger: author(args.author),
            message,
        });
        let tag: Tag = client
            .post(
                "/v1/tags",
                &TagRequest {
                    name: args.names[0].clone(),
                    target,
                    force: args.force,
                    annotation,
                },
            )
            .await?;
        println!("{} -> {}", tag.name, tag.target);
    }
    Ok(())
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
        None if before.map(|entry| (&entry.blob, &entry.manifest))
            != after.map(|entry| (&entry.blob, &entry.manifest)) =>
        {
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
        Command::Cache {
            dir,
            listen,
            maxmemory,
            appendfsync,
            binary,
        } => {
            return tokio::task::spawn_blocking(move || {
                scn::cache::serve(&binary, &dir, listen, &maxmemory, &appendfsync)
            })
            .await?;
        }
        Command::Shard(args) => match args.command {
            Some(ShardCommand::Controller {
                cache,
                namespace,
                shard,
                blob_ttl,
                listen,
                limits,
            }) => {
                let backend =
                    scn::cluster::Cluster::open(&cache, &namespace, &shard, blob_ttl).await?;
                return server::serve_router(
                    server::router_with_limits(Store::with_backend(Arc::new(backend)), limits),
                    &listen,
                )
                .await;
            }
            None => {
                let local = tokio::task::spawn_blocking(move || {
                    scn::storage::Local::open(&args.store, args.blobs.storage(&args.store)?)
                })
                .await??;
                return server::serve_router(
                    scn::shard::router_with_limits(local, args.limits),
                    &args.listen,
                )
                .await;
            }
        },
        Command::ImportGit {
            repository,
            url,
            rev,
        } => {
            let cache =
                scn::worker::Cleanup::new(scn::worker::io(|| Ok(tempfile::tempdir()?)).await?);
            let client = scn::client::Client::new(&url, cache.path()).await?;
            let commit = scn::import::import_git(&client, &repository, &rev).await?;
            println!("main -> {}", commit.id);
            return Ok(());
        }
        Command::Serve {
            store,
            listen,
            blobs,
            limits,
        } => {
            let store = tokio::task::spawn_blocking(move || {
                Store::with_blobs(&store, blobs.storage(&store)?)
            })
            .await??;
            return server::serve_router(server::router_with_limits(store, limits), &listen).await;
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
    let (mut workspace, lock) = scn::worker::io(|| {
        let workspace = Workspace::find(&std::env::current_dir()?)?;
        let lock = workspace.lock()?;
        Ok((workspace, lock))
    })
    .await?;
    let result = async {
        if let Command::Remote { verbose, command } = command {
            match command {
                Some(RemoteCommand::SetUrl { url, .. }) => {
                    scn::client::Client::new(&url, &workspace.root.join(".scn/cache")).await?;
                    workspace.state.server = url.trim_end_matches('/').to_owned();
                    workspace.state.remote_main = None;
                    workspace.on_disk(|w| w.save()).await?;
                }
                Some(RemoteCommand::GetUrl { .. }) => println!("{}", workspace.state.server),
                None if verbose => {
                    println!("origin\t{} (fetch)", workspace.state.server);
                    println!("origin\t{} (push)", workspace.state.server);
                }
                None => println!("origin"),
            }
            return Ok(());
        }
        let client = workspace.client().await?;
        let rebase = matches!(&command, Command::Rebase { .. });
        match command {
            Command::Tag(args) => tags(args, &workspace, &client).await?,
            Command::Status => {
                workspace
                    .on_disk(|w| {
                        let files = w.scan()?;
                        let index = w.staged_tree();
                        let staged = changes_between(&w.state.files, &index);
                        let unstaged = changes_between(&index, &files);
                        let paths: std::collections::BTreeSet<_> =
                            staged.keys().chain(unstaged.keys()).collect();
                        for path in &paths {
                            if !w.state.files.contains_key(*path) && !index.contains_key(*path) {
                                println!("?? {path}");
                                continue;
                            }
                            let status = |changes: &Changes, before: &Tree| match changes.get(*path)
                            {
                                None => ' ',
                                Some(None) => 'D',
                                Some(Some(_)) if !before.contains_key(*path) => 'A',
                                Some(_) => 'M',
                            };
                            println!(
                                "{}{} {path}",
                                status(&staged, &w.state.files),
                                status(&unstaged, &index)
                            );
                        }
                        if paths.is_empty() {
                            println!("Working copy clean");
                        }
                        Ok(())
                    })
                    .await?;
            }

            Command::Add { paths, all } => {
                let cwd = if all {
                    workspace.root.clone()
                } else {
                    std::env::current_dir()?
                };
                let paths = if all { vec![".".into()] } else { paths };
                workspace
                    .on_disk(move |w| w.stage(&paths, &cwd, false))
                    .await?;
            }
            Command::Commit {
                message,
                author: name,
                all,
            } => {
                if all {
                    workspace
                        .on_disk(|w| w.stage(&[".".into()], &w.root.clone(), true))
                        .await?;
                }
                let commit = workspace
                    .commit_staged(&client, message, author(name))
                    .await?;
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
            Command::Diff { from, to, cached } => {
                if let (Some(from), Some(to)) = (from, to) {
                    let from = revision_segment(&workspace.revision(&from)?)?;
                    let to = revision_segment(&workspace.revision(&to)?)?;
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
                    let (index, mut files) = workspace
                        .on_disk(move |w| {
                            let index = w.staged_tree();
                            let files = if cached { index.clone() } else { w.scan()? };
                            Ok((index, files))
                        })
                        .await?;
                    if !cached {
                        files.retain(|path, _| index.contains_key(path));
                    }
                    let base = if cached {
                        workspace.state.files.clone()
                    } else {
                        index.clone()
                    };
                    for (path, entry) in changes_between(&base, &files) {
                        let before = base.get(&path);
                        if before.is_some_and(|entry| entry.manifest.is_some())
                            || entry.as_ref().is_some_and(|entry| entry.manifest.is_some())
                        {
                            print_diff(&path, before, entry.as_ref(), None);
                            continue;
                        }
                        let old = if let Some(before) = before {
                            if !cached
                                && workspace
                                    .state
                                    .index
                                    .get(&path)
                                    .is_some_and(Option::is_some)
                            {
                                {
                                    let id = before.blob.clone();
                                    workspace.on_disk(move |w| w.staged_blob(&id)).await?
                                }
                            } else {
                                client.blob(&before.blob).await?
                            }
                        } else {
                            vec![]
                        };
                        let new = if let Some(entry) = &entry {
                            if cached {
                                {
                                    let id = entry.blob.clone();
                                    workspace.on_disk(move |w| w.staged_blob(&id)).await?
                                }
                            } else {
                                {
                                    let path = path.clone();
                                    workspace
                                        .on_disk(move |w| Ok(fs::read(w.path(&path)?)?))
                                        .await?
                                }
                            }
                        } else {
                            vec![]
                        };
                        let owned = path.clone();
                        let patch = scn::worker::cpu(move || Ok(patch(&owned, &old, &new))).await?;
                        print_diff(&path, before, entry.as_ref(), patch);
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
                let revision = revision_segment(&workspace.revision(&revision)?)?;
                let mut query = vec![("limit", limit.to_string())];
                if let Some(path) = path {
                    validate_path(&path)?;
                    query.push(("path", path));
                }
                let commits: Vec<Commit> =
                    client.get(&format!("/v1/log/{revision}"), &query).await?;
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
                    client.write_file(entry, io::stdout()).await?;
                } else {
                    println!("{}", serde_json::to_string_pretty(&commit)?);
                }
            }
            Command::Blame { path, rev } => {
                validate_path(&path)?;
                let revision = revision_segment(&workspace.revision(&rev)?)?;
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
                workspace.on_disk(|w| w.ensure_clean()).await?;
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
            Command::Fetch { .. } => {
                let target = workspace.fetch(&client).await?;
                println!("origin/main -> {}", target.as_deref().unwrap_or("empty"));
            }
            Command::Pull {
                rebase, ff_only, ..
            } => {
                workspace
                    .pull(&client, rebase, ff_only, author(None))
                    .await?;
                println!(
                    "HEAD -> {}",
                    workspace.state.revision.as_deref().unwrap_or("empty")
                );
            }
            Command::Push { .. } => {
                let head = workspace.revision("HEAD")?;
                client.land(head.clone()).await?;
                workspace.state.remote_main = Some(head.clone());
                workspace.on_disk(|w| w.save()).await?;
                println!("main -> {head}");
            }
            _ => bail!("unexpected command"),
        }
        Ok(())
    }
    .await;
    let released = lock.release().await;
    result.and(released)
}
