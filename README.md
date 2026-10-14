# scn

Scalable source control system in Rust, inspired by [Scion](https://www.anthropic.com/engineering/redesigning-source-control). Git-style CLI commands and an HTTP API for agents, with server-side commits, sparse checkouts, and chunked storage for gigabyte-scale binaries and datasets.

Its stateless controllers, SQLite shards, and S3 storage can potentially scale to multi-terabyte monorepos and thousands of concurrent users and agents.

## Architecture

![scn architecture: CLI users and AI agents connect to stateless controllers, backed by a persistent Valkey index, independent SQLite shards, and S3 blob storage](docs/architecture.svg)

- **Controllers** run commits, history queries, diff, blame, merge, and rebase. Multiple controllers share one repository without local durable state. Each keeps a bounded, disposable cache of reconstructed trees.
- **Valkey** holds permanent shard assignments, `main`, and tags, plus compressed blobs with a configurable TTL. AOF and RDB persist the index; `volatile-lru` evicts only expiring cache entries.
- **Shards** store immutable file deltas, commit headers, and verified blob lengths in independent SQLite WAL databases. New paths go to the shard with the fewest assignments. Existing paths never move.
- **Blob storage** is local disk per shard or a shared S3 bucket. Files over 32 MiB use 8 MiB chunks and a manifest. Chunks are addressed by SHA-256, deduplicated, and Zstandard compressed. New chunks can be distributed across shards.

`commit` creates an immutable changeset on the server. `push` separately advances `main` with compare-and-swap. Agents can do both entirely through the API, without a checkout. See [AGENTS.md](AGENTS.md) for the client and API guide.

## Install

Use the release binary for your platform. No Rust toolchain is required.

| Platform | Download |
| --- | --- |
| Linux amd64 | [scn-linux-amd64](https://raw.githubusercontent.com/adrianchifor/scn/main/releases/scn-linux-amd64) |
| Linux arm64 | [scn-linux-arm64](https://raw.githubusercontent.com/adrianchifor/scn/main/releases/scn-linux-arm64) |
| macOS arm64 | [scn-macos-arm64](https://raw.githubusercontent.com/adrianchifor/scn/main/releases/scn-macos-arm64) |

Install your chosen binary, for example on Apple Silicon:

```sh
mkdir -p ~/.local/bin
curl -fL https://raw.githubusercontent.com/adrianchifor/scn/main/releases/scn-macos-arm64 -o ~/.local/bin/scn
chmod +x ~/.local/bin/scn
export PATH="$HOME/.local/bin:$PATH"
scn --version
```

Linux binaries are statically linked. Checksums are in [SHA256SUMS](https://raw.githubusercontent.com/adrianchifor/scn/main/releases/SHA256SUMS).

## Local cluster with Docker Compose

Requires Docker with Compose. From this repository:

```sh
docker compose -f deploy/compose.yaml up --build -d --wait
docker compose -f deploy/compose.yaml ps
curl --fail http://127.0.0.1:7878/health
```

[deploy/compose.yaml](deploy/compose.yaml) starts two controllers behind an Nginx gateway, two SQLite shards, persistent Valkey, and [RustFS](https://docs.rustfs.com/en/installation/container/docker), an S3-compatible store. An initialization job creates the `scn` bucket before shards start.

| Endpoint | Purpose |
| --- | --- |
| `http://127.0.0.1:7878` | CLI and controller API, balanced across both controllers |
| `http://127.0.0.1:9000` | Local S3 API |
| `http://127.0.0.1:9001` | S3 web console |

Local S3 credentials default to `SCNLOCAL` / `scn-local-secret-change-me`. Override them with `SCN_S3_ACCESS_KEY` and `SCN_S3_SECRET_KEY` before starting the stack. `SCN_CACHE_MEMORY` sets Valkey's memory limit, default `256mb`. Only the gateway and S3 ports are published, bound to loopback.

`SCN_MAX_REQUESTS` and `SCN_TRANSFER_MEMORY` tune each controller and shard's request capacity and transfer budget in MiB, defaulting to `128` and `256`.

Each shard has its own volume. Valkey and S3 also have persistent volumes. Inspect or stop the stack with:

```sh
docker compose -f deploy/compose.yaml logs -f controller-a shard-a
docker compose -f deploy/compose.yaml down
```

`down` keeps data. `docker compose -f deploy/compose.yaml down -v` deletes the repository, index, and blobs.

## Point the CLI at a controller

After installing scn, clone through the gateway or your existing controller:

```sh
scn clone http://127.0.0.1:7878 ./work
cd work
scn remote -v
scn remote get-url origin
```

The clone saves the endpoint in `.scn/state.json`. Later commands use it automatically. To select another controller for the same repository:

```sh
scn remote set-url origin https://scn.example.com
scn fetch
```

Changing the endpoint preserves staged changes and working files, and clears the cached `origin/main` value. There is one remote, `origin`, and one shared branch, `main`.

## Git-style workflow

```sh
printf 'hello\n' > hello.txt
scn status
scn add hello.txt
scn diff --cached
scn commit -m "Add greeting"
scn push origin main
scn log -n 5
scn show HEAD hello.txt
```

`add` snapshots contents. Later edits stay unstaged. `commit -am "Update tracked files"` stages modifications and deletions of tracked files. `diff` shows unstaged tracked changes; `diff --cached` shows staged changes. `.gitignore` and `.scnignore` both work. Set `SCN_AUTHOR` or pass `commit --author` for your identity.

Synchronize with other clients:

```sh
scn fetch
scn log origin/main -n 5
scn pull --ff-only
```

`fetch` updates `origin/main` without changing files. `pull` fetches and merges. If histories diverge, use `scn pull --rebase` to replay your commits, then `scn push`. Push works with uncommitted edits; pull and checkout require a clean working copy. File conflicts leave working files unchanged.

For sparse working copies:

```sh
scn clone http://127.0.0.1:7878 ./agent-work --sparse src --sparse Cargo.toml
cd agent-work
scn checkout main --sparse docs
scn checkout main --full
```

Commits preserve omitted files. Revisions accept `HEAD`, `main`, `origin/main`, tag names, or a full scn ID. `checkout` has a `switch` alias; `push` has a `land` alias. There is no Git wire protocol or offline commit support.

## Large files

- Use normal `add`, `commit`, and checkout commands for binaries and datasets. Files over 32 MiB automatically use deduplicated 8 MiB Zstandard chunks with bounded memory.
- API requests are limited to 32 MiB each. Upload chunks separately, then call `POST /v1/files`; see [AGENTS.md](AGENTS.md).
- Chunked files use binary diff and have no line blame. Git import preserves LFS pointers without fetching external objects.

## Tags

```sh
scn tag v1.0.0
scn tag -a v1.1.0 HEAD -m "Release 1.1.0"
scn tag
scn tag -f v1.0.0 HEAD
scn tag -d v1.0.0
```

Tags default to `HEAD`. Use `-a -m` for annotations, `-f` to replace, and `-d` to delete. Changes are shared immediately without pushing. Use tag names as revisions, or `refs/tags/NAME` to disambiguate. API routes are in [AGENTS.md](AGENTS.md).

## Run a cluster without Docker

Install scn as above, Git, and `valkey-server`. Run each server command in its own terminal, in this order:

```sh
scn cache --dir /tmp/scn-cache --listen 127.0.0.1:6379 --maxmemory 1gb
scn shard --store /tmp/scn-shard-a --listen 127.0.0.1:7879
scn shard --store /tmp/scn-shard-b --listen 127.0.0.1:7880
scn shard controller --listen 127.0.0.1:7878 --cache redis://127.0.0.1:6379/ --namespace demo --shard http://127.0.0.1:7879 --shard http://127.0.0.1:7880
```

These commands use local blob storage. A second controller can discover registered shards from Valkey:

```sh
scn shard controller --listen 127.0.0.1:7881 --cache redis://127.0.0.1:6379/ --namespace demo --blob-ttl 600
```

Clients can use either controller, or a load balancer in front of them. Register another running shard by passing its URL with `--shard` when starting any controller. All controllers then allocate new paths to it without rebalancing. Shard URLs must remain stable; shard removal is not supported.

For a single process without Valkey, use `scn serve --store /tmp/scn-store`.

Small requests and transfers have separate pools of 128 slots. Transfers also share a 256 MiB payload budget: uploads reserve their announced size, while downloads and unknown-length bodies reserve 33 MiB. Requests wait up to one second in bounded queues before HTTP 503.

Tune each server with `--max-requests 256 --transfer-memory 1024`. The budget covers payloads; allow memory for compression, trees, and other buffers. Adding controllers and shards adds independent capacity.

## Use an existing S3 bucket

The bucket must exist. Supply AWS credentials through environment variables, an AWS profile, or an instance role. Start each shard with its own SQLite directory and a shared bucket/prefix:

```sh
scn shard --store /data/shard-a --listen 0.0.0.0:7879 --s3-bucket my-bucket --s3-region eu-west-1 --s3-prefix my-repo/blobs
```

For S3-compatible services, also pass `--s3-endpoint http://s3-host:9000`. The same storage flags work with `serve`. Changing backends does not copy blobs.

Use a separate Valkey namespace, shard directories, and bucket prefix per repository. Back up Valkey with the shard databases and blob store.

`scn cache` defaults to `appendfsync always`; `--appendfsync everysec` trades durability for throughput. Authentication and automatic failover are not built in.

## Import Git history

With an empty `main`:

```sh
scn import-git /path/to/git-repo http://127.0.0.1:7878 --rev main
```

- Imports the selected history plus tagged histories, preserving lightweight and annotated tags, metadata, and original Git IDs. The selected revision becomes `main` after success; other branches are not copied.
- Bare repositories work. Uncommitted files are untouched. Retries accept identical tags and reject conflicting ones; failed imports may leave drafts and tags.
- Paths and metadata must be UTF-8. Symlinks, submodules, shallow histories, octopus merges, and tags targeting trees or blobs are unsupported. Tag signatures are not verified.

## Development

```sh
cargo build --release --locked
cargo test
cargo clippy --all-targets -- -D warnings
cargo fmt --check
SCN_VALKEY_SERVER=valkey-server cargo test --release --test cluster -- --ignored
docker compose -f deploy/compose.yaml config --quiet
SCN_PERF_COMMITS=500 cargo test --release --test perf -- --ignored --nocapture
cargo test --release --test large_files -- --ignored
```

Tests cover cluster persistence, eviction, concurrent writes, S3 requests, and chunked files. Cluster tests include four concurrent 1 GiB transfers; the separate large-file test streams 5 GiB of repeated data.

The local performance test uses `SCN_PERF_COMMITS` for the total commit count (default 500). It measures sequential commits, warm tree reads, direct-parent checks, and 1 MiB blob reads. Cluster throughput also depends on network latency and Valkey persistence settings.
