# scn

A small, centralized source control system in Rust, inspired by [Scion](https://www.anthropic.com/engineering/redesigning-source-control). One binary provides the server and CLI.

## Architecture

- **Client:** manages working files, sparse checkouts, and a 64 MiB compressed blob cache. History stays on the server.
- **Server:** exposes commits, trees, history, diff, blame, merge, and rebase over HTTP.
- **Storage:** SQLite holds changesets and `main`. Changesets contain file deltas; SHA-256 addressed, zlib compressed blobs live on disk.

`commit` creates an immutable changeset on the server. `land` separately advances `main` with compare-and-swap. There is no staging area or push.

This is a single-server MVP for trusted use, without authentication or distributed storage. Trees are rebuilt from history on demand. Files are limited to 32 MiB, and merge conflicts are detected per file.

## Quick start

```sh
cargo install --path .
scn serve --store /tmp/scn-store
```

In another terminal:

```sh
scn clone http://127.0.0.1:7878 /tmp/work
cd /tmp/work
printf 'hello\n' > hello.txt
scn commit -m "Add greeting"
scn land
```

Set `SCN_AUTHOR` or pass `commit --author` to choose your author identity.

## Import a Git repository

With the server running and its `main` still empty:

```sh
scn import-git /path/to/git-repo http://127.0.0.1:7878
scn clone http://127.0.0.1:7878 /tmp/work
```

The importer reads `HEAD` by default; use `--rev main` to select another branch, tag, or commit. It preserves all reachable commits, author identities and timestamps, messages, empty commits, and merge parents. It prints the Git-to-scn ID mapping and advances `main` only after the whole import succeeds. Branch and tag refs are not copied.

Git must be installed. Bare repositories work. The source repository and uncommitted changes are left untouched. Shallow histories, symlinks, submodules, and merges with more than two parents are rejected. Commit metadata and paths must be UTF-8.

## Daily use

```sh
scn status
scn diff
scn commit -m "Describe the change"
scn log
scn show HEAD path/to/file
scn blame path/to/file
scn land
```

If another client lands first, run `scn rebase main` then `scn land`, or use `scn merge main` to retain both histories. Conflicts leave the working copy unchanged. Checkout, merge, rebase, and land require a clean working copy.

Use `clone --sparse src` or `checkout main --sparse src` for selected paths. Repeat `--sparse` for more paths; `checkout main --full` restores the full tree. Commits preserve omitted files. Revisions accept `HEAD`, `main`, or a full scn ID.

Add gitignore-style patterns to `.scnignore`. `.gitignore` is not read. Tracked files remain tracked when ignored.

Run `scn --help` for commands. Validate changes with `cargo test`, `cargo clippy --all-targets -- -D warnings`, and `cargo fmt --check`.
