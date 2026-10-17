# Using scn

This guide is for agents using an already configured scn server. Use the supplied working copy and controller API endpoint. Deployment and connection configuration are covered in the README.

## Choose a workflow

- Use the CLI when you need working files to run builds or tests.
- Use the HTTP API for repository queries or precise file changes without a checkout.
- Keep the commit ID returned by every write. Commits are immediately available on the server, but `main` changes only when you push or update its ref.
- Publish to `main` when the task calls for it. Otherwise return the draft commit ID.

## CLI workflow

In the supplied working copy:

```sh
scn status
scn fetch
scn log origin/main -n 10
scn pull --ff-only
```

Edit and test the requested files, then stage only your changes:

```sh
scn diff
scn add path/to/file path/to/deleted-file
scn diff --cached
scn commit -m "Describe the change"
scn push
```

`add` stages a content snapshot, including deletions. Edits after staging stay unstaged. `commit -a` stages changes to all tracked files, so use it only when those changes belong to your task. Use the supplied author identity through `SCN_AUTHOR` or `commit --author`.

`fetch` refreshes `origin/main` without changing files. `pull` fetches and merges; `pull --rebase` replays your commits. If a push is rejected because someone else advanced `main`, use `pull --rebase`, run the relevant tests again, and retry the push. Conflicts require a deliberate resolution; no conflict markers are written. Save your draft ID before checking out another revision to resolve a conflict.

Pull, merge, rebase, and checkout require a clean working copy, including untracked files. Push can publish committed work while local edits remain. Preserve unrelated work. Sparse commits leave omitted paths unchanged. `.gitignore` and `.scnignore` are honored.

Useful reads: `scn log --path path/to/file`, `scn show HEAD path/to/file`, `scn blame path/to/file`, and `scn diff FROM TO`. CLI revisions accept `HEAD`, `main`, `origin/main`, tag names, or a full scn commit ID.

## Tags

```sh
scn tag
scn tag v1.0.0 HEAD
scn tag -a v1.1.0 HEAD -m "Release 1.1.0"
scn tag -d v1.0.0
```

Tags default to `HEAD`; use `-f` to replace. Change tags only when requested: updates are shared immediately without pushing. Names follow Git ref rules; signing is unsupported.

| Request | Response |
| --- | --- |
| `GET /v1/tags` | Tag array sorted by name |
| `GET /v1/tags/NAME` | `{name, target, annotation?, git_id?}` |
| `POST /v1/tags` | Created/replaced tag |
| `DELETE /v1/tags/NAME` | HTTP 204, no body |

- POST body: `{"name":"v1.0.0","target":"COMMIT_ID"}`. Optionally add `"annotation":{"tagger":"YOUR_IDENTITY","message":"Release 1.0.0"}` or `"force":true` for replacement.
- After an uncertain write, read the tag before retrying. Duplicate names return 409; missing tags return 404.
- Git import retains tags and their referenced histories. See the [README](README.md#import-git-history).

## API data model

Use the controller endpoint supplied to your session. All routes below are relative to it. JSON requests use `Content-Type: application/json`. No working copy, staging area, Git repository, or direct shard access is needed.

Responses include `x-request-id` for debugging. You can supply one using 1–64 ASCII letters, digits, `.`, `_`, or `-`; invalid values are replaced. Include it when reporting a server error. CLI progress stays on stderr; use `--no-progress` to hide it.

- Commit and blob IDs are 64-character lowercase SHA-256 hex strings. Physical blob hashes cover raw bytes before compression.
- A tree maps repository-relative paths to file entries. Files up to 32 MiB use `{"blob":"BLOB_ID","executable":false}`. Larger files also have `"manifest":{"size":BYTE_LENGTH,"chunks":["CHUNK_ID",...]}`; their `blob` hashes the manifest, not the whole file's raw bytes.
- `changes` has the same path keys, with file entries for additions/modifications and `null` for deletions. Send only changed paths; omitted paths are preserved from the first parent.
- Normal commits have zero, one, or two parent IDs, a nonempty author, a nonempty message, and changes. A merge's changes are relative to its first parent. Read timestamps are Unix nanoseconds; the server sets them for normal writes.
- Paths must be UTF-8, relative, and slash-separated. Empty components, `.`, `..`, `.git`, `.scn`, backslashes, NUL, CR, and LF are rejected. Files are regular files; symlinks and submodules are unsupported. Empty directories are not tracked. CLI commands handle large-file chunking automatically; line diff and blame apply only to files up to 32 MiB.

## Read APIs

| Request | Response |
| --- | --- |
| `GET /health` | Text `ok` |
| `GET /v1/refs/main` | `{"target":"COMMIT_ID"}` or `{"target":null}` |
| `GET /v1/commits/REV` | `{id, parents, author, message, timestamp, changes}`; imports also have `git_id` |
| `GET /v1/tree/REV?prefix=src` | Tree, optionally limited to one file/directory prefix |
| `GET /v1/blobs/BLOB_ID` | Zstandard bytes, `Content-Type: application/zstd` |
| `HEAD /v1/blobs/BLOB_ID` | HTTP 204 if stored, 404 if absent |
| `GET /v1/log/REV?limit=20&path=src/file` | First-parent commit array; exact file history when `path` is given; maximum 1,000 results |
| `GET /v1/diff/FROM/TO` | Array of `{path, before, after, patch}`; null patch for binary or chunked files |
| `GET /v1/blame/REV?path=src/file` | UTF-8 line array `{commit, author, line}`, following first-parent history |
| `GET /v1/merge-base/LEFT/RIGHT` | `{"target":"COMMIT_ID"}` |

- API revisions accept `main`, tag names, `refs/tags/NAME`, or full commit IDs. `HEAD` and `origin/main` are CLI shortcuts. Use `refs/tags/NAME` to disambiguate tags.
- Encode revisions and tag names as single URL path segments, including slashes: `quote(value, safe="")`. URL-encode query parameters.
- Resolve refs once and use immutable IDs when preparing changes. Decompress downloaded blobs with Zstandard and verify their SHA-256 hashes.

## Make changes through the API

1. Read `GET /v1/refs/main` and save its exact `target`, including null for an empty repository.
2. Inspect the tree and relevant files at that commit ID.
3. For files up to 32 MiB, upload raw bytes with `POST /v1/blobs?path=ENCODED_PATH`. The response is `{"target":"BLOB_ID"}`. Do not compress or base64-encode uploads. The optional path lets the controller place new content near that file's owner. For larger files, use the chunk protocol below.
4. Send `POST /v1/commits` with `{parents, author, message, changes}`. Use `[base_id]` for parents, or `[]` for an empty repository. Upload blobs before referencing them. Delete only paths that exist in the parent tree.
5. Save the returned `id`. Review its diff and run appropriate validation. Creating a commit does not publish it to `main`.
6. To publish, send `POST /v1/refs/main` with `{"expected":base_id,"target":new_id}`. The expected value must be the exact value read in step 1. Success is HTTP 204 with no body.

This Python example creates a draft file change. `SCN_URL` and `SCN_AUTHOR` are inputs supplied by the caller:

```python
import json
import os
from urllib.parse import urlencode
from urllib.request import Request, urlopen

endpoint = os.environ["SCN_URL"].rstrip("/")
author = os.environ["SCN_AUTHOR"]

def request(method, path, body=None, raw=False):
    headers = {"Content-Type": "application/octet-stream" if raw else "application/json"}
    data = body if raw else json.dumps(body).encode() if body is not None else None
    with urlopen(Request(endpoint + path, data=data, headers=headers, method=method), timeout=120) as response:
        payload = response.read()
        return json.loads(payload) if payload else None

base = request("GET", "/v1/refs/main")["target"]
path = "docs/agent-example.txt"
blob = request("POST", "/v1/blobs?" + urlencode({"path": path}), b"Created by an agent.\n", raw=True)["target"]
commit = request("POST", "/v1/commits", {
    "parents": [base] if base else [],
    "author": author,
    "message": "Add agent example",
    "changes": {path: {"blob": blob, "executable": False}},
})
print(json.dumps({"base": base, "commit": commit["id"]}))
```

When publication is requested, send `{"expected":base,"target":commit["id"]}` to `POST /v1/refs/main` after validation. Keep the saved base instead of re-reading `main` and substituting a different expected value.

## Large files through the API

The CLI handles chunking automatically. For API uploads over 32 MiB:

1. Stream 8 MiB chunks, with a shorter final chunk if needed. Upload each distinct chunk's raw bytes to `POST /v1/blobs` without `path`; verify returned SHA-256 IDs. Optionally use HEAD to skip stored chunks.
2. Send `POST /v1/files` with `{"size":BYTE_LENGTH,"chunks":["CHUNK_ID",...],"executable":false}`. Preserve chunk order and repeated IDs.
3. Put the returned file entry in `changes` and commit normally.

To download, fetch `manifest.chunks` in order, decompress and verify hashes and lengths, then assemble a temporary file before replacing the destination. `entry.blob` identifies the manifest and cannot be downloaded as a physical blob.

Each request remains limited to 32 MiB (HTTP 413). Chunked files use binary diff and have no line blame.

HTTP 503 means retry with backoff and honor `Retry-After`; CLI and controller requests retry automatically.

## Concurrent changes and errors

If publishing returns 409, inspect the JSON `error`, read the latest `main`, and preserve your draft. To integrate, send `POST /v1/rebase` with `{"head":"DRAFT_ID","onto":"LATEST_MAIN_ID","author":"YOUR_IDENTITY"}`. Alternatively, `POST /v1/merge` accepts the same body and retains both parent histories. Both return a commit. Review/test the result, then publish its ID with `expected` set to the latest main ID used for integration. Bound retries and stop on unresolved file conflicts.

Merges combine independent file edits and identical edits. Competing edits or a modification versus deletion of the same file conflict. Rebase requires linear first-parent history; histories with multiple merge bases are unsupported. Failed integration never advances `main`.

Application errors are JSON `{"error":"message"}`. HTTP 400 includes validation and file conflicts; 404 indicates a missing tag; 409 is a rejected main update or conflicting tag creation; 413 means a request exceeds the size limit; 422 indicates invalid JSON/schema. Framework errors may have a text body. Blob uploads are safe to retry because contents are addressed by hash. Normal commit creation is not idempotent: retain its response and avoid blind retries after an uncertain timeout. There is no force-push API.
