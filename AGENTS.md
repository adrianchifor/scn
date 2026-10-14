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
scn tag -l 'releases/*'
scn tag v1.0.0 HEAD
scn tag -a releases/v1.1.0 HEAD -m "Release 1.1.0"
scn show releases/v1.1.0 path/to/file
scn tag -f v1.0.0 HEAD
scn tag -d v1.0.0
```

Create, replace, or delete tags only when the task requests it: these operations publish a shared server ref immediately. No tag push or fetch is needed. Tags leave `main`, staged changes, and working files unchanged. Lightweight tags store a name and commit ID; annotated tags also store the tagger, message, and server timestamp. `-m` implies annotation; `-a` requires `-m`. Use `SCN_AUTHOR` or `--author` for the tagger. Replacement requires `-f`; deletion accepts multiple names.

Tag names follow Git ref rules. Slashes and Unicode are supported; spaces, control characters, `..`, `@{`, and reserved ref punctuation are rejected. Use `refs/tags/NAME` to disambiguate names matching `main`, `HEAD`, `origin/main`, or commit IDs. Tags can be used by clone, checkout, show, log, diff, blame, merge, and rebase. Signing and signature verification are unsupported.

Tag API routes:

| Request | Response |
| --- | --- |
| `GET /v1/tags` | Tag array sorted by name |
| `GET /v1/tags/NAME` | `{name, target, annotation?, git_id?}` |
| `POST /v1/tags` | Created/replaced tag |
| `DELETE /v1/tags/NAME` | HTTP 204, no body |

Create a lightweight tag with `{"name":"v1.0.0","target":"COMMIT_ID"}`. To annotate it, include `"annotation":{"tagger":"YOUR_IDENTITY","message":"Release 1.0.0"}`. The returned annotation also has a Unix nanosecond `timestamp`. `target` accepts `main`, a tag name, `refs/tags/NAME`, or a full commit ID; the stored target is always a concrete commit ID. Read targets once and use their IDs for reproducible writes. Set `"force":true` only for an explicitly requested replacement. Duplicate names return 409; missing tags return 404. After an uncertain creation response, read the tag and compare its target and annotation instead of blindly replacing it.

For Git import, `scn import-git REPOSITORY CONTROLLER_URL --rev REV` requires an empty `main` and imports all commit tags and their referenced histories. Annotated tag metadata and original Git tag object IDs are retained. Nested tags resolve to commits; tree/blob tags are rejected. `main` advances to the selected revision only after all imports succeed. Failed imports may leave draft commits and tags; retrying identical imported tags is safe. The importer uses `POST /v1/import/tags` with the complete tag object and a concrete scn target ID; this endpoint rejects conflicting existing tags and preserves imported timestamps. Normal tag creation should use `/v1/tags`.

## API data model

Use the controller endpoint supplied to your session. All routes below are relative to it. JSON requests use `Content-Type: application/json`. No working copy, staging area, Git repository, or direct shard access is needed.

- Commit and blob IDs are 64-character lowercase SHA-256 hex strings. Blob hashes cover raw file bytes.
- A tree is a map from repository-relative paths to `{"blob":"BLOB_ID","executable":false}`.
- `changes` has the same path keys, with file entries for additions/modifications and `null` for deletions. Send only changed paths; omitted paths are preserved from the first parent.
- Normal commits have zero, one, or two parent IDs, a nonempty author, a nonempty message, and changes. A merge's changes are relative to its first parent. Read timestamps are Unix nanoseconds; the server sets them for normal writes.
- Paths must be UTF-8, relative, and slash-separated. Empty components, `.`, `..`, `.git`, `.scn`, backslashes, NUL, CR, and LF are rejected. Files are regular files up to 32 MiB; symlinks and submodules are unsupported. Empty directories are not tracked.

## Read APIs

| Request | Response |
| --- | --- |
| `GET /health` | Text `ok` |
| `GET /v1/refs/main` | `{"target":"COMMIT_ID"}` or `{"target":null}` |
| `GET /v1/commits/REV` | `{id, parents, author, message, timestamp, changes}`; imports also have `git_id` |
| `GET /v1/tree/REV?prefix=src` | Tree, optionally limited to one file/directory prefix |
| `GET /v1/blobs/BLOB_ID` | Zstandard bytes, `Content-Type: application/zstd` |
| `GET /v1/log/REV?limit=20&path=src/file` | First-parent commit array; exact file history when `path` is given; maximum 1,000 results |
| `GET /v1/diff/FROM/TO` | Array of `{path, before, after, patch}`; null patch for binary files |
| `GET /v1/blame/REV?path=src/file` | UTF-8 line array `{commit, author, line}`, following first-parent history |
| `GET /v1/merge-base/LEFT/RIGHT` | `{"target":"COMMIT_ID"}` |

API revisions accept `main`, tag names, `refs/tags/NAME`, or a full commit ID. `HEAD` and `origin/main` are CLI shortcuts; qualify tags with those names. Encode an entire revision as one URL path segment, including its slashes: Python's `quote(revision, safe="")` turns `releases/v1` into `releases%2Fv1`. Encode tag names in tag routes the same way, and URL-encode query parameters. Resolve a ref once, then use its immutable commit ID for all reads used to prepare a change. Decompress downloaded blobs with Zstandard and verify SHA-256 against the blob ID.

## Make changes through the API

1. Read `GET /v1/refs/main` and save its exact `target`, including null for an empty repository.
2. Inspect the tree and relevant files at that commit ID.
3. Upload each changed file's raw bytes with `POST /v1/blobs?path=ENCODED_PATH`. The response is `{"target":"BLOB_ID"}`. Do not compress or base64-encode uploads. The optional path lets the controller place new content near that file's owner.
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

## Concurrent changes and errors

If publishing returns 409, inspect the JSON `error`, read the latest `main`, and preserve your draft. To integrate, send `POST /v1/rebase` with `{"head":"DRAFT_ID","onto":"LATEST_MAIN_ID","author":"YOUR_IDENTITY"}`. Alternatively, `POST /v1/merge` accepts the same body and retains both parent histories. Both return a commit. Review/test the result, then publish its ID with `expected` set to the latest main ID used for integration. Bound retries and stop on unresolved file conflicts.

Merges combine independent file edits and identical edits. Competing edits or a modification versus deletion of the same file conflict. Rebase requires linear first-parent history; histories with multiple merge bases are unsupported. Failed integration never advances `main`.

Application errors are JSON `{"error":"message"}`. HTTP 400 includes validation and file conflicts; 404 indicates a missing tag; 409 is a rejected main update or conflicting tag creation; 413 means a request exceeds the size limit; 422 indicates invalid JSON/schema. Framework errors may have a text body. Blob uploads are safe to retry because contents are addressed by hash. Normal commit creation is not idempotent: retain its response and avoid blind retries after an uncertain timeout. There is no force-push API.
