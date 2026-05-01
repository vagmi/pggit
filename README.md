# pggit

A Rust library that stores Git repositories in PostgreSQL. Built for applications that need to manage hundreds of small Git repos entirely through code.

pggit implements custom [libgit2](https://libgit2.org/) ODB and RefDB backends via FFI, so every git object (blob, tree, commit, tag) and every ref lives in PostgreSQL tables. On top of this, it provides an async porcelain API for the common operations: commit files, read files, view history, compute diffs, and checkout to a local directory.


I sort of wanted something like FossilSCM but on top of the git tooling. Et voila. If you have
tons of tiny git repos, pggit might be better than git.


## Quick start

Add to your `Cargo.toml`:

```toml
[dependencies]
pggit = { git="https://github.com/vagmi/pggit.git" }
tokio = { version = "1", features = ["full"] }
```

```rust
use pggit::PgGitStore;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let store = PgGitStore::connect("postgresql://postgres@localhost/mydb").await?;
    store.migrate().await?;

    let repo = store.get_or_create_repository("my-repo").await?;

    // Commit files
    repo.commit(
        "main",
        &[
            ("src/main.rs", b"fn main() { println!(\"hello\"); }\n"),
            ("README.md", b"# My Project\n"),
        ],
        "Initial commit",
        "Alice",
        "alice@example.com",
    ).await?;

    // Read a file
    let content = repo.read_file("main", "src/main.rs").await?;
    println!("{}", std::str::from_utf8(&content.unwrap())?);

    // List files
    let files = repo.list_files("main").await?;
    println!("{:?}", files); // ["README.md", "src/main.rs"]

    // Commit more changes -- unchanged files are preserved automatically
    repo.commit(
        "main",
        &[("src/main.rs", b"fn main() { println!(\"world\"); }\n")],
        "Update greeting",
        "Bob",
        "bob@example.com",
    ).await?;

    // View log
    for entry in repo.log("main", 10).await? {
        println!("{} - {}", &entry.oid.to_string()[..8], entry.message.trim());
    }

    // Diff between commits
    let log = repo.log("main", 2).await?;
    let diff = repo.diff(log[1].oid, log[0].oid).await?;
    println!("{}", diff.stat_line());
    println!("{}", diff.to_patch());

    // Checkout to a local directory (creates a valid git repo)
    repo.checkout("main", "/tmp/my-repo-checkout").await?;
    // You can now run: cd /tmp/my-repo-checkout && git log

    Ok(())
}
```

## API overview

### Store

```rust
// Connect
let store = PgGitStore::connect("postgresql://...").await?;
let store = PgGitStore::from_pool(existing_pool); // or from an existing sqlx pool

// Schema
store.migrate().await?; // creates tables if they don't exist

// Repositories
let repo = store.get_or_create_repository("name").await?;
let repo = store.repository("name").await?; // existing repo only
```

### Repository (porcelain)

```rust
// Write files and commit (auto-detects initial vs incremental)
let oid = repo.commit("main", &[("path", content)], "message", "name", "email").await?;

// Read
let bytes = repo.read_file("main", "path/to/file").await?;    // Option<Vec<u8>>
let files = repo.list_files("main").await?;                    // Vec<String>
let log   = repo.log("main", 50).await?;                      // Vec<LogEntry>

// Diff
let diff = repo.diff(oid_old, oid_new).await?;                // DiffSummary
let diff = repo.diff_refs("main", "feature").await?;          // between branch tips
let patch = diff.to_patch();                                   // unified diff string
let stat  = diff.stat_line();                                  // "3 file(s) changed, ..."

// Checkout to local filesystem (creates a real git repo)
repo.checkout("main", "/tmp/workdir").await?;
```

### Low-level git2 access

For operations not covered by the porcelain API, you can drop down to the full [git2](https://docs.rs/git2) API:

```rust
let store = PgGitStore::connect("postgresql://...").await?;
let repo_id = store.create_repository("low-level").await?;

// Must run on a blocking thread (callbacks use block_on internally)
tokio::task::spawn_blocking(move || {
    let repo = store.open_repository(repo_id)?;

    // Full git2 API works: blobs, trees, commits, refs, revwalks, etc.
    let oid = repo.blob(b"hello")?;
    let blob = repo.find_blob(oid)?;
    // ...
}).await?;
```

## Database schema

pggit creates four tables (via `store.migrate()`):

| Table | Purpose | Key columns |
|-------|---------|-------------|
| `repositories` | One row per repo | `id`, `name`, `created_at` |
| `objects` | Git objects (blobs, trees, commits, tags) | `repo_id`, `oid`, `type`, `size`, `content` |
| `refs` | Branches, tags, HEAD | `repo_id`, `name`, `oid` or `symbolic` |
| `reflog` | History of ref changes | `repo_id`, `ref_name`, `old_oid`, `new_oid`, `committer`, `message` |

All tables are keyed by `repo_id`, so repos are fully isolated. You can query them directly with SQL:

```sql
-- List all commits in a repo
SELECT encode(oid, 'hex'), size
FROM objects
WHERE repo_id = 1 AND type = 1;

-- Show all branches
SELECT name, encode(oid, 'hex')
FROM refs
WHERE repo_id = 1;

-- Recent reflog entries
SELECT ref_name, committer, message, created_at
FROM reflog
WHERE repo_id = 1
ORDER BY id DESC
LIMIT 20;
```

## Architecture

pggit implements custom libgit2 backends via `libgit2-sys` FFI:

```
  ┌─────────────────────────────────────────┐
  │           Your application              │
  │  (async Rust with tokio)                │
  ├─────────────────────────────────────────┤
  │         Porcelain API                   │
  │  commit, read_file, diff, checkout      │
  │  (async, runs git2 on spawn_blocking)   │
  ├─────────────────────────────────────────┤
  │              git2 (libgit2)             │
  │  blob, tree, commit, revwalk, diff ...  │
  ├──────────────────┬──────────────────────┤
  │  ODB backend     │  RefDB backend       │
  │  (objects table)  │  (refs + reflog)     │
  │  extern "C" FFI  │  extern "C" FFI      │
  ├──────────────────┴──────────────────────┤
  │            sqlx + PostgreSQL            │
  └─────────────────────────────────────────┘
```

The custom backends translate libgit2's storage calls into SQL queries. Since libgit2 handles all the git format details (SHA-1 hashing, tree serialization, commit formatting, diff computation), pggit gets full git compatibility without reimplementing any of it.

**Async/sync bridge**: libgit2 callbacks are synchronous C function pointers. Inside each callback, `tokio::runtime::Handle::block_on()` executes the sqlx query. The porcelain API wraps all git2 calls in `tokio::task::spawn_blocking` so the async runtime is never blocked.

## Tradeoffs

### When pggit makes sense

- **Many small repos** (configs, documents, templates). The per-repo overhead is one row in `repositories` -- no filesystem directories, no packfiles, no loose objects.
- **App-layer access patterns**. If your application creates commits programmatically and reads files by path, the porcelain API is simpler than shelling out to `git`.
- **Unified infrastructure**. Your repos are backed up, replicated, and monitored with the same Postgres tooling you already have. No separate git hosting to maintain.
- **SQL queryability**. You can `JOIN` git objects with your application tables, run analytics across repos, or build custom views.

### When filesystem git is better

- **Large repos**. Git's packfile format uses delta compression -- a 1 GB repo might only take 100 MB on disk. pggit stores every object at full size in the `objects` table. A repo with 1000 versions of a 10 KB file stores 1000 x 10 KB = 10 MB of blobs. With git packfiles, that might be 500 KB.
- **Clone/push over the network**. pggit doesn't implement the git pack protocol. You can't `git push` to it or `git clone` from it (yet). Use `checkout()` to materialize to a filesystem repo if you need CLI interop.
- **Heavy concurrent writes to the same branch**. Ref updates use `SELECT FOR UPDATE` with transaction-scoped advisory locks. This serializes concurrent writes to the same ref. Different repos or different branches are fully parallel.

### Storage overhead

| Scenario | Filesystem git | pggit |
|----------|---------------|-------|
| 1 file, 1 commit | ~1 KB (loose) | 3 rows in `objects` (blob + tree + commit) |
| 100 commits, 10 files | ~50 KB (packed) | ~3000 rows, mostly deduplicated blobs |
| Binary files (images, PDFs) | Delta-compressed in packfiles | Full copy per version |

**Rule of thumb**: pggit uses roughly 2-10x the storage of a packed git repo for workloads. Postgres TOAST compression helps: `bytea` columns over ~2 KB are automatically compressed with LZ4 (PG 14+) or pglz, which recovers some of the delta compression advantage for text content.

## Testing

Integration tests require a PostgreSQL instance:

```bash
export DATABASE_URL="postgresql://postgres@localhost:5432/pggit_test"
cargo test --test integration
```

The tests create isolated repos (unique names per test run) and validate compatibility using the `git` CLI binary -- checking that `git log`, `git status`, `git cat-file`, and `git diff` all produce correct results on checked-out repos.

## Acknowledgements


* [fossilscm](https://fossil-scm.org/home/doc/trunk/www/index.wiki) - A source control built on SQLite3
* [gitgres](https://github.com/nesbitt/gitgres) - A very similar project in C. The database schema 
and backend design are derived from that project.

## License

MIT
