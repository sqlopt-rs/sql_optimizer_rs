# sql_optimizer_rs

[![crates.io](https://img.shields.io/crates/v/sqlopt.svg)](https://crates.io/crates/sqlopt)
[![docs.rs](https://img.shields.io/docsrs/sqlopt)](https://docs.rs/sqlopt)

Blazingly fast SQL query analyzer. Detects N+1 queries and suggests indexes from your logs in seconds. Written in Rust.

## Installation

```bash
cargo install sqlopt
```

## CLI usage

```bash
# Suggest an index
sqlopt analyze "SELECT * FROM users WHERE age > 18"

# Detect repeated query templates (common N+1 symptom)
# (flags are counts-per-window-of-N-queries, not timestamps)
sqlopt detect-n1 examples/queries.log --threshold 5 --window 50

# Heuristic rewrite (JOIN -> IN subquery) for simple filter joins
sqlopt rewrite "SELECT o.* FROM orders o JOIN users u ON o.user_id = u.id WHERE u.active = true"
```

## Quick Look

**Input:** a log file with many repeated queries.

```bash
sqlopt detect-n1 examples/queries.log --threshold 5 --window 50
```

**Output:**

```text
CRITICAL: Detected repeated query templates (threshold=5, window=50).
COUNT=50 TOTAL=847 TEMPLATE=select * from posts where user_id = ?
```

Recommendation: batch/eager load related records when you see repeated templates; for index suggestions on a specific query, run `sqlopt analyze "..."`.

## How it works

`sqlopt` reads SQL (and log files) as plain text and applies fast, deterministic heuristics:

- `detect-n1` streams a log file line-by-line, normalizes each query template (strips values, collapses whitespace, lowercases), then uses a sliding window counter to flag repeated templates.
- `analyze` parses SQL with `sqlparser` and suggests single-column indexes based on WHERE/JOIN predicate columns.
- `rewrite` applies a narrow, guarded JOIN-to-IN rewrite and emits warnings when it does.

## Examples

`examples/queries.log` contains 847 `SELECT * FROM posts WHERE user_id = ...` lines to exercise `detect-n1`.

## Sample output

These are copied from real runs of the commands above.

```text
$ sqlopt analyze "SELECT * FROM users WHERE age > 18"
SUGGESTION: CREATE INDEX CONCURRENTLY idx_users_age ON users(age);
ESTIMATED: 1164ms -> 8ms (99%)
```

```text
$ sqlopt detect-n1 examples/queries.log --threshold 5 --window 50
CRITICAL: Detected repeated query templates (threshold=5, window=50).
COUNT=50 TOTAL=847 TEMPLATE=select * from posts where user_id = ?
```

```text
$ sqlopt rewrite "SELECT o.* FROM orders o JOIN users u ON o.user_id = u.id WHERE u.active = true"
ORIGINAL: SELECT o.* FROM orders o JOIN users u ON o.user_id = u.id WHERE u.active = true
WARNING: avoid wildcard projections (SELECT *); select only needed columns
WARNING: heuristic rewrite: verify projection if you relied on JOINed columns
OPTIMIZED: SELECT o.* FROM orders o WHERE o.user_id IN (SELECT id FROM users WHERE active = true)
```

## Comparison (rough)

Notes:
- The `sqlopt` speed is an example from the `index_suggestion` benchmark in the `Benchmarks` section. Results vary by machine; run it locally to reproduce.
- `sqlopt`'s N+1 detection is log-based (repeated query templates), not ORM-aware instrumentation.
- `sqlopt`'s index suggestions are based on query structure (e.g., columns in `WHERE` clauses) and do not use database statistics.
- `sqlparser-rs` is a parser library; a speed comparison is not applicable.
- `Bullet` and `Prosopite` are runtime instrumentation tools, so a direct speed comparison is not applicable.

| Tool | Language | Speed (1M queries) | N+1 Detect | Index Suggest | CLI |
| --- | --- | ------------------: | :---: | :---: | :---: |
| [sqlopt](https://github.com/sqlopt-rs/sql_optimizer_rs) | Rust | ~8.8s | Yes (heuristic) | Yes (heuristic) | Yes |
| [sqlparser-rs](https://github.com/sqlparser-rs/sqlparser-rs) | Rust | N/A | No | No | No |
| [Bullet](https://github.com/flyerhzm/bullet) | Ruby | N/A | Yes | No | No |
| [Prosopite](https://github.com/charkost/prosopite) | Ruby | N/A | Yes | No | No |

## Benchmarks

Run the index suggestion benchmark:

```bash
cargo bench --bench index_suggestion -- 1_000_000
```

Result (this machine):

| Workload | Iterations | Elapsed (s) | Throughput (queries/s) |
| --- | ---: | ---: | ---: |
| index_suggestion | 1,000,000 | 8.864s | 112,814 queries/s |

At 1,000 queries/s, processing 1,000,000 queries takes ~16.7 minutes.

See `DESIGN.md` for the MVP boundaries and testing approach.
