# sql_optimizer_rs

Rust-based SQL query analyzer / optimizer (MVP).

## Build

```bash
cargo build -p sqlopt
```

## CLI usage

```bash
# Suggest an index
cargo run -p sqlopt -- analyze "SELECT * FROM users WHERE age > 18"

# Detect repeated query templates (common N+1 symptom)
# (flags are counts-per-window-of-N-queries, not timestamps)
cargo run -p sqlopt -- detect-n1 examples/queries.log --threshold 5 --window 50

# Heuristic rewrite (JOIN -> IN subquery) for simple filter joins
cargo run -p sqlopt -- rewrite "SELECT o.* FROM orders o JOIN users u ON o.user_id = u.id WHERE u.active = true"
```

## Examples

`examples/queries.log` contains 847 `SELECT * FROM posts WHERE user_id = ...` lines to exercise `detect-n1`.

## Sample output

These are copied from real runs of the commands above.

```text
$ cargo run -p sqlopt -- analyze "SELECT * FROM users WHERE age > 18"
SUGGESTION: CREATE INDEX CONCURRENTLY idx_users_age ON users(age);
ESTIMATED: 1164ms -> 8ms (99%)
```

```text
$ cargo run -p sqlopt -- detect-n1 examples/queries.log --threshold 5 --window 50
CRITICAL: Detected repeated query templates (threshold=5, window=50).
COUNT=50 TOTAL=847 TEMPLATE=select * from posts where user_id = ?
```

```text
$ cargo run -p sqlopt -- rewrite "SELECT o.* FROM orders o JOIN users u ON o.user_id = u.id WHERE u.active = true"
ORIGINAL: SELECT o.* FROM orders o JOIN users u ON o.user_id = u.id WHERE u.active = true
WARNING: avoid wildcard projections (SELECT *); select only needed columns
WARNING: heuristic rewrite: verify projection if you relied on JOINed columns
OPTIMIZED: SELECT o.* FROM orders o WHERE o.user_id IN (SELECT id FROM users WHERE active = true)
```

## Benchmarks

Run the index suggestion benchmark:

```bash
cargo bench --bench index_suggestion -- 1_000_000
```

Result (this machine):

| Workload | Iterations | Elapsed | Throughput |
| --- | ---: | ---: | ---: |
| index_suggestion | 1,000,000 | 8.864s | 112,814 queries/s |

For a rough comparison point, at 1,000 queries/s a Python tool would take ~16.7 minutes for 1,000,000 queries.

See `DESIGN.md` for the MVP boundaries and testing approach.
