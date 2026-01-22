# sql_optimizer_rs

Early scaffold for a Rust-based SQL query analyzer / optimizer.

## Build

```bash
cargo build -p sqlopt
```

## CLI usage

```bash
# Suggest an index
cargo run -p sqlopt -- analyze "SELECT * FROM users WHERE age > 18"

# Detect repeated query templates (common N+1 symptom)
cargo run -p sqlopt -- detect-n1 queries.log

# Heuristic rewrite (JOIN -> IN subquery) for simple filter joins
cargo run -p sqlopt -- rewrite "SELECT * FROM orders o JOIN users u ON o.user_id = u.id WHERE u.active = true"
```
