# sql_optimizer_rs design (MVP)

## Goal / acceptance

- Provide a fast, deterministic CLI (`sqlopt`) to (1) suggest indexes, (2) detect likely N+1 patterns in logs, and (3) produce conservative query rewrites for common slow patterns.
- Make output stable (same input → same output), with actionable messages and no hidden env/time dependencies.

## Ownership / boundaries

- `crates/sql_optimizer`: pure core (parsing + analysis + rewrite). No file I/O, no printing, no clocks.
- `crates/sqlopt`: runtime glue (argument parsing, file I/O, formatting, exit codes).

## Invariants

- Single-statement parsing only; unsupported shapes return a clear `Unsupported(...)` error or a no-op with warnings.
- Stable ordering: all suggestions/findings are explicitly sorted; no reliance on `HashMap` iteration order.

## Core API shapes (stable)

- `sql_optimizer::analyze(query: &str) -> Result<AnalyzeResult, SqlOptError>`
- `sql_optimizer::rewrite(query: &str) -> Result<RewriteResult, SqlOptError>`
- `sql_optimizer::detect_n1_from_log(log: &str, opts: N1Options) -> N1Report`

## Side effects

- Only in `sqlopt`: read log files, print human-friendly output, map errors to non-zero exit.

## Error model

- `SqlOptError::ParseError(String)` for SQL parsing failures.
- `SqlOptError::Unsupported(String)` for unsupported query shapes.

## Performance notes

- Parse each query once per command; keep analysis as AST walks + stable sorting.
- N+1 detection is O(n) via sliding window counts over normalized templates.

## Tests

- Unit tests in `crates/sql_optimizer` for pure core behaviors (normalization, extraction, rewrite rules).
- E2E integration tests in `crates/sqlopt/tests` that exercise real CLI workflows and chain features.

