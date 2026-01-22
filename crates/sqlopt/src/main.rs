use std::fs;
use std::path::PathBuf;

use anyhow::Context;
use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(name = "sqlopt")]
#[command(about = "A tiny SQL analysis/optimization CLI (early scaffold).")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Suggest indexes based on WHERE/JOIN columns
    Analyze { query: String },
    /// Suggest a heuristic rewrite for a slow query
    Rewrite { query: String },
    /// Detect repeated query templates (a common N+1 symptom)
    DetectN1 {
        path: PathBuf,
        #[arg(long, default_value_t = 5)]
        threshold: usize,
    },
}

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Command::Analyze { query } => {
            let result = sql_optimizer::analyze(&query)?;
            for warning in result.warnings {
                println!("WARNING: {warning}");
            }
            if result.index_suggestions.is_empty() {
                println!("OK: No index suggestions.");
            } else {
                for suggestion in result.index_suggestions {
                    println!("SUGGESTION: {}", suggestion.statement);
                }
            }
        }
        Command::Rewrite { query } => {
            let result = sql_optimizer::rewrite(&query)?;
            println!("ORIGINAL: {}", result.original);
            for warning in result.warnings {
                println!("WARNING: {warning}");
            }
            match result.rewritten {
                Some(rewritten) => println!("OPTIMIZED: {rewritten}"),
                None => println!("OK: No rewrite available for this query."),
            }
        }
        Command::DetectN1 { path, threshold } => {
            let contents = fs::read_to_string(&path)
                .with_context(|| format!("failed to read log file {}", path.display()))?;
            let report = sql_optimizer::detect_n1_from_log(&contents, threshold);
            if report.findings.is_empty() {
                println!("OK: No repeated query templates found (threshold={threshold}).");
            } else {
                println!("CRITICAL: Detected repeated query templates (threshold={threshold}).");
                for finding in report.findings {
                    println!("COUNT={} TEMPLATE={}", finding.count, finding.template);
                }
            }
        }
    }

    Ok(())
}
