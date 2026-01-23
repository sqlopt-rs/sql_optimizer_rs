use std::env;
use std::hint::black_box;
use std::time::Instant;

fn parse_iterations(args: impl Iterator<Item = String>) -> Result<u64, String> {
    let mut value: Option<u64> = None;

    for arg in args {
        if arg == "--help" || arg == "-h" {
            return Err(usage());
        }

        // `cargo bench` injects `--bench` even for `harness = false` targets.
        if arg == "--bench" {
            continue;
        }

        if arg.starts_with('-') {
            return Err(format!("unknown argument: {arg}\n\n{}", usage()));
        }

        let cleaned = arg.replace('_', "");
        match cleaned.parse::<u64>() {
            Ok(parsed) if parsed > 0 => {
                if value.is_some() {
                    return Err(format!("too many arguments: {arg}\n\n{}", usage()));
                }
                value = Some(parsed);
            }
            _ => return Err(format!("invalid iterations value: {arg}\n\n{}", usage())),
        }
    }

    value.ok_or_else(usage)
}

fn usage() -> String {
    [
        "Usage:",
        "  cargo bench --bench index_suggestion -- <iterations>",
        "",
        "Example:",
        "  cargo bench --bench index_suggestion -- 1_000_000",
    ]
    .join("\n")
}

fn main() {
    let iterations = match parse_iterations(env::args().skip(1)) {
        Ok(value) => value,
        Err(message) => {
            eprintln!("{message}");
            std::process::exit(2);
        }
    };

    let query = "SELECT * FROM posts WHERE user_id = 123";

    let start = Instant::now();
    for _ in 0..iterations {
        let result = sql_optimizer::analyze(black_box(query)).expect("query should parse");
        black_box(result);
    }
    let elapsed = start.elapsed();
    let seconds = elapsed.as_secs_f64();
    let throughput = (iterations as f64) / seconds.max(f64::MIN_POSITIVE);

    println!("workload=index_suggestion");
    println!("iterations={iterations}");
    println!("elapsed_seconds={seconds:.6}");
    println!("throughput_qps={throughput:.0}");
}
