use std::fs;
use std::path::PathBuf;
use std::process::Command;

fn run_sqlopt(args: &[&str]) -> (i32, String, String) {
    let exe = env!("CARGO_BIN_EXE_sqlopt");
    let output = Command::new(exe).args(args).output().expect("run sqlopt");
    let code = output.status.code().unwrap_or(-1);
    let stdout = String::from_utf8_lossy(&output.stdout).replace("\r\n", "\n");
    let stderr = String::from_utf8_lossy(&output.stderr).replace("\r\n", "\n");
    (code, stdout, stderr)
}

#[test]
fn e2e_chains_rewrite_analyze_and_detect_n1() {
    let join_query =
        "SELECT o.* FROM orders o JOIN users u ON o.user_id = u.id WHERE u.active = true";

    let (code, stdout, stderr) = run_sqlopt(&["rewrite", join_query]);
    assert_eq!(code, 0, "stderr: {stderr}");
    assert!(stdout.contains("ORIGINAL:"), "{stdout}");
    assert!(stdout.contains("OPTIMIZED:"), "{stdout}");

    let optimized = stdout
        .lines()
        .find_map(|line| line.strip_prefix("OPTIMIZED: ").map(str::to_string))
        .expect("OPTIMIZED line present");

    let (code, stdout, stderr) = run_sqlopt(&["analyze", &optimized]);
    assert_eq!(code, 0, "stderr: {stderr}");
    assert!(stdout.contains("SUGGESTION:"), "{stdout}");
    assert!(
        stdout.contains("CREATE INDEX CONCURRENTLY idx_orders_user_id ON orders(user_id);"),
        "{stdout}"
    );
    assert!(
        stdout.contains("CREATE INDEX CONCURRENTLY idx_users_active ON users(active);"),
        "{stdout}"
    );

    let log_path = temp_log_path("sqlopt_e2e_queries.log");
    let log = r#"
SELECT * FROM users WHERE id = 1;
SELECT * FROM users WHERE id = 2;
SELECT * FROM users WHERE id = 3;
SELECT * FROM users WHERE id = 4;
SELECT * FROM users WHERE id = 5;
"#;
    fs::write(&log_path, log).expect("write log");

    let (code, stdout, stderr) = run_sqlopt(&[
        "detect-n1",
        log_path.to_str().expect("utf-8 path"),
        "--threshold",
        "5",
        "--window",
        "10",
    ]);
    assert_eq!(code, 0, "stderr: {stderr}");
    assert!(stdout.contains("CRITICAL:"), "{stdout}");
    assert!(
        stdout.contains("TEMPLATE=select * from users where id = ?"),
        "{stdout}"
    );
}

#[test]
fn e2e_analyze_prints_ok_when_no_suggestions() {
    let (code, stdout, stderr) = run_sqlopt(&["analyze", "SELECT 1"]);
    assert_eq!(code, 0, "stderr: {stderr}");
    assert!(stdout.contains("WARNING:"), "{stdout}");
    assert!(stdout.contains("OK: No index suggestions."), "{stdout}");
}

#[test]
fn e2e_rewrite_prints_ok_when_no_rewrite_available() {
    let (code, stdout, stderr) = run_sqlopt(&["rewrite", "SELECT * FROM users"]);
    assert_eq!(code, 0, "stderr: {stderr}");
    assert!(stdout.contains("ORIGINAL:"), "{stdout}");
    assert!(
        stdout.contains("OK: No rewrite available for this query."),
        "{stdout}"
    );
}

#[test]
fn e2e_detect_n1_prints_ok_when_below_threshold() {
    let log_path = temp_log_path("sqlopt_e2e_queries_below_threshold.log");
    let log = r#"
SELECT * FROM users WHERE id = 1;
SELECT * FROM users WHERE id = 2;
"#;
    fs::write(&log_path, log).expect("write log");

    let (code, stdout, stderr) = run_sqlopt(&[
        "detect-n1",
        log_path.to_str().expect("utf-8 path"),
        "--threshold",
        "5",
        "--window",
        "10",
    ]);
    assert_eq!(code, 0, "stderr: {stderr}");
    assert!(
        stdout.contains("OK: No repeated query templates found"),
        "{stdout}"
    );
}

#[test]
fn e2e_analyze_errors_on_invalid_sql() {
    let (code, _stdout, stderr) = run_sqlopt(&["analyze", "SELECT FROM"]);
    assert_ne!(code, 0, "stderr: {stderr}");
    assert!(stderr.contains("failed to parse SQL"), "{stderr}");
}

#[test]
fn e2e_detect_n1_errors_on_missing_log_file() {
    let log_path = temp_log_path("does_not_exist.log");
    let _ = fs::remove_file(&log_path);

    let (code, _stdout, stderr) =
        run_sqlopt(&["detect-n1", log_path.to_str().expect("utf-8 path")]);
    assert_ne!(code, 0, "stderr: {stderr}");
    assert!(stderr.contains("failed to read log file"), "{stderr}");
}

fn temp_log_path(file_name: &str) -> PathBuf {
    let mut path = std::env::temp_dir();
    path.push(format!("sqlopt-{}", std::process::id()));
    fs::create_dir_all(&path).expect("create temp dir");
    path.push(file_name);
    path
}
