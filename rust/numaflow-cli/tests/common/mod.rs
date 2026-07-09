use std::process::Command;
use std::time::Duration;

/// Wait for a socket file to appear (the SDK creates it on startup).
pub async fn wait_for_socket(path: &std::path::Path) {
    for _ in 0..100 {
        if path.exists() {
            // Small extra grace so the server is actually listening.
            tokio::time::sleep(Duration::from_millis(50)).await;
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("socket {path:?} never appeared");
}

/// Run the nfcli binary, optionally feeding `stdin`, and capture (exit_code, stdout, stderr).
pub fn run_nfcli(args: &[&str], stdin: Option<&str>) -> (i32, String, String) {
    use std::io::Write;
    use std::process::Stdio;

    let bin = env!("CARGO_BIN_EXE_nfcli");
    let mut child = Command::new(bin)
        .args(args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn nfcli");
    if let Some(s) = stdin {
        child.stdin.take().unwrap().write_all(s.as_bytes()).unwrap();
    }
    let out = child.wait_with_output().expect("wait nfcli");
    (
        out.status.code().unwrap_or(-1),
        String::from_utf8_lossy(&out.stdout).into_owned(),
        String::from_utf8_lossy(&out.stderr).into_owned(),
    )
}
