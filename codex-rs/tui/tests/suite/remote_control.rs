use std::collections::HashMap;
use std::io::Read;
use std::io::Write;
use std::net::SocketAddr;
use std::net::TcpStream;
use std::path::Path;
use std::time::Duration;

use anyhow::Context;
use regex_lite::Regex;
use tokio::select;
use tokio::time::timeout;

#[tokio::test]
async fn remote_control_posts_into_normal_codex_tui() -> anyhow::Result<()> {
    if cfg!(windows) {
        return Ok(());
    }

    let tmp = tempfile::tempdir()?;
    let codex_home = tmp.path();
    let cwd = std::env::current_dir()?;
    let config_contents = format!(
        r#"
model_provider = "ollama"

[projects]
"{cwd}" = {{ trust_level = "trusted" }}
"#,
        cwd = cwd.display()
    );
    std::fs::write(codex_home.join("config.toml"), config_contents)?;

    let prompt = "remote control smoke test prompt";
    let codex_cli = codex_utils_cargo_bin::cargo_bin("codex")?;
    let args = vec![
        "-c".to_string(),
        "analytics.enabled=false".to_string(),
        "--remote-control".to_string(),
        "--remote-control-bind".to_string(),
        "127.0.0.1:0".to_string(),
    ];
    let spawned = spawn_codex_cli(&codex_cli, &args, codex_home, &cwd).await?;
    let codex_utils_pty::SpawnedProcess {
        session,
        stdout_rx,
        stderr_rx,
        exit_rx,
    } = spawned;
    let mut output_rx = codex_utils_pty::combine_output_receivers(stdout_rx, stderr_rx);
    let mut exit_rx = exit_rx;
    let writer_tx = session.writer_sender();
    let url_regex = Regex::new(r"http://127\.0\.0\.1:\d+\?token=[A-Za-z0-9_-]+")?;
    let mut output = Vec::new();
    let mut posted = false;

    let proof = timeout(Duration::from_secs(20), async {
        loop {
            select! {
                result = output_rx.recv() => match result {
                    Ok(chunk) => {
                        if chunk.windows(4).any(|window| window == b"\x1b[6n") {
                            let _ = writer_tx.send(b"\x1b[1;1R".to_vec()).await;
                        }
                        output.extend_from_slice(&chunk);
                        let visible_output = String::from_utf8_lossy(&output);
                        if !posted
                            && let Some(found) = url_regex.find(&visible_output)
                        {
                            post_remote_control_message(found.as_str(), prompt)?;
                            posted = true;
                        }
                        if posted && visible_output.contains(prompt) {
                            return Ok(());
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                        anyhow::bail!("codex output closed before remote-control prompt appeared: {}", output_tail(&output));
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {}
                },
                result = &mut exit_rx => {
                    let exit_code = result.context("codex exit channel closed")?;
                    anyhow::bail!("codex exited with {exit_code} before remote-control prompt appeared: {}", output_tail(&output));
                }
            }
        }
    })
    .await;

    session.terminate();
    match proof {
        Ok(result) => result,
        Err(_) => anyhow::bail!(
            "timed out waiting for remote-control prompt in normal Codex TUI: {}",
            output_tail(&output)
        ),
    }
}

async fn spawn_codex_cli(
    codex_cli: &Path,
    args: &[String],
    codex_home: impl AsRef<Path>,
    cwd: impl AsRef<Path>,
) -> anyhow::Result<codex_utils_pty::SpawnedProcess> {
    let mut env = HashMap::new();
    env.insert(
        "CODEX_HOME".to_string(),
        codex_home.as_ref().display().to_string(),
    );
    codex_utils_pty::spawn_pty_process(
        codex_cli.to_string_lossy().as_ref(),
        args,
        cwd.as_ref(),
        &env,
        &None,
        codex_utils_pty::TerminalSize {
            rows: 40,
            cols: 160,
        },
    )
    .await
}

fn post_remote_control_message(control_url: &str, prompt: &str) -> anyhow::Result<()> {
    let url = url::Url::parse(control_url).context("remote-control URL should parse")?;
    let port = url
        .port()
        .context("remote-control URL should include port")?;
    let token = url
        .query_pairs()
        .find_map(|(name, value)| (name == "token").then(|| value.into_owned()))
        .context("remote-control URL should include token")?;
    let addr = SocketAddr::from(([127, 0, 0, 1], port));
    let body = serde_json::json!({ "message": prompt }).to_string();
    let request = format!(
        "POST /api/message?token={token} HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    let mut stream = TcpStream::connect(addr).context("connect remote-control server")?;
    stream
        .set_read_timeout(Some(Duration::from_secs(2)))
        .context("set remote-control read timeout")?;
    stream
        .write_all(request.as_bytes())
        .context("write remote-control request")?;
    let mut response = String::new();
    stream
        .read_to_string(&mut response)
        .context("read remote-control response")?;
    if response.starts_with("HTTP/1.1 202 Accepted") {
        Ok(())
    } else {
        anyhow::bail!("unexpected remote-control response: {response}");
    }
}

fn output_tail(output: &[u8]) -> String {
    let start = output.len().saturating_sub(4096);
    String::from_utf8_lossy(&output[start..]).to_string()
}
