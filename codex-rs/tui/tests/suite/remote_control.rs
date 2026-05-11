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
use tokio::time::sleep;
use tokio::time::timeout;

#[tokio::test]
async fn remote_control_posts_into_normal_codex_tui() -> anyhow::Result<()> {
    let args = vec![
        "-c".to_string(),
        "analytics.enabled=false".to_string(),
        "--remote-control".to_string(),
        "--remote-control-bind".to_string(),
        "127.0.0.1:0".to_string(),
    ];
    run_remote_control_tui_smoke(
        args,
        RemoteControlStart::CommandLineFlag,
        "remote control smoke test prompt",
    )
    .await
}

#[tokio::test]
async fn remote_control_slash_command_starts_sidecar() -> anyhow::Result<()> {
    let args = vec!["-c".to_string(), "analytics.enabled=false".to_string()];
    run_remote_control_tui_smoke(
        args,
        RemoteControlStart::SlashCommand,
        "remote control slash smoke test prompt",
    )
    .await
}

#[tokio::test]
async fn remote_control_slash_command_stops_sidecar() -> anyhow::Result<()> {
    if cfg!(windows) {
        return Ok(());
    }

    let args = vec!["-c".to_string(), "analytics.enabled=false".to_string()];
    let spawned_tui = spawn_remote_control_tui(&args).await?;
    let codex_utils_pty::SpawnedProcess {
        session,
        stdout_rx,
        stderr_rx,
        exit_rx,
    } = spawned_tui.spawned;
    let mut output_rx = codex_utils_pty::combine_output_receivers(stdout_rx, stderr_rx);
    let mut exit_rx = exit_rx;
    let writer_tx = session.writer_sender();
    let url_regex =
        Regex::new(r"http://(?:127\.0\.0\.1|\d+\.\d+\.\d+\.\d+):\d+\?token=[A-Za-z0-9_-]+")?;
    let mut output = Vec::new();
    let mut start_sent = false;
    let mut start_submitted = false;
    let mut stop_sent = false;
    let mut stop_submitted = false;
    let mut control_url = None;

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
                        let plain_output = plain_terminal_text(&output);
                        if !start_sent && plain_output.contains("gpt-5.5 default") {
                            let _ = writer_tx.send(b"/remote-control".to_vec()).await;
                            start_sent = true;
                        }
                        if start_sent
                            && !start_submitted
                            && plain_output.contains("/remote-control")
                        {
                            let _ = writer_tx.send(b"\r".to_vec()).await;
                            start_submitted = true;
                        }
                        if start_submitted
                            && !stop_sent
                            && let Some(found) = url_regex.find(&visible_output)
                        {
                            control_url = Some(found.as_str().to_string());
                            let _ = writer_tx.send(b"/remote-control stop".to_vec()).await;
                            stop_sent = true;
                        }
                        if stop_sent
                            && !stop_submitted
                            && plain_output.contains("/remote-control stop")
                        {
                            let _ = writer_tx.send(b"\r".to_vec()).await;
                            stop_submitted = true;
                        }
                        if stop_submitted && visible_output.contains("Remote control stopped.") {
                            let control_url = control_url
                                .as_deref()
                                .context("remote-control URL should be captured before stop")?;
                            wait_for_remote_control_server_to_stop(control_url).await?;
                            return Ok(());
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                        anyhow::bail!("codex output closed before remote-control stopped: {}", output_tail(&output));
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {}
                },
                result = &mut exit_rx => {
                    let exit_code = result.context("codex exit channel closed")?;
                    anyhow::bail!("codex exited with {exit_code} before remote-control stopped: {}", output_tail(&output));
                }
            }
        }
    })
    .await;

    session.terminate();
    match proof {
        Ok(result) => result,
        Err(_) => anyhow::bail!(
            "timed out waiting for remote-control stop in normal Codex TUI: {}",
            output_tail(&output)
        ),
    }
}

enum RemoteControlStart {
    CommandLineFlag,
    SlashCommand,
}

async fn run_remote_control_tui_smoke(
    args: Vec<String>,
    start: RemoteControlStart,
    prompt: &str,
) -> anyhow::Result<()> {
    if cfg!(windows) {
        return Ok(());
    }

    let spawned_tui = spawn_remote_control_tui(&args).await?;
    let codex_utils_pty::SpawnedProcess {
        session,
        stdout_rx,
        stderr_rx,
        exit_rx,
    } = spawned_tui.spawned;
    let mut output_rx = codex_utils_pty::combine_output_receivers(stdout_rx, stderr_rx);
    let mut exit_rx = exit_rx;
    let writer_tx = session.writer_sender();
    let url_regex =
        Regex::new(r"http://(?:127\.0\.0\.1|\d+\.\d+\.\d+\.\d+):\d+\?token=[A-Za-z0-9_-]+")?;
    let mut output = Vec::new();
    let mut posted = false;
    let mut slash_command_sent = matches!(start, RemoteControlStart::CommandLineFlag);
    let mut slash_command_submitted = matches!(start, RemoteControlStart::CommandLineFlag);

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
                        let plain_output = plain_terminal_text(&output);
                        if !slash_command_sent && plain_output.contains("gpt-5.5 default") {
                            let _ = writer_tx.send(b"/remote-control".to_vec()).await;
                            slash_command_sent = true;
                        }
                        if slash_command_sent
                            && !slash_command_submitted
                            && plain_output.contains("/remote-control")
                        {
                            let _ = writer_tx.send(b"\r".to_vec()).await;
                            slash_command_submitted = true;
                        }
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

struct SpawnedRemoteControlTui {
    _codex_home: tempfile::TempDir,
    spawned: codex_utils_pty::SpawnedProcess,
}

async fn spawn_remote_control_tui(args: &[String]) -> anyhow::Result<SpawnedRemoteControlTui> {
    let tmp = tempfile::tempdir()?;
    let codex_home = tmp.path();
    let cwd = std::env::current_dir()?;
    let repo_root = cwd
        .ancestors()
        .nth(2)
        .context("tui test should run from inside the codex repo")?;
    let config_contents = format!(
        r#"
model_provider = "ollama"

[projects]
"{cwd}" = {{ trust_level = "trusted" }}
"{repo_root}" = {{ trust_level = "trusted" }}
"#,
        cwd = cwd.display(),
        repo_root = repo_root.display()
    );
    std::fs::write(codex_home.join("config.toml"), config_contents)?;

    let codex_cli = codex_utils_cargo_bin::cargo_bin("codex")?;
    let spawned = spawn_codex_cli(&codex_cli, args, codex_home, &cwd).await?;
    Ok(SpawnedRemoteControlTui {
        _codex_home: tmp,
        spawned,
    })
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

async fn wait_for_remote_control_server_to_stop(control_url: &str) -> anyhow::Result<()> {
    let url = url::Url::parse(control_url).context("remote-control URL should parse")?;
    let port = url
        .port()
        .context("remote-control URL should include port")?;
    let addr = SocketAddr::from(([127, 0, 0, 1], port));
    for _ in 0..40 {
        match TcpStream::connect_timeout(&addr, Duration::from_millis(100)) {
            Ok(stream) => {
                drop(stream);
                sleep(Duration::from_millis(50)).await;
            }
            Err(_) => return Ok(()),
        }
    }

    anyhow::bail!("remote-control server still accepts connections after /remote-control stop")
}

fn output_tail(output: &[u8]) -> String {
    let start = output.len().saturating_sub(4096);
    String::from_utf8_lossy(&output[start..]).to_string()
}

fn plain_terminal_text(output: &[u8]) -> String {
    let mut plain = String::new();
    let mut index = 0;
    while index < output.len() {
        if output[index] == 0x1b {
            index += 1;
            if index >= output.len() {
                break;
            }
            match output[index] {
                b'[' => {
                    index += 1;
                    while index < output.len() && !(0x40..=0x7e).contains(&output[index]) {
                        index += 1;
                    }
                    index += usize::from(index < output.len());
                }
                b']' => {
                    index += 1;
                    while index < output.len() {
                        if output[index] == 0x07 {
                            index += 1;
                            break;
                        }
                        if output[index] == 0x1b
                            && output.get(index + 1).is_some_and(|byte| *byte == b'\\')
                        {
                            index += 2;
                            break;
                        }
                        index += 1;
                    }
                }
                _ => {
                    index += 1;
                }
            }
            continue;
        }

        let byte = output[index];
        if byte == b'\n'
            || byte == b'\r'
            || byte == b'\t'
            || byte.is_ascii_graphic()
            || byte == b' '
        {
            plain.push(byte as char);
        }
        index += 1;
    }
    plain
}
