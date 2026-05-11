use std::fs;
use std::io::Read;
use std::io::Write;
use std::net::TcpStream;
use std::net::ToSocketAddrs;
use std::path::Path;
use std::path::PathBuf;
use std::time::Duration;

use anyhow::Context;
use anyhow::Result;
use anyhow::bail;
use codex_core::config::find_codex_home;
use codex_protocol::ThreadId;
use serde_json::Value;

const HTTP_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Debug)]
pub(crate) struct RemoteForkImport {
    pub(crate) thread_id: String,
    #[allow(dead_code)]
    pub(crate) imported_path: PathBuf,
}

#[derive(Debug, PartialEq, Eq)]
struct RemoteForkBundle {
    thread_id: String,
    cwd: String,
    rollout_file_name: String,
    rollout_jsonl: String,
}

#[derive(Debug, PartialEq, Eq)]
struct HttpUrl {
    host: String,
    port: u16,
    authority: String,
    target: String,
}

pub(crate) async fn import_remote_fork(code: &str) -> Result<RemoteForkImport> {
    let bundle = fetch_remote_fork_bundle(code)?;
    let codex_home = find_codex_home()?;
    import_bundle_to_codex_home(&bundle, codex_home.as_path())
}

fn fetch_remote_fork_bundle(code: &str) -> Result<RemoteForkBundle> {
    let response = http_get(code)?;
    let value: Value = serde_json::from_str(&response).context("remote fork bundle is not JSON")?;
    let protocol_version = value
        .get("protocolVersion")
        .and_then(Value::as_u64)
        .context("remote fork bundle missing protocolVersion")?;
    if protocol_version != 1 {
        bail!("unsupported remote fork protocol version {protocol_version}");
    }
    Ok(RemoteForkBundle {
        thread_id: required_string(&value, "threadId")?,
        cwd: required_string(&value, "cwd")?,
        rollout_file_name: required_string(&value, "rolloutFileName")?,
        rollout_jsonl: required_string(&value, "rolloutJsonl")?,
    })
}

fn import_bundle_to_codex_home(
    bundle: &RemoteForkBundle,
    codex_home: &Path,
) -> Result<RemoteForkImport> {
    ThreadId::from_string(&bundle.thread_id).with_context(|| {
        format!(
            "remote fork bundle has invalid thread id {}",
            bundle.thread_id
        )
    })?;
    validate_rollout_file_name(&bundle.rollout_file_name, &bundle.thread_id)?;

    let imports_dir = codex_home.join("sessions").join("remote-forks");
    fs::create_dir_all(&imports_dir).with_context(|| {
        format!(
            "failed to create remote fork import directory {}",
            imports_dir.display()
        )
    })?;
    let imported_path = imports_dir.join(&bundle.rollout_file_name);
    fs::write(&imported_path, &bundle.rollout_jsonl)
        .with_context(|| format!("failed to import remote fork {}", imported_path.display()))?;

    Ok(RemoteForkImport {
        thread_id: bundle.thread_id.clone(),
        imported_path,
    })
}

fn validate_rollout_file_name(file_name: &str, thread_id: &str) -> Result<()> {
    if file_name.contains('/') || file_name.contains('\\') {
        bail!("remote fork rollout file name must not contain path separators");
    }
    if !file_name.starts_with("rollout-") || !file_name.ends_with(".jsonl") {
        bail!("remote fork rollout file name is not a Codex rollout file");
    }
    let expected_suffix = format!("{thread_id}.jsonl");
    if !file_name.ends_with(&expected_suffix) {
        bail!("remote fork rollout file name does not match thread id {thread_id}");
    }
    Ok(())
}

fn required_string(value: &Value, field: &str) -> Result<String> {
    value
        .get(field)
        .and_then(Value::as_str)
        .map(str::to_string)
        .with_context(|| format!("remote fork bundle missing {field}"))
}

fn http_get(input: &str) -> Result<String> {
    let url = parse_http_url(input)?;
    let addr = (url.host.as_str(), url.port)
        .to_socket_addrs()
        .with_context(|| format!("failed to resolve {}", url.host))?
        .next()
        .with_context(|| format!("no addresses resolved for {}", url.host))?;
    let mut stream = TcpStream::connect_timeout(&addr, HTTP_TIMEOUT)
        .context("failed to connect to remote fork host")?;
    stream.set_read_timeout(Some(HTTP_TIMEOUT))?;
    stream.set_write_timeout(Some(HTTP_TIMEOUT))?;
    stream.write_all(
        format!(
            "GET {} HTTP/1.1\r\nHost: {}\r\nConnection: close\r\n\r\n",
            url.target, url.authority
        )
        .as_bytes(),
    )?;
    let mut response = String::new();
    stream.read_to_string(&mut response)?;
    let (headers, body) = response
        .split_once("\r\n\r\n")
        .context("remote fork host returned an invalid HTTP response")?;
    let status = headers
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .context("remote fork host returned an invalid HTTP status")?;
    if !status.starts_with('2') {
        bail!("remote fork host returned HTTP {status}: {body}");
    }
    Ok(body.to_string())
}

fn parse_http_url(input: &str) -> Result<HttpUrl> {
    let rest = input
        .strip_prefix("http://")
        .context("remote fork code must be an http:// claim URL for this local demo")?;
    let (authority, path_and_query) = rest
        .split_once('/')
        .map_or((rest, ""), |(authority, path)| (authority, path));
    if authority.is_empty() {
        bail!("remote fork claim URL is missing a host");
    }
    let target = format!("/{path_and_query}");
    let (host, port) = parse_authority(authority)?;
    Ok(HttpUrl {
        host,
        port,
        authority: authority.to_string(),
        target,
    })
}

fn parse_authority(authority: &str) -> Result<(String, u16)> {
    if let Some(rest) = authority.strip_prefix('[') {
        let (host, after_host) = rest
            .split_once(']')
            .context("remote fork IPv6 host is missing closing bracket")?;
        let port = after_host
            .strip_prefix(':')
            .map(str::parse::<u16>)
            .transpose()
            .context("remote fork port is invalid")?
            .unwrap_or(80);
        return Ok((host.to_string(), port));
    }

    let (host, port) = authority
        .rsplit_once(':')
        .map(|(host, port)| {
            let parsed_port = port.parse::<u16>().context("remote fork port is invalid")?;
            Ok::<_, anyhow::Error>((host.to_string(), parsed_port))
        })
        .transpose()?
        .unwrap_or_else(|| (authority.to_string(), 80));
    if host.is_empty() {
        bail!("remote fork claim URL is missing a host");
    }
    Ok((host, port))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_http_url_handles_host_port_path_and_query() {
        let parsed = parse_http_url("http://127.0.0.1:4567/api/forks/abc?token=t").expect("parse");
        assert_eq!(
            parsed,
            HttpUrl {
                host: "127.0.0.1".to_string(),
                port: 4567,
                authority: "127.0.0.1:4567".to_string(),
                target: "/api/forks/abc?token=t".to_string(),
            }
        );
    }

    #[test]
    fn import_bundle_writes_rollout_under_remote_forks() {
        let temp = tempfile::TempDir::new().expect("temp dir");
        let thread_id = "00000000-0000-4000-8000-000000000456";
        let bundle = RemoteForkBundle {
            thread_id: thread_id.to_string(),
            cwd: "/tmp/codex".to_string(),
            rollout_file_name: format!("rollout-2026-05-11T00-00-00-{thread_id}.jsonl"),
            rollout_jsonl: "rollout contents".to_string(),
        };

        let imported = import_bundle_to_codex_home(&bundle, temp.path()).expect("import bundle");

        assert_eq!(imported.thread_id, thread_id);
        assert_eq!(
            fs::read_to_string(imported.imported_path).expect("read imported rollout"),
            "rollout contents"
        );
    }

    #[test]
    fn import_bundle_rejects_path_traversal_file_name() {
        let temp = tempfile::TempDir::new().expect("temp dir");
        let thread_id = "00000000-0000-4000-8000-000000000456";
        let bundle = RemoteForkBundle {
            thread_id: thread_id.to_string(),
            cwd: "/tmp/codex".to_string(),
            rollout_file_name: format!("../rollout-2026-05-11T00-00-00-{thread_id}.jsonl"),
            rollout_jsonl: "rollout contents".to_string(),
        };

        let err = import_bundle_to_codex_home(&bundle, temp.path()).expect_err("reject traversal");

        assert!(
            err.to_string().contains("path separators"),
            "unexpected error: {err}"
        );
    }
}
