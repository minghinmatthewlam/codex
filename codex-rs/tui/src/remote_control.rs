use std::io;
use std::io::Read;
use std::io::Write;
use std::net::IpAddr;
use std::net::Ipv4Addr;
use std::net::SocketAddr;
use std::net::TcpListener;
use std::net::TcpStream;
use std::net::UdpSocket;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::thread;
use std::time::Duration;

use base64::Engine;
use rand::RngCore;
use serde::Deserialize;

use crate::app_event::AppEvent;
use crate::app_event_sender::AppEventSender;

const DEFAULT_RESPONSE_TIMEOUT: Duration = Duration::from_secs(2);
const SERVER_IDLE_SLEEP: Duration = Duration::from_millis(25);
const MAX_REQUEST_BYTES: usize = 64 * 1024;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LocalRemoteControlOptions {
    pub bind_addr: SocketAddr,
}

pub(crate) struct LocalRemoteControlServer {
    url: String,
    shutdown: Arc<AtomicBool>,
    join_handle: Option<thread::JoinHandle<()>>,
}

impl LocalRemoteControlServer {
    pub(crate) fn url(&self) -> &str {
        &self.url
    }
}

impl Drop for LocalRemoteControlServer {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::Release);
        if let Some(join_handle) = self.join_handle.take()
            && let Err(err) = join_handle.join()
        {
            tracing::warn!("remote-control server thread join failed: {err:?}");
        }
    }
}

pub(crate) fn start_local_server(
    options: LocalRemoteControlOptions,
    app_event_tx: AppEventSender,
) -> io::Result<LocalRemoteControlServer> {
    let listener = TcpListener::bind(options.bind_addr)?;
    listener.set_nonblocking(true)?;
    let local_addr = listener.local_addr()?;
    let token = generate_token();
    let url = control_url(local_addr, &token);
    let shutdown = Arc::new(AtomicBool::new(false));
    let thread_shutdown = Arc::clone(&shutdown);
    let join_handle = thread::Builder::new()
        .name("codex-remote-control".to_string())
        .spawn(move || serve(listener, token, app_event_tx, thread_shutdown))
        .map_err(io::Error::other)?;

    Ok(LocalRemoteControlServer {
        url,
        shutdown,
        join_handle: Some(join_handle),
    })
}

fn serve(
    listener: TcpListener,
    token: String,
    app_event_tx: AppEventSender,
    shutdown: Arc<AtomicBool>,
) {
    while !shutdown.load(Ordering::Acquire) {
        match listener.accept() {
            Ok((mut stream, _)) => {
                let _ = stream.set_read_timeout(Some(DEFAULT_RESPONSE_TIMEOUT));
                let _ = stream.set_write_timeout(Some(DEFAULT_RESPONSE_TIMEOUT));
                if let Err(err) = handle_connection(&mut stream, &token, &app_event_tx) {
                    tracing::debug!("remote-control request failed: {err}");
                }
            }
            Err(err) if err.kind() == io::ErrorKind::WouldBlock => {
                thread::sleep(SERVER_IDLE_SLEEP);
            }
            Err(err) => {
                tracing::warn!("remote-control accept failed: {err}");
                thread::sleep(SERVER_IDLE_SLEEP);
            }
        }
    }
}

fn handle_connection(
    stream: &mut TcpStream,
    token: &str,
    app_event_tx: &AppEventSender,
) -> io::Result<()> {
    let request = read_request(stream)?;
    if !is_authorized(&request, token) {
        return write_response(
            stream,
            "401 Unauthorized",
            "text/plain; charset=utf-8",
            "Unauthorized\n",
        );
    }

    match (request.method.as_str(), request.path.as_str()) {
        ("GET", "/") => write_response(
            stream,
            "200 OK",
            "text/html; charset=utf-8",
            &remote_control_html(token),
        ),
        ("GET", "/health") => {
            write_response(stream, "200 OK", "application/json", r#"{"ok":true}"#)
        }
        ("POST", "/api/message") => handle_message_post(stream, request, app_event_tx),
        _ => write_response(
            stream,
            "404 Not Found",
            "text/plain; charset=utf-8",
            "Not found\n",
        ),
    }
}

fn handle_message_post(
    stream: &mut TcpStream,
    request: HttpRequest,
    app_event_tx: &AppEventSender,
) -> io::Result<()> {
    let body = std::str::from_utf8(&request.body).map_err(|err| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("request body must be UTF-8: {err}"),
        )
    })?;
    let payload: MessagePost = serde_json::from_str(body).map_err(|err| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("request body must be JSON: {err}"),
        )
    })?;
    let message = payload.message.trim().to_string();
    if message.is_empty() {
        return write_response(
            stream,
            "400 Bad Request",
            "text/plain; charset=utf-8",
            "Message is empty\n",
        );
    }

    app_event_tx.send(AppEvent::SubmitRemoteControlUserMessage { text: message });
    write_response(stream, "202 Accepted", "application/json", r#"{"ok":true}"#)
}

#[derive(Debug)]
struct HttpRequest {
    method: String,
    path: String,
    query: Option<String>,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
}

#[derive(Deserialize)]
struct MessagePost {
    message: String,
}

fn read_request(stream: &mut TcpStream) -> io::Result<HttpRequest> {
    let mut buffer = Vec::new();
    let mut scratch = [0u8; 2048];
    let header_end = loop {
        let bytes_read = stream.read(&mut scratch)?;
        if bytes_read == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "connection closed before HTTP headers completed",
            ));
        }
        buffer.extend_from_slice(&scratch[..bytes_read]);
        if buffer.len() > MAX_REQUEST_BYTES {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "remote-control request is too large",
            ));
        }
        if let Some(header_end) = find_header_end(&buffer) {
            break header_end;
        }
    };

    let headers_text = std::str::from_utf8(&buffer[..header_end]).map_err(|err| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("HTTP headers must be UTF-8: {err}"),
        )
    })?;
    let mut lines = headers_text.split("\r\n");
    let request_line = lines
        .next()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "missing HTTP request line"))?;
    let mut request_parts = request_line.split_whitespace();
    let method = request_parts.next().unwrap_or_default().to_string();
    let target = request_parts.next().unwrap_or_default().to_string();
    if method.is_empty() || target.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "invalid HTTP request line",
        ));
    }

    let headers = lines
        .filter_map(|line| {
            let (name, value) = line.split_once(':')?;
            Some((name.trim().to_ascii_lowercase(), value.trim().to_string()))
        })
        .collect::<Vec<_>>();
    let content_length = header_value(&headers, "content-length")
        .map(str::parse::<usize>)
        .transpose()
        .map_err(|err| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("invalid Content-Length: {err}"),
            )
        })?
        .unwrap_or(0);
    if content_length > MAX_REQUEST_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "remote-control request body is too large",
        ));
    }

    let body_start = header_end + 4;
    while buffer.len().saturating_sub(body_start) < content_length {
        let bytes_read = stream.read(&mut scratch)?;
        if bytes_read == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "connection closed before HTTP body completed",
            ));
        }
        buffer.extend_from_slice(&scratch[..bytes_read]);
        if buffer.len() > header_end + 4 + MAX_REQUEST_BYTES {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "remote-control request is too large",
            ));
        }
    }

    let (path, query) = target
        .split_once('?')
        .map_or((target.to_string(), None), |(path, query)| {
            (path.to_string(), Some(query.to_string()))
        });
    let body = buffer[body_start..body_start + content_length].to_vec();
    Ok(HttpRequest {
        method,
        path,
        query,
        headers,
        body,
    })
}

fn find_header_end(buffer: &[u8]) -> Option<usize> {
    buffer.windows(4).position(|window| window == b"\r\n\r\n")
}

fn is_authorized(request: &HttpRequest, token: &str) -> bool {
    let query_token_matches = request.query.as_deref().is_some_and(|query| {
        query.split('&').any(|pair| {
            pair.split_once('=')
                .is_some_and(|(name, value)| name == "token" && value == token)
        })
    });
    let bearer_token_matches = header_value(&request.headers, "authorization")
        .and_then(|value| value.strip_prefix("Bearer "))
        .is_some_and(|value| value == token);
    query_token_matches || bearer_token_matches
}

fn header_value<'a>(headers: &'a [(String, String)], name: &str) -> Option<&'a str> {
    headers
        .iter()
        .find_map(|(header_name, value)| (header_name == name).then_some(value.as_str()))
}

fn write_response(
    stream: &mut TcpStream,
    status: &str,
    content_type: &str,
    body: &str,
) -> io::Result<()> {
    let response = format!(
        "HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    stream.write_all(response.as_bytes())
}

fn generate_token() -> String {
    let mut bytes = [0u8; 32];
    rand::rng().fill_bytes(&mut bytes);
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes)
}

fn control_url(addr: SocketAddr, token: &str) -> String {
    let host = match addr.ip() {
        IpAddr::V4(ip) if ip.is_unspecified() => {
            local_lan_ip().unwrap_or(IpAddr::V4(Ipv4Addr::LOCALHOST))
        }
        IpAddr::V6(ip) if ip.is_unspecified() => {
            local_lan_ip().unwrap_or(IpAddr::V4(Ipv4Addr::LOCALHOST))
        }
        ip => ip,
    };
    format!("http://{}:{}?token={token}", format_host(host), addr.port())
}

fn format_host(host: IpAddr) -> String {
    match host {
        IpAddr::V4(ip) => ip.to_string(),
        IpAddr::V6(ip) => format!("[{ip}]"),
    }
}

fn local_lan_ip() -> Option<IpAddr> {
    let socket = UdpSocket::bind((Ipv4Addr::UNSPECIFIED, 0)).ok()?;
    socket.connect((Ipv4Addr::new(8, 8, 8, 8), 80)).ok()?;
    let ip = socket.local_addr().ok()?.ip();
    (!ip.is_unspecified()).then_some(ip)
}

fn remote_control_html(token: &str) -> String {
    format!(
        r#"<!doctype html>
<html>
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Codex Remote Control</title>
<style>
body {{ margin: 0; font: 16px -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; background: #0f1115; color: #f3f4f6; }}
main {{ max-width: 720px; margin: 0 auto; padding: 24px; }}
textarea {{ box-sizing: border-box; width: 100%; min-height: 180px; padding: 14px; border-radius: 8px; border: 1px solid #374151; background: #171a21; color: inherit; font: inherit; }}
button {{ margin-top: 12px; width: 100%; min-height: 48px; border: 0; border-radius: 8px; background: #10a37f; color: white; font: inherit; font-weight: 700; }}
.status {{ min-height: 24px; margin-top: 12px; color: #9ca3af; }}
</style>
</head>
<body>
<main>
<h1>Codex</h1>
<textarea id="message" autofocus placeholder="Send a prompt to the Codex session on your computer"></textarea>
<button id="send">Send</button>
<div class="status" id="status"></div>
</main>
<script>
const token = "{token}";
const message = document.getElementById("message");
const status = document.getElementById("status");
document.getElementById("send").addEventListener("click", async () => {{
  const text = message.value.trim();
  if (!text) return;
  status.textContent = "Sending...";
  const response = await fetch(`/api/message?token=${{encodeURIComponent(token)}}`, {{
    method: "POST",
    headers: {{ "Content-Type": "application/json" }},
    body: JSON.stringify({{ message: text }})
  }});
  if (!response.ok) {{
    status.textContent = `Failed: ${{response.status}}`;
    return;
  }}
  message.value = "";
  status.textContent = "Sent to Codex.";
}});
</script>
</body>
</html>"#
    )
}

#[cfg(test)]
mod tests {
    use std::io::Read;
    use std::io::Write;
    use std::net::SocketAddr;
    use std::net::TcpStream;
    use std::time::Duration;

    use crate::app_event::AppEvent;
    use crate::app_event_sender::AppEventSender;
    use tokio::sync::mpsc::unbounded_channel;

    use super::LocalRemoteControlOptions;
    use super::start_local_server;

    #[tokio::test]
    async fn post_message_submits_remote_control_event() {
        let (tx, mut rx) = unbounded_channel();
        let server = start_local_server(
            LocalRemoteControlOptions {
                bind_addr: "127.0.0.1:0".parse().expect("loopback addr"),
            },
            AppEventSender::new(tx),
        )
        .expect("server should start");
        let url = url::Url::parse(server.url()).expect("server URL should parse");
        let token = url
            .query_pairs()
            .find_map(|(name, value)| (name == "token").then(|| value.into_owned()))
            .expect("server URL should include token");
        let addr: SocketAddr = format!(
            "127.0.0.1:{}",
            url.port().expect("server URL should include port")
        )
        .parse()
        .expect("socket addr");
        let body = r#"{"message":"hello from phone"}"#;
        let response = send_request(
            addr,
            &format!(
                "POST /api/message?token={token} HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            ),
        );
        assert!(
            response.starts_with("HTTP/1.1 202 Accepted"),
            "unexpected response: {response}"
        );

        let event = tokio::time::timeout(Duration::from_secs(1), rx.recv())
            .await
            .expect("event should arrive")
            .expect("event channel should remain open");
        match event {
            AppEvent::SubmitRemoteControlUserMessage { text } => {
                assert_eq!(text, "hello from phone");
            }
            other => panic!("expected remote-control submit event, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn post_message_rejects_missing_token() {
        let (tx, _rx) = unbounded_channel();
        let server = start_local_server(
            LocalRemoteControlOptions {
                bind_addr: "127.0.0.1:0".parse().expect("loopback addr"),
            },
            AppEventSender::new(tx),
        )
        .expect("server should start");
        let url = url::Url::parse(server.url()).expect("server URL should parse");
        let addr: SocketAddr = format!(
            "127.0.0.1:{}",
            url.port().expect("server URL should include port")
        )
        .parse()
        .expect("socket addr");
        let body = r#"{"message":"hello from phone"}"#;
        let response = send_request(
            addr,
            &format!(
                "POST /api/message HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            ),
        );
        assert!(
            response.starts_with("HTTP/1.1 401 Unauthorized"),
            "unexpected response: {response}"
        );
    }

    fn send_request(addr: SocketAddr, request: &str) -> String {
        let mut stream = TcpStream::connect(addr).expect("connect");
        stream
            .set_read_timeout(Some(Duration::from_secs(2)))
            .expect("set read timeout");
        stream.write_all(request.as_bytes()).expect("write request");
        let mut response = String::new();
        stream.read_to_string(&mut response).expect("read response");
        response
    }
}
