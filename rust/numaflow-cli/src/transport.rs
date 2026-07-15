//! Transport: builds a tonic [`Channel`] over either a unix domain socket (UDS) or plain
//! TCP, mirroring how numa connects to UDF servers in k8s. The UDS connector is ported from
//! `numaflow-core/src/shared/grpc.rs`.

use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, anyhow};
use clap::Args;
use tokio::net::UnixStream;
use tonic::transport::{Channel, Endpoint, Uri};
use tower::service_fn;

/// Dummy URI used for UDS connections; the connector ignores it, tonic only requires a
/// syntactically valid authority.
const UDS_DUMMY_URI: &str = "http://[::1]:50051";

/// The two mutually exclusive ways to reach a UDF server.
#[derive(Args, Debug, Clone)]
pub struct ConnectOpts {
    /// Unix domain socket path to the UDF server (e.g. /var/run/numaflow/map.sock).
    #[arg(long, value_name = "path")]
    pub socket: Option<PathBuf>,

    /// TCP endpoint: `[host:]port`; host defaults to localhost. IPv6 needs brackets: [::1]:50051.
    #[arg(long, value_name = "[host:]port")]
    pub tcp: Option<String>,
}

/// A resolved target with a human-readable label for output.
#[derive(Debug, Clone)]
pub enum Target {
    Uds(PathBuf),
    Tcp(String),
}

impl Target {
    /// Resolve exactly one of `--socket` / `--tcp`. Neither/both is a usage error.
    pub fn resolve(opts: &ConnectOpts) -> anyhow::Result<Target> {
        match (&opts.socket, &opts.tcp) {
            (Some(_), Some(_)) => Err(anyhow!(
                "provide exactly one of --socket or --tcp, not both"
            )),
            (None, None) => Err(anyhow!("one of --socket or --tcp is required")),
            (Some(p), None) => Ok(Target::Uds(p.clone())),
            (None, Some(hp)) => Ok(Target::Tcp(normalize_tcp(hp)?)),
        }
    }

    /// Label shown in output, e.g. `uds:/var/run/…` or `tcp:localhost:50051`.
    pub fn label(&self) -> String {
        match self {
            Target::Uds(p) => format!("uds:{}", p.display()),
            Target::Tcp(hp) => format!("tcp:{hp}"),
        }
    }
}

/// Normalize a `--tcp` value into `host:port`, defaulting host to localhost. Accepts a bare
/// port (`50051`) or `host:port`.
fn normalize_tcp(input: &str) -> anyhow::Result<String> {
    let input = input.trim();
    if input.is_empty() {
        return Err(anyhow!("--tcp value is empty"));
    }
    // Bare port?
    if input.parse::<u16>().is_ok() {
        return Ok(format!("localhost:{input}"));
    }
    // host:port — validate the port part.
    let (host, port) = input
        .rsplit_once(':')
        .ok_or_else(|| anyhow!("--tcp must be a port or host:port, got {input:?}"))?;
    if host.is_empty() {
        return Err(anyhow!("--tcp host is empty in {input:?}"));
    }
    if host.contains(':') && !(host.starts_with('[') && host.ends_with(']')) {
        return Err(anyhow!("--tcp IPv6 hosts need brackets, e.g. [::1]:50051"));
    }
    port.parse::<u16>()
        .map_err(|_| anyhow!("--tcp port must be a number, got {port:?}"))?;
    Ok(input.to_string())
}

/// Connect once (no retry) to the target, applying max message-size limits at the channel
/// level is not possible in tonic; callers apply it per-client. Returns the raw channel.
pub async fn connect(target: &Target, timeout: Duration) -> anyhow::Result<Channel> {
    match target {
        Target::Uds(path) => connect_uds(path.clone()).await,
        Target::Tcp(hp) => connect_tcp(hp, timeout).await,
    }
}

async fn connect_uds(uds_path: PathBuf) -> anyhow::Result<Channel> {
    let channel = Endpoint::try_from(UDS_DUMMY_URI)
        .context("failed to build UDS endpoint")?
        .connect_with_connector(service_fn(move |_: Uri| {
            let uds_socket = uds_path.clone();
            async move {
                Ok::<_, std::io::Error>(hyper_util::rt::TokioIo::new(
                    UnixStream::connect(uds_socket).await?,
                ))
            }
        }))
        .await
        .with_context(|| "failed to connect to UDS socket")?;
    Ok(channel)
}

async fn connect_tcp(host_port: &str, timeout: Duration) -> anyhow::Result<Channel> {
    let uri = format!("http://{host_port}");
    let channel = Endpoint::try_from(uri)
        .context("failed to build TCP endpoint")?
        .connect_timeout(timeout)
        .connect()
        .await
        .with_context(|| format!("failed to connect to tcp://{host_port}"))?;
    Ok(channel)
}

/// Retry [`connect`] once per second until it succeeds or `timeout` elapses. The nap between
/// attempts is capped at the time left so a failure just before the deadline doesn't overshoot
/// `--timeout` by a whole retry interval.
pub async fn connect_with_retry(target: &Target, timeout: Duration) -> anyhow::Result<Channel> {
    const RETRY_INTERVAL: Duration = Duration::from_secs(1);
    let deadline = tokio::time::Instant::now() + timeout;
    let mut last_err: Option<anyhow::Error> = None;
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        match tokio::time::timeout(remaining, connect(target, remaining)).await {
            Ok(Ok(ch)) => return Ok(ch),
            Ok(Err(e)) => last_err = Some(e),
            // The attempt consumed the whole window; keep an earlier, more specific error.
            Err(_) => {
                if last_err.is_none() {
                    last_err = Some(anyhow!("connect attempt timed out"));
                }
            }
        }
        let left = deadline.saturating_duration_since(tokio::time::Instant::now());
        if left.is_zero() {
            let e = last_err.unwrap_or_else(|| anyhow!("connect attempt timed out"));
            return Err(e).with_context(|| {
                format!("could not connect to {} within {timeout:?}", target.label())
            });
        }
        tokio::time::sleep(RETRY_INTERVAL.min(left)).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalize_bare_port() {
        assert_eq!(normalize_tcp("50051").unwrap(), "localhost:50051");
    }

    #[test]
    fn normalize_host_port() {
        assert_eq!(normalize_tcp("example:1234").unwrap(), "example:1234");
        assert_eq!(normalize_tcp("localhost:50051").unwrap(), "localhost:50051");
    }

    #[test]
    fn normalize_bracketed_ipv6() {
        assert_eq!(normalize_tcp("[::1]:50051").unwrap(), "[::1]:50051");
    }

    #[test]
    fn normalize_rejects_bad() {
        assert!(normalize_tcp("").is_err());
        assert!(normalize_tcp("host:notaport").is_err());
        assert!(normalize_tcp(":50051").is_err());
        let err = normalize_tcp("::1:50051").unwrap_err();
        assert!(format!("{err:#}").contains("IPv6"));
    }

    #[test]
    fn resolve_requires_exactly_one() {
        let both = ConnectOpts {
            socket: Some("/x".into()),
            tcp: Some("50051".into()),
        };
        assert!(Target::resolve(&both).is_err());
        let neither = ConnectOpts {
            socket: None,
            tcp: None,
        };
        assert!(Target::resolve(&neither).is_err());
        let uds = ConnectOpts {
            socket: Some("/x".into()),
            tcp: None,
        };
        assert!(matches!(Target::resolve(&uds).unwrap(), Target::Uds(_)));
    }
}
