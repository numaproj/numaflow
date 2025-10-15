//! HTTP source for Numaflow.

use crate::config::{get_vertex_name, get_vertex_replica};
use crate::error::Result;
use crate::message::{Message, MessageID, Offset, StringOffset};
use crate::metadata::Metadata;
use crate::source;
use crate::source::{SourceAcker, SourceReader};
use numaflow_http::HttpMessage;
use std::sync::Arc;
use tracing::error;

impl From<numaflow_http::Error> for crate::error::Error {
    fn from(value: numaflow_http::Error) -> Self {
        use numaflow_http::Error;
        match value {
            Error::ChannelFull() => Self::Source(format!("HTTP source: {value:?}")),
            Error::Server(_) | Error::ChannelSend(_) | Error::ChannelRecv(_) => {
                Self::Source(format!("HTTP source: {value:?}"))
            }
        }
    }
}

impl From<HttpMessage> for Message {
    fn from(value: HttpMessage) -> Self {
        Message {
            typ: Default::default(),
            keys: Arc::from(vec![]),
            tags: None,
            value: value.body,
            offset: Offset::String(StringOffset::new(value.id.clone(), *get_vertex_replica())),
            event_time: value.event_time,
            watermark: None,
            id: MessageID {
                vertex_name: get_vertex_name().to_string().into(),
                offset: value.id.into(),
                index: 0,
            },
            headers: Arc::new(value.headers),
            // Set default metadata so that metadata is always present.
            metadata: Some(Arc::new(Metadata::default())),
            is_late: false,
            ack_handle: None,
        }
    }
}

#[derive(Clone)]
pub(crate) struct CoreHttpSource {
    batch_size: usize,
    http_source: numaflow_http::HttpSourceHandle,
}

impl CoreHttpSource {
    pub(crate) fn new(batch_size: usize, http_source: numaflow_http::HttpSourceHandle) -> Self {
        Self {
            batch_size,
            http_source,
        }
    }
}

impl SourceReader for CoreHttpSource {
    fn name(&self) -> &'static str {
        "HTTP"
    }

    async fn read(&mut self) -> Option<Result<Vec<Message>>> {
        match self.http_source.read(self.batch_size).await {
            Some(Ok(msgs)) => Some(Ok(msgs.into_iter().map(|m| m.into()).collect())),
            Some(Err(e)) => Some(Err(e.into())),
            None => None,
        }
    }

    async fn partitions(&mut self) -> Result<Vec<u16>> {
        Ok(vec![*get_vertex_replica()])
    }
}

impl SourceAcker for CoreHttpSource {
    async fn ack(&mut self, offsets: Vec<Offset>) -> Result<()> {
        // extract the ids from the offsets, id was used to create the offset
        let ids = offsets
            .into_iter()
            .filter_map(|o| match o {
                Offset::String(s) => Some(s.offset),
                Offset::Int(_) => {
                    // this should not happen since we create the offsets
                    error!("HTTP offsets should be string");
                    None
                }
            })
            .collect::<Vec<_>>();

        self.http_source
            .ack(
                ids.into_iter()
                    .map(|o| String::from_utf8(o.to_vec()).expect("UTF-8 error"))
                    .collect(),
            )
            .await
            .map_err(|e| e.into())
    }

    async fn nack(&mut self, offsets: Vec<Offset>) -> Result<()> {
        // extract the ids from the offsets, id was used to create the offset
        let ids = offsets
            .into_iter()
            .filter_map(|o| match o {
                Offset::String(s) => Some(s.offset),
                Offset::Int(_) => {
                    // this should not happen since we create the offsets
                    error!("HTTP offsets should be string");
                    None
                }
            })
            .collect::<Vec<_>>();

        self.http_source
            .nack(
                ids.into_iter()
                    .map(|o| String::from_utf8(o.to_vec()).expect("UTF-8 error"))
                    .collect(),
            )
            .await
            .map_err(|e| e.into())
    }
}

impl source::LagReader for CoreHttpSource {
    async fn pending(&mut self) -> Result<Option<usize>> {
        Ok(self.http_source.pending().await)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::source::{LagReader, SourceAcker, SourceReader};
    use chrono::Utc;
    use hyper::{Method, Request};
    use hyper_util::client::legacy::Client;
    use hyper_util::rt::TokioExecutor;
    use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
    use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
    use rustls::{DigitallySignedStruct, SignatureScheme};
    use std::net::TcpListener;
    use std::time::Duration;
    use tokio::task::JoinSet;
    use tokio::time::sleep;
    use tokio_util::sync::CancellationToken;

    // Custom certificate verifier that accepts any certificate (for testing)
    #[derive(Debug)]
    struct AcceptAnyCertVerifier;

    impl ServerCertVerifier for AcceptAnyCertVerifier {
        fn verify_server_cert(
            &self,
            _end_entity: &CertificateDer<'_>,
            _intermediates: &[CertificateDer<'_>],
            _server_name: &ServerName<'_>,
            _ocsp_response: &[u8],
            _now: UnixTime,
        ) -> std::result::Result<ServerCertVerified, rustls::Error> {
            Ok(ServerCertVerified::assertion())
        }

        fn verify_tls12_signature(
            &self,
            _message: &[u8],
            _cert: &CertificateDer<'_>,
            _dss: &DigitallySignedStruct,
        ) -> std::result::Result<HandshakeSignatureValid, rustls::Error> {
            Ok(HandshakeSignatureValid::assertion())
        }

        fn verify_tls13_signature(
            &self,
            _message: &[u8],
            _cert: &CertificateDer<'_>,
            _dss: &DigitallySignedStruct,
        ) -> std::result::Result<HandshakeSignatureValid, rustls::Error> {
            Ok(HandshakeSignatureValid::assertion())
        }

        fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
            vec![
                SignatureScheme::RSA_PKCS1_SHA1,
                SignatureScheme::ECDSA_SHA1_Legacy,
                SignatureScheme::RSA_PKCS1_SHA256,
                SignatureScheme::ECDSA_NISTP256_SHA256,
                SignatureScheme::RSA_PKCS1_SHA384,
                SignatureScheme::ECDSA_NISTP384_SHA384,
                SignatureScheme::RSA_PKCS1_SHA512,
                SignatureScheme::ECDSA_NISTP521_SHA512,
                SignatureScheme::RSA_PSS_SHA256,
                SignatureScheme::RSA_PSS_SHA384,
                SignatureScheme::RSA_PSS_SHA512,
                SignatureScheme::ED25519,
                SignatureScheme::ED448,
            ]
        }
    }

    #[tokio::test]
    async fn test_core_http_source() {
        // Setup the CryptoProvider for rustls
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

        // Bind to a random available port
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener); // Release the listener so HttpSource can bind to it

        // Create HttpSource config
        let http_source_config = numaflow_http::HttpSourceConfigBuilder::new("test")
            .addr(addr)
            .buffer_size(10)
            .timeout(Duration::from_millis(100))
            .build();

        // Create CancellationToken and HttpSourceHandle
        let cln_token = CancellationToken::new();
        let http_source =
            numaflow_http::HttpSourceHandle::new(http_source_config, cln_token.clone()).await;

        // Create CoreHttpSource with batch size 5
        let batch_size = 5;
        let mut core_http_source = CoreHttpSource::new(batch_size, http_source);

        // Wait a bit for the server to start
        sleep(Duration::from_millis(100)).await;

        // Configure TLS client to accept any certificate (for testing with self-signed certs)
        let tls_config = rustls::ClientConfig::builder()
            .dangerous()
            .with_custom_certificate_verifier(std::sync::Arc::new(AcceptAnyCertVerifier))
            .with_no_client_auth();

        let https_connector = hyper_rustls::HttpsConnectorBuilder::new()
            .with_tls_config(tls_config)
            .https_or_http()
            .enable_http1()
            .build();
        let client = Client::builder(TokioExecutor::new()).build(https_connector);

        let request_handle = tokio::spawn(async move {
            // concurrently invoke each request in a separate tokio task and join at the end
            let mut join_set = JoinSet::new();
            // Send test requests
            for i in 0..7 {
                let client = client.clone();
                join_set.spawn(async move {
                    let request = Request::builder()
                        .method(Method::POST)
                        .uri(format!("https://{}/vertices/test", addr))
                        .header("Content-Type", "application/json")
                        .header("X-Numaflow-Id", format!("test-id-{}", i))
                        .body(format!(r#"{{"message": "test{}"}}"#, i))
                        .unwrap();

                    let response = client.request(request).await.unwrap();
                    assert_eq!(response.status(), 200);
                });
            }

            while let Some(task) = join_set.join_next().await {
                task.unwrap();
            }
        });

        // wait for 1s to make sure all the requests are sent
        let start = tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let pending = core_http_source.pending().await.unwrap();
                if pending == Some(7) {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await;
        assert!(start.is_ok(), "Timeout occurred before pending became 7");

        // Test partitions
        let partitions = core_http_source.partitions().await.unwrap();
        assert_eq!(partitions.len(), 1, "Should have 1 partition");

        // Test read method - should get batch_size (5) messages
        let messages = core_http_source.read().await.unwrap().unwrap();
        assert_eq!(messages.len(), 5, "Should read 5 messages (batch size)");

        let current_time = Utc::now();

        // Verify message contents
        for (i, message) in messages.iter().enumerate() {
            assert!(message.headers.contains_key("X-Numaflow-Id"));
            assert!(message.headers.contains_key("content-type"));

            // Ensure current time is set when x-numaflow-event-time header is not specified
            assert!(
                current_time
                    .signed_duration_since(message.event_time)
                    .num_seconds()
                    .abs()
                    < 1
            );

            let body_str = String::from_utf8(message.value.to_vec()).unwrap();
            assert!(body_str.contains(&format!("test{}", i)));
        }

        // Ack the messages
        let offsets = messages.iter().map(|m| m.offset.clone()).collect();
        core_http_source.ack(offsets).await.unwrap();

        // Test pending count after reading and acking
        let pending = core_http_source.pending().await.unwrap();
        assert_eq!(
            pending,
            Some(2),
            "Should have 2 pending messages after reading 5"
        );

        // Read remaining messages
        let messages = core_http_source.read().await.unwrap().unwrap();
        assert_eq!(messages.len(), 2, "Should read remaining 2 messages");

        // Ack the remaining messages
        let offsets = messages.iter().map(|m| m.offset.clone()).collect();
        core_http_source.ack(offsets).await.unwrap();

        // Verify no more pending messages
        let pending = core_http_source.pending().await.unwrap();
        assert_eq!(pending, Some(0), "Should have 0 pending messages");

        request_handle.await.unwrap();
    }
}
