use std::sync::Arc;

use bytes::Bytes;
use hyper_rustls::HttpsConnector;
use hyper_util::{
    client::legacy::{connect::HttpConnector, Client},
    rt::TokioExecutor,
};
use rustls::{self, pki_types::CertificateDer, ClientConfig};
use tonic::Status;

#[derive(Debug)]
struct SkipServerVerification;

// TLS server certificate verifier to accept self-signed certs when using rustls.
// The rustls does not provide a direct option to equivalent to Golang's `tls.Config.InsecureSkipVerify`.
impl SkipServerVerification {
    fn new() -> Arc<Self> {
        Arc::new(Self)
    }
}

impl rustls::client::danger::ServerCertVerifier for SkipServerVerification {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &rustls::pki_types::ServerName<'_>,
        _ocsp_response: &[u8],
        _now: rustls::pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        vec![
            rustls::SignatureScheme::RSA_PKCS1_SHA1,
            rustls::SignatureScheme::ECDSA_SHA1_Legacy,
            rustls::SignatureScheme::RSA_PKCS1_SHA256,
            rustls::SignatureScheme::ECDSA_NISTP256_SHA256,
            rustls::SignatureScheme::RSA_PKCS1_SHA384,
            rustls::SignatureScheme::ECDSA_NISTP384_SHA384,
            rustls::SignatureScheme::RSA_PKCS1_SHA512,
            rustls::SignatureScheme::ECDSA_NISTP521_SHA512,
            rustls::SignatureScheme::RSA_PSS_SHA256,
            rustls::SignatureScheme::RSA_PSS_SHA384,
            rustls::SignatureScheme::RSA_PSS_SHA512,
            rustls::SignatureScheme::ED25519,
            rustls::SignatureScheme::ED448,
        ]
    }
}

pub(crate) type HTTPSClient = Client<
    HttpsConnector<HttpConnector>,
    http_body_util::combinators::UnsyncBoxBody<Bytes, Status>,
>;

/// Creates an HTTPS client that can be used to connect to servers that use self-signed certificates
/// It can be used to create tonic gRPC client.
/// Eg:
/// ```
///     let client = new_https_client().unwrap();
///     let uri = hyper::Uri::builder()
///          .authority("localhost:4327")
///          .scheme("https")
///          .path_and_query("/")
///          .build()
///          .unwrap();
///     let mut client = ExampleServiceClient::with_origin(client, uri);
/// ```
/// Or to talk to an REST API server that uses self-signed certs:
/// ```
/// use bytes::Bytes;
/// use http_body_util::Full;
/// use hyper::{Method, Request};
/// use hyper_util::rt::TokioExecutor;
/// let req: Request<Full<Bytes>> = Request::builder()
///     .method(Method::GET)
///     .uri("https://localhost:4327/api/v1/metrics")
///     .body(Full::from(""))
///     .expect("request builder");
/// let resp = client.request(req).await.unwrap();
/// let mut body = resp.into_body();
/// use http_body_util::BodyExt;
/// use tokio::io::{self, AsyncWriteExt as _};
/// while let Some(next) = body.frame().await {
///     let frame = next?;
///     if let Some(chunk) = frame.data_ref() {
///         io::stdout().write_all(chunk).await?;
///     }
/// }
/// ```
/// For REST API, https://docs.rs/reqwest/0.12.9/reqwest/struct.ClientBuilder.html#method.danger_accept_invalid_certs would be the easier option.
pub fn new_https_client() -> Result<HTTPSClient, Box<dyn std::error::Error + Send + Sync + 'static>>
{
    let tls = ClientConfig::builder()
        .dangerous()
        .with_custom_certificate_verifier(SkipServerVerification::new())
        .with_no_client_auth();

    let mut http = HttpConnector::new();
    http.enforce_http(false);

    let connector = hyper_rustls::HttpsConnectorBuilder::new()
        .with_tls_config(tls)
        .https_only()
        .enable_http2()
        .wrap_connector(http);

    let client = hyper_util::client::legacy::Client::builder(TokioExecutor::new())
        .http2_only(true)
        .build(connector);
    Ok(client)
}
