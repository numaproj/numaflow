use crate::error::{Error, Result};
use crate::message::Message;
use crate::shared::{connect_with_uds, utc_from_timestamp};
use crate::transformer::proto::SourceTransformRequest;
use backoff::retry::Retry;
use backoff::strategy::fixed;
use tonic::transport::Channel;
use tonic::Request;

pub mod proto {
    tonic::include_proto!("sourcetransformer.v1");
}

const DROP: &str = "U+005C__DROP__";
const RECONNECT_INTERVAL: u64 = 1000;
const MAX_RECONNECT_ATTEMPTS: usize = 5;
const TRANSFORMER_SOCKET: &str = "/var/run/numaflow/sourcetransform.sock";
const TRANSFORMER_SERVER_INFO_FILE: &str = "/var/run/numaflow/sourcetransformer-server-info";

/// TransformerConfig is the configuration for the transformer server.
#[derive(Debug, Clone)]
pub struct TransformerConfig {
    pub socket_path: String,
    pub server_info_file: String,
    pub max_message_size: usize,
}

impl Default for TransformerConfig {
    fn default() -> Self {
        TransformerConfig {
            socket_path: TRANSFORMER_SOCKET.to_string(),
            server_info_file: TRANSFORMER_SERVER_INFO_FILE.to_string(),
            max_message_size: 64 * 1024 * 1024, // 64 MB
        }
    }
}

/// TransformerClient is a client to interact with the transformer server.
#[derive(Clone)]
pub struct TransformerClient {
    client: proto::source_transform_client::SourceTransformClient<Channel>,
}

impl TransformerClient {
    pub(crate) async fn connect(config: TransformerConfig) -> Result<Self> {
        let interval =
            fixed::Interval::from_millis(RECONNECT_INTERVAL).take(MAX_RECONNECT_ATTEMPTS);

        let channel = Retry::retry(
            interval,
            || async { connect_with_uds(config.socket_path.clone().into()).await },
            |_: &Error| true,
        )
        .await?;

        let client = proto::source_transform_client::SourceTransformClient::new(channel)
            .max_decoding_message_size(config.max_message_size)
            .max_encoding_message_size(config.max_message_size);
        Ok(Self { client })
    }

    pub(crate) async fn transform_fn(&mut self, message: Message) -> Result<Option<Vec<Message>>> {
        // fields which will not be changed
        let offset = message.offset.clone();
        let id = message.id.clone();
        let headers = message.headers.clone();

        // TODO: is this complex? the reason to do this is, tomorrow when we have the normal
        //   Pipeline CRD, we can require the Into trait.
        let response = self
            .client
            .source_transform_fn(<Message as Into<SourceTransformRequest>>::into(message))
            .await?
            .into_inner();

        let mut messages = Vec::new();
        for result in response.results {
            // if the message is tagged with DROP, we will not forward it.
            if result.tags.contains(&DROP.to_string()) {
                return Ok(None);
            }
            let message = Message {
                keys: result.keys,
                value: result.value,
                offset: offset.clone(),
                id: id.clone(),
                event_time: utc_from_timestamp(result.event_time),
                headers: headers.clone(),
            };
            messages.push(message);
        }

        Ok(Some(messages))
    }

    pub(crate) async fn is_ready(&mut self) -> bool {
        self.client.is_ready(Request::new(())).await.is_ok()
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use numaflow::sourcetransform;
    use tempfile::TempDir;

    use crate::transformer::{TransformerClient, TransformerConfig};

    struct NowCat;

    #[tonic::async_trait]
    impl sourcetransform::SourceTransformer for NowCat {
        async fn transform(
            &self,
            input: sourcetransform::SourceTransformRequest,
        ) -> Vec<sourcetransform::Message> {
            let message = sourcetransform::Message::new(input.value, chrono::offset::Utc::now())
                .keys(input.keys)
                .tags(vec![]);
            vec![message]
        }
    }

    #[tokio::test]
    async fn transformer_operations() -> Result<(), Box<dyn Error>> {
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let tmp_dir = TempDir::new()?;
        let sock_file = tmp_dir.path().join("sourcetransform.sock");
        let server_info_file = tmp_dir.path().join("sourcetransformer-server-info");

        let server_info = server_info_file.clone();
        let server_socket = sock_file.clone();
        let handle = tokio::spawn(async move {
            sourcetransform::Server::new(NowCat)
                .with_socket_file(server_socket)
                .with_server_info_file(server_info)
                .start_with_shutdown(shutdown_rx)
                .await
                .expect("server failed");
        });

        // wait for the server to start
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        let mut client = TransformerClient::connect(TransformerConfig {
            socket_path: sock_file.to_str().unwrap().to_string(),
            server_info_file: server_info_file.to_str().unwrap().to_string(),
            max_message_size: 4 * 1024 * 1024,
        })
        .await?;

        let message = crate::message::Message {
            keys: vec!["first".into()],
            value: "hello".into(),
            offset: crate::message::Offset {
                partition_id: 0,
                offset: "0".into(),
            },
            event_time: chrono::Utc::now(),
            id: "".to_string(),
            headers: Default::default(),
        };

        let resp = client.is_ready().await;
        assert!(resp);

        let resp = client.transform_fn(message).await?;
        assert!(resp.is_some());
        assert_eq!(resp.unwrap().len(), 1);

        shutdown_tx
            .send(())
            .expect("failed to send shutdown signal");
        handle.await.expect("failed to join server task");
        Ok(())
    }

    struct FilterCat;

    #[tonic::async_trait]
    impl sourcetransform::SourceTransformer for FilterCat {
        async fn transform(
            &self,
            input: sourcetransform::SourceTransformRequest,
        ) -> Vec<sourcetransform::Message> {
            let message = sourcetransform::Message::new(input.value, chrono::offset::Utc::now())
                .keys(input.keys)
                .tags(vec!["U+005C__DROP__".to_string()]);
            vec![message]
        }
    }

    #[tokio::test]
    async fn transformer_operations_with_drop() -> Result<(), Box<dyn Error>> {
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let tmp_dir = TempDir::new()?;
        let sock_file = tmp_dir.path().join("sourcetransform.sock");
        let server_info_file = tmp_dir.path().join("sourcetransformer-server-info");

        let server_info = server_info_file.clone();
        let server_socket = sock_file.clone();
        let handle = tokio::spawn(async move {
            sourcetransform::Server::new(FilterCat)
                .with_socket_file(server_socket)
                .with_server_info_file(server_info)
                .start_with_shutdown(shutdown_rx)
                .await
                .expect("server failed");
        });

        // wait for the server to start
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        let mut client = TransformerClient::connect(TransformerConfig {
            socket_path: sock_file.to_str().unwrap().to_string(),
            server_info_file: server_info_file.to_str().unwrap().to_string(),
            max_message_size: 4 * 1024 * 1024,
        })
        .await?;

        let message = crate::message::Message {
            keys: vec!["second".into()],
            value: "hello".into(),
            offset: crate::message::Offset {
                partition_id: 0,
                offset: "0".into(),
            },
            event_time: chrono::Utc::now(),
            id: "".to_string(),
            headers: Default::default(),
        };

        let resp = client.is_ready().await;
        assert!(resp);

        let resp = client.transform_fn(message).await?;
        assert!(resp.is_none());

        shutdown_tx
            .send(())
            .expect("failed to send shutdown signal");
        handle.await.expect("failed to join server task");
        Ok(())
    }
}
