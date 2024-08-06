use std::path::PathBuf;

use tonic::transport::Channel;
use tonic::Request;

use crate::error::Result;
use crate::message::Message;
use crate::shared::{connect_with_uds, prost_timestamp_from_utc, utc_from_timestamp};
use crate::transformer::proto::SourceTransformRequest;

pub mod proto {
    tonic::include_proto!("sourcetransformer.v1");
}

/// TransformerClient is a client to interact with the transformer server.
#[derive(Clone)]
pub struct TransformerClient {
    client: proto::source_transform_client::SourceTransformClient<Channel>,
}

impl TransformerClient {
    pub(crate) async fn connect(uds_path: PathBuf) -> Result<Self> {
        let channel = connect_with_uds(uds_path).await?;
        let client = proto::source_transform_client::SourceTransformClient::new(channel);
        Ok(Self { client })
    }

    pub(crate) async fn transform_fn(&mut self, message: Message) -> Result<Vec<Message>> {
        // fields which will not be changed
        let offset = message.offset.clone();
        let headers = message.headers.clone();

        let request = SourceTransformRequest {
            keys: message.keys,
            value: message.value,
            event_time: prost_timestamp_from_utc(message.event_time),
            watermark: None,
            headers: message.headers,
        };

        let response = self.client.source_transform_fn(request).await?.into_inner();

        let mut messages = Vec::new();
        for result in response.results {
            let message = Message {
                keys: result.keys,
                value: result.value,
                offset: offset.clone(),
                event_time: utc_from_timestamp(result.event_time),
                headers: headers.clone(),
            };
            messages.push(message);
        }

        Ok(messages)
    }

    pub(crate) async fn is_ready(&mut self) -> Result<proto::ReadyResponse> {
        let request = Request::new(());
        let response = self.client.is_ready(request).await?.into_inner();
        Ok(response)
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use numaflow::sourcetransform;
    use tempfile::TempDir;

    use crate::transformer::TransformerClient;

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

        let server_socket = sock_file.clone();
        let handle = tokio::spawn(async move {
            let server_info_file = tmp_dir.path().join("sourcetransformer-server-info");
            sourcetransform::Server::new(NowCat)
                .with_socket_file(server_socket)
                .with_server_info_file(server_info_file)
                .start_with_shutdown(shutdown_rx)
                .await
                .expect("server failed");
        });

        // wait for the server to start
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        let mut client = TransformerClient::connect(sock_file).await?;

        let message = crate::message::Message {
            keys: vec!["first".into(), "second".into()],
            value: "hello".into(),
            offset: crate::message::Offset {
                partition_id: 0,
                offset: "0".into(),
            },
            event_time: chrono::Utc::now(),
            headers: Default::default(),
        };

        let resp = client.is_ready().await?;
        assert_eq!(resp.ready, true);

        let resp = client.transform_fn(message).await?;
        assert_eq!(resp.len(), 1);

        shutdown_tx
            .send(())
            .expect("failed to send shutdown signal");
        handle.await.expect("failed to join server task");
        Ok(())
    }
}
