use crate::error::Result;
use crate::message::Message;
use crate::shared::utc_from_timestamp;
use crate::sourcetransformpb::source_transform_client::SourceTransformClient;
use crate::sourcetransformpb::SourceTransformRequest;
use tonic::transport::Channel;

const DROP: &str = "U+005C__DROP__";
pub(crate) const TRANSFORMER_SOCKET: &str = "/var/run/numaflow/sourcetransform.sock";
pub(crate) const TRANSFORMER_SERVER_INFO_FILE: &str =
    "/var/run/numaflow/sourcetransformer-server-info";

/// TransformerClient is a client to interact with the transformer server.
#[derive(Clone)]
pub struct SourceTransformer {
    client: SourceTransformClient<Channel>,
}

impl SourceTransformer {
    pub(crate) async fn new(client: SourceTransformClient<Channel>) -> Result<Self> {
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
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use crate::shared::create_rpc_channel;
    use crate::sourcetransformpb::source_transform_client::SourceTransformClient;
    use crate::transformer::SourceTransformer;
    use numaflow::sourcetransform;
    use tempfile::TempDir;

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

        let mut client = SourceTransformer::new(SourceTransformClient::new(
            create_rpc_channel(sock_file).await?,
        ))
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
                .tags(vec![crate::transformer::DROP.to_string()]);
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

        let mut client = SourceTransformer::new(SourceTransformClient::new(
            create_rpc_channel(sock_file).await?,
        ))
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

        let resp = client.transform_fn(message).await?;
        assert!(resp.is_none());

        shutdown_tx
            .send(())
            .expect("failed to send shutdown signal");
        handle.await.expect("failed to join server task");
        Ok(())
    }
}
