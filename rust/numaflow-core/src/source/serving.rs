use std::sync::Arc;

pub(crate) use serving::ServingSource;

use crate::config::get_vertex_replica;
use crate::message::{MessageID, StringOffset};
use crate::Error;
use crate::Result;

use super::{get_vertex_name, Message, Offset};

impl TryFrom<serving::Message> for Message {
    type Error = Error;

    fn try_from(message: serving::Message) -> Result<Self> {
        let offset = Offset::String(StringOffset::new(message.id.clone(), *get_vertex_replica()));

        Ok(Message {
            // we do not support keys from HTTP client
            keys: Arc::from(vec![]),
            tags: None,
            value: message.value,
            offset: Some(offset.clone()),
            event_time: Default::default(),
            id: MessageID {
                vertex_name: get_vertex_name().to_string().into(),
                offset: offset.to_string().into(),
                index: 0,
            },
            headers: message.headers,
        })
    }
}

impl From<serving::Error> for Error {
    fn from(value: serving::Error) -> Self {
        Error::Source(value.to_string())
    }
}

impl super::SourceReader for ServingSource {
    fn name(&self) -> &'static str {
        "serving"
    }

    async fn read(&mut self) -> Result<Vec<Message>> {
        self.read_messages()
            .await?
            .into_iter()
            .map(|msg| msg.try_into())
            .collect()
    }

    fn partitions(&self) -> Vec<u16> {
        vec![*get_vertex_replica()]
    }
}

impl super::SourceAcker for ServingSource {
    /// HTTP response is sent only once we have confirmation that the message has been written to the ISB.
    // TODO: Current implementation only works for `/v1/process/async` endpoint.
    //       For `/v1/process/{sync,sync_serve}` endpoints: https://github.com/numaproj/numaflow/issues/2308
    async fn ack(&mut self, offsets: Vec<Offset>) -> Result<()> {
        let mut serving_offsets = vec![];
        for offset in offsets {
            let Offset::String(offset) = offset else {
                return Err(Error::Source(format!(
                    "Expected string offset for Serving source. Got {offset:?}"
                )));
            };
            serving_offsets.push(offset.to_string());
        }
        self.ack_messages(serving_offsets).await?;
        Ok(())
    }
}

impl super::LagReader for ServingSource {
    async fn pending(&mut self) -> Result<Option<usize>> {
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        message::{Message, MessageID, Offset, StringOffset},
        source::{SourceAcker, SourceReader},
    };
    use std::{collections::HashMap, sync::Arc, time::Duration};

    use bytes::Bytes;
    use serving::{ServingSource, Settings};

    use super::get_vertex_replica;

    type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

    #[test]
    fn test_message_conversion() -> Result<()> {
        const MSG_ID: &str = "b149ad7a-5690-4f0a";

        let mut headers = HashMap::new();
        headers.insert("header-key".to_owned(), "header-value".to_owned());

        let serving_message = serving::Message {
            value: Bytes::from_static(b"test"),
            id: MSG_ID.into(),
            headers: headers.clone(),
        };
        let message: Message = serving_message.try_into()?;
        assert_eq!(message.value, Bytes::from_static(b"test"));
        assert_eq!(
            message.offset,
            Some(Offset::String(StringOffset::new(MSG_ID.into(), 0)))
        );
        assert_eq!(
            message.id,
            MessageID {
                vertex_name: Bytes::new(),
                offset: format!("{MSG_ID}-0").into(),
                index: 0
            }
        );

        assert_eq!(message.headers, headers);

        Ok(())
    }

    #[test]
    fn test_error_conversion() {
        use crate::error::Error;
        let error: Error = serving::Error::ParseConfig("Invalid config".to_owned()).into();
        if let Error::Source(val) = error {
            assert_eq!(val, "ParseConfig Error - Invalid config".to_owned());
        } else {
            panic!("Expected Error::Source() variant");
        }
    }

    #[tokio::test]
    async fn test_serving_source_reader_acker() -> Result<()> {
        let settings = Settings {
            app_listen_port: 2000,
            ..Default::default()
        };
        let settings = Arc::new(settings);
        let mut serving_source = ServingSource::new(
            Arc::clone(&settings),
            10,
            Duration::from_millis(1),
            *get_vertex_replica(),
        )
        .await?;

        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(2))
            .danger_accept_invalid_certs(true)
            .build()
            .unwrap();

        // Wait for the server
        for _ in 0..10 {
            let resp = client
                .get(format!(
                    "https://localhost:{}/livez",
                    settings.app_listen_port
                ))
                .send()
                .await;
            if resp.is_ok() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        let task_handle = tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_millis(10)).await;
                let mut messages = serving_source.read().await.unwrap();
                if messages.is_empty() {
                    // Server has not received any requests yet
                    continue;
                }
                assert_eq!(messages.len(), 1);
                let msg = messages.remove(0);
                serving_source.ack(vec![msg.offset.unwrap()]).await.unwrap();
                break;
            }
        });

        let resp = client
            .post(format!(
                "https://localhost:{}/v1/process/async",
                settings.app_listen_port
            ))
            .json("test-payload")
            .send()
            .await?;

        assert!(resp.status().is_success());
        assert!(task_handle.await.is_ok());
        Ok(())
    }
}
