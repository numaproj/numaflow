use std::sync::Arc;

pub(crate) use serving::ServingSource;

use crate::message::{MessageID, StringOffset};
use crate::Error;
use crate::Result;

use super::{get_vertex_name, Message, Offset};

impl TryFrom<serving::Message> for Message {
    type Error = Error;

    fn try_from(message: serving::Message) -> Result<Self> {
        let offset = Offset::String(StringOffset::new(message.id.clone(), 0));

        Ok(Message {
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
        vec![]
    }
}

impl super::SourceAcker for ServingSource {
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
    use crate::message::{Message, MessageID, Offset, StringOffset};
    use std::collections::HashMap;

    use bytes::Bytes;

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
}
