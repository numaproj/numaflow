use crate::error;
use crate::message::Message;
use crate::sink::{ResponseFromSink, ResponseStatusFromSink, Sink};
use numaflow_sqs::sink::{SqsSink, SqsSinkMessage};
use crate::error::Error;

impl TryFrom<Message> for SqsSinkMessage {
    type Error = error::Error;

    fn try_from(msg: Message) -> crate::Result<Self> {
        let id = msg.id.to_string();
        Ok(SqsSinkMessage {
            id,
            message_body: msg.value,
        })
    }
}

impl From<numaflow_sqs::SqsSinkError> for Error {
    fn from(value: numaflow_sqs::SqsSinkError) -> Self {
        match value {
            numaflow_sqs::SqsSinkError::Error(numaflow_sqs::Error::Sqs(e)) => Error::Source(e.to_string()),
            numaflow_sqs::SqsSinkError::Error(numaflow_sqs::Error::ActorTaskTerminated(_))=> {
                Error::ActorPatternRecv(value.to_string())
            },
            numaflow_sqs::SqsSinkError::Error(numaflow_sqs::Error::InvalidConfig(e)) => Error::Source(e),
            numaflow_sqs::SqsSinkError::Error(numaflow_sqs::Error::Other(e)) => Error::Source(e),
        }
    }
}


impl Sink for SqsSink {
    async fn sink(&mut self, messages: Vec<Message>) -> error::Result<Vec<ResponseFromSink>> {
        
        let mut result = Vec::with_capacity(messages.len());
        
        let sqs_messages: Vec<SqsSinkMessage> = messages
            .iter()
            .map(|msg| SqsSinkMessage::try_from(msg.clone()))
            .collect::<Result<Vec<_>, _>>()?;
        
        let sqs_sink_result = self.sink_messages(sqs_messages).await;
        
        if sqs_sink_result.is_err() {
            // TODO: fix this error handling
            return Err(Error::from(sqs_sink_result.err().unwrap()));
        }
        for sqs_response in sqs_sink_result?.iter() {
            match &sqs_response.status {
                Ok(_) => {
                    result.push(ResponseFromSink {
                        id: sqs_response.id.clone(),
                        status: ResponseStatusFromSink::Success,
                        serve_response: None,
                    });
                }
                Err(err) => {
                    result.push(ResponseFromSink {
                        id: sqs_response.id.clone(),
                        status: ResponseStatusFromSink::Failed(err.to_string()),
                        serve_response: None,
                    });
                }
            }
        }
        Ok(result)
    }
}