use numaflow_sqs::sink::{SqsSink, SqsSinkMessage};

use crate::error;
use crate::error::Error;
use crate::message::Message;
use crate::sinker::sink::{ResponseFromSink, ResponseStatusFromSink, Sink};

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
            numaflow_sqs::SqsSinkError::Error(numaflow_sqs::Error::Sqs(e)) => {
                Error::Sink(e.to_string())
            }
            numaflow_sqs::SqsSinkError::Error(numaflow_sqs::Error::Sts(e)) => {
                Error::Sink(e.to_string())
            }
            numaflow_sqs::SqsSinkError::Error(numaflow_sqs::Error::ActorTaskTerminated(_)) => {
                Error::ActorPatternRecv(value.to_string())
            }
            numaflow_sqs::SqsSinkError::Error(numaflow_sqs::Error::InvalidConfig(e)) => {
                Error::Sink(e)
            }
            numaflow_sqs::SqsSinkError::Error(numaflow_sqs::Error::Other(e)) => Error::Sink(e),
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
            return Err(Error::from(sqs_sink_result.err().unwrap()));
        }
        for sqs_response in sqs_sink_result?.iter() {
            match &sqs_response.status {
                Ok(_) => {
                    result.push(ResponseFromSink {
                        id: sqs_response.id.clone(),
                        status: ResponseStatusFromSink::Success,
                    });
                }
                Err(err) => {
                    result.push(ResponseFromSink {
                        id: sqs_response.id.clone(),
                        status: ResponseStatusFromSink::Failed(err.to_string()),
                    });
                }
            }
        }
        Ok(result)
    }
}

#[cfg(feature = "sqs-tests")]
#[cfg(test)]
pub mod tests {
    use std::collections::HashSet;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use crate::monovertex::forwarder::Forwarder;
    use crate::shared::grpc::create_rpc_channel;
    use crate::sinker::sink::SinkWriter;
    use crate::source::user_defined::new_source;
    use crate::source::{Source, SourceType};
    use crate::tracker::Tracker;
    use aws_sdk_sqs::Config;
    use aws_sdk_sqs::config::BehaviorVersion;
    use aws_sdk_sqs::types::BatchResultErrorEntry;
    use aws_smithy_mocks::{MockResponseInterceptor, Rule, RuleMode, mock};
    use chrono::Utc;
    use numaflow::shared::ServerExtras;
    use numaflow::source;
    use numaflow::source::{Message, Offset, SourceReadRequest};
    use numaflow_pb::clients::source::source_client::SourceClient;
    use numaflow_sqs::sink::{SQS_DEFAULT_REGION, SqsSinkBuilder, SqsSinkConfig};
    use tempfile::TempDir;
    use tokio::sync::mpsc::Sender;
    use tokio::sync::oneshot;
    use tokio::task::JoinHandle;
    use tokio_util::sync::CancellationToken;

    struct SimpleSource {
        num: usize,
        sent_count: AtomicUsize,
        yet_to_ack: std::sync::RwLock<HashSet<String>>,
    }

    impl SimpleSource {
        fn new(num: usize) -> Self {
            Self {
                num,
                sent_count: AtomicUsize::new(0),
                yet_to_ack: std::sync::RwLock::new(HashSet::new()),
            }
        }
    }

    #[tonic::async_trait]
    impl source::Sourcer for SimpleSource {
        async fn read(&self, request: SourceReadRequest, transmitter: Sender<Message>) {
            let event_time = Utc::now();
            let mut message_offsets = Vec::with_capacity(request.count);

            for i in 0..request.count {
                if self.sent_count.load(Ordering::SeqCst) >= self.num {
                    return;
                }

                // create a unique predictable offset for each message using a fixed prefix
                // and the message index
                let prefix = "source-testsinke2e-";

                let offset = format!("{}-{}", prefix, i);
                transmitter
                    .send(Message {
                        value: b"hello".to_vec(),
                        event_time,
                        offset: Offset {
                            offset: offset.clone().into_bytes(),
                            partition_id: 0,
                        },
                        keys: vec![],
                        headers: Default::default(),
                        user_metadata: None,
                    })
                    .await
                    .unwrap();
                message_offsets.push(offset);
                self.sent_count.fetch_add(1, Ordering::SeqCst);
            }
            self.yet_to_ack.write().unwrap().extend(message_offsets);
        }

        async fn ack(&self, offsets: Vec<Offset>) {
            for offset in offsets {
                self.yet_to_ack
                    .write()
                    .unwrap()
                    .remove(&String::from_utf8(offset.offset).unwrap());
            }
        }

        async fn nack(&self, _offsets: Vec<Offset>) {}

        async fn pending(&self) -> Option<usize> {
            Some(
                self.num - self.sent_count.load(Ordering::SeqCst)
                    + self.yet_to_ack.read().unwrap().len(),
            )
        }

        async fn partitions(&self) -> Option<Vec<i32>> {
            Some(vec![1, 2])
        }
    }

    #[tokio::test]
    async fn test_sqs_sink_e2e() {
        let tracker = Tracker::new(None, CancellationToken::new());
        let cln_token = CancellationToken::new();

        let (source, src_handle, src_shutdown_tx) =
            get_simple_source(tracker.clone(), cln_token.clone()).await;
        // let source = get_sqs_source().await;
        let sink_writer = get_sqs_sink().await;
        // create the forwarder with the source, transformer, and writer
        let forwarder = Forwarder::new(source.clone(), None, sink_writer);

        let cancel_token = cln_token.clone();
        let forwarder_handle: JoinHandle<crate::error::Result<()>> = tokio::spawn(async move {
            forwarder.start(cancel_token).await?;
            Ok(())
        });

        // wait for one sec to check if the pending becomes zero, because all the messages
        // should be read and acked; if it doesn't, then fail the test
        let tokio_result = tokio::time::timeout(Duration::from_secs(1), async move {
            loop {
                let pending = source.pending().await.unwrap();
                if pending == Some(0) {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await;

        assert!(
            tokio_result.is_ok(),
            "Timeout occurred before pending became zero"
        );
        cln_token.cancel();
        forwarder_handle.await.unwrap().unwrap();
        src_shutdown_tx.send(()).unwrap();
        src_handle.await.unwrap();
    }

    async fn get_simple_source(
        tracker: Tracker,
        cln_token: CancellationToken,
    ) -> (
        Source<crate::typ::WithoutRateLimiter>,
        JoinHandle<()>,
        oneshot::Sender<()>,
    ) {
        let (src_shutdown_tx, src_shutdown_rx) = oneshot::channel();
        let tmp_dir = TempDir::new().unwrap();
        let sock_file = tmp_dir.path().join("source.sock");
        let server_info_file = tmp_dir.path().join("source-server-info");

        let server_info = server_info_file.clone();
        let server_socket = sock_file.clone();
        let source_handle = tokio::spawn(async move {
            // a simple source which generates total of 100 messages
            source::Server::new(SimpleSource::new(5))
                .with_socket_file(server_socket)
                .with_server_info_file(server_info)
                .start_with_shutdown(src_shutdown_rx)
                .await
                .unwrap()
        });

        // wait for the server to start
        // TODO: flaky
        tokio::time::sleep(Duration::from_millis(50)).await;

        let client = SourceClient::new(create_rpc_channel(sock_file).await.unwrap());

        let (src_read, src_ack, lag_reader) =
            new_source(client, 5, Duration::from_millis(100), cln_token, true)
                .await
                .map_err(|e| panic!("failed to create source reader: {:?}", e))
                .unwrap();
        (
            Source::new(
                5,
                SourceType::UserDefinedSource(Box::new(src_read), Box::new(src_ack), lag_reader),
                tracker.clone(),
                true,
                None,
                None,
                None,
            )
            .await,
            source_handle,
            src_shutdown_tx,
        )
    }

    async fn get_sqs_sink() -> SinkWriter {
        let queue_url_output = get_queue_url_output();
        let sqs_operation_mocks = MockResponseInterceptor::new()
            .rule_mode(RuleMode::MatchAny)
            .with_rule(&queue_url_output)
            .with_rule(&get_send_message_output(5));

        let sqs_client =
            aws_sdk_sqs::Client::from_conf(get_test_config_with_interceptor(sqs_operation_mocks));

        let sqs_sink = SqsSinkBuilder::new(SqsSinkConfig {
            region: SQS_DEFAULT_REGION,
            queue_name: "test-q",
            queue_owner_aws_account_id: "12345678912",
            assume_role_config: None,
        })
        .client(sqs_client)
        .build()
        .await
        .unwrap();

        // create sink writer
        use crate::sinker::sink::{SinkClientType, SinkWriterBuilder};
        SinkWriterBuilder::new(5, Duration::from_millis(100), SinkClientType::Sqs(sqs_sink))
            .build()
            .await
            .unwrap()
    }

    fn get_send_message_output(count: i32) -> Rule {
        let mut successful = vec![];
        for i in 0..count {
            let msg_id = format!("msg-id-source-testsinke2e-{}", i.clone());
            // IMPORTANT: id must match the sink's generated batch entry IDs (e.g., "msg_{i}")
            let id = format!("msg_{}", i.clone());
            let entry = aws_sdk_sqs::types::SendMessageBatchResultEntry::builder()
                .id(id)
                .message_id(msg_id)
                .md5_of_message_body("f11a425906289abf8cce1733622834c8")
                .build()
                .unwrap();

            successful.push(entry);
        }

        let send_message_output = mock!(aws_sdk_sqs::Client::send_message_batch)
            .match_requests(|inp| {
                inp.queue_url().unwrap()
                    == "https://sqs.us-west-2.amazonaws.com/926113353675/test-q/"
            })
            .then_output(move || {
                // Create a vector of successful entries
                let successful_entries = successful.clone();

                // Create an empty vector for failed entries
                let failed_entries: Vec<BatchResultErrorEntry> = Vec::new();

                aws_sdk_sqs::operation::send_message_batch::SendMessageBatchOutput::builder()
                    .set_successful(Some(successful_entries))
                    .set_failed(Some(failed_entries))
                    .build()
                    .unwrap()
            });
        send_message_output
    }

    fn get_queue_url_output() -> Rule {
        let queue_url_output = mock!(aws_sdk_sqs::Client::get_queue_url)
            .match_requests(|inp| inp.queue_name().unwrap() == "test-q")
            .then_output(|| {
                aws_sdk_sqs::operation::get_queue_url::GetQueueUrlOutput::builder()
                    .queue_url("https://sqs.us-west-2.amazonaws.com/926113353675/test-q/")
                    .build()
            });
        queue_url_output
    }

    fn get_test_config_with_interceptor(interceptor: MockResponseInterceptor) -> Config {
        Config::builder()
            .behavior_version(BehaviorVersion::latest())
            .credentials_provider(make_sqs_test_credentials())
            .region(aws_sdk_sqs::config::Region::new("us-west-2"))
            .interceptor(interceptor)
            .build()
    }

    fn make_sqs_test_credentials() -> aws_sdk_sqs::config::Credentials {
        aws_sdk_sqs::config::Credentials::new(
            "ATESTCLIENT",
            "astestsecretkey",
            Some("atestsessiontoken".to_string()),
            None,
            "",
        )
    }
}
