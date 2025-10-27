use numaflow_pb::clients::serving::serving_store_client::ServingStoreClient;
use numaflow_pb::clients::sink::sink_client::SinkClient;
use std::time::Duration;
use tokio::sync::mpsc;
use tonic::transport::Channel;
use tracing::error;

use crate::Result;
use crate::config::components::sink::RetryConfig;
use crate::sinker::actor::SinkActor;
use crate::sinker::sink::serve::ServingStore;
use crate::sinker::sink::user_defined::UserDefinedSink;
use crate::sinker::sink::{SinkClientType, SinkWriter};
use crate::sinker::sink::{blackhole, log, serve};

/// User defined clients which will be used for doing sidecar health checks.
#[derive(Clone)]
pub(crate) struct HealthCheckClients {
    pub(crate) sink_client: Option<SinkClient<Channel>>,
    pub(crate) fb_sink_client: Option<SinkClient<Channel>>,
    pub(crate) store_client: Option<ServingStoreClient<Channel>>,
    pub(crate) on_success_sink_client: Option<SinkClient<Channel>>,
}

impl HealthCheckClients {
    pub(crate) async fn ready(&mut self) -> bool {
        let sink = if let Some(sink_client) = &mut self.sink_client {
            match sink_client.is_ready(tonic::Request::new(())).await {
                Ok(ready) => ready.into_inner().ready,
                Err(e) => {
                    error!(?e, "Sink client is not ready");
                    false
                }
            }
        } else {
            true
        };

        let fb_sink = if let Some(fb_sink_client) = &mut self.fb_sink_client {
            match fb_sink_client.is_ready(tonic::Request::new(())).await {
                Ok(ready) => ready.into_inner().ready,
                Err(e) => {
                    error!(?e, "Fallback Sink client is not ready");
                    false
                }
            }
        } else {
            true
        };

        let serve_store = if let Some(store_client) = &mut self.store_client {
            match store_client.is_ready(tonic::Request::new(())).await {
                Ok(ready) => ready.into_inner().ready,
                Err(e) => {
                    error!(?e, "Store client is not ready");
                    false
                }
            }
        } else {
            true
        };

        let on_success_sink = if let Some(on_success_sink_client) = &mut self.on_success_sink_client
        {
            match on_success_sink_client
                .is_ready(tonic::Request::new(()))
                .await
            {
                Ok(ready) => ready.into_inner().ready,
                Err(e) => {
                    error!(?e, "OnSuccess Sink client is not ready");
                    false
                }
            }
        } else {
            true
        };

        sink && fb_sink && serve_store && on_success_sink
    }
}

/// HealthCheckClientsBuilder is a builder for HealthCheckClients.
pub(crate) struct HealthCheckClientsBuilder {
    sink_client: Option<SinkClient<Channel>>,
    fb_sink_client: Option<SinkClient<Channel>>,
    store_client: Option<ServingStoreClient<Channel>>,
    on_success_sink_client: Option<SinkClient<Channel>>,
}

impl HealthCheckClientsBuilder {
    pub(crate) fn new() -> Self {
        Self {
            sink_client: None,
            fb_sink_client: None,
            store_client: None,
            on_success_sink_client: None,
        }
    }

    pub(crate) fn sink_client(mut self, sink_client: SinkClient<Channel>) -> Self {
        self.sink_client = Some(sink_client);
        self
    }

    pub(crate) fn fb_sink_client(mut self, fb_sink_client: SinkClient<Channel>) -> Self {
        self.fb_sink_client = Some(fb_sink_client);
        self
    }

    pub(crate) fn store_client(mut self, store_client: ServingStoreClient<Channel>) -> Self {
        self.store_client = Some(store_client);
        self
    }

    pub(crate) fn on_success_sink_client(
        mut self,
        on_success_sink_client: SinkClient<Channel>,
    ) -> Self {
        self.on_success_sink_client = Some(on_success_sink_client);
        self
    }

    pub(crate) fn build(self) -> HealthCheckClients {
        HealthCheckClients {
            sink_client: self.sink_client,
            fb_sink_client: self.fb_sink_client,
            store_client: self.store_client,
            on_success_sink_client: self.on_success_sink_client,
        }
    }
}

/// SinkWriterBuilder is a builder to build a SinkWriter.
pub(crate) struct SinkWriterBuilder {
    batch_size: usize,
    chunk_timeout: Duration,
    retry_config: RetryConfig,
    sink_client: SinkClientType,
    fb_sink_client: Option<SinkClientType>,
    on_success_sink_client: Option<SinkClientType>,
    serving_store: Option<ServingStore>,
}

impl SinkWriterBuilder {
    pub(crate) fn new(
        batch_size: usize,
        chunk_timeout: Duration,
        sink_type: SinkClientType,
    ) -> Self {
        Self {
            batch_size,
            chunk_timeout,
            retry_config: RetryConfig::default(),
            sink_client: sink_type,
            fb_sink_client: None,
            on_success_sink_client: None,
            serving_store: None,
        }
    }

    pub(crate) fn retry_config(mut self, retry_config: RetryConfig) -> Self {
        self.retry_config = retry_config;
        self
    }

    pub(crate) fn fb_sink_client(mut self, fb_sink_client: SinkClientType) -> Self {
        self.fb_sink_client = Some(fb_sink_client);
        self
    }

    pub(crate) fn serving_store(mut self, serving_store: ServingStore) -> Self {
        self.serving_store = Some(serving_store);
        self
    }

    pub(crate) fn on_success_sink_client(mut self, on_success_sink_client: SinkClientType) -> Self {
        self.on_success_sink_client = Some(on_success_sink_client);
        self
    }

    /// Build the SinkWriter, it also starts the SinkActor to handle messages.
    pub(crate) async fn build(self) -> Result<SinkWriter> {
        let (sink_handle, receiver) = mpsc::channel(self.batch_size);
        let mut health_check_builder = HealthCheckClientsBuilder::new();
        let retry_config = self.retry_config.clone();

        // starting sinks
        match self.sink_client {
            SinkClientType::Log => {
                let log_sink = log::LogSink;
                tokio::spawn(async move {
                    let actor = SinkActor::new(receiver, log_sink, retry_config);
                    actor.run().await;
                });
            }
            SinkClientType::Serve => {
                let serve_sink = serve::ServeSink;
                tokio::spawn(async move {
                    let actor = SinkActor::new(receiver, serve_sink, retry_config);
                    actor.run().await;
                });
            }
            SinkClientType::Blackhole => {
                let blackhole_sink = blackhole::BlackholeSink;
                tokio::spawn(async move {
                    let actor = SinkActor::new(receiver, blackhole_sink, retry_config);
                    actor.run().await;
                });
            }
            SinkClientType::UserDefined(sink_client) => {
                health_check_builder = health_check_builder.sink_client(sink_client.clone());
                let sink = UserDefinedSink::new(sink_client).await?;
                tokio::spawn(async move {
                    let actor = SinkActor::new(receiver, sink, retry_config);
                    actor.run().await;
                });
            }
            SinkClientType::Sqs(sqs_sink) => {
                tokio::spawn(async move {
                    let actor = SinkActor::new(receiver, sqs_sink, retry_config);
                    actor.run().await;
                });
            }
            SinkClientType::Kafka(kafka_sink) => {
                tokio::spawn(async move {
                    let actor = SinkActor::new(receiver, kafka_sink, retry_config);
                    actor.run().await;
                });
            }
            SinkClientType::Pulsar(pulsar_sink) => {
                tokio::spawn(async move {
                    let actor = SinkActor::new(receiver, *pulsar_sink, retry_config);
                    actor.run().await;
                });
            }
        };

        // start fallback sinks
        let fb_sink_handle = if let Some(fb_sink_client) = self.fb_sink_client {
            let (fb_sender, fb_receiver) = mpsc::channel(self.batch_size);
            let fb_retry_config = self.retry_config.clone();
            match fb_sink_client {
                SinkClientType::Log => {
                    let log_sink = log::LogSink;
                    tokio::spawn(async move {
                        let actor = SinkActor::new(fb_receiver, log_sink, fb_retry_config);
                        actor.run().await;
                    });
                }
                SinkClientType::Serve => {
                    let serve_sink = serve::ServeSink;
                    tokio::spawn(async move {
                        let actor = SinkActor::new(fb_receiver, serve_sink, fb_retry_config);
                        actor.run().await;
                    });
                }
                SinkClientType::Blackhole => {
                    let blackhole_sink = blackhole::BlackholeSink;
                    tokio::spawn(async move {
                        let actor = SinkActor::new(fb_receiver, blackhole_sink, fb_retry_config);
                        actor.run().await;
                    });
                }
                SinkClientType::UserDefined(sink_client) => {
                    health_check_builder = health_check_builder.fb_sink_client(sink_client.clone());
                    let sink = UserDefinedSink::new(sink_client).await?;
                    tokio::spawn(async move {
                        let actor = SinkActor::new(fb_receiver, sink, fb_retry_config);
                        actor.run().await;
                    });
                }
                SinkClientType::Sqs(sqs_sink) => {
                    tokio::spawn(async move {
                        let actor = SinkActor::new(fb_receiver, sqs_sink, fb_retry_config);
                        actor.run().await;
                    });
                }
                SinkClientType::Kafka(kafka_sink) => {
                    tokio::spawn(async move {
                        let actor = SinkActor::new(fb_receiver, kafka_sink, fb_retry_config);
                        actor.run().await;
                    });
                }
                SinkClientType::Pulsar(pulsar_sink) => {
                    tokio::spawn(async move {
                        let actor = SinkActor::new(fb_receiver, *pulsar_sink, fb_retry_config);
                        actor.run().await;
                    });
                }
            };
            Some(fb_sender)
        } else {
            None
        };

        // start onSuccess sink
        let os_sink_handle = if let Some(os_sink_client) = self.on_success_sink_client {
            let (os_sender, fb_receiver) = mpsc::channel(self.batch_size);
            let fb_retry_config = self.retry_config.clone();
            match os_sink_client {
                SinkClientType::Log => {
                    let log_sink = log::LogSink;
                    tokio::spawn(async move {
                        let actor = SinkActor::new(fb_receiver, log_sink, fb_retry_config);
                        actor.run().await;
                    });
                }
                SinkClientType::Serve => {
                    let serve_sink = serve::ServeSink;
                    tokio::spawn(async move {
                        let actor = SinkActor::new(fb_receiver, serve_sink, fb_retry_config);
                        actor.run().await;
                    });
                }
                SinkClientType::Blackhole => {
                    let blackhole_sink = blackhole::BlackholeSink;
                    tokio::spawn(async move {
                        let actor = SinkActor::new(fb_receiver, blackhole_sink, fb_retry_config);
                        actor.run().await;
                    });
                }
                SinkClientType::UserDefined(sink_client) => {
                    health_check_builder =
                        health_check_builder.on_success_sink_client(sink_client.clone());
                    let sink = UserDefinedSink::new(sink_client).await?;
                    tokio::spawn(async move {
                        let actor = SinkActor::new(fb_receiver, sink, fb_retry_config);
                        actor.run().await;
                    });
                }
                SinkClientType::Sqs(sqs_sink) => {
                    tokio::spawn(async move {
                        let actor = SinkActor::new(fb_receiver, sqs_sink, fb_retry_config);
                        actor.run().await;
                    });
                }
                SinkClientType::Kafka(kafka_sink) => {
                    tokio::spawn(async move {
                        let actor = SinkActor::new(fb_receiver, kafka_sink, fb_retry_config);
                        actor.run().await;
                    });
                }
                SinkClientType::Pulsar(pulsar_sink) => {
                    tokio::spawn(async move {
                        let actor = SinkActor::new(fb_receiver, *pulsar_sink, fb_retry_config);
                        actor.run().await;
                    });
                }
            };
            Some(os_sender)
        } else {
            None
        };

        // NOTE: we do not start the serving store sink because it is over unary while the rest are
        // streaming.
        if let Some(ServingStore::UserDefined(store)) = &self.serving_store {
            health_check_builder = health_check_builder.store_client(store.get_store_client());
        }

        let health_check_clients = health_check_builder.build();

        Ok(SinkWriter::new(
            self.batch_size,
            self.chunk_timeout,
            sink_handle,
            fb_sink_handle,
            os_sink_handle,
            self.serving_store,
            health_check_clients,
        ))
    }
}
