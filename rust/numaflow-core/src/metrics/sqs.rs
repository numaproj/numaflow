use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::family::Family;
use prometheus_client::metrics::histogram::Histogram;
use std::sync::OnceLock;

use super::{exponential_buckets_range, global_registry};

// SQS related metric configs
const SQS_REGISTRY_PREFIX: &str = "sqs";
const SQS_PRODUCER_REGISTRY_PREFIX: &str = "producer";
const SQS_PRODUCER_PUBLISH_SUCCESS_TOTAL: &str = "publish_success";
const SQS_PRODUCER_PUBLISH_FAILURE_TOTAL: &str = "publish_failure";
const SQS_PRODUCER_PUBLISH_LATENCY: &str = "publish_latency";

pub(crate) struct SQSProducerMetrics {
    pub(crate) publish_latency: Family<Vec<(String, String)>, Histogram>,
    pub(crate) publish_success: Family<Vec<(String, String)>, Counter>,
    pub(crate) publish_failure: Family<Vec<(String, String)>, Counter>,
}

pub(crate) struct SQSMetrics {
    pub(crate) producer: SQSProducerMetrics,
}

impl SQSMetrics {
    pub(crate) fn new() -> Self {
        let metrics = Self {
            producer: SQSProducerMetrics {
                publish_latency: Family::<Vec<(String, String)>, Histogram>::new_with_constructor(
                    || Histogram::new(exponential_buckets_range(100.0, 60000000.0 * 15.0, 10)),
                ),
                publish_success: Family::<Vec<(String, String)>, Counter>::default(),
                publish_failure: Family::<Vec<(String, String)>, Counter>::default(),
            },
        };

        let mut registry = global_registry().registry.lock();
        let sqs_registry = registry.sub_registry_with_prefix(SQS_REGISTRY_PREFIX);
        let producer_registery =
            sqs_registry.sub_registry_with_prefix(SQS_PRODUCER_REGISTRY_PREFIX);

        producer_registery.register(
            SQS_PRODUCER_PUBLISH_LATENCY,
            "Latency of SQS publish operations in microseconds",
            metrics.producer.publish_latency.clone(),
        );

        producer_registery.register(
            SQS_PRODUCER_PUBLISH_SUCCESS_TOTAL,
            "Number of messages successfully published to SQS",
            metrics.producer.publish_success.clone(),
        );

        producer_registery.register(
            SQS_PRODUCER_PUBLISH_FAILURE_TOTAL,
            "Number of messages that failed to publish to SQS",
            metrics.producer.publish_failure.clone(),
        );

        metrics
    }
}

static SQS_METRICS: OnceLock<SQSMetrics> = OnceLock::new();
pub(crate) fn sqs_metrics() -> &'static SQSMetrics {
    SQS_METRICS.get_or_init(SQSMetrics::new)
}
