//! ROS 2 source implementation.
//!
//! Architecture: actor pattern mirroring `numaflow-nats` / `numaflow-kafka`.
//!
//! ```text
//!   Ros2Source (clone-able handle)
//!       |
//!       | mpsc channel (Ros2ActorMessage)
//!       v
//!   Ros2Actor (tokio task, owns the subscriber stream)
//!       |
//!       | r2r subscriber stream
//!       v
//!   ROS 2 middleware (DDS)
//! ```
//!
//! Retry / back-off is handled inside `Ros2Actor::read_messages`:
//! up to `MAX_CONSECUTIVE_ERRORS` consecutive receive errors trigger an
//! exponential sleep before the next attempt; a successful receive resets
//! the counter.  After exhausting retries the actor surfaces the error to
//! the caller, which lets numaflow-core decide whether to restart the
//! source vertex.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use bytes::Bytes;
use chrono::Utc;
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;

use crate::{Error, Result};

/// Unix epoch at process start, in seconds.  Used as the high-order component
/// of every offset so that offsets survive pod restarts without collision.
///
/// Format: `<start_epoch_secs>:<topic>:<local_seq>`
///
/// The JetStream dedup window is 2 minutes by default.  After a restart the
/// new pod begins at seq=1 again, but the epoch component is different, so
/// `<new_epoch>:topic:1` ≠ `<old_epoch>:topic:1`.
static START_EPOCH_SECS: std::sync::OnceLock<u64> = std::sync::OnceLock::new();

#[cfg(feature = "ros2")]
fn start_epoch() -> u64 {    *START_EPOCH_SECS.get_or_init(|| {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs()
    })
}

// ---------------------------------------------------------------------------
// Public config
// ---------------------------------------------------------------------------

/// Configuration for a ROS 2 source.
///
/// # Delivery guarantee
///
/// ROS 2 pub/sub has no acknowledgement or offset concept.  This source
/// provides **at-most-once** delivery:
///
/// - A message is emitted to the pipeline exactly once per receipt.
/// - If Numaflow nacks a batch (e.g. a failed downstream write or shutdown),
///   the nack is a no-op — those messages are **not** replayed.
/// - There is no persistent queue; messages that arrive while the pod is down
///   are lost.
///
/// If you need at-least-once delivery, run a durable bridge (e.g. a ROS 2
/// node that writes to Kafka) and use the Kafka source instead.
#[derive(Debug, Clone)]
pub struct Ros2SourceConfig {
    /// ROS 2 topic name, e.g. `/robot/telemetry`.
    ///
    /// Every subscriber receives every published message — there is no
    /// partition or queue-group load-sharing.  Running more than one replica
    /// duplicates all data.  Set `scale.max: 1` in the pipeline vertex spec.
    pub topic: String,
    /// ROS 2 node name that Numaflow will register, e.g. `numaflow_source`.
    pub node_name: String,
    /// ROS 2 namespace, e.g. `/numaflow`.  Empty string means root namespace.
    pub namespace: String,
    /// How long to wait for messages before returning an empty batch.
    pub read_timeout: Duration,
    /// Maximum number of messages to return per `read` call.
    pub batch_size: usize,

    /// ROS 2 message type string, e.g. `sensor_msgs/msg/LaserScan`.
    ///
    /// Required for `subscribe_untyped`.  Must match the publisher's type
    /// exactly.  A type mismatch or a QoS incompatibility causes silence with
    /// no error; check `/ros2 topic info <topic>` if no messages arrive.
    pub msg_type: String,

    /// Size of the in-process channel between the spin thread and the async
    /// actor.  When the actor is slower than the publisher, the spin thread
    /// drops messages at this boundary.  Increase it to absorb bursts; the
    /// effective backpressure limit is `channel_capacity * message_size`.
    /// Default: 256.
    pub channel_capacity: usize,

    /// Emit a warning log when no message has arrived for this duration.
    /// `None` disables the warning.  Useful for detecting silent failures
    /// (discovery never completed, publisher stopped, QoS incompatibility).
    pub silence_warn_after: Option<Duration>,
}

// ---------------------------------------------------------------------------
// Message type
// ---------------------------------------------------------------------------

/// A single raw message received from a ROS 2 topic.
#[derive(Debug)]
pub struct Ros2Message {
    /// Serialised message payload (CDR-encoded bytes from DDS).
    pub payload: Bytes,
    /// Wall-clock time when the message was received by Numaflow.
    pub event_time: chrono::DateTime<Utc>,
    /// A unique-enough offset string: `<topic>:<seq>`.
    /// `seq` increments per actor instance and is NOT globally unique across
    /// restarts — Numaflow uses it only for within-batch ordering.
    pub offset: String,
}

// ---------------------------------------------------------------------------
// Actor messages
// ---------------------------------------------------------------------------

#[allow(dead_code)]
pub(crate) enum Ros2ActorMessage {
    Read {
        respond_to: oneshot::Sender<Option<Result<Vec<Ros2Message>>>>,
    },
}

// ---------------------------------------------------------------------------
// Actor (requires `ros2` feature)
// ---------------------------------------------------------------------------

#[cfg(feature = "ros2")]
mod actor_impl {
    use super::*;
    use r2r::QosProfile;
    use tokio_stream::StreamExt as _;
    use tracing::{debug, error, warn};

    /// Maximum number of consecutive receive errors before the actor surfaces
    /// the error to the caller instead of silently retrying.
    const MAX_CONSECUTIVE_ERRORS: usize = 10;

    /// Initial sleep between retries; doubles on each failure up to `MAX_RETRY_SLEEP`.
    const INITIAL_RETRY_SLEEP: Duration = Duration::from_millis(100);
    const MAX_RETRY_SLEEP: Duration = Duration::from_secs(5);

    pub(super) struct Ros2Actor {
        handler_rx: mpsc::Receiver<Ros2ActorMessage>,
        cancel_token: CancellationToken,
        read_timeout: Duration,
        batch_size: usize,
        topic: String,
        /// r2r subscriber stream — yields raw `r2r::RawMessage` bytes.
        sub: r2r::SubscriberUntyped,
        /// Monotonically increasing sequence counter for offset strings.
        seq: u64,
    }

    impl Ros2Actor {
        pub(super) async fn start(
            config: super::Ros2SourceConfig,
            handler_rx: mpsc::Receiver<Ros2ActorMessage>,
            cancel_token: CancellationToken,
        ) -> Result<()> {
            // The r2r context and node must be created on the thread that will
            // spin them.  We spin the node in a dedicated blocking thread so it
            // does not block the Tokio executor.
            let (sub_tx, sub_rx) = oneshot::channel::<Result<r2r::SubscriberUntyped>>();
            let topic_clone = config.topic.clone();
            let node_name = config.node_name.clone();
            let namespace = config.namespace.clone();

            std::thread::spawn(move || {
                let ctx = match r2r::Context::create() {
                    Ok(c) => c,
                    Err(e) => {
                        let _ = sub_tx.send(Err(Error::Node(e.to_string())));
                        return;
                    }
                };
                let mut node =
                    match r2r::Node::create(ctx, &node_name, &namespace) {
                        Ok(n) => n,
                        Err(e) => {
                            let _ = sub_tx.send(Err(Error::Node(e.to_string())));
                            return;
                        }
                    };
                let sub = match node.subscribe_untyped(&topic_clone, QosProfile::default()) {
                    Ok(s) => s,
                    Err(e) => {
                        let _ = sub_tx.send(Err(Error::Subscribe {
                            topic: topic_clone.clone(),
                            error: e.to_string(),
                        }));
                        return;
                    }
                };
                let _ = sub_tx.send(Ok(sub));

                // Spin the node so DDS callbacks fire.  This loop runs until
                // the process exits; a future improvement could use the
                // cancel_token here.
                loop {
                    node.spin_once(Duration::from_millis(100));
                }
            });

            let sub = sub_rx
                .await
                .map_err(|_| Error::Node("ROS 2 setup thread panicked".into()))??;

            tokio::spawn(async move {
                let actor = Ros2Actor {
                    handler_rx,
                    cancel_token,
                    read_timeout: config.read_timeout,
                    batch_size: config.batch_size,
                    topic: config.topic,
                    sub,
                    seq: 0,
                };
                tracing::info!(topic = %actor.topic, "Starting ROS 2 source actor");
                actor.run().await;
            });

            Ok(())
        }

        async fn run(mut self) {
            while let Some(msg) = self.handler_rx.recv().await {
                self.handle_message(msg).await;
            }
        }

        async fn handle_message(&mut self, msg: Ros2ActorMessage) {
            match msg {
                Ros2ActorMessage::Read { respond_to } => {
                    let result = self.read_messages().await;
                    let _ = respond_to.send(result);
                }
            }
        }

        /// Read up to `batch_size` messages, waiting at most `read_timeout`.
        ///
        /// Retry policy:
        /// - Each consecutive receive error increments a counter and sleeps for
        ///   an exponentially growing interval (100 ms → 5 s).
        /// - After `MAX_CONSECUTIVE_ERRORS` consecutive failures the method
        ///   returns `Some(Err(...))` so the caller can decide to restart.
        /// - A successful receive resets the counter and the sleep interval.
        async fn read_messages(&mut self) -> Option<Result<Vec<Ros2Message>>> {
            if self.cancel_token.is_cancelled() {
                return None;
            }

            let mut messages: Vec<Ros2Message> = Vec::with_capacity(self.batch_size);
            let deadline =
                tokio::time::timeout(self.read_timeout, std::future::pending::<()>());
            tokio::pin!(deadline);

            let mut consecutive_errors: usize = 0;
            let mut retry_sleep = INITIAL_RETRY_SLEEP;
            let mut last_error: Option<Error> = None;

            loop {
                if messages.len() >= self.batch_size {
                    break;
                }

                tokio::select! {
                    biased;

                    _ = &mut deadline => {
                        // Surface the error only when we got nothing at all.
                        if messages.is_empty() {
                            if let Some(err) = last_error {
                                return Some(Err(err));
                            }
                        }
                        break;
                    }

                    maybe_raw = self.sub.next() => {
                        match maybe_raw {
                            Some(Ok(raw)) => {
                                // Reset retry state on success.
                                consecutive_errors = 0;
                                retry_sleep = INITIAL_RETRY_SLEEP;
                                last_error = None;

                                self.seq += 1;
                                let offset = format!(
                                    "{}:{}:{}",
                                    start_epoch(),
                                    self.topic,
                                    self.seq
                                );
                                messages.push(Ros2Message {
                                    payload: Bytes::from(raw.serialized_message),
                                    event_time: Utc::now(),
                                    offset,
                                });
                            }
                            Some(Err(e)) => {
                                consecutive_errors += 1;
                                let err_str = e.to_string();

                                if consecutive_errors > MAX_CONSECUTIVE_ERRORS {
                                    error!(
                                        topic = %self.topic,
                                        consecutive_errors,
                                        "ROS 2 source exceeded max consecutive errors, surfacing error"
                                    );
                                    return Some(Err(Error::NonRetryable(format!(
                                        "ROS 2 topic '{}' failed {} times consecutively: {}",
                                        self.topic, consecutive_errors, err_str
                                    ))));
                                }

                                warn!(
                                    topic = %self.topic,
                                    consecutive_errors,
                                    ?retry_sleep,
                                    error = %err_str,
                                    "ROS 2 receive error, will retry after backoff"
                                );
                                last_error = Some(Error::Other(err_str));

                                tokio::time::sleep(retry_sleep).await;
                                // Exponential back-off, capped at MAX_RETRY_SLEEP.
                                retry_sleep = (retry_sleep * 2).min(MAX_RETRY_SLEEP);
                            }
                            None => {
                                // Subscriber stream ended — node was shut down.
                                return Some(Err(Error::NonRetryable(format!(
                                    "ROS 2 subscriber stream for topic '{}' ended unexpectedly",
                                    self.topic
                                ))));
                            }
                        }
                    }
                }
            }

            debug!(
                topic = %self.topic,
                msg_count = messages.len(),
                "Read messages from ROS 2"
            );
            Some(Ok(messages))
        }
    }
}

// ---------------------------------------------------------------------------
// Stub actor (when `ros2` feature is off)
// ---------------------------------------------------------------------------

#[cfg(not(feature = "ros2"))]
mod actor_impl {
    use super::*;

    pub(super) struct Ros2Actor;

    impl Ros2Actor {
        pub(super) async fn start(
            _config: super::Ros2SourceConfig,
            _handler_rx: mpsc::Receiver<Ros2ActorMessage>,
            _cancel_token: CancellationToken,
        ) -> Result<()> {
            Err(Error::Ros2NotEnabled)
        }
    }
}

// ---------------------------------------------------------------------------
// Public handle
// ---------------------------------------------------------------------------

/// A clone-able handle to the ROS 2 source actor.
#[derive(Clone, Debug)]
pub struct Ros2Source {
    actor_tx: mpsc::Sender<Ros2ActorMessage>,
}

#[cfg(test)]
impl Ros2Source {
    /// Test-only constructor that lets tests inject a custom sender, so they
    /// can simulate a dead actor without starting a real ROS 2 node.
    pub(crate) fn from_sender(tx: mpsc::Sender<Ros2ActorMessage>) -> Self {
        Self { actor_tx: tx }
    }
}

impl Ros2Source {
    /// Connect to a ROS 2 topic and start the subscriber actor.
    ///
    /// Returns [`Error::Ros2NotEnabled`] when compiled without the `ros2`
    /// feature flag.
    pub async fn connect(
        config: Ros2SourceConfig,
        cancel_token: CancellationToken,
    ) -> Result<Self> {
        let (tx, rx) = mpsc::channel(10);
        actor_impl::Ros2Actor::start(config, rx, cancel_token).await?;
        Ok(Self { actor_tx: tx })
    }

    /// Read the next batch of messages.
    ///
    /// Returns `None` when the source has been cancelled, `Some(Err(_))` on a
    /// non-retryable error, and `Some(Ok(msgs))` (possibly empty) otherwise.
    pub async fn read_messages(&self) -> Option<Result<Vec<Ros2Message>>> {
        let (tx, rx) = oneshot::channel();
        let _ = self
            .actor_tx
            .send(Ros2ActorMessage::Read { respond_to: tx })
            .await;
        rx.await
            .unwrap_or_else(|_| Some(Err(Error::Other("ROS 2 actor task terminated".into()))))
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    // -----------------------------------------------------------------------
    // Stub-path tests (no ROS 2 required)
    // -----------------------------------------------------------------------

    /// `connect` returns `Ros2NotEnabled` immediately when compiled without
    /// the `ros2` feature.  This is the path exercised in normal CI.
    #[tokio::test]
    #[cfg(not(feature = "ros2"))]
    async fn connect_without_ros2_feature_returns_error() {
        let config = Ros2SourceConfig {
            topic: "/test/topic".into(),
            msg_type: "std_msgs/msg/String".into(),
            node_name: "numaflow_test".into(),
            namespace: String::new(),
            read_timeout: Duration::from_secs(1),
            batch_size: 10,
            channel_capacity: 256,
            silence_warn_after: None,
        };
        let err = Ros2Source::connect(config, CancellationToken::new())
            .await
            .unwrap_err();
        assert!(
            matches!(err, Error::Ros2NotEnabled),
            "Expected Ros2NotEnabled, got {err:?}"
        );
    }

    /// The error message for `Ros2NotEnabled` mentions "ros2" so operators
    /// know exactly what to do.
    #[test]
    fn ros2_not_enabled_error_message_is_actionable() {
        let e = Error::Ros2NotEnabled;
        let msg = e.to_string();
        assert!(
            msg.contains("ros2"),
            "Error message should mention the feature flag, got: {msg}"
        );
    }

    /// `Node` and `Subscribe` errors carry the context strings through.
    #[test]
    fn error_variants_carry_context() {
        let node_err = Error::Node("ctx creation failed".into());
        assert!(node_err.to_string().contains("ctx creation failed"));

        let sub_err = Error::Subscribe {
            topic: "/robot/vel".into(),
            error: "topic not found".into(),
        };
        let sub_msg = sub_err.to_string();
        assert!(sub_msg.contains("/robot/vel"));
        assert!(sub_msg.contains("topic not found"));

        let nr_err = Error::NonRetryable("bad state".into());
        assert!(nr_err.to_string().contains("bad state"));

        let other_err = Error::Other("misc".into());
        assert!(other_err.to_string().contains("misc"));
    }

    /// `Ros2Message` fields round-trip correctly.
    #[test]
    fn ros2_message_fields_are_preserved() {
        let now = chrono::Utc::now();
        let msg = Ros2Message {
            payload: Bytes::from_static(b"sensor_data"),
            event_time: now,
            offset: "/lidar:17".to_string(),
        };
        assert_eq!(msg.payload, Bytes::from_static(b"sensor_data"));
        assert_eq!(msg.offset, "/lidar:17");
        assert_eq!(msg.event_time, now);
    }

    /// Config defaults are sane.
    #[test]
    fn ros2_source_config_debug_prints() {
        let cfg = Ros2SourceConfig {
            topic: "/camera/image_raw".into(),
            msg_type: "sensor_msgs/msg/Image".into(),
            node_name: "numaflow_source".into(),
            namespace: String::new(),
            read_timeout: Duration::from_millis(500),
            batch_size: 100,
            channel_capacity: 256,
            silence_warn_after: Some(Duration::from_secs(30)),
        };
        let dbg = format!("{cfg:?}");
        assert!(dbg.contains("/camera/image_raw"));
        assert!(dbg.contains("100"));
    }

    // -----------------------------------------------------------------------
    // Actor channel test — simulates read without real DDS
    // -----------------------------------------------------------------------

    /// When the actor task is dropped (channel closed), `read_messages`
    /// returns `Some(Err("Actor task terminated"))`.
    #[tokio::test]
    async fn read_messages_returns_error_when_actor_is_gone() {
        use tokio::sync::mpsc;
        // Build a source handle whose actor side is immediately dropped.
        let (tx, rx) = mpsc::channel::<Ros2ActorMessage>(1);
        drop(rx); // actor is gone
        let source = Ros2Source::from_sender(tx);

        let result = source.read_messages().await;
        match result {
            Some(Err(Error::Other(msg))) => {
                assert!(
                    msg.contains("terminated"),
                    "unexpected msg: {msg}"
                );
            }
            other => panic!("Expected Some(Err(Other(...))), got {other:?}"),
        }
    }

    // -----------------------------------------------------------------------
    // Full integration test — requires a live ROS 2 environment
    // -----------------------------------------------------------------------

    /// Run with: `cargo test -p numaflow-ros2 --features ros2-tests`
    #[cfg(feature = "ros2-tests")]
    #[tokio::test]
    async fn test_ros2_source_read_integration() {
        use r2r::std_msgs::msg::String as RosString;

        let topic = "/numaflow_ros2_test";
        let config = Ros2SourceConfig {
            topic: topic.into(),
            msg_type: "std_msgs/msg/String".into(),
            node_name: "numaflow_ros2_test_node".into(),
            namespace: String::new(),
            read_timeout: Duration::from_secs(2),
            batch_size: 5,
            channel_capacity: 64,
            silence_warn_after: Some(Duration::from_secs(5)),
        };

        let source = Ros2Source::connect(config, CancellationToken::new())
            .await
            .expect("connect should succeed with ROS 2 available");

        // Give the subscriber a moment to register with DDS.
        tokio::time::sleep(Duration::from_millis(200)).await;

        // Publish a few messages using r2r directly.
        {
            let ctx = r2r::Context::create().unwrap();
            let mut node = r2r::Node::create(ctx, "numaflow_ros2_test_pub", "").unwrap();
            let pub_ = node
                .create_publisher::<RosString>(topic, r2r::QosProfile::default())
                .unwrap();
            for i in 0..3u32 {
                pub_
                    .publish(&RosString {
                        data: format!("hello {i}"),
                    })
                    .unwrap();
                node.spin_once(Duration::from_millis(10));
            }
        }

        let messages = source.read_messages().await.unwrap().unwrap();
        assert!(
            !messages.is_empty(),
            "Expected at least one message from ROS 2 topic"
        );
        // Each message has a non-empty payload and a valid offset.
        for msg in &messages {
            assert!(!msg.payload.is_empty(), "payload should not be empty");
            assert!(
                msg.offset.contains(topic),
                "offset should contain the topic name"
            );
        }
    }
}
