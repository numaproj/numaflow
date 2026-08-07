use rdkafka::error::KafkaError;
use rdkafka::types::RDKafkaErrorCode;
use rdkafka::types::RDKafkaErrorCode::{
    AllBrokersDown, BrokerNotAvailable, BrokerTransportFailure, CoordinatorLoadInProgress,
    CoordinatorNotAvailable, IllegalGeneration, NetworkException, NotCoordinator,
    OperationTimedOut, RebalanceInProgress, RequestTimedOut, StaleMemberEpoch, UnknownMemberId,
    WaitingForCoordinator,
};

use crate::Error;

/// Returns true when a synchronous offset commit failed because the consumer group is
/// rebalancing or the member generation is stale. These commits can be dropped safely:
/// the message will be redelivered after the group stabilizes.
pub(crate) fn is_recoverable_commit_error(code: RDKafkaErrorCode) -> bool {
    use RDKafkaErrorCode::{
        IllegalGeneration, RebalanceInProgress, StaleMemberEpoch, UnknownMemberId,
    };
    matches!(
        code,
        RebalanceInProgress | IllegalGeneration | UnknownMemberId | StaleMemberEpoch
    )
}

/// Returns true when a fatal commit error should stop the source forwarder.
pub(crate) fn is_fatal_commit_error(code: RDKafkaErrorCode) -> bool {
    use RDKafkaErrorCode::{FencedInstanceId, FencedMemberEpoch, GroupAuthorizationFailed};
    matches!(
        code,
        GroupAuthorizationFailed | FencedInstanceId | FencedMemberEpoch
    )
}

/// Returns true when the initial pending-message probe can fail transiently during
/// broker metadata fetch, watermark lookup, or rebalance without invalidating credentials.
pub(crate) fn is_recoverable_startup_error(err: &Error) -> bool {
    match err {
        Error::KafkaClient { source, .. } => source
            .rdkafka_error_code()
            .is_some_and(is_recoverable_runtime_code),
        _ => false,
    }
}

fn is_recoverable_runtime_code(code: RDKafkaErrorCode) -> bool {
    matches!(
        code,
        OperationTimedOut
            | RequestTimedOut
            | BrokerTransportFailure
            | AllBrokersDown
            | BrokerNotAvailable
            | NetworkException
            | RebalanceInProgress
            | IllegalGeneration
            | UnknownMemberId
            | StaleMemberEpoch
            | WaitingForCoordinator
            | CoordinatorLoadInProgress
            | CoordinatorNotAvailable
            | NotCoordinator
    )
}

/// Returns true when a read failure is caused by transient broker connectivity,
/// coordinator, or rebalance state. These should let librdkafka reconnect without
/// forcing the numa container to restart.
pub(crate) fn is_recoverable_read_error(err: &KafkaError) -> bool {
    match err {
        KafkaError::MessageConsumption(code) => is_recoverable_runtime_code(*code),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rdkafka::types::RDKafkaErrorCode;

    #[test]
    fn recoverable_commit_errors() {
        for code in [
            RDKafkaErrorCode::RebalanceInProgress,
            RDKafkaErrorCode::IllegalGeneration,
            RDKafkaErrorCode::UnknownMemberId,
            RDKafkaErrorCode::StaleMemberEpoch,
        ] {
            assert!(
                is_recoverable_commit_error(code),
                "{code:?} should be recoverable"
            );
            assert!(!is_fatal_commit_error(code), "{code:?} should not be fatal");
        }
    }

    #[test]
    fn fatal_commit_errors() {
        for code in [
            RDKafkaErrorCode::GroupAuthorizationFailed,
            RDKafkaErrorCode::FencedInstanceId,
            RDKafkaErrorCode::FencedMemberEpoch,
        ] {
            assert!(is_fatal_commit_error(code), "{code:?} should be fatal");
            assert!(
                !is_recoverable_commit_error(code),
                "{code:?} should not be recoverable"
            );
        }
    }

    #[test]
    fn recoverable_startup_errors() {
        for code in [
            RDKafkaErrorCode::OperationTimedOut,
            RDKafkaErrorCode::BrokerTransportFailure,
            RDKafkaErrorCode::RebalanceInProgress,
            RDKafkaErrorCode::IllegalGeneration,
        ] {
            assert!(
                is_recoverable_startup_error(&Error::KafkaClient {
                    operation: "pending messages",
                    source: KafkaError::MetadataFetch(code),
                }),
                "{code:?} should be recoverable during startup"
            );
        }
    }

    #[test]
    fn fatal_startup_errors() {
        for code in [
            RDKafkaErrorCode::GroupAuthorizationFailed,
            RDKafkaErrorCode::TopicAuthorizationFailed,
            RDKafkaErrorCode::SaslAuthenticationFailed,
            RDKafkaErrorCode::UnknownTopicOrPartition,
        ] {
            assert!(
                !is_recoverable_startup_error(&Error::KafkaClient {
                    operation: "pending messages",
                    source: KafkaError::MetadataFetch(code),
                }),
                "{code:?} should not be recoverable during startup"
            );
        }
        assert!(!is_recoverable_startup_error(&Error::NonRetryable(
            "auth failed".into()
        )));
    }

    #[test]
    fn recoverable_read_errors() {
        for code in [
            RDKafkaErrorCode::BrokerTransportFailure,
            RDKafkaErrorCode::AllBrokersDown,
            RDKafkaErrorCode::OperationTimedOut,
            RDKafkaErrorCode::NetworkException,
        ] {
            assert!(
                is_recoverable_read_error(&KafkaError::MessageConsumption(code)),
                "{code:?} should be recoverable while reading"
            );
        }
    }

    #[test]
    fn fatal_read_errors() {
        for code in [
            RDKafkaErrorCode::TopicAuthorizationFailed,
            RDKafkaErrorCode::SaslAuthenticationFailed,
            RDKafkaErrorCode::UnknownTopicOrPartition,
        ] {
            assert!(
                !is_recoverable_read_error(&KafkaError::MessageConsumption(code)),
                "{code:?} should not be recoverable while reading"
            );
        }
        assert!(!is_recoverable_read_error(
            &KafkaError::MessageConsumptionFatal(RDKafkaErrorCode::BrokerTransportFailure)
        ));
    }
}
