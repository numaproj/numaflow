use rdkafka::types::RDKafkaErrorCode;

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
        Error::Kafka(msg) => {
            let lower = msg.to_lowercase();
            lower.contains("timed out")
                || lower.contains("rebalance")
                || lower.contains("illegal generation")
                || lower.contains("unknown member")
                || lower.contains("stale member")
                || lower.contains("transport")
                || lower.contains("broker not available")
                || lower.contains("all broker connections are down")
        }
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
        assert!(is_recoverable_startup_error(&Error::Kafka(
            "Failed to fetch metadata: Broker transport failure".into()
        )));
        assert!(is_recoverable_startup_error(&Error::Kafka(
            "Failed to get committed offsets: Local: Timed out".into()
        )));
        assert!(!is_recoverable_startup_error(&Error::Kafka(
            "Failed to add partition offset for acknowledging messages: invalid".into()
        )));
        assert!(!is_recoverable_startup_error(&Error::NonRetryable(
            "auth failed".into()
        )));
    }
}
