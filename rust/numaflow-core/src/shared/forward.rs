use std::hash::{DefaultHasher, Hasher};
use std::sync::Arc;

use numaflow_models::models::ForwardConditions;

/// Checks if the message should be written to downstream vertex based on the conditions
/// and message tags. If no tags are provided, we treat it as empty tags and still perform
/// the condition check.
pub(crate) fn should_forward(
    tags: Option<Arc<[String]>>,
    conditions: Option<Box<ForwardConditions>>,
) -> bool {
    // we should forward the message to downstream vertex if there are no edge conditions
    let Some(conditions) = conditions else {
        return true;
    };

    // Return true if there are no tags in the edge condition
    if conditions.tags.values.is_empty() {
        return true;
    }

    // Treat missing tags as empty and check the condition
    let tags = tags.unwrap_or_else(|| Arc::from(vec![]));
    // Default operator is "or", if not specified
    let operator = conditions.tags.operator.as_deref().unwrap_or("or");
    check_operator_condition(operator, &conditions.tags.values, &tags)
}

/// Determine the partition to write the message to by hashing the message id.
pub(crate) fn determine_partition(shuffle_key: String, partitions_count: u16) -> u16 {
    // if there is only one partition, we don't need to hash
    if partitions_count == 1 {
        return 0;
    }

    let mut hasher = DefaultHasher::new();
    hasher.write(shuffle_key.as_bytes());
    let hash_value = hasher.finish();
    (hash_value % partitions_count as u64) as u16
}

/// Check whether a message should be forwarded to the next vertex based on the tags and tags in the
/// edge condition.
fn check_operator_condition(
    set_operator: &str,
    tags_from_edge_condition: &[String],
    tags_from_message: &[String],
) -> bool {
    match set_operator {
        "and" => {
            // returns true if all the elements of vec a are in vec b
            tags_from_edge_condition
                .iter()
                .all(|val| tags_from_message.contains(val))
        }
        "or" => {
            // returns true if any of the elements of vec a are in vec b
            tags_from_edge_condition
                .iter()
                .any(|val| tags_from_message.contains(val))
        }
        "not" => {
            // returns false if any of the elements of vec a are in vec b
            !tags_from_edge_condition
                .iter()
                .any(|val| tags_from_message.contains(val))
        }
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use numaflow_models::models::TagConditions;

    use super::*;

    #[tokio::test]
    async fn test_evaluate_write_condition_no_conditions() {
        let result = should_forward(None, None);
        assert!(result);
    }

    #[tokio::test]
    async fn test_evaluate_write_condition_and_operator() {
        let mut tag_conditions = TagConditions::new(vec!["tag1".to_string(), "tag2".to_string()]);
        tag_conditions.operator = Some("and".to_string());
        let conditions = ForwardConditions::new(tag_conditions);
        let tags = Some(Arc::from(vec!["tag1".to_string(), "tag2".to_string()]));
        let result = should_forward(tags, Some(Box::new(conditions)));
        assert!(result);
    }

    #[tokio::test]
    async fn test_evaluate_write_condition_or_operator() {
        let mut tag_conditions = TagConditions::new(vec!["tag1".to_string()]);
        tag_conditions.operator = Some("or".to_string());
        let conditions = ForwardConditions::new(tag_conditions);
        let tags = Some(Arc::from(vec!["tag2".to_string(), "tag1".to_string()]));
        let result = should_forward(tags, Some(Box::new(conditions)));
        assert!(result);
    }

    #[tokio::test]
    async fn test_evaluate_write_condition_not_operator() {
        let mut tag_conditions = TagConditions::new(vec!["tag1".to_string()]);
        tag_conditions.operator = Some("not".to_string());
        let conditions = ForwardConditions::new(tag_conditions);
        let tags = Some(Arc::from(vec!["tag2".to_string()]));
        let result = should_forward(tags, Some(Box::new(conditions)));
        assert!(result);
    }

    #[tokio::test]
    async fn test_empty_tags_with_and_operator() {
        let mut tag_conditions = TagConditions::new(vec!["tag1".to_string(), "tag2".to_string()]);
        tag_conditions.operator = Some("and".to_string());
        let conditions = ForwardConditions::new(tag_conditions);

        // Empty tags array (explicit empty)
        let tags = Some(Arc::from(Vec::<String>::new()));
        let result = should_forward(tags, Some(Box::new(conditions.clone())));
        assert!(!result, "AND condition should fail with empty tags");

        // None tags
        let result = should_forward(None, Some(Box::new(conditions)));
        assert!(!result, "AND condition should fail with None tags");
    }

    #[tokio::test]
    async fn test_empty_tags_with_or_operator() {
        let mut tag_conditions = TagConditions::new(vec!["tag1".to_string(), "tag2".to_string()]);
        tag_conditions.operator = Some("or".to_string());
        let conditions = ForwardConditions::new(tag_conditions);

        // Empty tags array (explicit empty)
        let tags = Some(Arc::from(Vec::<String>::new()));
        let result = should_forward(tags, Some(Box::new(conditions.clone())));
        assert!(!result, "OR condition should fail with empty tags");

        // None tags
        let result = should_forward(None, Some(Box::new(conditions)));
        assert!(!result, "OR condition should fail with None tags");
    }

    #[tokio::test]
    async fn test_empty_tags_with_not_operator() {
        let mut tag_conditions = TagConditions::new(vec!["tag1".to_string(), "tag2".to_string()]);
        tag_conditions.operator = Some("not".to_string());
        let conditions = ForwardConditions::new(tag_conditions);

        // Empty tags array (explicit empty)
        let tags = Some(Arc::from(Vec::<String>::new()));
        let result = should_forward(tags, Some(Box::new(conditions.clone())));
        assert!(result, "NOT condition should pass with empty tags");

        // None tags
        let result = should_forward(None, Some(Box::new(conditions)));
        assert!(result, "NOT condition should pass with None tags");
    }

    #[tokio::test]
    async fn test_default_operator() {
        let tag_conditions = TagConditions::new(vec!["tag1".to_string(), "tag2".to_string()]);
        let conditions = ForwardConditions::new(tag_conditions);
        let tags = Some(Arc::from(vec!["tag1".to_string(), "tag2".to_string()]));
        let result = should_forward(tags, Some(Box::new(conditions)));
        assert!(result);
    }
}
