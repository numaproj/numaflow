use numaflow_models::models::ForwardConditions;
use std::hash::{DefaultHasher, Hasher};

/// Checks if the message should to written to downstream vertex based the conditions
/// and message tags. If not tags are provided by there are edge conditions present, we will
/// still forward to all vertices.
pub(crate) fn should_forward(
    tags: Option<Vec<String>>,
    conditions: Option<Box<ForwardConditions>>,
) -> bool {
    conditions.map_or(true, |conditions| {
        conditions.tags.operator.as_ref().map_or(true, |operator| {
            tags.as_ref().map_or(true, |tags| {
                !conditions.tags.values.is_empty()
                    && check_operator_condition(operator, &conditions.tags.values, tags)
            })
        })
    })
}
/// Determine the partition to write the message to by hashing the message id.
pub(crate) fn determine_partition(
    message_id: String,
    partitions_count: u16,
    hash: &mut DefaultHasher,
) -> u16 {
    hash.write(message_id.as_bytes());
    let hash_value = hash.finish();
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
    use super::*;
    use numaflow_models::models::TagConditions;

    #[tokio::test]
    async fn test_evaluate_write_condition_no_conditions() {
        let result = should_forward(None, None);
        assert!(result);
    }

    #[tokio::test]
    async fn test_evaluate_write_condition_no_tags() {
        let conditions = ForwardConditions::new(TagConditions::new(vec!["tag1".to_string()]));
        let result = should_forward(None, Some(Box::new(conditions)));
        assert!(result);
    }

    #[tokio::test]
    async fn test_evaluate_write_condition_and_operator() {
        let mut tag_conditions = TagConditions::new(vec!["tag1".to_string(), "tag2".to_string()]);
        tag_conditions.operator = Some("and".to_string());
        let conditions = ForwardConditions::new(tag_conditions);
        let tags = Some(vec!["tag1".to_string(), "tag2".to_string()]);
        let result = should_forward(tags, Some(Box::new(conditions)));
        assert!(result);
    }

    #[tokio::test]
    async fn test_evaluate_write_condition_or_operator() {
        let mut tag_conditions = TagConditions::new(vec!["tag1".to_string()]);
        tag_conditions.operator = Some("or".to_string());
        let conditions = ForwardConditions::new(tag_conditions);
        let tags = Some(vec!["tag2".to_string(), "tag1".to_string()]);
        let result = should_forward(tags, Some(Box::new(conditions)));
        assert!(result);
    }

    #[tokio::test]
    async fn test_evaluate_write_condition_not_operator() {
        let mut tag_conditions = TagConditions::new(vec!["tag1".to_string()]);
        tag_conditions.operator = Some("not".to_string());
        let conditions = ForwardConditions::new(tag_conditions);
        let tags = Some(vec!["tag2".to_string()]);
        let result = should_forward(tags, Some(Box::new(conditions)));
        assert!(result);
    }
}
