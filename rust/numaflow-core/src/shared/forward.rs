use numaflow_models::models::ForwardConditions;
use std::hash::{DefaultHasher, Hasher};

/// checks if the message should to written to downstream vertex based the conditions
/// and message tags
pub(crate) fn evaluate_write_condition(
    tags: Option<Vec<String>>,
    conditions: Option<Box<ForwardConditions>>,
) -> bool {
    conditions.map_or(true, |conditions| {
        conditions.tags.operator.as_ref().map_or(true, |operator| {
            tags.as_ref().map_or(true, |tags| {
                !conditions.tags.values.is_empty()
                    && compare_slice(operator, &conditions.tags.values, tags)
            })
        })
    })
}
/// determine the partition to write the message to by hashing the message id
pub(crate) fn determine_partition(
    message_id: String,
    partitions_count: u16,
    hash: &mut DefaultHasher,
) -> u16 {
    hash.write(message_id.as_bytes());
    let hash_value = hash.finish();
    (hash_value % partitions_count as u64) as u16
}

// compare the slice of strings based on the operator
fn compare_slice(operator: &str, a: &[String], b: &[String]) -> bool {
    match operator {
        "and" => {
            // returns true if all the elements of vec a are in vec b
            a.iter().all(|val| b.contains(val))
        }
        "or" => {
            // returns true if any of the elements of vec a are in vec b
            a.iter().any(|val| b.contains(val))
        }
        "not" => {
            // returns false if any of the elements of vec a are in vec b
            !a.iter().any(|val| b.contains(val))
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
        let result = evaluate_write_condition(None, None);
        assert!(result);
    }

    #[tokio::test]
    async fn test_evaluate_write_condition_no_tags() {
        let conditions = ForwardConditions::new(TagConditions::new(vec!["tag1".to_string()]));
        let result = evaluate_write_condition(None, Some(Box::new(conditions)));
        assert!(!result);
    }

    #[tokio::test]
    async fn test_evaluate_write_condition_and_operator() {
        let mut tag_conditions = TagConditions::new(vec!["tag1".to_string(), "tag2".to_string()]);
        tag_conditions.operator = Some("and".to_string());
        let conditions = ForwardConditions::new(tag_conditions);
        let tags = Some(vec!["tag1".to_string(), "tag2".to_string()]);
        let result = evaluate_write_condition(tags, Some(Box::new(conditions)));
        assert!(result);
    }

    #[tokio::test]
    async fn test_evaluate_write_condition_or_operator() {
        let mut tag_conditions = TagConditions::new(vec!["tag1".to_string()]);
        tag_conditions.operator = Some("or".to_string());
        let conditions = ForwardConditions::new(tag_conditions);
        let tags = Some(vec!["tag2".to_string(), "tag1".to_string()]);
        let result = evaluate_write_condition(tags, Some(Box::new(conditions)));
        assert!(result);
    }

    #[tokio::test]
    async fn test_evaluate_write_condition_not_operator() {
        let mut tag_conditions = TagConditions::new(vec!["tag1".to_string()]);
        tag_conditions.operator = Some("not".to_string());
        let conditions = ForwardConditions::new(tag_conditions);
        let tags = Some(vec!["tag2".to_string()]);
        let result = evaluate_write_condition(tags, Some(Box::new(conditions)));
        assert!(result);
    }
}
