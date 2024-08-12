use std::time::Duration;

/// A Fixed Interval strategy which repeats itself every X duration.
pub struct Interval {
    interval: Duration,
}

impl Interval {
    // Build interval from Duration
    pub fn new(interval: Duration) -> Self {
        Self { interval }
    }

    // Build interval from millis
    pub fn from_millis(millis: u64) -> Self {
        Self {
            interval: Duration::from_millis(millis),
        }
    }
}

impl Iterator for Interval {
    type Item = Duration;

    fn next(&mut self) -> Option<Self::Item> {
        Some(self.interval)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new() {
        let mut interval = Interval::new(Duration::from_millis(1));
        assert_eq!(interval.next(), Some(Duration::from_millis(1)));
    }

    #[test]
    fn from_millis() {
        let mut interval = Interval::from_millis(1);
        assert_eq!(interval.next(), Some(Duration::from_millis(1)));
        assert_eq!(interval.next(), Some(Duration::from_millis(1)));
    }
}
