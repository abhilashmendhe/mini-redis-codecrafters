pub mod zadd_ops;
pub mod zrank_ops;
pub mod zrange_ops;
pub mod zcard_ops;

use std::cmp::Ordering;

#[derive(Debug, PartialEq, Clone)]
pub struct SortedSetValues {
    pub v1: f64,
    pub v2: String
}

impl Eq for SortedSetValues {}

impl Ord for SortedSetValues {
    fn cmp(&self, other: &Self) -> Ordering {
        // First compare by v1
        match self.v1.partial_cmp(&other.v1) {
            Some(ord) => {
                if ord == Ordering::Equal {
                    self.v2.cmp(&other.v2)
                } else {
                    ord
                }
            }
            None => Ordering::Equal, 
        }
    }
}

impl PartialOrd for SortedSetValues {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
