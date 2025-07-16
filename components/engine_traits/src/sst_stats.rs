use std::cmp::{Ordering, Reverse};

use keyed_priority_queue::KeyedPriorityQueue;

use crate::RangeStats;

#[derive(Default, Debug)]
pub struct SstFileStats {
    pub range_stats: RangeStats,
    pub file_name: String,
    pub min_commit_ts: u64,
}

impl PartialEq for SstFileStats {
    fn eq(&self, other: &Self) -> bool {
        self.min_commit_ts == other.min_commit_ts && self.file_name == other.file_name
    }
}

impl Eq for SstFileStats {}

impl PartialOrd for SstFileStats {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        // First compare by min_commit_ts
        match self.min_commit_ts.partial_cmp(&other.min_commit_ts) {
            Some(Ordering::Equal) => {
                // If min_commit_ts is equal, compare by file_name for deterministic ordering
                self.file_name.partial_cmp(&other.file_name)
            }
            other => other,
        }
    }
}

impl Ord for SstFileStats {
    fn cmp(&self, other: &Self) -> Ordering {
        // First compare by min_commit_ts
        match self.min_commit_ts.cmp(&other.min_commit_ts) {
            Ordering::Equal => {
                // If min_commit_ts is equal, compare by file_name for deterministic ordering
                self.file_name.cmp(&other.file_name)
            }
            other => other,
        }
    }
}

#[derive(Debug)]
pub struct SstStatsQueue {
    queue: KeyedPriorityQueue<String, Reverse<SstFileStats>>, // key: file_name
}

impl SstStatsQueue {
    pub fn debug(&self) -> String {
        let mut result = String::new();
        for (file_name, stats) in self.queue.iter() {
            result.push_str(&format!("{}: {:?}\n", file_name, stats.0.min_commit_ts));
        }
        result
    }

    pub fn new() -> Self {
        Self {
            queue: KeyedPriorityQueue::new(),
        }
    }

    pub fn len(&self) -> usize {
        self.queue.len()
    }

    pub fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }

    pub fn add(&mut self, stats: SstFileStats) {
        self.queue.push(stats.file_name.clone(), Reverse(stats));
    }

    pub fn remove(&mut self, file_name: &str) -> Option<SstFileStats> {
        self.queue.remove(file_name).map(|stats| stats.0)
    }

    pub fn pop_before_ts(&mut self, max_ts: u64) -> Option<SstFileStats> {
        if let Some((_, stats)) = self.queue.peek() {
            if stats.0.min_commit_ts <= max_ts {
                if let Some((_, stats)) = self.queue.pop() {
                    return Some(stats.0);
                }
            } else {
                return None;
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sst_stats_queue() {
        let mut queue = SstStatsQueue::new();
        assert!(queue.is_empty());

        let stats1 = SstFileStats {
            range_stats: RangeStats::default(),
            file_name: "file1.sst".to_string(),
            min_commit_ts: 100,
        };

        let stats2 = SstFileStats {
            range_stats: RangeStats::default(),
            file_name: "file2.sst".to_string(),
            min_commit_ts: 50,
        };

        // Test with same min_commit_ts but different file names
        let stats3 = SstFileStats {
            range_stats: RangeStats::default(),
            file_name: "file3.sst".to_string(),
            min_commit_ts: 50,
        };

        queue.add(stats1);
        queue.add(stats2);
        queue.add(stats3);
        assert_eq!(queue.len(), 3);

        // [2, 3, 1] -> [3, 1]
        let popped = queue.pop_before_ts(75);
        assert!(popped.is_some());
        assert_eq!(popped.unwrap().file_name, "file2.sst");
        assert_eq!(queue.len(), 2);

        // [3, 1] -> [3]
        let removed = queue.remove("file1.sst");
        assert!(removed.is_some());
        assert_eq!(removed.unwrap().min_commit_ts, 100);
        assert_eq!(queue.len(), 1);

        let stats4 = SstFileStats {
            range_stats: RangeStats::default(),
            file_name: "file4.sst".to_string(),
            min_commit_ts: 150,
        };

        // [3] -> [3, 4]
        queue.add(stats4);
        assert_eq!(queue.len(), 2);

        // [3, 4] -> [4]
        let popped = queue.pop_before_ts(125);
        assert!(popped.is_some());
        assert_eq!(queue.len(), 1);
        assert_eq!(popped.unwrap().file_name, "file3.sst");

        // [4] -> [4]
        let popped = queue.pop_before_ts(125);
        assert!(popped.is_none());
        assert_eq!(queue.len(), 1);
    }
}
