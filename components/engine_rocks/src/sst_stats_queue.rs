use std::cmp::Reverse;

use engine_traits::SstFileStats;
use keyed_priority_queue::KeyedPriorityQueue;

#[derive(Debug)]
pub struct SstStatsQueue {
    queue: KeyedPriorityQueue<String, Reverse<SstFileStats>>, // key: file_name
    tombstones_queue: KeyedPriorityQueue<String, u64>,        // on tombstone score
}

impl SstStatsQueue {
    pub fn debug(&self) -> String {
        let mut result = String::new();
        result.push_str("***queue***:");
        for (file_name, stats) in self.queue.iter() {
            result.push_str(&format!("{}: {:?}", file_name, stats.0.min_commit_ts));
        }
        result.push_str("**tombstones***:");
        for (file_name, score) in self.tombstones_queue.iter() {
            result.push_str(&format!("{}: {:?}\n", file_name, score));
        }
        result
    }

    pub fn new() -> Self {
        Self {
            queue: KeyedPriorityQueue::new(),
            tombstones_queue: KeyedPriorityQueue::new(),
        }
    }

    pub fn len(&self) -> usize {
        self.queue.len()
    }

    pub fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }

    pub fn add(&mut self, stats: SstFileStats) {
        let file_name = stats.file_name.clone();
        let score = self.compute_tombstone_score(&stats);
        self.queue.push(file_name.clone(), Reverse(stats));
        self.tombstones_queue.push(file_name, score);
    }

    pub fn remove(&mut self, file_name: &str) -> Option<SstFileStats> {
        self.tombstones_queue.remove(file_name);
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

    pub fn peek_tombstone(&self) -> Option<(&String, &u64)> {
        self.tombstones_queue
            .peek()
            .map(|(key, score)| (key, score))
    }

    pub fn pop_tombstone(&mut self) -> Option<(String, u64)> {
        let (key, score) = self.tombstones_queue.pop()?;
        self.queue.remove(&key);
        Some((key, score))
    }

    fn compute_tombstone_score(&self, stats: &SstFileStats) -> u64 {
        if stats.range_stats.num_entries < stats.range_stats.num_versions {
            return 0;
        }
        let num_del = stats.range_stats.num_entries - stats.range_stats.num_versions;
        let num_ent = stats.range_stats.num_entries.max(1);
        (num_del * 100 / num_ent) as u64
    }
}

#[cfg(test)]
mod tests {
    use engine_traits::RangeStats;

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

    #[test]
    fn test_tombstone_score() {
        let mut queue = SstStatsQueue::new();
        let stats = SstFileStats {
            range_stats: RangeStats {
                num_entries: 100,
                num_versions: 100,
                num_rows: 100,
                num_deletes: 100,
            },
            file_name: "file1.sst".to_string(),
            min_commit_ts: 100,
        };

        let stats2 = SstFileStats {
            range_stats: RangeStats {
                num_entries: 100,
                num_versions: 100,
                num_rows: 100,
                num_deletes: 0,
            },
            file_name: "file2.sst".to_string(),
            min_commit_ts: 101,
        };

        queue.add(stats);
        queue.add(stats2);

        let (key, score) = queue.peek_tombstone().unwrap();
        assert_eq!(key, "file1.sst");
        assert_eq!(*score, 100u64);
        let (key, score) = queue.pop_tombstone().unwrap();
        assert_eq!(key, "file1.sst");
        assert_eq!(score, 100);

        let (key, score) = queue.peek_tombstone().unwrap();
        assert_eq!(key, "file2.sst");
        assert_eq!(*score, 0);
        let (key, score) = queue.pop_tombstone().unwrap();
        assert_eq!(key, "file2.sst");
        assert_eq!(score, 0);
        assert_eq!(queue.peek_tombstone(), None);
        assert_eq!(queue.len(), 0);
    }
}
