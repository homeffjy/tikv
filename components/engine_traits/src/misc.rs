// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

//! This trait contains miscellaneous features that have
//! not been carefully factored into other traits.
//!
//! FIXME: Things here need to be moved elsewhere.
use std::cmp::{Ordering, Reverse};

use keyed_priority_queue::KeyedPriorityQueue;

use crate::{
    KvEngine, WriteBatchExt, WriteOptions, cf_names::CfNamesExt, errors::Result,
    flow_control_factors::FlowControlFactorsExt, range::Range,
};

#[derive(Clone, Debug)]
pub enum DeleteStrategy {
    /// Delete the SST files that are fullly fit in range. However, the SST
    /// files that are partially overlapped with the range will not be
    /// touched.
    ///
    /// Note:
    ///    - After this operation, some keys in the range might still exist in
    ///      the database.
    ///    - After this operation, some keys in the range might be removed from
    ///      existing snapshot, so you shouldn't expect to be able to read data
    ///      from the range using existing snapshots any more.
    ///
    /// Ref: <https://github.com/facebook/rocksdb/wiki/Delete-A-Range-Of-Keys>
    DeleteFiles,
    /// Delete the data stored in Titan.
    DeleteBlobs,
    /// Scan for keys and then delete. Useful when we know the keys in range are
    /// not too many.
    DeleteByKey,
    /// Delete by range. Note that this is experimental and you should check
    /// whether it is enbaled in config before using it.
    DeleteByRange,
    /// Delete by ingesting a SST file with deletions. Useful when the number of
    /// ranges is too many.
    /// Set `allow_write_during_ingestion` to true to minimize the impact on
    /// foreground performance, but you must ensure that no concurrent
    /// writes overlap with the data being ingested.
    DeleteByWriter {
        sst_path: String,
        allow_write_during_ingestion: bool,
    },
}

/// `StatisticsReporter` can be used to report engine's private statistics to
/// prometheus metrics. For one single engine, using it is equivalent to calling
/// `KvEngine::flush_metrics("name")`. For multiple engines, it can aggregate
/// statistics accordingly.
/// Note that it is not responsible for managing the statistics from
/// user-provided collectors that are potentially shared between engines.
pub trait StatisticsReporter<T: ?Sized> {
    fn new(name: &str) -> Self;

    /// Collect statistics from one single engine.
    fn collect(&mut self, engine: &T);

    /// Aggregate and report statistics to prometheus metrics counters. The
    /// statistics are not cleared afterwards.
    fn flush(&mut self);
}

#[derive(Default, Debug)]
pub struct RangeStats {
    // The number of entries in write cf.
    pub num_entries: u64,
    // The number of MVCC versions of all rows (num_entries - tombstones).
    pub num_versions: u64,
    // The number of rows.
    pub num_rows: u64,
    // The number of MVCC deletes of all rows.
    pub num_deletes: u64,
}

impl RangeStats {
    /// The number of redundant keys in the range.
    /// It's calculated by `num_entries - num_versions + num_deleted`.
    pub fn redundant_keys(&self) -> u64 {
        // Consider the number of `mvcc_deletes` as the number of redundant keys.
        self.num_entries
            .saturating_sub(self.num_rows)
            .saturating_add(self.num_deletes)
    }
}

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

    pub fn add_stats(&mut self, stats: SstFileStats) {
        self.queue.push(stats.file_name.clone(), Reverse(stats));
    }

    pub fn remove_stats(&mut self, file_name: &str) -> Option<SstFileStats> {
        self.queue.remove(file_name).map(|stats| stats.0)
    }

    pub fn pop_before_ts(&mut self, max_ts: u64) -> Vec<SstFileStats> {
        let mut result = Vec::new();

        while let Some((_, stats)) = self.queue.peek() {
            if stats.0.min_commit_ts <= max_ts {
                if let Some((_, stats)) = self.queue.pop() {
                    result.push(stats.0);
                }
            } else {
                break;
            }
        }

        result
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

        queue.add_stats(stats1);
        queue.add_stats(stats2);
        queue.add_stats(stats3);
        assert_eq!(queue.len(), 3);

        let popped = queue.pop_before_ts(75);
        assert_eq!(popped.len(), 2);
        // Should get both files with min_commit_ts = 50
        assert!(popped.iter().any(|s| s.file_name == "file2.sst" && s.min_commit_ts == 50));
        assert!(popped.iter().any(|s| s.file_name == "file3.sst" && s.min_commit_ts == 50));
        assert_eq!(queue.len(), 1);

        let removed = queue.remove_stats("file1.sst");
        assert!(removed.is_some());
        assert_eq!(removed.unwrap().min_commit_ts, 100);
        assert!(queue.is_empty());

        // Test ordering with same min_commit_ts
        let stats_a = SstFileStats {
            range_stats: RangeStats::default(),
            file_name: "a.sst".to_string(),
            min_commit_ts: 100,
        };
        let stats_b = SstFileStats {
            range_stats: RangeStats::default(),
            file_name: "b.sst".to_string(),
            min_commit_ts: 100,
        };

        // Files with same min_commit_ts should not be considered equal
        assert_ne!(stats_a, stats_b);
        
        queue.add_stats(stats_a);
        queue.add_stats(stats_b);
        assert_eq!(queue.len(), 2);
    }

    #[test]
    fn test_sst_stats_queue_timestamp_max_handling() {
        let mut queue = SstStatsQueue::new();
        
        // Test file with no timestamped data (should use u64::MAX for min_commit_ts)
        let stats_no_timestamp = SstFileStats {
            range_stats: RangeStats::default(),
            file_name: "no_timestamp.sst".to_string(),
            min_commit_ts: u64::MAX, // This would be set when mvcc_props.min_ts == TimeStamp::max()
        };

        // Test file with actual timestamp data
        let stats_with_timestamp = SstFileStats {
            range_stats: RangeStats::default(),
            file_name: "with_timestamp.sst".to_string(),
            min_commit_ts: 100,
        };

        queue.add_stats(stats_no_timestamp);
        queue.add_stats(stats_with_timestamp);
        assert_eq!(queue.len(), 2);

        // When gc_safe_point is very high, only files with actual timestamps should be returned
        let popped = queue.pop_before_ts(u64::MAX - 1);
        assert_eq!(popped.len(), 1);
        assert_eq!(popped[0].file_name, "with_timestamp.sst");
        assert_eq!(popped[0].min_commit_ts, 100);
        
        // The file with u64::MAX min_commit_ts should remain in queue
        assert_eq!(queue.len(), 1);
        
        // Only when gc_safe_point is u64::MAX should the remaining file be returned
        let popped2 = queue.pop_before_ts(u64::MAX);
        assert_eq!(popped2.len(), 1);
        assert_eq!(popped2[0].file_name, "no_timestamp.sst");
        assert_eq!(popped2[0].min_commit_ts, u64::MAX);
        
        assert!(queue.is_empty());
    }
}

pub trait MiscExt: CfNamesExt + FlowControlFactorsExt + WriteBatchExt {
    type StatisticsReporter: StatisticsReporter<Self>;

    /// Flush all specified column families at once.
    ///
    /// If `cfs` is empty, it will try to flush all available column families.
    fn flush_cfs(&self, cfs: &[&str], wait: bool) -> Result<()>;

    fn flush_cf(&self, cf: &str, wait: bool) -> Result<()>;

    /// Returns `false` if all memtables are created after `threshold`.
    fn flush_oldest_cf(&self, wait: bool, threshold: Option<std::time::SystemTime>)
    -> Result<bool>;

    /// Returns whether there's data written through kv interface.
    fn delete_ranges_cfs(
        &self,
        wopts: &WriteOptions,
        strategy: DeleteStrategy,
        ranges: &[Range<'_>],
    ) -> Result<bool> {
        let mut written = false;
        for cf in self.cf_names() {
            written |= self.delete_ranges_cf(wopts, cf, strategy.clone(), ranges)?;
        }
        Ok(written)
    }

    /// Returns whether there's data written through kv interface.
    fn delete_ranges_cf(
        &self,
        wopts: &WriteOptions,
        cf: &str,
        strategy: DeleteStrategy,
        ranges: &[Range<'_>],
    ) -> Result<bool>;

    /// Return the approximate number of records and size in the range of
    /// memtables of the cf.
    fn get_approximate_memtable_stats_cf(&self, cf: &str, range: &Range<'_>) -> Result<(u64, u64)>;

    fn ingest_maybe_slowdown_writes(&self, cf: &str) -> Result<bool>;

    /// Gets total used size of rocksdb engine, including:
    /// * total size (bytes) of all SST files.
    /// * total size (bytes) of active and unflushed immutable memtables.
    /// * total size (bytes) of all blob files.
    fn get_engine_used_size(&self) -> Result<u64>;

    /// The path to the directory on the filesystem where the database is stored
    fn path(&self) -> &str;

    fn sync_wal(&self) -> Result<()>;

    /// Disable manual compactions, some on-going manual compactions may be
    /// aborted.
    fn disable_manual_compaction(&self) -> Result<()>;

    fn enable_manual_compaction(&self) -> Result<()>;

    /// Depending on the implementation, some on-going manual compactions may be
    /// aborted.
    fn pause_background_work(&self) -> Result<()>;

    fn continue_background_work(&self) -> Result<()>;

    /// Check whether a database exists at a given path
    fn exists(path: &str) -> bool;

    fn locked(path: &str) -> Result<bool>;

    /// Dump stats about the database into a string.
    ///
    /// For debugging. The format and content is unspecified.
    fn dump_stats(&self) -> Result<String>;

    fn get_latest_sequence_number(&self) -> u64;

    fn get_oldest_snapshot_sequence_number(&self) -> Option<u64>;

    fn get_total_sst_files_size_cf(&self, cf: &str) -> Result<Option<u64>>;

    fn get_num_keys(&self) -> Result<u64>;

    fn get_range_stats(&self, cf: &str, start: &[u8], end: &[u8]) -> Result<Option<RangeStats>>;

    fn get_range_sst_stats(
        &self,
        cf: &str,
        start: &[u8],
        end: &[u8],
    ) -> Result<Option<Vec<SstFileStats>>>;

    fn is_stalled_or_stopped(&self) -> bool;

    /// Get files from SST stats queue that need compaction.
    /// Returns file names of SST files where the minimum commit timestamp is less than
    /// gc_safe_point and that meet the compaction criteria.
    fn get_files_from_sst_stats_queue(
        &self,
        gc_safe_point: u64,
        tombstones_percent_threshold: u64,
        redundant_rows_percent_threshold: u64,
    ) -> Result<Option<Vec<String>>>;

    /// Returns size and creation time of active memtable if there's one.
    fn get_active_memtable_stats_cf(
        &self,
        cf: &str,
    ) -> Result<Option<(u64, std::time::SystemTime)>>;

    /// Whether there's active memtable with creation time older than
    /// `threshold`.
    fn has_old_active_memtable(&self, threshold: std::time::SystemTime) -> bool {
        for cf in self.cf_names() {
            if let Ok(Some((_, age))) = self.get_active_memtable_stats_cf(cf) {
                if age < threshold {
                    return true;
                }
            }
        }
        false
    }

    // Global method.
    fn get_accumulated_flush_count_cf(cf: &str) -> Result<u64>;

    fn get_accumulated_flush_count() -> Result<u64> {
        let mut n = 0;
        for cf in crate::ALL_CFS {
            n += Self::get_accumulated_flush_count_cf(cf)?;
        }
        Ok(n)
    }

    type DiskEngine: KvEngine;
    fn get_disk_engine(&self) -> &Self::DiskEngine;
}
