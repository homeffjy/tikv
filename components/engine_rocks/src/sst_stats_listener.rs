use std::{
    cmp::Reverse,
    collections::HashSet,
    sync::{Arc, Mutex},
};

use engine_traits::{RangeStats, SstFileStats};
use keyed_priority_queue::KeyedPriorityQueue;
use rocksdb::{CompactionJobInfo, EventListener, FlushJobInfo};
use tikv_util::{info, warn};
use txn_types::TimeStamp;

use crate::mvcc_properties::RocksMvccProperties;

struct SstStatsQueue {
    queue: KeyedPriorityQueue<String, Reverse<SstFileStats>>, // key: file_name
}

impl SstStatsQueue {
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
        if let Some(file_name) = &stats.file_name {
            self.queue.push(file_name.clone(), Reverse(stats));
        }
    }

    pub fn remove_stats(&mut self, file_name: &str) -> Option<SstFileStats> {
        self.queue.remove(file_name).map(|stats| stats.0)
    }

    pub fn pop_before_ts(&mut self, max_ts: u64) -> Vec<SstFileStats> {
        let mut result = Vec::new();

        while let Some((_, stats)) = self.queue.peek() {
            if let Some(min_ts) = stats.0.min_commit_ts {
                if min_ts <= max_ts {
                    if let Some((_, stats)) = self.queue.pop() {
                        result.push(stats.0);
                    }
                } else {
                    break;
                }
            } else {
                break;
            }
        }

        result
    }
}

pub struct SstStatsListener {
    db_name: String,
    stats_queue: Arc<Mutex<SstStatsQueue>>,
}

impl SstStatsListener {
    pub fn new(db_name: &str) -> SstStatsListener {
        SstStatsListener {
            db_name: db_name.to_owned(),
            stats_queue: Arc::new(Mutex::new(SstStatsQueue::new())),
        }
    }

    fn extract_sst_stats(
        &self,
        file_path: &str,
        table_properties: &rocksdb::TableProperties,
    ) -> Option<SstFileStats> {
        let user_props = table_properties.user_collected_properties();

        let mvcc_props = match RocksMvccProperties::decode(user_props) {
            Ok(props) => props,
            Err(e) => {
                warn!(
                    "Failed to decode MVCC properties for SST file: {}, error: {:?}",
                    file_path, e
                );
                return None;
            }
        };

        Some(SstFileStats {
            range_stats: RangeStats {
                num_entries: table_properties.num_entries(),
                num_versions: mvcc_props.num_versions,
                num_rows: mvcc_props.num_rows,
                num_deletes: mvcc_props.num_deletes,
            },
            file_name: None,
            min_commit_ts: if mvcc_props.min_ts == TimeStamp::max() {
                None
            } else {
                Some(mvcc_props.min_ts.into_inner())
            },
        })
    }
}

impl EventListener for SstStatsListener {
    fn on_flush_completed(&self, info: &FlushJobInfo) {
        let table_props = info.table_properties();

        if let Some(stats) = self.extract_sst_stats(
            info.file_path().to_string_lossy().to_string().as_str(),
            table_props,
        ) {
            info!(
                "SST file created by flush";
                "db" => &self.db_name,
                "cf" => info.cf_name(),
                "file" => stats.file_name.as_ref().unwrap(),
                "num_entries" => stats.range_stats.num_entries,
                "num_versions" => stats.range_stats.num_versions,
                "min_commit_ts" => ?stats.min_commit_ts,
            );
            self.stats_queue.lock().unwrap().add_stats(stats);
        }
    }

    fn on_compaction_completed(&self, info: &CompactionJobInfo) {
        if info.status().is_err() {
            return;
        }

        let table_props_map = info.table_properties();

        let mut input_files = HashSet::new();
        for i in 0..info.input_file_count() {
            let file_path = info.input_file_at(i).to_string_lossy().to_string();
            input_files.insert(file_path);
        }

        let mut output_files = HashSet::new();
        for i in 0..info.output_file_count() {
            let file_path = info.output_file_at(i).to_string_lossy().to_string();
            output_files.insert(file_path);
        }

        table_props_map.iter().for_each(|(file_path, table_props)| {
            if input_files.contains(file_path) {
                if let Some(stats) = self.extract_sst_stats(file_path, table_props) {
                    info!(
                        "SST file deleted by compaction";
                        "db" => &self.db_name,
                        "cf" => info.cf_name(),
                        "file" => stats.file_name.as_ref().unwrap(),
                    );
                    self.stats_queue.lock().unwrap().remove_stats(file_path);
                }
                return;
            }
            assert!(output_files.contains(file_path));
            if let Some(stats) = self.extract_sst_stats(file_path, table_props) {
                info!(
                    "SST file created by compaction";
                    "db" => &self.db_name,
                    "cf" => info.cf_name(),
                    "file" => stats.file_name.as_ref().unwrap(),
                );
                self.stats_queue.lock().unwrap().add_stats(stats);
            }
        });
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
            file_name: Some("file1.sst".to_string()),
            min_commit_ts: Some(100),
        };

        let stats2 = SstFileStats {
            range_stats: RangeStats::default(),
            file_name: Some("file2.sst".to_string()),
            min_commit_ts: Some(50),
        };

        queue.add_stats(stats1);
        queue.add_stats(stats2);
        assert_eq!(queue.len(), 2);

        let popped = queue.pop_before_ts(75);
        assert_eq!(popped.len(), 1);
        assert_eq!(popped[0].min_commit_ts, Some(50));
        assert_eq!(queue.len(), 1);

        let removed = queue.remove_stats("file1.sst");
        assert!(removed.is_some());
        assert_eq!(removed.unwrap().min_commit_ts, Some(100));
        assert!(queue.is_empty());
    }
}
