use std::{
    collections::HashSet,
    sync::{Arc, Mutex},
};

use engine_traits::{RangeStats, SstFileStats};
use rocksdb::{CompactionJobInfo, EventListener, FlushJobInfo};
use tikv_util::{debug, warn};
use txn_types::TimeStamp;

use crate::{SstStatsQueue, mvcc_properties::RocksMvccProperties};

#[derive(Clone)]
pub struct SstStatsListener {
    _db_name: String,
    stats_queue: Arc<Mutex<SstStatsQueue>>,
}

impl SstStatsListener {
    pub fn new(db_name: &str) -> SstStatsListener {
        SstStatsListener {
            _db_name: db_name.to_owned(),
            stats_queue: Arc::new(Mutex::new(SstStatsQueue::new())),
        }
    }

    pub fn stats_queue(&self) -> Arc<Mutex<SstStatsQueue>> {
        self.stats_queue.clone()
    }

    fn extract_sst_stats(
        &self,
        file_path: &str,
        table_properties: &rocksdb::TableProperties,
    ) -> Option<SstFileStats> {
        let user_props = table_properties.user_collected_properties();

        let mvcc_props = match RocksMvccProperties::decode(user_props) {
            Ok(props) => props,
            Err(_) => return None,
        };

        Some(SstFileStats {
            range_stats: RangeStats {
                num_entries: table_properties.num_entries(),
                num_versions: mvcc_props.num_versions,
                num_rows: mvcc_props.num_rows,
                num_deletes: mvcc_props.num_deletes,
            },
            file_name: file_path.to_string(),
            min_commit_ts: if mvcc_props.min_ts == TimeStamp::max() {
                // TimeStamp::max() means no timestamped data found in this SST file.
                // Use u64::MAX to indicate this file should never be considered for GC
                // since there's no MVCC data to garbage collect.
                u64::MAX
            } else {
                mvcc_props.min_ts.into_inner()
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
            debug!("SST file created by flush: {:?}", stats);
            match self.stats_queue.lock() {
                Ok(mut queue) => {
                    queue.add(stats);
                }
                Err(poisoned) => {
                    warn!("SST stats queue mutex is poisoned during flush, attempting to recover");
                    poisoned.into_inner().add(stats);
                }
            }
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
                    debug!("SST file deleted by compaction: {:?}", stats);
                    match self.stats_queue.lock() {
                        Ok(mut queue) => {
                            queue.remove(file_path);
                        }
                        Err(poisoned) => {
                            warn!("SST stats queue mutex is poisoned during compaction remove, attempting to recover");
                            poisoned.into_inner().remove(file_path);
                        }
                    }
                }
                return;
            }
            assert!(output_files.contains(file_path));
            if let Some(stats) = self.extract_sst_stats(file_path, table_props) {
                debug!("SST file created by compaction: {:?}", stats);
                match self.stats_queue.lock() {
                    Ok(mut queue) => {
                        queue.add(stats);
                    }
                    Err(poisoned) => {
                        warn!("SST stats queue mutex is poisoned during compaction add, attempting to recover");
                        poisoned.into_inner().add(stats);
                    }
                }
            }
        });
    }
}
