use std::collections::HashSet;

use engine_traits::{RangeStats, SstFileStats};
use rocksdb::{CompactionJobInfo, EventListener, FlushJobInfo};
use tikv_util::{info, warn};
use txn_types::TimeStamp;

use crate::mvcc_properties::RocksMvccProperties;

pub struct SstStatsListener {
    db_name: String,
}

impl SstStatsListener {
    pub fn new(db_name: &str) -> SstStatsListener {
        SstStatsListener {
            db_name: db_name.to_owned(),
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
            // TODO(fjy): add stats to a priority queue.
            info!(
                "SST file created by flush";
                "db" => &self.db_name,
                "cf" => info.cf_name(),
                "file" => &stats.file_name,
                "num_entries" => stats.range_stats.num_entries,
                "num_versions" => stats.range_stats.num_versions,
                "min_commit_ts" => ?stats.min_commit_ts,
            );
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
                // TODO(fjy): remove stats from a priority queue.
                if let Some(stats) = self.extract_sst_stats(file_path, table_props) {
                    info!(
                        "SST file deleted by compaction";
                        "db" => &self.db_name,
                        "cf" => info.cf_name(),
                        "file" => &stats.file_name,
                    );
                }
                return;
            }
            assert!(output_files.contains(file_path));
            if let Some(stats) = self.extract_sst_stats(file_path, table_props) {
                // TODO(fjy): add stats to a priority queue.
                info!(
                    "SST file created by compaction";
                    "db" => &self.db_name,
                    "cf" => info.cf_name(),
                    "file" => &stats.file_name,
                );
            }
        });
    }
}
