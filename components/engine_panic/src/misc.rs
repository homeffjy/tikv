// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use engine_traits::{
    DeleteStrategy, MiscExt, Range, RangeStats, Result, SstFileStats, StatisticsReporter,
    WriteOptions,
};

use crate::engine::PanicEngine;

pub struct PanicReporter;

impl StatisticsReporter<PanicEngine> for PanicReporter {
    fn new(name: &str) -> Self {
        panic!()
    }

    fn collect(&mut self, engine: &PanicEngine) {
        panic!()
    }

    fn flush(&mut self) {
        panic!()
    }
}

impl MiscExt for PanicEngine {
    type StatisticsReporter = PanicReporter;

    fn flush_cfs(&self, cfs: &[&str], wait: bool) -> Result<()> {
        panic!()
    }

    fn flush_cf(&self, cf: &str, wait: bool) -> Result<()> {
        panic!()
    }

    fn flush_oldest_cf(
        &self,
        wait: bool,
        age_threshold: Option<std::time::SystemTime>,
    ) -> Result<bool> {
        panic!()
    }

    fn delete_ranges_cf(
        &self,
        wopts: &WriteOptions,
        cf: &str,
        strategy: DeleteStrategy,
        ranges: &[Range<'_>],
    ) -> Result<bool> {
        panic!()
    }

    fn get_approximate_memtable_stats_cf(&self, cf: &str, range: &Range<'_>) -> Result<(u64, u64)> {
        panic!()
    }

    fn ingest_maybe_slowdown_writes(&self, cf: &str) -> Result<bool> {
        panic!()
    }

    fn get_engine_used_size(&self) -> Result<u64> {
        panic!()
    }

    fn path(&self) -> &str {
        panic!()
    }

    fn sync_wal(&self) -> Result<()> {
        panic!()
    }

    fn disable_manual_compaction(&self) -> Result<()> {
        panic!()
    }

    fn enable_manual_compaction(&self) -> Result<()> {
        panic!()
    }

    fn pause_background_work(&self) -> Result<()> {
        panic!()
    }

    fn continue_background_work(&self) -> Result<()> {
        panic!()
    }

    fn exists(path: &str) -> bool {
        panic!()
    }

    fn locked(path: &str) -> Result<bool> {
        panic!()
    }

    fn dump_stats(&self) -> Result<String> {
        panic!()
    }

    fn get_latest_sequence_number(&self) -> u64 {
        panic!()
    }

    fn get_oldest_snapshot_sequence_number(&self) -> Option<u64> {
        panic!()
    }

    fn get_total_sst_files_size_cf(&self, cf: &str) -> Result<Option<u64>> {
        panic!()
    }

    fn get_num_keys(&self) -> Result<u64> {
        panic!()
    }

    fn get_range_stats(&self, cf: &str, start: &[u8], end: &[u8]) -> Result<Option<RangeStats>> {
        panic!()
    }

    fn get_range_sst_stats(
        &self,
        cf: &str,
        start: &[u8],
        end: &[u8],
    ) -> Result<Option<Vec<SstFileStats>>> {
        panic!()
    }

    fn is_stalled_or_stopped(&self) -> bool {
        panic!()
    }

    fn get_files_from_sst_stats_queue(
        &self,
        gc_safe_point: u64,
        tombstones_percent_threshold: u64,
        redundant_rows_percent_threshold: u64,
    ) -> Result<Option<Vec<String>>> {
        panic!()
    }

    fn get_active_memtable_stats_cf(
        &self,
        cf: &str,
    ) -> Result<Option<(u64, std::time::SystemTime)>> {
        panic!()
    }

    fn get_accumulated_flush_count_cf(cf: &str) -> Result<u64> {
        panic!()
    }

    type DiskEngine = PanicEngine;
    fn get_disk_engine(&self) -> &Self::DiskEngine {
        panic!()
    }
}
