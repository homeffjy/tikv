// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use engine_traits::{
    CfNamesExt, DeleteStrategy, ImportExt, IterOptions, Iterable, Iterator, MiscExt, Mutable,
    Range, RangeStats, Result, SstFileStats, SstWriter, SstWriterBuilder, WriteBatch,
    WriteBatchExt, WriteOptions,
};
use fail::fail_point;
use rocksdb::{FlushOptions, Range as RocksRange};
use tikv_util::{box_try, keybuilder::KeyBuilder};

use crate::{
    RocksSstWriter,
    engine::RocksEngine,
    r2e,
    rocks_metrics::{RocksStatisticsReporter, STORE_ENGINE_EVENT_COUNTER_VEC},
    rocks_metrics_defs::*,
    sst::RocksSstWriterBuilder,
    util,
};

pub const MAX_DELETE_COUNT_BY_KEY: usize = 2048;

impl RocksEngine {
    fn is_titan(&self) -> bool {
        self.as_inner().is_titan()
    }

    // We store all data which would be deleted in memory at first because the data
    // of region will never be larger than max-region-size.
    fn delete_all_in_range_cf_by_ingest(
        &self,
        wopts: &WriteOptions,
        cf: &str,
        sst_path: String,
        ranges: &[Range<'_>],
        allow_write_during_ingestion: bool,
    ) -> Result<bool> {
        let mut written = false;
        let mut ranges = ranges.to_owned();
        ranges.sort_by(|a, b| a.start_key.cmp(b.start_key));

        let mut writer_wrapper: Option<RocksSstWriter> = None;
        let mut data: Vec<Vec<u8>> = vec![];
        let mut last_end_key: Option<Vec<u8>> = None;
        for r in &ranges {
            // There may be a range overlap with next range
            if last_end_key
                .as_ref()
                .is_some_and(|key| key.as_slice() > r.start_key)
            {
                written |= self.delete_all_in_range_cf_by_key(wopts, cf, r)?;
                continue;
            }
            last_end_key = Some(r.end_key.to_owned());

            let mut opts = IterOptions::new(
                Some(KeyBuilder::from_slice(r.start_key, 0, 0)),
                Some(KeyBuilder::from_slice(r.end_key, 0, 0)),
                false,
            );
            if self.is_titan() {
                // Cause DeleteFilesInRange may expose old blob index keys, setting key only for
                // Titan to avoid referring to missing blob files.
                opts.set_key_only(true);
            }
            let mut it = self.iterator_opt(cf, opts)?;
            let mut it_valid = it.seek(r.start_key)?;
            while it_valid {
                if it.key() >= r.end_key {
                    break;
                }
                if let Some(writer) = writer_wrapper.as_mut() {
                    writer.delete(it.key())?;
                } else {
                    data.push(it.key().to_vec());
                }
                let max_delete_count_by_key = (|| {
                    fail_point!("manually_set_max_delete_count_by_key", |_| { 0 });
                    MAX_DELETE_COUNT_BY_KEY
                })();
                if data.len() > max_delete_count_by_key {
                    let builder = RocksSstWriterBuilder::new().set_db(self).set_cf(cf);
                    let mut writer = builder.build(sst_path.as_str())?;
                    for key in data.iter() {
                        writer.delete(key)?;
                    }
                    data.clear();
                    writer_wrapper = Some(writer);
                }
                it_valid = it.next()?;
            }
        }

        if let Some(writer) = writer_wrapper {
            writer.finish()?;
            let (min_start_key, max_end_key) = ranges.iter().fold(
                (ranges[0].start_key, ranges[0].end_key),
                |(min_start, max_end), range| {
                    (
                        std::cmp::min(min_start, range.start_key),
                        std::cmp::max(max_end, range.end_key),
                    )
                },
            );
            let range_to_lock = if allow_write_during_ingestion {
                Some(Range {
                    start_key: min_start_key,
                    end_key: max_end_key,
                })
            } else {
                None
            };
            self.ingest_external_file_cf(
                cf,
                &[sst_path.as_str()],
                range_to_lock,
                allow_write_during_ingestion,
            )?;
        } else {
            let mut wb = self.write_batch();
            for key in data.iter() {
                if wb.count() >= Self::WRITE_BATCH_MAX_KEYS {
                    wb.write_opt(wopts)?;
                    wb.clear();
                }
                wb.delete_cf(cf, key)?;
            }
            if wb.count() > 0 {
                wb.write_opt(wopts)?;
                written = true;
            }
        }
        Ok(written)
    }

    fn delete_all_in_range_cf_by_key(
        &self,
        wopts: &WriteOptions,
        cf: &str,
        range: &Range<'_>,
    ) -> Result<bool> {
        let start = KeyBuilder::from_slice(range.start_key, 0, 0);
        let end = KeyBuilder::from_slice(range.end_key, 0, 0);
        let mut opts = IterOptions::new(Some(start), Some(end), false);
        if self.is_titan() {
            // Cause DeleteFilesInRange may expose old blob index keys, setting key only for
            // Titan to avoid referring to missing blob files.
            opts.set_key_only(true);
        }
        let mut it = self.iterator_opt(cf, opts)?;
        let mut it_valid = it.seek(range.start_key)?;
        let mut wb = self.write_batch();
        while it_valid {
            if wb.count() >= Self::WRITE_BATCH_MAX_KEYS {
                wb.write_opt(wopts)?;
                wb.clear();
            }
            wb.delete_cf(cf, it.key())?;
            it_valid = it.next()?;
        }
        if wb.count() > 0 {
            wb.write_opt(wopts)?;
            if !wopts.disable_wal() {
                self.sync_wal()?;
            }
            Ok(true)
        } else {
            Ok(false)
        }
    }
}

impl MiscExt for RocksEngine {
    type StatisticsReporter = RocksStatisticsReporter;

    fn flush_cfs(&self, cfs: &[&str], wait: bool) -> Result<()> {
        let mut handles = vec![];
        for cf in cfs {
            handles.push(util::get_cf_handle(self.as_inner(), cf)?);
        }
        if handles.is_empty() {
            for cf in self.cf_names() {
                handles.push(util::get_cf_handle(self.as_inner(), cf)?);
            }
        }
        let mut fopts = FlushOptions::default();
        fopts.set_wait(wait);
        fopts.set_allow_write_stall(true);
        self.as_inner().flush_cfs(&handles, &fopts).map_err(r2e)
    }

    fn flush_cf(&self, cf: &str, wait: bool) -> Result<()> {
        let handle = util::get_cf_handle(self.as_inner(), cf)?;
        let mut fopts = FlushOptions::default();
        fopts.set_wait(wait);
        fopts.set_allow_write_stall(true);
        self.as_inner().flush_cf(handle, &fopts).map_err(r2e)
    }

    // Don't flush if a memtable is just flushed within the threshold.
    fn flush_oldest_cf(
        &self,
        wait: bool,
        age_threshold: Option<std::time::SystemTime>,
    ) -> Result<bool> {
        let cfs = self.cf_names();
        let mut handles = Vec::with_capacity(cfs.len());
        for cf in cfs {
            handles.push(util::get_cf_handle(self.as_inner(), cf)?);
        }
        if let Some((handle, time)) = handles
            .into_iter()
            .filter_map(|handle| {
                self.as_inner()
                    .get_approximate_active_memtable_stats_cf(handle)
                    .map(|(_, time)| (handle, time))
            })
            .min_by(|(_, a), (_, b)| a.cmp(b))
            && age_threshold.is_none_or(|threshold| time <= threshold)
        {
            let mut fopts = FlushOptions::default();
            fopts.set_wait(wait);
            fopts.set_allow_write_stall(true);
            fopts.set_check_if_compaction_disabled(true);
            fopts.set_expected_oldest_key_time(time);
            self.as_inner().flush_cf(handle, &fopts).map_err(r2e)?;
            return Ok(true);
        }
        Ok(false)
    }

    fn delete_ranges_cf(
        &self,
        wopts: &WriteOptions,
        cf: &str,
        strategy: DeleteStrategy,
        ranges: &[Range<'_>],
    ) -> Result<bool> {
        let mut written = false;
        if ranges.is_empty() {
            return Ok(written);
        }
        match strategy {
            DeleteStrategy::DeleteFiles => {
                let handle = util::get_cf_handle(self.as_inner(), cf)?;
                let rocks_ranges: Vec<_> = ranges
                    .iter()
                    .filter_map(|r| {
                        if r.start_key >= r.end_key {
                            None
                        } else {
                            Some(RocksRange::new(r.start_key, r.end_key))
                        }
                    })
                    .collect();
                if rocks_ranges.is_empty() {
                    return Ok(written);
                }
                self.as_inner()
                    .delete_files_in_ranges_cf(handle, &rocks_ranges, false)
                    .map_err(r2e)?;
            }
            DeleteStrategy::DeleteBlobs => {
                let handle = util::get_cf_handle(self.as_inner(), cf)?;
                if self.is_titan() {
                    let rocks_ranges: Vec<_> = ranges
                        .iter()
                        .filter_map(|r| {
                            if r.start_key >= r.end_key {
                                None
                            } else {
                                Some(RocksRange::new(r.start_key, r.end_key))
                            }
                        })
                        .collect();
                    if rocks_ranges.is_empty() {
                        return Ok(written);
                    }
                    self.as_inner()
                        .delete_blob_files_in_ranges_cf(handle, &rocks_ranges, false)
                        .map_err(r2e)?;
                }
            }
            DeleteStrategy::DeleteByRange => {
                let mut wb = self.write_batch();
                for r in ranges.iter() {
                    wb.delete_range_cf(cf, r.start_key, r.end_key)?;
                }
                wb.write_opt(wopts)?;
                written = true;
            }
            DeleteStrategy::DeleteByKey => {
                for r in ranges {
                    written |= self.delete_all_in_range_cf_by_key(wopts, cf, r)?;
                }
            }
            DeleteStrategy::DeleteByWriter {
                sst_path,
                allow_write_during_ingestion,
            } => {
                written |= self.delete_all_in_range_cf_by_ingest(
                    wopts,
                    cf,
                    sst_path,
                    ranges,
                    allow_write_during_ingestion,
                )?;
            }
        }
        Ok(written)
    }

    fn get_approximate_memtable_stats_cf(&self, cf: &str, range: &Range<'_>) -> Result<(u64, u64)> {
        let range = util::range_to_rocks_range(range);
        let handle = util::get_cf_handle(self.as_inner(), cf)?;
        Ok(self
            .as_inner()
            .get_approximate_memtable_stats_cf(handle, &range))
    }

    fn ingest_maybe_slowdown_writes(&self, cf: &str) -> Result<bool> {
        let handle = util::get_cf_handle(self.as_inner(), cf)?;
        if let Some(n) = util::get_cf_num_files_at_level(self.as_inner(), handle, 0) {
            let options = self.as_inner().get_options_cf(handle);
            let slowdown_trigger = options.get_level_zero_slowdown_writes_trigger();
            let compaction_trigger = options.get_level_zero_file_num_compaction_trigger() as u64;
            // Leave enough buffer to tolerate heavy write workload,
            // which may flush some memtables in a short time.
            if n > u64::from(slowdown_trigger) / 2 && n >= compaction_trigger {
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn get_engine_used_size(&self) -> Result<u64> {
        let mut used_size: u64 = 0;
        for cf in self.cf_names() {
            let handle = util::get_cf_handle(self.as_inner(), cf)?;
            used_size += util::get_engine_cf_used_size(self.as_inner(), handle);
        }
        Ok(used_size)
    }

    fn path(&self) -> &str {
        self.as_inner().path()
    }

    fn sync_wal(&self) -> Result<()> {
        self.as_inner().sync_wal().map_err(r2e)
    }

    /// Disables all manual compaction operations.
    ///
    /// After calling this function:
    /// - All incoming manual compaction requests will be rejected
    /// - All pending manual compaction jobs will not be executed
    /// - All in-progress manual compaction jobs will be stopped
    ///
    /// This function should only be used during shutdown to ensure clean
    /// termination.
    fn disable_manual_compaction(&self) -> Result<()> {
        self.as_inner().disable_manual_compaction(true);
        Ok(())
    }

    fn enable_manual_compaction(&self) -> Result<()> {
        self.as_inner().enable_manual_compaction();
        Ok(())
    }

    fn pause_background_work(&self) -> Result<()> {
        // This will make manual compaction return error instead of waiting. In practice
        // we might want to identify this case by parsing error message.
        // WARNING: Setting global manual compaction canceled to false when multiple DB
        // instances exist, as it affects the global state shared across all instances.
        // This could lead to unexpected behavior in other instances.
        self.as_inner().disable_manual_compaction(false);
        self.as_inner().pause_bg_work();
        Ok(())
    }

    fn continue_background_work(&self) -> Result<()> {
        self.enable_manual_compaction()?;
        self.as_inner().continue_bg_work();
        Ok(())
    }

    fn exists(path: &str) -> bool {
        crate::util::db_exist(path)
    }

    fn locked(path: &str) -> Result<bool> {
        let env = rocksdb::Env::default();
        env.is_db_locked(path).map_err(r2e)
    }

    fn dump_stats(&self) -> Result<String> {
        const ROCKSDB_DB_STATS_KEY: &str = "rocksdb.dbstats";
        const ROCKSDB_CF_STATS_KEY: &str = "rocksdb.cfstats";

        let mut s = Vec::with_capacity(1024);
        // common rocksdb stats.
        for name in self.cf_names() {
            let handler = util::get_cf_handle(self.as_inner(), name)?;
            if let Some(v) = self
                .as_inner()
                .get_property_value_cf(handler, ROCKSDB_CF_STATS_KEY)
            {
                s.extend_from_slice(v.as_bytes());
            }
        }

        if let Some(v) = self.as_inner().get_property_value(ROCKSDB_DB_STATS_KEY) {
            s.extend_from_slice(v.as_bytes());
        }

        Ok(box_try!(String::from_utf8(s)))
    }

    fn get_latest_sequence_number(&self) -> u64 {
        self.as_inner().get_latest_sequence_number()
    }

    fn get_oldest_snapshot_sequence_number(&self) -> Option<u64> {
        match self
            .as_inner()
            .get_property_int(ROCKSDB_OLDEST_SNAPSHOT_SEQUENCE)
        {
            // Some(0) indicates that no snapshot is in use
            Some(0) => None,
            s => s,
        }
    }

    fn get_total_sst_files_size_cf(&self, cf: &str) -> Result<Option<u64>> {
        let handle = util::get_cf_handle(self.as_inner(), cf)?;
        Ok(self
            .as_inner()
            .get_property_int_cf(handle, ROCKSDB_TOTAL_SST_FILES_SIZE))
    }

    fn get_num_keys(&self) -> Result<u64> {
        let mut total = 0;
        for cf in self.cf_names() {
            let handle = util::get_cf_handle(self.as_inner(), cf).unwrap();
            total += self
                .as_inner()
                .get_property_int_cf(handle, ROCKSDB_ESTIMATE_NUM_KEYS)
                .unwrap_or_default();
        }
        Ok(total)
    }

    fn get_range_stats(&self, cf: &str, start: &[u8], end: &[u8]) -> Result<Option<RangeStats>> {
        Ok(crate::properties::get_range_stats(self, cf, start, end))
    }

    fn get_range_sst_stats(
        &self,
        cf: &str,
        start: &[u8],
        end: &[u8],
    ) -> Result<Option<Vec<SstFileStats>>> {
        Ok(crate::properties::get_range_sst_stats(self, cf, start, end))
    }

    fn is_stalled_or_stopped(&self) -> bool {
        const ROCKSDB_IS_WRITE_STALLED: &str = "rocksdb.is-write-stalled";
        const ROCKSDB_IS_WRITE_STOPPED: &str = "rocksdb.is-write-stopped";
        self.as_inner()
            .get_property_int(ROCKSDB_IS_WRITE_STALLED)
            .unwrap_or_default()
            != 0
            || self
                .as_inner()
                .get_property_int(ROCKSDB_IS_WRITE_STOPPED)
                .unwrap_or_default()
                != 0
    }

    fn get_active_memtable_stats_cf(
        &self,
        cf: &str,
    ) -> Result<Option<(u64, std::time::SystemTime)>> {
        let handle = util::get_cf_handle(self.as_inner(), cf)?;
        Ok(self
            .as_inner()
            .get_approximate_active_memtable_stats_cf(handle))
    }

    fn get_accumulated_flush_count_cf(cf: &str) -> Result<u64> {
        let n = STORE_ENGINE_EVENT_COUNTER_VEC
            .with_label_values(&["kv", cf, "flush"])
            .get();
        Ok(n)
    }

    type DiskEngine = RocksEngine;
    fn get_disk_engine(&self) -> &Self::DiskEngine {
        self
    }

    fn get_files_from_sst_stats_queue(
        &self,
        gc_safe_point: u64,
        tombstones_percent_threshold: u64,
        redundant_rows_percent_threshold: u64,
    ) -> Result<Option<Vec<String>>> {
        Ok(crate::properties::get_files_from_sst_stats_queue(
            self,
            gc_safe_point,
            tombstones_percent_threshold,
            redundant_rows_percent_threshold,
        ))
    }
}

#[cfg(test)]
mod tests {
    use engine_traits::{
        ALL_CFS, DeleteStrategy, Iterable, Iterator, Mutable, SyncMutable, WriteBatchExt,
    };
    use tempfile::Builder;

    use super::*;
    use crate::{
        RocksCfOptions, RocksDbOptions,
        engine::RocksEngine,
        util::{new_engine, new_engine_opt},
    };

    fn check_data(db: &RocksEngine, cfs: &[&str], expected: &[(&[u8], &[u8])]) {
        for cf in cfs {
            let mut iter = db.iterator(cf).unwrap();
            iter.seek_to_first().unwrap();
            for &(k, v) in expected {
                assert_eq!(k, iter.key());
                assert_eq!(v, iter.value());
                iter.next().unwrap();
            }
            assert!(!iter.valid().unwrap());
        }
    }

    fn test_delete_ranges(strategy: DeleteStrategy, origin_keys: &[Vec<u8>], ranges: &[Range<'_>]) {
        let path = Builder::new()
            .prefix("engine_delete_ranges")
            .tempdir()
            .unwrap();
        let path_str = path.path().to_str().unwrap();

        let db = new_engine(path_str, ALL_CFS).unwrap();

        let mut wb = db.write_batch();
        let ts: u8 = 12;
        let keys: Vec<_> = origin_keys
            .iter()
            .map(|k| {
                let mut k2 = k.clone();
                k2.append(&mut vec![ts; 8]);
                k2
            })
            .collect();

        let mut kvs: Vec<(&[u8], &[u8])> = vec![];
        for key in keys.iter() {
            kvs.push((key.as_slice(), b"value"));
        }
        for &(k, v) in kvs.as_slice() {
            for cf in ALL_CFS {
                wb.put_cf(cf, k, v).unwrap();
            }
        }
        wb.write().unwrap();
        check_data(&db, ALL_CFS, kvs.as_slice());

        db.delete_ranges_cfs(&WriteOptions::default(), strategy, ranges)
            .unwrap();

        let mut kvs_left: Vec<_> = kvs;
        for r in ranges {
            kvs_left.retain(|k| k.0 < r.start_key || k.0 >= r.end_key);
        }
        check_data(&db, ALL_CFS, kvs_left.as_slice());
    }

    #[test]
    fn test_delete_all_in_range_use_delete_range() {
        let data = vec![
            b"k0".to_vec(),
            b"k1".to_vec(),
            b"k2".to_vec(),
            b"k3".to_vec(),
            b"k4".to_vec(),
        ];
        // Single range.
        test_delete_ranges(
            DeleteStrategy::DeleteByRange,
            &data,
            &[Range::new(b"k1", b"k4")],
        );
        // Two ranges without overlap.
        test_delete_ranges(
            DeleteStrategy::DeleteByRange,
            &data,
            &[Range::new(b"k0", b"k1"), Range::new(b"k3", b"k4")],
        );
        // Two ranges with overlap.
        test_delete_ranges(
            DeleteStrategy::DeleteByRange,
            &data,
            &[Range::new(b"k1", b"k3"), Range::new(b"k2", b"k4")],
        );
        // One range contains the other range.
        test_delete_ranges(
            DeleteStrategy::DeleteByRange,
            &data,
            &[Range::new(b"k1", b"k4"), Range::new(b"k2", b"k3")],
        );
    }

    #[test]
    fn test_delete_all_in_range_by_key() {
        let data = vec![
            b"k0".to_vec(),
            b"k1".to_vec(),
            b"k2".to_vec(),
            b"k3".to_vec(),
            b"k4".to_vec(),
        ];
        // Single range.
        test_delete_ranges(
            DeleteStrategy::DeleteByKey,
            &data,
            &[Range::new(b"k1", b"k4")],
        );
        // Two ranges without overlap.
        test_delete_ranges(
            DeleteStrategy::DeleteByKey,
            &data,
            &[Range::new(b"k0", b"k1"), Range::new(b"k3", b"k4")],
        );
        // Two ranges with overlap.
        test_delete_ranges(
            DeleteStrategy::DeleteByKey,
            &data,
            &[Range::new(b"k1", b"k3"), Range::new(b"k2", b"k4")],
        );
        // One range contains the other range.
        test_delete_ranges(
            DeleteStrategy::DeleteByKey,
            &data,
            &[Range::new(b"k1", b"k4"), Range::new(b"k2", b"k3")],
        );
    }

    #[test]
    fn test_delete_all_in_range_by_writer() {
        let path = Builder::new()
            .prefix("test_delete_all_in_range_by_writer")
            .tempdir()
            .unwrap();
        let path_str = path.path();
        let sst_path = path_str.join("tmp_file").to_str().unwrap().to_owned();
        let mut data = vec![];
        for i in 1000..5000 {
            data.push(i.to_string().as_bytes().to_vec());
        }
        test_delete_ranges(
            DeleteStrategy::DeleteByWriter {
                sst_path,
                allow_write_during_ingestion: false,
            },
            &data,
            &[
                Range::new(&data[2], &data[499]),
                Range::new(&data[502], &data[999]),
                Range::new(&data[1002], &data[1999]),
                Range::new(&data[1499], &data[2499]),
                Range::new(&data[2502], &data[3999]),
                Range::new(&data[3002], &data[3499]),
            ],
        );
    }

    #[test]
    fn test_delete_all_files_in_range() {
        let path = Builder::new()
            .prefix("engine_delete_all_files_in_range")
            .tempdir()
            .unwrap();
        let path_str = path.path().to_str().unwrap();

        let cfs_opts = ALL_CFS
            .iter()
            .map(|cf| {
                let mut cf_opts = RocksCfOptions::default();
                cf_opts.set_level_zero_file_num_compaction_trigger(1);
                (*cf, cf_opts)
            })
            .collect();
        let db = new_engine_opt(path_str, RocksDbOptions::default(), cfs_opts).unwrap();

        let keys = vec![b"k1", b"k2", b"k3", b"k4"];

        let mut kvs: Vec<(&[u8], &[u8])> = vec![];
        for key in keys {
            kvs.push((key, b"value"));
        }
        let kvs_left: Vec<(&[u8], &[u8])> = vec![(kvs[0].0, kvs[0].1), (kvs[3].0, kvs[3].1)];
        for cf in ALL_CFS {
            for &(k, v) in kvs.as_slice() {
                db.put_cf(cf, k, v).unwrap();
                db.flush_cf(cf, true).unwrap();
            }
        }
        check_data(&db, ALL_CFS, kvs.as_slice());

        db.delete_ranges_cfs(
            &WriteOptions::default(),
            DeleteStrategy::DeleteFiles,
            &[Range::new(b"k2", b"k4")],
        )
        .unwrap();
        db.delete_ranges_cfs(
            &WriteOptions::default(),
            DeleteStrategy::DeleteBlobs,
            &[Range::new(b"k2", b"k4")],
        )
        .unwrap();
        check_data(&db, ALL_CFS, kvs_left.as_slice());
    }

    #[test]
    fn test_delete_range_prefix_bloom_case() {
        let path = Builder::new()
            .prefix("engine_delete_range_prefix_bloom")
            .tempdir()
            .unwrap();
        let path_str = path.path().to_str().unwrap();

        let mut opts = RocksDbOptions::default();
        opts.create_if_missing(true);
        opts.enable_multi_batch_write(true);

        let mut cf_opts = RocksCfOptions::default();
        // Prefix extractor(trim the timestamp at tail) for write cf.
        cf_opts
            .set_prefix_extractor(
                "FixedSuffixSliceTransform",
                crate::util::FixedSuffixSliceTransform::new(8),
            )
            .unwrap_or_else(|err| panic!("{:?}", err));
        // Create prefix bloom filter for memtable.
        cf_opts.set_memtable_prefix_bloom_size_ratio(0.1_f64);
        let cf = "default";
        let db = new_engine_opt(path_str, opts, vec![(cf, cf_opts)]).unwrap();
        let mut wb = db.write_batch();
        let kvs: Vec<(&[u8], &[u8])> = vec![
            (b"kabcdefg1", b"v1"),
            (b"kabcdefg2", b"v2"),
            (b"kabcdefg3", b"v3"),
            (b"kabcdefg4", b"v4"),
        ];
        let kvs_left: Vec<(&[u8], &[u8])> = vec![(b"kabcdefg1", b"v1"), (b"kabcdefg4", b"v4")];

        for &(k, v) in kvs.as_slice() {
            wb.put_cf(cf, k, v).unwrap();
        }
        wb.write().unwrap();
        check_data(&db, &[cf], kvs.as_slice());

        // Delete all in ["k2", "k4").
        db.delete_ranges_cfs(
            &WriteOptions::default(),
            DeleteStrategy::DeleteByRange,
            &[Range::new(b"kabcdefg2", b"kabcdefg4")],
        )
        .unwrap();
        check_data(&db, &[cf], kvs_left.as_slice());
    }

    #[test]
    fn test_flush_oldest() {
        let path = Builder::new()
            .prefix("test_flush_oldest")
            .tempdir()
            .unwrap();
        let path_str = path.path().to_str().unwrap();

        let mut opts = RocksDbOptions::default();
        opts.create_if_missing(true);

        let db = new_engine(path_str, ALL_CFS).unwrap();
        db.put_cf("default", b"k", b"v").unwrap();
        std::thread::sleep(std::time::Duration::from_secs(1));
        db.put_cf("write", b"k", b"v").unwrap();
        db.put_cf("lock", b"k", b"v").unwrap();
        assert_eq!(
            db.get_total_sst_files_size_cf("default").unwrap().unwrap(),
            0
        );
        assert_eq!(db.get_total_sst_files_size_cf("write").unwrap().unwrap(), 0);
        assert_eq!(db.get_total_sst_files_size_cf("lock").unwrap().unwrap(), 0);
        let now = std::time::SystemTime::now();
        assert!(
            !db.flush_oldest_cf(true, Some(now - std::time::Duration::from_secs(5)))
                .unwrap()
        );
        assert_eq!(
            db.get_total_sst_files_size_cf("default").unwrap().unwrap(),
            0
        );
        assert_eq!(db.get_total_sst_files_size_cf("write").unwrap().unwrap(), 0);
        assert_eq!(db.get_total_sst_files_size_cf("lock").unwrap().unwrap(), 0);
        assert!(
            db.flush_oldest_cf(true, Some(now - std::time::Duration::from_secs(1)))
                .unwrap()
        );
        assert_eq!(db.get_total_sst_files_size_cf("write").unwrap().unwrap(), 0);
        assert_eq!(db.get_total_sst_files_size_cf("lock").unwrap().unwrap(), 0);
        assert!(db.get_total_sst_files_size_cf("default").unwrap().unwrap() > 0);
    }
}
