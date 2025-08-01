// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    cmp,
    collections::HashMap,
    io::Read,
    ops::{Deref, DerefMut},
};

use api_version::{ApiV2, KeyMode, KvFormat};
use engine_traits::{MvccProperties, Range, RangeStats, SstFileStats, raw_ttl::ttl_current_ts};
use rocksdb::{
    DBEntryType, TablePropertiesCollector, TablePropertiesCollectorFactory, TitanBlobIndex,
    UserCollectedProperties,
};
use tikv_util::{
    codec::{
        Error, Result,
        number::{self, NumberEncoder},
    },
    debug, info, warn,
};
use txn_types::{Key, TimeStamp, Write, WriteType};

use crate::{
    decode_properties::{DecodeProperties, IndexHandle, IndexHandles},
    mvcc_properties::*,
};

const PROP_TOTAL_SIZE: &str = "tikv.total_size";
const PROP_SIZE_INDEX: &str = "tikv.size_index";
const PROP_RANGE_INDEX: &str = "tikv.range_index";
pub const DEFAULT_PROP_SIZE_INDEX_DISTANCE: u64 = 4 * 1024 * 1024;
pub const DEFAULT_PROP_KEYS_INDEX_DISTANCE: u64 = 40 * 1024;

fn get_entry_size(value: &[u8], entry_type: DBEntryType) -> std::result::Result<u64, ()> {
    match entry_type {
        DBEntryType::Put => Ok(value.len() as u64),
        DBEntryType::BlobIndex => match TitanBlobIndex::decode(value) {
            Ok(index) => Ok(index.blob_size + value.len() as u64),
            Err(_) => Err(()),
        },
        _ => Err(()),
    }
}

// Deprecated. Only for compatible issue from v2.0 or older version.
#[derive(Debug, Default)]
pub struct SizeProperties {
    pub total_size: u64,
    pub index_handles: IndexHandles,
}

impl SizeProperties {
    pub fn encode(&self) -> UserProperties {
        let mut props = UserProperties::new();
        props.encode_u64(PROP_TOTAL_SIZE, self.total_size);
        props.encode_handles(PROP_SIZE_INDEX, &self.index_handles);
        props
    }

    pub fn decode<T: DecodeProperties>(props: &T) -> Result<SizeProperties> {
        Ok(SizeProperties {
            total_size: props.decode_u64(PROP_TOTAL_SIZE)?,
            index_handles: props.decode_handles(PROP_SIZE_INDEX)?,
        })
    }
}

pub struct UserProperties(pub HashMap<Vec<u8>, Vec<u8>>);

impl Deref for UserProperties {
    type Target = HashMap<Vec<u8>, Vec<u8>>;
    fn deref(&self) -> &HashMap<Vec<u8>, Vec<u8>> {
        &self.0
    }
}

impl DerefMut for UserProperties {
    fn deref_mut(&mut self) -> &mut HashMap<Vec<u8>, Vec<u8>> {
        &mut self.0
    }
}

impl UserProperties {
    pub fn new() -> UserProperties {
        UserProperties(HashMap::new())
    }

    fn encode(&mut self, name: &str, value: Vec<u8>) {
        self.insert(name.as_bytes().to_owned(), value);
    }

    pub fn encode_u64(&mut self, name: &str, value: u64) {
        let mut buf = Vec::with_capacity(8);
        buf.encode_u64(value).unwrap();
        self.encode(name, buf);
    }

    pub fn encode_handles(&mut self, name: &str, handles: &IndexHandles) {
        self.encode(name, handles.encode())
    }
}

impl Default for UserProperties {
    fn default() -> Self {
        Self::new()
    }
}

impl DecodeProperties for UserProperties {
    fn decode(&self, k: &str) -> Result<&[u8]> {
        match self.0.get(k.as_bytes()) {
            Some(v) => Ok(v.as_slice()),
            None => Err(Error::KeyNotFound),
        }
    }
}

// FIXME: This is a temporary hack to implement a foreign trait on a foreign
// type until the engine abstraction situation is straightened out.
pub struct UserCollectedPropertiesDecoder<'a>(pub &'a UserCollectedProperties);

impl DecodeProperties for UserCollectedPropertiesDecoder<'_> {
    fn decode(&self, k: &str) -> Result<&[u8]> {
        match self.0.get(k.as_bytes()) {
            Some(v) => Ok(v),
            None => Err(Error::KeyNotFound),
        }
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct RangeOffsets {
    pub size: u64,
    pub keys: u64,
}

#[derive(Debug, Default)]
pub struct RangeProperties {
    pub offsets: Vec<(Vec<u8>, RangeOffsets)>,
}

impl RangeProperties {
    pub fn get(&self, key: &[u8]) -> &RangeOffsets {
        let idx = self.offsets.binary_search_by_key(&key, |(k, _)| k).unwrap();
        &self.offsets[idx].1
    }

    pub fn encode(&self) -> UserProperties {
        let mut buf = Vec::with_capacity(1024);
        for (k, offsets) in &self.offsets {
            buf.encode_u64(k.len() as u64).unwrap();
            buf.extend(k);
            buf.encode_u64(offsets.size).unwrap();
            buf.encode_u64(offsets.keys).unwrap();
        }
        let mut props = UserProperties::new();
        props.encode(PROP_RANGE_INDEX, buf);
        props
    }

    pub fn decode<T: DecodeProperties>(props: &T) -> Result<RangeProperties> {
        match RangeProperties::decode_from_range_properties(props) {
            Ok(res) => return Ok(res),
            Err(e) => info!(
                "decode to RangeProperties failed with err: {:?}, try to decode to SizeProperties, maybe upgrade from v2.0 or older version?",
                e
            ),
        }
        SizeProperties::decode(props).map(|res| res.into())
    }

    fn decode_from_range_properties<T: DecodeProperties>(props: &T) -> Result<RangeProperties> {
        let mut res = RangeProperties::default();
        let mut buf = props.decode(PROP_RANGE_INDEX)?;
        while !buf.is_empty() {
            let klen = number::decode_u64(&mut buf)?;
            let mut k = vec![0; klen as usize];
            buf.read_exact(&mut k)?;
            let offsets = RangeOffsets {
                size: number::decode_u64(&mut buf)?,
                keys: number::decode_u64(&mut buf)?,
            };
            res.offsets.push((k, offsets));
        }
        Ok(res)
    }

    pub fn get_approximate_size_in_range(&self, start: &[u8], end: &[u8]) -> u64 {
        self.get_approximate_distance_in_range(start, end).0
    }

    pub fn get_approximate_keys_in_range(&self, start: &[u8], end: &[u8]) -> u64 {
        self.get_approximate_distance_in_range(start, end).1
    }

    /// Returns `size` and `keys`.
    pub fn get_approximate_distance_in_range(&self, start: &[u8], end: &[u8]) -> (u64, u64) {
        assert!(start <= end);
        if start == end {
            return (0, 0);
        }
        let start_offset = match self.offsets.binary_search_by_key(&start, |(k, _)| k) {
            Ok(idx) => Some(idx),
            Err(next_idx) => next_idx.checked_sub(1),
        };
        let end_offset = match self.offsets.binary_search_by_key(&end, |(k, _)| k) {
            Ok(idx) => Some(idx),
            Err(next_idx) => next_idx.checked_sub(1),
        };
        let start = start_offset.map_or_else(|| Default::default(), |x| self.offsets[x].1);
        let end = end_offset.map_or_else(|| Default::default(), |x| self.offsets[x].1);
        assert!(end.size >= start.size && end.keys >= start.keys);
        (end.size - start.size, end.keys - start.keys)
    }

    // equivalent to range(Excluded(start_key), Excluded(end_key))
    pub fn take_excluded_range(
        mut self,
        start_key: &[u8],
        end_key: &[u8],
    ) -> Vec<(Vec<u8>, RangeOffsets)> {
        let start_offset = match self.offsets.binary_search_by_key(&start_key, |(k, _)| k) {
            Ok(idx) => {
                if idx == self.offsets.len() - 1 {
                    return vec![];
                } else {
                    idx + 1
                }
            }
            Err(next_idx) => next_idx,
        };

        let end_offset = match self.offsets.binary_search_by_key(&end_key, |(k, _)| k) {
            Ok(idx) => {
                if idx == 0 {
                    return vec![];
                } else {
                    idx - 1
                }
            }
            Err(next_idx) => {
                if next_idx == 0 {
                    return vec![];
                } else {
                    next_idx - 1
                }
            }
        };

        if start_offset > end_offset {
            return vec![];
        }

        self.offsets.drain(start_offset..=end_offset).collect()
    }

    pub fn smallest_key(&self) -> Option<Vec<u8>> {
        self.offsets.first().map(|(k, _)| k.to_owned())
    }

    pub fn largest_key(&self) -> Option<Vec<u8>> {
        self.offsets.last().map(|(k, _)| k.to_owned())
    }
}

impl From<SizeProperties> for RangeProperties {
    fn from(p: SizeProperties) -> RangeProperties {
        let mut res = RangeProperties::default();
        for (key, size_handle) in p.index_handles.into_map() {
            let range = RangeOffsets {
                size: size_handle.offset,
                ..Default::default()
            };
            res.offsets.push((key, range));
        }
        res
    }
}

pub struct RangePropertiesCollector {
    props: RangeProperties,
    last_offsets: RangeOffsets,
    last_key: Vec<u8>,
    cur_offsets: RangeOffsets,
    prop_size_index_distance: u64,
    prop_keys_index_distance: u64,
}

impl Default for RangePropertiesCollector {
    fn default() -> Self {
        RangePropertiesCollector {
            props: RangeProperties::default(),
            last_offsets: RangeOffsets::default(),
            last_key: vec![],
            cur_offsets: RangeOffsets::default(),
            prop_size_index_distance: DEFAULT_PROP_SIZE_INDEX_DISTANCE,
            prop_keys_index_distance: DEFAULT_PROP_KEYS_INDEX_DISTANCE,
        }
    }
}

impl RangePropertiesCollector {
    pub fn new(prop_size_index_distance: u64, prop_keys_index_distance: u64) -> Self {
        RangePropertiesCollector {
            prop_size_index_distance,
            prop_keys_index_distance,
            ..Default::default()
        }
    }

    fn size_in_last_range(&self) -> u64 {
        self.cur_offsets.size - self.last_offsets.size
    }

    fn keys_in_last_range(&self) -> u64 {
        self.cur_offsets.keys - self.last_offsets.keys
    }

    fn insert_new_point(&mut self, key: Vec<u8>) {
        self.last_offsets = self.cur_offsets;
        self.props.offsets.push((key, self.cur_offsets));
    }
}

impl TablePropertiesCollector for RangePropertiesCollector {
    fn add(&mut self, key: &[u8], value: &[u8], entry_type: DBEntryType, _: u64, _: u64) {
        // size
        let size = match get_entry_size(value, entry_type) {
            Ok(entry_size) => key.len() as u64 + entry_size,
            Err(_) => return,
        };
        self.cur_offsets.size += size;
        // keys
        self.cur_offsets.keys += 1;
        // Add the start key for convenience.
        if self.last_key.is_empty()
            || self.size_in_last_range() >= self.prop_size_index_distance
            || self.keys_in_last_range() >= self.prop_keys_index_distance
        {
            self.insert_new_point(key.to_owned());
        }
        self.last_key.clear();
        self.last_key.extend_from_slice(key);
    }

    fn finish(&mut self) -> HashMap<Vec<u8>, Vec<u8>> {
        if self.size_in_last_range() > 0 || self.keys_in_last_range() > 0 {
            let key = self.last_key.clone();
            self.insert_new_point(key);
        }
        self.props.encode().0
    }
}

pub struct RangePropertiesCollectorFactory {
    pub prop_size_index_distance: u64,
    pub prop_keys_index_distance: u64,
}

impl Default for RangePropertiesCollectorFactory {
    fn default() -> Self {
        RangePropertiesCollectorFactory {
            prop_size_index_distance: DEFAULT_PROP_SIZE_INDEX_DISTANCE,
            prop_keys_index_distance: DEFAULT_PROP_KEYS_INDEX_DISTANCE,
        }
    }
}

impl TablePropertiesCollectorFactory<RangePropertiesCollector> for RangePropertiesCollectorFactory {
    fn create_table_properties_collector(&mut self, _: u32) -> RangePropertiesCollector {
        RangePropertiesCollector::new(self.prop_size_index_distance, self.prop_keys_index_distance)
    }
}

/// Can be used for write CF in TiDB & TxnKV scenario, or be used for default CF
/// in RawKV scenario.
pub struct MvccPropertiesCollector {
    props: MvccProperties,
    last_row: Vec<u8>,
    num_errors: u64,
    row_versions: u64,
    cur_index_handle: IndexHandle,
    row_index_handles: IndexHandles,
    key_mode: KeyMode, // Use KeyMode::Txn for both TiDB & TxnKV, KeyMode::Raw for RawKV.
    current_ts: u64,
}

impl MvccPropertiesCollector {
    fn new(key_mode: KeyMode) -> MvccPropertiesCollector {
        MvccPropertiesCollector {
            props: MvccProperties::new(),
            last_row: Vec::new(),
            num_errors: 0,
            row_versions: 0,
            cur_index_handle: IndexHandle::default(),
            row_index_handles: IndexHandles::new(),
            key_mode,
            current_ts: ttl_current_ts(),
        }
    }
}

impl TablePropertiesCollector for MvccPropertiesCollector {
    fn add(&mut self, key: &[u8], value: &[u8], entry_type: DBEntryType, _: u64, _: u64) {
        // TsFilter filters sst based on max_ts and min_ts during iterating.
        // To prevent seeing outdated (GC) records, we should consider
        // RocksDB delete entry type.
        if entry_type != DBEntryType::Put
            && entry_type != DBEntryType::Delete
            && entry_type != DBEntryType::BlobIndex
        {
            return;
        }

        if !keys::validate_data_key(key) {
            self.num_errors += 1;
            return;
        }

        let (k, ts) = match Key::split_on_ts_for(key) {
            Ok((k, ts)) => (k, ts),
            Err(_) => {
                self.num_errors += 1;
                return;
            }
        };

        self.props.min_ts = cmp::min(self.props.min_ts, ts);
        self.props.max_ts = cmp::max(self.props.max_ts, ts);
        if entry_type == DBEntryType::Delete {
            // Empty value for delete entry type, skip following properties.
            return;
        }

        self.props.num_versions += 1;

        if k != self.last_row.as_slice() {
            self.props.num_rows += 1;
            self.row_versions = 1;
            self.last_row.clear();
            self.last_row.extend(k);
        } else {
            self.row_versions += 1;
        }
        if self.row_versions > self.props.max_row_versions {
            self.props.max_row_versions = self.row_versions;
        }

        if entry_type != DBEntryType::BlobIndex {
            if self.key_mode == KeyMode::Raw {
                let decode_raw_value = ApiV2::decode_raw_value(value);
                match decode_raw_value {
                    Ok(raw_value) => {
                        if raw_value.is_valid(self.current_ts) {
                            self.props.num_puts += 1;
                        } else {
                            self.props.num_deletes += 1;
                        }
                        if let Some(expire_ts) = raw_value.expire_ts {
                            self.props.ttl.add(expire_ts);
                        }
                    }
                    Err(_) => {
                        self.num_errors += 1;
                    }
                }
            } else {
                let write_type = match Write::parse_type(value) {
                    Ok(v) => v,
                    Err(_) => {
                        self.num_errors += 1;
                        return;
                    }
                };

                match write_type {
                    WriteType::Put => self.props.num_puts += 1,
                    WriteType::Delete => self.props.num_deletes += 1,
                    _ => {}
                }
            }
        } else {
            // NOTE: if titan is enabled, the entry will always be treated as PUT.
            // Be careful if you try to enable Titan on CF_WRITE.
            self.props.num_puts += 1;
        }

        // Add new row.
        if self.row_versions == 1 {
            self.cur_index_handle.size += 1;
            self.cur_index_handle.offset += 1;
            if self.cur_index_handle.offset == 1
                || self.cur_index_handle.size >= PROP_ROWS_INDEX_DISTANCE
            {
                self.row_index_handles
                    .insert(self.last_row.clone(), self.cur_index_handle.clone());
                self.cur_index_handle.size = 0;
            }
        }
    }

    fn finish(&mut self) -> HashMap<Vec<u8>, Vec<u8>> {
        // Insert last handle.
        if self.cur_index_handle.size > 0 {
            self.row_index_handles
                .insert(self.last_row.clone(), self.cur_index_handle.clone());
        }
        let mut res = RocksMvccProperties::encode(&self.props);
        res.encode_u64(PROP_NUM_ERRORS, self.num_errors);
        res.encode_handles(PROP_ROWS_INDEX, &self.row_index_handles);
        res.0
    }
}

/// Can be used for write CF of TiDB/TxnKV, default CF of RawKV.
#[derive(Default)]
pub struct MvccPropertiesCollectorFactory {}

impl TablePropertiesCollectorFactory<MvccPropertiesCollector> for MvccPropertiesCollectorFactory {
    fn create_table_properties_collector(&mut self, _: u32) -> MvccPropertiesCollector {
        MvccPropertiesCollector::new(KeyMode::Txn)
    }
}

#[derive(Default)]
pub struct RawMvccPropertiesCollectorFactory {}

impl TablePropertiesCollectorFactory<MvccPropertiesCollector>
    for RawMvccPropertiesCollectorFactory
{
    fn create_table_properties_collector(&mut self, _: u32) -> MvccPropertiesCollector {
        MvccPropertiesCollector::new(KeyMode::Raw)
    }
}

pub fn get_range_stats(
    engine: &crate::RocksEngine,
    cf: &str,
    start: &[u8],
    end: &[u8],
) -> Option<RangeStats> {
    let range = Range::new(start, end);
    let collection = match engine.get_properties_of_tables_in_range(cf, &[range]) {
        Ok(v) => v,
        Err(_) => return None,
    };

    if collection.is_empty() {
        return None;
    }

    // Aggregate total MVCC properties and total number entries.
    let mut props = MvccProperties::new();
    let mut num_entries = 0;
    for (_, v) in collection.iter() {
        let mvcc = match RocksMvccProperties::decode(v.user_collected_properties()) {
            Ok(v) => v,
            Err(_) => return None,
        };
        num_entries += v.num_entries();
        props.add(&mvcc);
    }
    Some(RangeStats {
        num_entries,
        num_versions: props.num_versions,
        num_rows: props.num_rows,
        num_deletes: props.num_deletes,
    })
}

pub fn get_range_sst_stats(
    engine: &crate::RocksEngine,
    cf: &str,
    start: &[u8],
    end: &[u8],
) -> Option<Vec<SstFileStats>> {
    let range = Range::new(start, end);
    let collection = match engine.get_properties_of_tables_in_range(cf, &[range]) {
        Ok(v) => v,
        Err(_) => return None,
    };

    if collection.is_empty() {
        return None;
    }

    let mut result_stats = Vec::with_capacity(collection.len());
    for (name, v) in collection.iter() {
        let mvcc = match RocksMvccProperties::decode(v.user_collected_properties()) {
            Ok(props) => props,
            Err(_) => return None,
        };
        let stats = SstFileStats {
            range_stats: RangeStats {
                num_entries: v.num_entries(),
                num_versions: mvcc.num_versions,
                num_rows: mvcc.num_rows,
                num_deletes: mvcc.num_deletes,
            },
            file_name: name.to_string(),
            min_commit_ts: if mvcc.min_ts == TimeStamp::max() {
                // TimeStamp::max() means no timestamped data found in this SST file.
                // Use u64::MAX to indicate this file should never be considered for GC
                // since there's no MVCC data to garbage collect.
                u64::MAX
            } else {
                mvcc.min_ts.into_inner()
            },
        };
        result_stats.push(stats);
    }
    Some(result_stats)
}

pub fn get_files_from_sst_stats_queue(
    engine: &crate::RocksEngine,
    gc_safe_point: u64,
    tombstones_percent_threshold: u64,
    redundant_rows_percent_threshold: u64,
) -> Option<Vec<String>> {
    let stats_queue = match &engine.sst_stats_queue {
        Some(q) => q,
        None => return None,
    };

    let mut queue = match stats_queue.lock() {
        Ok(q) => q,
        Err(poisoned) => {
            // If the mutex is poisoned, we can still try to access the data
            // but we log a warning as this indicates a panic occurred while holding the
            // lock
            warn!("SST stats queue mutex is poisoned, attempting to recover");
            poisoned.into_inner()
        }
    };

    let candidates = queue.pop_before_ts(gc_safe_point);
    debug!("candidates: {:?}", candidates);

    let mut files_to_compact = Vec::new();
    for stats in candidates.iter() {
        if need_compact_sst_internal(
            stats,
            tombstones_percent_threshold,
            redundant_rows_percent_threshold,
        ) {
            files_to_compact.push(stats.file_name.clone());
        }
    }

    debug!("files_to_compact: {:?}", files_to_compact);

    match files_to_compact.len() {
        0 => None,
        _ => Some(files_to_compact),
    }
}

fn need_compact_sst_internal(
    sst_stats: &SstFileStats,
    tombstones_percent_threshold: u64,
    redundant_rows_percent_threshold: u64,
) -> bool {
    let range_stats = &sst_stats.range_stats;
    if range_stats.num_entries < range_stats.num_versions {
        return false;
    }

    let estimate_num_del = range_stats.num_entries - range_stats.num_versions;
    let redundant_keys = range_stats.redundant_keys();

    estimate_num_del * 100 >= tombstones_percent_threshold * range_stats.num_entries
        || redundant_keys * 100 >= redundant_rows_percent_threshold * range_stats.num_entries
}

#[cfg(test)]
mod tests {
    use api_version::RawValue;
    use engine_traits::{CF_WRITE, LARGE_CFS, MiscExt, SyncMutable};
    use rand::Rng;
    use tempfile::Builder;
    use test::Bencher;
    use txn_types::{Key, Write, WriteType};

    use super::*;
    use crate::{
        RocksCfOptions, RocksDbOptions,
        raw::{DBEntryType, TablePropertiesCollector},
    };

    #[allow(clippy::many_single_char_names)]
    #[test]
    fn test_range_properties() {
        let cases = [
            ("a", 0, 1),
            // handle "a": size(size = 1, offset = 1),keys(1,1)
            ("b", DEFAULT_PROP_SIZE_INDEX_DISTANCE / 8, 1),
            ("c", DEFAULT_PROP_SIZE_INDEX_DISTANCE / 4, 1),
            ("d", DEFAULT_PROP_SIZE_INDEX_DISTANCE / 2, 1),
            ("e", DEFAULT_PROP_SIZE_INDEX_DISTANCE / 8, 1),
            // handle "e": size(size = DISTANCE + 4, offset = DISTANCE + 5),keys(4,5)
            ("f", DEFAULT_PROP_SIZE_INDEX_DISTANCE / 4, 1),
            ("g", DEFAULT_PROP_SIZE_INDEX_DISTANCE / 2, 1),
            ("h", DEFAULT_PROP_SIZE_INDEX_DISTANCE / 8, 1),
            ("i", DEFAULT_PROP_SIZE_INDEX_DISTANCE / 4, 1),
            // handle "i": size(size = DISTANCE / 8 * 9 + 4, offset = DISTANCE / 8 * 17 +
            // 9),keys(4,5)
            ("j", DEFAULT_PROP_SIZE_INDEX_DISTANCE / 2, 1),
            ("k", DEFAULT_PROP_SIZE_INDEX_DISTANCE / 2, 1),
            // handle "k": size(size = DISTANCE + 2, offset = DISTANCE / 8 * 25 + 11),keys(2,11)
            ("l", 0, DEFAULT_PROP_KEYS_INDEX_DISTANCE / 2),
            ("m", 0, DEFAULT_PROP_KEYS_INDEX_DISTANCE / 2),
            // handle "m": keys = DEFAULT_PROP_KEYS_INDEX_DISTANCE,offset =
            // 11+DEFAULT_PROP_KEYS_INDEX_DISTANCE
            ("n", 1, DEFAULT_PROP_KEYS_INDEX_DISTANCE),
            // handle "n": keys = DEFAULT_PROP_KEYS_INDEX_DISTANCE, offset =
            // 11+2*DEFAULT_PROP_KEYS_INDEX_DISTANCE
            ("o", 1, 1),
            // handle　"o": keys = 1, offset = 12 + 2*DEFAULT_PROP_KEYS_INDEX_DISTANCE
        ];

        let mut collector = RangePropertiesCollector::default();
        for &(k, vlen, count) in &cases {
            let v = vec![0; vlen as usize];
            for _ in 0..count {
                collector.add(k.as_bytes(), &v, DBEntryType::Put, 0, 0);
            }
        }
        for &(k, vlen, _) in &cases {
            let v = vec![0; vlen as usize];
            collector.add(k.as_bytes(), &v, DBEntryType::Other, 0, 0);
        }
        let result = UserProperties(collector.finish());

        let props = RangeProperties::decode(&result).unwrap();
        assert_eq!(props.smallest_key().unwrap(), cases[0].0.as_bytes());
        assert_eq!(
            props.largest_key().unwrap(),
            cases[cases.len() - 1].0.as_bytes()
        );
        assert_eq!(
            props.get_approximate_size_in_range(b"", b"k"),
            DEFAULT_PROP_SIZE_INDEX_DISTANCE / 8 * 25 + 11
        );
        assert_eq!(props.get_approximate_keys_in_range(b"", b"k"), 11_u64);

        assert_eq!(props.offsets.len(), 7);
        let a = props.get(b"a");
        assert_eq!(a.size, 1);
        let e = props.get(b"e");
        assert_eq!(e.size, DEFAULT_PROP_SIZE_INDEX_DISTANCE + 5);
        let i = props.get(b"i");
        assert_eq!(i.size, DEFAULT_PROP_SIZE_INDEX_DISTANCE / 8 * 17 + 9);
        let k = props.get(b"k");
        assert_eq!(k.size, DEFAULT_PROP_SIZE_INDEX_DISTANCE / 8 * 25 + 11);
        let m = props.get(b"m");
        assert_eq!(m.keys, 11 + DEFAULT_PROP_KEYS_INDEX_DISTANCE);
        let n = props.get(b"n");
        assert_eq!(n.keys, 11 + 2 * DEFAULT_PROP_KEYS_INDEX_DISTANCE);
        let o = props.get(b"o");
        assert_eq!(o.keys, 12 + 2 * DEFAULT_PROP_KEYS_INDEX_DISTANCE);
        let empty = RangeOffsets::default();
        let cases = [
            (" ", "k", k, &empty, 3),
            (" ", " ", &empty, &empty, 0),
            ("k", "k", k, k, 0),
            ("a", "k", k, a, 2),
            ("a", "i", i, a, 1),
            ("e", "h", e, e, 0),
            ("b", "h", e, a, 1),
            ("g", "g", i, i, 0),
        ];
        for &(start, end, end_idx, start_idx, count) in &cases {
            let props = RangeProperties::decode(&result).unwrap();
            let size = end_idx.size - start_idx.size;
            assert_eq!(
                props.get_approximate_size_in_range(start.as_bytes(), end.as_bytes()),
                size
            );
            let keys = end_idx.keys - start_idx.keys;
            assert_eq!(
                props.get_approximate_keys_in_range(start.as_bytes(), end.as_bytes()),
                keys
            );
            assert_eq!(
                props
                    .take_excluded_range(start.as_bytes(), end.as_bytes())
                    .len(),
                count
            );
        }
    }

    #[test]
    fn test_range_properties_with_blob_index() {
        let cases = [
            ("a", 0),
            // handle "a": size(size = 1, offset = 1),keys(1,1)
            ("b", DEFAULT_PROP_SIZE_INDEX_DISTANCE / 8),
            ("c", DEFAULT_PROP_SIZE_INDEX_DISTANCE / 4),
            ("d", DEFAULT_PROP_SIZE_INDEX_DISTANCE / 2),
            ("e", DEFAULT_PROP_SIZE_INDEX_DISTANCE / 8),
            // handle "e": size(size = DISTANCE + 4, offset = DISTANCE + 5),keys(4,5)
            ("f", DEFAULT_PROP_SIZE_INDEX_DISTANCE / 4),
            ("g", DEFAULT_PROP_SIZE_INDEX_DISTANCE / 2),
            ("h", DEFAULT_PROP_SIZE_INDEX_DISTANCE / 8),
            ("i", DEFAULT_PROP_SIZE_INDEX_DISTANCE / 4),
            // handle "i": size(size = DISTANCE / 8 * 9 + 4, offset = DISTANCE / 8 * 17 +
            // 9),keys(4,5)
            ("j", DEFAULT_PROP_SIZE_INDEX_DISTANCE / 2),
            ("k", DEFAULT_PROP_SIZE_INDEX_DISTANCE / 2),
            // handle "k": size(size = DISTANCE + 2, offset = DISTANCE / 8 * 25 + 11),keys(2,11)
        ];

        let handles = ["a", "e", "i", "k"];

        let mut rng = rand::thread_rng();
        let mut collector = RangePropertiesCollector::default();
        let mut extra_value_size: u64 = 0;
        for &(k, vlen) in &cases {
            if handles.contains(&k) || rng.gen_range(0..2) == 0 {
                let v = vec![0; vlen as usize - extra_value_size as usize];
                extra_value_size = 0;
                collector.add(k.as_bytes(), &v, DBEntryType::Put, 0, 0);
            } else {
                let mut blob_index = TitanBlobIndex::default();
                blob_index.blob_size = vlen - extra_value_size;
                let v = blob_index.encode();
                extra_value_size = v.len() as u64;
                collector.add(k.as_bytes(), &v, DBEntryType::BlobIndex, 0, 0);
            }
        }
        let result = UserProperties(collector.finish());

        let props = RangeProperties::decode(&result).unwrap();
        assert_eq!(props.smallest_key().unwrap(), cases[0].0.as_bytes());
        assert_eq!(
            props.largest_key().unwrap(),
            cases[cases.len() - 1].0.as_bytes()
        );
        assert_eq!(
            props.get_approximate_size_in_range(b"e", b"i"),
            DEFAULT_PROP_SIZE_INDEX_DISTANCE / 8 * 9 + 4
        );
        assert_eq!(
            props.get_approximate_size_in_range(b"", b"k"),
            DEFAULT_PROP_SIZE_INDEX_DISTANCE / 8 * 25 + 11
        );
    }

    #[test]
    fn test_get_range_entries_and_versions() {
        let path = Builder::new()
            .prefix("_test_get_range_entries_and_versions")
            .tempdir()
            .unwrap();
        let path_str = path.path().to_str().unwrap();
        let db_opts = RocksDbOptions::default();
        let mut cf_opts = RocksCfOptions::default();
        cf_opts.set_level_zero_file_num_compaction_trigger(10);
        cf_opts.add_table_properties_collector_factory(
            "tikv.mvcc-properties-collector",
            MvccPropertiesCollectorFactory::default(),
        );
        let cfs_opts = LARGE_CFS.iter().map(|cf| (*cf, cf_opts.clone())).collect();
        let db = crate::util::new_engine_opt(path_str, db_opts, cfs_opts).unwrap();

        let cases = ["a", "b", "c"];
        for &key in &cases {
            let k1 = keys::data_key(
                Key::from_raw(key.as_bytes())
                    .append_ts(2.into())
                    .as_encoded(),
            );
            db.put_cf(CF_WRITE, &k1, b"v1").unwrap();
            db.delete_cf(CF_WRITE, &k1).unwrap();
            let key = keys::data_key(
                Key::from_raw(key.as_bytes())
                    .append_ts(3.into())
                    .as_encoded(),
            );
            db.put_cf(CF_WRITE, &key, b"v2").unwrap();
            db.flush_cf(CF_WRITE, true).unwrap();
        }

        let start_keys = keys::data_key(&[]);
        let end_keys = keys::data_end_key(&[]);
        let range_stats = get_range_stats(&db, CF_WRITE, &start_keys, &end_keys).unwrap();
        assert_eq!(range_stats.num_entries, (cases.len() * 2) as u64);
        assert_eq!(range_stats.num_versions, cases.len() as u64);
    }

    #[test]
    fn test_mvcc_properties() {
        let cases = [
            ("ab", 2, WriteType::Put, DBEntryType::Put),
            ("ab", 1, WriteType::Delete, DBEntryType::Put),
            ("ab", 1, WriteType::Delete, DBEntryType::Delete),
            ("cd", 5, WriteType::Delete, DBEntryType::Put),
            ("cd", 4, WriteType::Put, DBEntryType::Put),
            ("cd", 3, WriteType::Put, DBEntryType::Put),
            ("ef", 6, WriteType::Put, DBEntryType::Put),
            ("ef", 6, WriteType::Put, DBEntryType::Delete),
            ("gh", 7, WriteType::Delete, DBEntryType::Put),
        ];
        let mut collector = MvccPropertiesCollector::new(KeyMode::Txn);
        for &(key, ts, write_type, entry_type) in &cases {
            let ts = ts.into();
            let k = Key::from_raw(key.as_bytes()).append_ts(ts);
            let k = keys::data_key(k.as_encoded());
            let v = Write::new(write_type, ts, None).as_ref().to_bytes();
            collector.add(&k, &v, entry_type, 0, 0);
        }
        let result = UserProperties(collector.finish());

        let props = RocksMvccProperties::decode(&result).unwrap();
        assert_eq!(props.min_ts, 1.into());
        assert_eq!(props.max_ts, 7.into());
        assert_eq!(props.num_rows, 4);
        assert_eq!(props.num_puts, 4);
        assert_eq!(props.num_versions, 7);
        assert_eq!(props.max_row_versions, 3);
    }

    #[test]
    fn test_mvcc_properties_rawkv_mode() {
        let test_raws = vec![
            (b"r\0a", 1, false, u64::MAX),
            (b"r\0a", 5, false, u64::MAX),
            (b"r\0a", 7, false, u64::MAX),
            (b"r\0b", 1, false, u64::MAX),
            (b"r\0b", 1, true, u64::MAX),
            (b"r\0c", 1, true, 10),
            (b"r\0d", 1, true, 10),
        ];

        let mut collector = MvccPropertiesCollector::new(KeyMode::Raw);
        for &(key, ts, is_delete, expire_ts) in &test_raws {
            let encode_key = ApiV2::encode_raw_key(key, Some(ts.into()));
            let k = keys::data_key(encode_key.as_encoded());
            let v = ApiV2::encode_raw_value(RawValue {
                user_value: &[0; 10][..],
                expire_ts: Some(expire_ts),
                is_delete,
            });
            collector.add(&k, &v, DBEntryType::Put, 0, 0);
        }

        let result = UserProperties(collector.finish());

        let props = RocksMvccProperties::decode(&result).unwrap();
        assert_eq!(props.min_ts, 1.into());
        assert_eq!(props.max_ts, 7.into());
        assert_eq!(props.num_rows, 4);
        assert_eq!(props.num_deletes, 3);
        assert_eq!(props.num_puts, 4);
        assert_eq!(props.num_versions, 7);
        assert_eq!(props.max_row_versions, 3);
        assert_eq!(props.ttl.max_expire_ts, Some(u64::MAX));
        assert_eq!(props.ttl.min_expire_ts, Some(10));
    }

    #[bench]
    fn bench_mvcc_properties(b: &mut Bencher) {
        let ts = 1.into();
        let num_entries = 100;
        let mut entries = Vec::new();
        for i in 0..num_entries {
            let s = format!("{:032}", i);
            let k = Key::from_raw(s.as_bytes()).append_ts(ts);
            let k = keys::data_key(k.as_encoded());
            let w = Write::new(WriteType::Put, ts, Some(s.as_bytes().to_owned()));
            entries.push((k, w.as_ref().to_bytes()));
        }

        let mut collector = MvccPropertiesCollector::new(KeyMode::Txn);
        b.iter(|| {
            for (k, v) in &entries {
                collector.add(k, v, DBEntryType::Put, 0, 0);
            }
        });
    }
}
