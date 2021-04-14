use crate::api;
use crate::error::BkesError;
use prost::Message;
use rdkafka::Timestamp;
use sled::Transactional;
use std::env;
use std::path::Path;
use tokio::io::{AsyncReadExt, AsyncWriteExt};


#[derive(Debug, Clone)]
pub struct Storage {
    db: sled::Db,
    records: sled::Tree,
    offsets: sled::Tree,
}

fn get_time() -> i64 {
    Timestamp::now().to_millis().unwrap()
}

fn should_pop(last_record: Option<&api::Record>, offset: i64) -> bool {
    match last_record {
        None => false,
        Some(r) => {
            if r.offset == -1 {
                true
            } else {
                r.offset >= offset
            }
        }
    }
}

fn insert_record(bytes: &sled::IVec, record: api::Record) -> Result<Vec<api::Record>, BkesError> {
    let mut records: Vec<api::Record> = api::StoredRecords::decode(&**bytes)?.records;
    while should_pop(records.last(), record.offset) {
        records.pop();
    }
    records.push(record);
    Ok(records)
}

fn check_ignorable_start_record(
    bytes: sled::IVec,
    timestamp: i64,
) -> Result<Option<sled::IVec>, BkesError> {
    let records: Vec<api::Record> = api::StoredRecords::decode(&*bytes)?.records;
    if records.len() > 1 {
        return Err(BkesError::User(format!(
            "Already {} records for this key, cant start aggregate",
            records.len()
        )));
    }
    if records[0].offset != -1 {
        return Err(BkesError::User(String::from(
            "Already a verified record for this key, can't start aggregate",
        )));
    }
    if records[0].timestamp + 10000 > timestamp {
        return Err(BkesError::User(format!(
            "Ten seconds haven't passed to verify the current start record, try in {} ms",
            records[0].timestamp + 10000 - timestamp
        )));
    }
    Ok(Some(bytes))
}

fn unverified_record(value: Vec<u8>, timestamp: i64) -> api::Record {
    let partition = -1;
    let offset = -1;
    api::Record {
        value,
        partition,
        offset,
        timestamp,
    }
}

impl Storage {
    pub fn new() -> Storage {
        let path = match env::var("DATA_PATH") {
            Ok(val) => Path::new(&val).to_path_buf(),
            Err(_) => Path::new("./data").to_path_buf(),
        };
        let db = sled::open(path).expect("Initializing sled db failed");
        let records = db
            .open_tree(b"records")
            .expect("Opening the records tree failed");
        let offsets = db
            .open_tree(b"offsets")
            .expect("Opening the offsets tree failed");
        Storage {
            db,
            records,
            offsets,
        }
    }
    pub fn start(&self, key: Vec<u8>, value: Vec<u8>) -> Result<i64, BkesError> {
        let timestamp = get_time();
        let current_bytes = match self.records.get(&key)? {
            None => None,
            Some(bytes) => check_ignorable_start_record(bytes, timestamp)?,
        };
        let record = unverified_record(value, timestamp);
        let records = api::StoredRecords {
            records: vec![record],
        };
        let mut buf_records: Vec<u8> = Vec::with_capacity(records.encoded_len());
        records.encode(&mut buf_records).ok();
        self.records
            .compare_and_swap(key, current_bytes, Some(buf_records))??;
        Ok(timestamp)
    }
    pub fn add(&self, key: Vec<u8>, value: Vec<u8>, order: u32) -> Result<i64, BkesError> {
        let current_bytes = match self.records.get(&key)? {
            None => {
                return Err(BkesError::User(format!(
                    "No aggregate was created with key: '{}', so can't add",
                    String::from_utf8_lossy(&key)
                )))
            }
            Some(bytes) => bytes,
        };
        let mut records = api::StoredRecords::decode(&*current_bytes)?.records;
        if records.len() as u32 != order {
            return Err(BkesError::User(format!(
                "Could not add records because there are currently {} records instead of {} for key: '{}'",
                records.len(),
                order,
                String::from_utf8_lossy(&key)
            )));
        }
        let timestamp = get_time();
        let record = unverified_record(value, timestamp);
        records.push(record);
        let new_records = api::StoredRecords { records };
        let mut buf_new_records: Vec<u8> = Vec::with_capacity(new_records.encoded_len());
        new_records.encode(&mut buf_new_records).ok();
        self.records
            .compare_and_swap(key, Some(current_bytes), Some(buf_new_records))??;
        Ok(timestamp)
    }
    pub fn retrieve(&self, key: Vec<u8>) -> Result<api::StoredRecords, BkesError> {
        match self.records.get(&key)? {
            None => Err(BkesError::User(format!(
                "No records where found with key: '{}'",
                String::from_utf8_lossy(&key)
            ))),
            Some(bytes) => Ok(api::StoredRecords::decode(&*bytes)?),
        }
    }
    pub(crate) async fn add_from_record(
        &self,
        key: &[u8],
        value: &[u8],
        partition: i32,
        offset: i64,
        timestamp: i64,
    ) -> Result<(), BkesError> {
        let current_bytes = self.records.get(key)?;
        let record = api::Record {
            timestamp,
            value: value.to_vec(),
            partition,
            offset,
        };
        let new_records = match &current_bytes {
            None => api::StoredRecords {
                records: vec![record],
            },
            Some(bytes) => api::StoredRecords {
                records: insert_record(bytes, record)?,
            },
        };
        let mut partition_bytes = Vec::new();
        partition_bytes.write_i32(partition).await?;
        let mut offset_bytes = Vec::new();
        offset_bytes.write_i64(offset).await?;
        (&self.records, &self.offsets).transaction(|(tx_r, tx_o)| {
            let mut buf_new_records: Vec<u8> = Vec::with_capacity(new_records.encoded_len());
            new_records.encode(&mut buf_new_records).ok();
            tx_r.insert(key, buf_new_records)?;
            tx_o.insert(partition_bytes.clone(), offset_bytes.clone())?;
            Ok(())
        })?;
        Ok(())
    }
    pub(crate) async fn get_offset(&self, partition: i32) -> Result<rdkafka::Offset, BkesError> {
        let mut partition_bytes = Vec::new();
        partition_bytes.write_i32(partition).await?;
        match self.offsets.get(&partition_bytes)? {
            None => Ok(rdkafka::Offset::Beginning),
            Some(b) => Ok(rdkafka::Offset::Offset(b.as_ref().read_i64().await?)),
        }
    }
}
