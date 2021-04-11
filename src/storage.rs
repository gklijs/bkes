use crate::api;
use crate::error::BkesError;
use prost::Message;
use std::env;
use std::path::Path;
use std::time::SystemTime;

#[derive(Debug)]
pub struct Storage {
    db: sled::Db,
}

fn get_time() -> Result<u64, BkesError> {
    Ok(SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)?
        .as_secs())
}

impl Storage {
    pub fn new() -> Storage {
        let path = match env::var("DATA_PATH") {
            Ok(val) => Path::new(&val).to_path_buf(),
            Err(_) => Path::new("./data").to_path_buf(),
        };
        let db = sled::open(path).unwrap();
        Storage { db }
    }
    pub fn start(&self, key: Vec<u8>, value: Vec<u8>) -> Result<u64, BkesError> {
        let timestamp = get_time()?;
        let record = api::Record { timestamp, value };
        let records = api::StoredRecords {
            records: vec![record],
        };
        let mut buf_records: Vec<u8> = Vec::with_capacity(records.encoded_len());
        records.encode(&mut buf_records).ok();
        self.db
            .compare_and_swap(key, None as Option<&[u8]>, Some(buf_records))??;
        Ok(timestamp)
    }
    pub fn add(&self, key: Vec<u8>, value: Vec<u8>, order: u32) -> Result<u64, BkesError> {
        let current_bytes = match self.db.get(&key)? {
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
        let timestamp = get_time()?;
        let record = api::Record { timestamp, value };
        records.push(record);
        let new_records = api::StoredRecords { records };
        let mut buf_new_records: Vec<u8> = Vec::with_capacity(new_records.encoded_len());
        new_records.encode(&mut buf_new_records).ok();
        self.db
            .compare_and_swap(key, Some(current_bytes), Some(buf_new_records))??;
        Ok(timestamp)
    }
    pub fn retrieve(&self, key: Vec<u8>) -> Result<api::StoredRecords, BkesError> {
        match self.db.get(&key)? {
            None => Err(BkesError::User(format!(
                "No records where found with key: '{}'",
                String::from_utf8_lossy(&key)
            ))),
            Some(bytes) => Ok(api::StoredRecords::decode(&*bytes)?),
        }
    }
}
