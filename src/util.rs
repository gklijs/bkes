use rdkafka::Timestamp;

pub(crate) fn get_time() -> i64 {
    Timestamp::now().to_millis().unwrap()
}