use crate::error::BkesError;
use crate::storage::Storage;

use log::{info, warn};
use rdkafka::consumer::{BaseConsumer, Consumer, ConsumerContext, Rebalance};
use rdkafka::error::KafkaResult;
use rdkafka::util::Timeout;
use rdkafka::{ClientConfig, ClientContext, Message, TopicPartitionList};
use std::ops::Deref;
use tokio::time::Duration;
use tokio::sync::Mutex;
use std::sync::Arc;
use crate::util::get_time;

struct CustomContext;

impl ClientContext for CustomContext {}

impl ConsumerContext for CustomContext {
    fn pre_rebalance(&self, rebalance: &Rebalance) {
        info!("Pre rebalance {:?}", rebalance);
    }

    fn post_rebalance(&self, rebalance: &Rebalance) {
        info!("Post rebalance {:?}", rebalance);
    }

    fn commit_callback(&self, result: KafkaResult<()>, _offsets: &TopicPartitionList) {
        info!("Committing offsets: {:?}", result);
    }
}

type LoggingConsumer = BaseConsumer<CustomContext>;

fn get_partition_count(consumer_config: ClientConfig, topic: &str) -> Result<usize, BkesError> {
    let context = CustomContext;
    let consumer: LoggingConsumer = consumer_config.create_with_context(context)?;
    let metadata = consumer.fetch_metadata(Some(&topic), Timeout::Never)?;
    Ok(metadata.topics().deref()[0].partitions().len())
}

async fn load_partition_to_db(consumer_config: ClientConfig, topic: String, partition: i32, storage: Arc<Mutex<Storage>>) -> Result<i64, BkesError> {
    let timestamp = get_time();
    let mut result: i64 = -1;
    let context = CustomContext;
    let consumer: LoggingConsumer = consumer_config.create_with_context(context)?;
    let mut topic_partition_list = TopicPartitionList::with_capacity(1);
    topic_partition_list.add_partition(&topic, partition);
    consumer
        .assign(&topic_partition_list)
        .expect("Can't subscribe to specified topic-partition");
    consumer.poll(Timeout::After(Duration::from_millis(300)));
    loop {
        let message = consumer.poll(Timeout::After(Duration::from_millis(300)));
        match message {
            None => return Ok(result),
            Some(r) => match r {
                Ok(b) => {
                    let record_time = b.timestamp().to_millis().expect("Timestamp present on all records");
                    //when used while another bkes is still running we want to stop consuming at some point
                    if record_time > timestamp {
                        return Ok(result);
                    } else {
                        result = b.offset()
                    }
                    let bytes = b.payload().expect("bytes present");
                    let db_lock = storage.lock().await;
                    db_lock.add_from_record(topic.as_bytes(), bytes, record_time)?;
                }
                Err(e) => warn!("Error reading from Kafka: {}", e),
            },
        }
    }
}

async fn try_load_partition_to_db(consumer_config: ClientConfig, topic: String, partition: i32, storage: Arc<Mutex<Storage>>) -> (i32,Option<i64>) {
    match load_partition_to_db(consumer_config, topic, partition, storage).await {
        Ok(offset) => {
            if offset == -1 {
                warn!("No records were read for partition: {}", partition);
                (partition, None)
            } else {
                (partition, Some (offset))
            }
        }
        Err(e) => {
            warn!("Error storing partitions: {}", e);
            (partition, None)
        }
    }
}

pub(crate) async fn sync(kafka_topic: String, kafka_brokers: String) -> Result<Storage, BkesError> {
    let mut consumer_config: ClientConfig = ClientConfig::new();
    consumer_config
        .set("group.id", "bkes")
        .set("bootstrap.servers", kafka_brokers)
        .set("enable.partition.eof", "false")
        .set("session.timeout.ms", "6000")
        .set("enable.auto.commit", "false")
        .set("statistics.interval.ms", "30000")
        .set("auto.offset.reset", "smallest");
    let partitions = get_partition_count(consumer_config.clone(), &kafka_topic)?;
    if partitions == 0 {
        return Err(BkesError::User(format!("No partitions found for topic {}, the topic should be initialized before running bkes", &kafka_topic)));
    }
    info!("Found {} partitions", partitions);
    let storage = Arc::new(Mutex::new(Storage::new()));
    let mut handles = vec![];
    for partition in 0..partitions as i32 {
        let kafka_topic_copy = String::from(&kafka_topic);
        let storage_copy = Arc::clone(&storage);
        let handle = tokio::spawn({
            try_load_partition_to_db(consumer_config.clone(), kafka_topic_copy, partition, storage_copy)
        });
        handles.push(handle);
    }
    for handle in handles {
        let (partition, offset) = handle.await.unwrap();
        info!("Received offset: {:?} for partition: {}", offset, partition)
    }
    storage.lock().await.store().await?;
    Ok(Arc::try_unwrap(storage).unwrap().into_inner())
}
