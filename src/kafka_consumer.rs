use crate::error::BkesError;
use crate::storage::Storage;

use log::info;
use rdkafka::consumer::{BaseConsumer, Consumer, ConsumerContext, Rebalance};
use rdkafka::error::KafkaResult;
use rdkafka::util::Timeout;
use rdkafka::{ClientConfig, ClientContext, Message, Offset, TopicPartitionList};
use std::ops::Deref;
use std::time::SystemTime;
use tokio::sync::mpsc;
use tokio::sync::mpsc::Sender;

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

async fn load_partition_to_db(
    config: ClientConfig,
    topic: String,
    partition: i32,
    storage: Storage,
    sender: Sender<i32>,
) {
    info!("Created thread for partition: {}", partition);
    let context = CustomContext;
    let consumer: LoggingConsumer = config.create_with_context(context).unwrap();
    let mut topic_partition_list = TopicPartitionList::with_capacity(1);
    let offset = storage.get_offset(partition).await.unwrap();
    info!("Found offset: {:?} for partition: {}", offset, partition);
    topic_partition_list
        .add_partition_offset(&topic, partition, offset)
        .unwrap();
    consumer
        .assign(&topic_partition_list)
        .expect("Can't subscribe to specified topic-partition");
    let (_, high_watermark) = consumer
        .fetch_watermarks(&topic, partition, Timeout::Never)
        .unwrap();
    match offset {
        Offset::Beginning => {
            if high_watermark == 0 {
                sender.send(partition).await.unwrap()
            }
        }
        Offset::Offset(o) => {
            if o == high_watermark {
                sender.send(partition).await.unwrap()
            }
        }
        _ => (),
    };
    info!(
        "Found high watermark: {:?} for partition: {}",
        high_watermark, partition
    );
    for message in consumer.iter() {
        let bm = message.unwrap();
        let partition = bm.partition();
        let offset = bm.offset();
        let record_time = bm
            .timestamp()
            .to_millis()
            .expect("Timestamp present on all records");
        let key = bm.key().expect("key present");
        let value = bm.payload().expect("value present");
        info!(
            "reading message with offset {:?} for partition: {}",
            offset, partition
        );
        storage
            .add_from_record(key, value, partition, offset, record_time)
            .await
            .unwrap();
        if offset + 1 == high_watermark {
            sender.send(partition).await.unwrap()
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
    let partitions = get_partition_count(consumer_config.clone(), &kafka_topic)? as i32;
    if partitions == 0 {
        return Err(BkesError::User(format!(
            "No partitions found for topic {}, the topic should be initialized before running bkes",
            &kafka_topic
        )));
    }
    info!("Found {} partitions", partitions);
    let storage = Storage::new();
    let start_time = SystemTime::now();
    let (sender, mut receiver) = mpsc::channel(partitions as usize);
    for partition in 0..partitions {
        let topic = String::from(&kafka_topic);
        tokio::spawn(load_partition_to_db(
            consumer_config.clone(),
            topic,
            partition,
            storage.clone(),
            sender.clone(),
        ));
    }
    for _ in 0..partitions {
        let p = receiver.recv().await.unwrap();
        info!("partition {} is ready", p)
    }
    info!(
        "Done reading offsets to end, took {:?} seconds",
        SystemTime::now()
            .duration_since(start_time)
            .unwrap()
            .as_secs()
    );
    Ok(storage)
}
