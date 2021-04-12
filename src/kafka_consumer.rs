use crate::error::BkesError;
use crate::storage::Storage;

use log::{info, warn};
use rdkafka::consumer::{BaseConsumer, Consumer, ConsumerContext, Rebalance};
use rdkafka::error::KafkaResult;
use rdkafka::util::Timeout;
use rdkafka::{ClientConfig, ClientContext, Message, TopicPartitionList};
use std::ops::Deref;
use tokio::time::Duration;

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

pub(crate) async fn sync(kafka_topic: String, kafka_brokers: String) -> Result<Storage, BkesError> {
    let storage = Storage::new();
    let context = CustomContext;
    let mut consumer_config: ClientConfig = ClientConfig::new();
    consumer_config
        .set("group.id", "bkes")
        .set("bootstrap.servers", kafka_brokers)
        .set("enable.partition.eof", "false")
        .set("session.timeout.ms", "6000")
        .set("enable.auto.commit", "false")
        .set("statistics.interval.ms", "30000")
        .set("auto.offset.reset", "smallest");
    let consumer: LoggingConsumer = consumer_config
        .create_with_context(context)
        .expect("Consumer creation failed");
    let metadata = consumer.fetch_metadata(Some(&kafka_topic), Timeout::Never)?;
    let partitions = metadata.topics().deref()[0].partitions().len();
    info!("Found {} partitions", partitions);
    let mut topic_partition_list = TopicPartitionList::with_capacity(1);
    topic_partition_list.add_partition(&kafka_topic, 0);
    consumer
        .assign(&topic_partition_list)
        .expect("Can't subscribe to specified topic-partition");
    consumer.poll(Timeout::After(Duration::from_millis(100)));
    loop {
        let message = consumer.poll(Timeout::After(Duration::from_millis(100)));
        match message {
            None => return Ok(storage),
            Some(r) => match r {
                Ok(b) => info!("Received: {:?}", b.payload().expect("bytes present")),
                Err(e) => warn!("Error reading from Kafka: {}", e),
            },
        }
    }
}
