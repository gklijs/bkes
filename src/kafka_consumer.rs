use crate::error::BkesError;
use crate::storage::Storage;

use log::{debug, info};
use rdkafka::consumer::{BaseConsumer, Consumer, ConsumerContext};
use rdkafka::message::BorrowedMessage;
use rdkafka::util::Timeout;
use rdkafka::{ClientConfig, ClientContext, Message, Offset, TopicPartitionList};
use std::cmp::min;
use std::ops::Deref;
use std::sync::mpsc;
use std::sync::mpsc::Sender;
use std::time::SystemTime;
use std::{env, thread};

struct CustomContext;

impl ClientContext for CustomContext {}

impl ConsumerContext for CustomContext {}

type LoggingConsumer = BaseConsumer<CustomContext>;

struct OffsetChecker {
    partitions: Vec<i32>,
    goal: Vec<(i32, i64)>,
    sender: Option<Sender<Vec<i32>>>,
}

impl OffsetChecker {
    fn new(partitions: Vec<i32>, goal: Vec<(i32, i64)>, sender: Sender<Vec<i32>>) -> OffsetChecker {
        if goal.is_empty() {
            sender.send(partitions.clone()).unwrap();
            OffsetChecker {
                partitions,
                goal,
                sender: None,
            }
        } else {
            OffsetChecker {
                partitions,
                goal,
                sender: Some(sender),
            }
        }
    }
    fn check(&mut self, bm: BorrowedMessage) {
        if let Some(sender) = &self.sender {
            let goal_part = (bm.partition(), bm.offset());
            if let Some(pos) = self.goal.iter().position(|n| *n == goal_part) {
                self.goal.remove(pos);
                if self.goal.is_empty() {
                    sender.send(self.partitions.clone()).unwrap();
                    self.sender = None;
                }
            }
        }
    }
}

fn get_partition_count(consumer_config: ClientConfig, topic: &str) -> Result<usize, BkesError> {
    let context = CustomContext;
    let consumer: LoggingConsumer = consumer_config.create_with_context(context)?;
    let metadata = consumer.fetch_metadata(Some(&topic), Timeout::Never)?;
    Ok(metadata.topics().deref()[0].partitions().len())
}

fn load_partitions_to_db(
    config: ClientConfig,
    topic: String,
    partitions: Vec<i32>,
    storage: Storage,
    sender: Sender<Vec<i32>>,
) {
    let context = CustomContext;
    let consumer: LoggingConsumer = config.create_with_context(context).unwrap();
    let mut topic_partition_list = TopicPartitionList::with_capacity(partitions.len());
    let mut goal = vec![];
    for partition in &partitions {
        let offset = storage.get_offset(*partition).unwrap();
        debug!("Found offset: {:?} for partition: {}", offset, partition);
        topic_partition_list
            .add_partition_offset(&topic, *partition, offset)
            .unwrap();
        consumer
            .assign(&topic_partition_list)
            .expect("Can't subscribe to specified topic-partition");
        let (_, high_watermark) = consumer
            .fetch_watermarks(&topic, *partition, Timeout::Never)
            .unwrap_or((0, 0));
        debug!(
            "Found high watermark: {:?} for partition: {}",
            high_watermark, partition
        );
        match offset {
            Offset::Beginning => {
                if high_watermark != 0 {
                    goal.push((*partition, high_watermark - 1))
                }
            }
            Offset::Offset(o) => {
                if o > high_watermark {
                    panic!("Stored offset is greater than watermark. Likely a volume is used that belongs to another Kafka topic.")
                }
                if o < high_watermark {
                    goal.push((*partition, high_watermark - 1))
                }
            }
            _ => (),
        };
    }
    let mut checker = OffsetChecker::new(partitions, goal, sender);
    for message in consumer.iter() {
        let bm = message.unwrap();
        let partition = bm.partition();
        let offset = bm.offset();
        let timestamp = bm
            .timestamp()
            .to_millis()
            .expect("Timestamp present on all records");
        let key = bm.key().expect("key present");
        let value = bm.payload().expect("value present");
        storage
            .add_from_record(key, value, partition, offset, timestamp)
            .unwrap();
        debug!(
            "stored offset: {} for partition: {}, with timestamp: {}",
            offset, partition, timestamp
        );
        checker.check(bm);
    }
}

pub(crate) fn sync(kafka_topic: String, kafka_brokers: String) -> Result<Storage, BkesError> {
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
    let threads = min(
        partitions,
        env::var("MAX_CONSUMER_THREADS")
            .expect("MAX_CONSUMER_THREADS env property not set")
            .parse::<i32>()
            .expect("set value for MAX_CONSUMER_THREADS cant be parsed to a number"),
    );
    let mut partition_groups = vec![];
    for _ in 0..threads {
        partition_groups.push(vec![])
    }
    for partition in 0..partitions {
        partition_groups
            .get_mut((partition % threads) as usize)
            .unwrap()
            .push(partition)
    }
    let storage = Storage::new();
    let (s, receiver) = mpsc::channel();
    let start_time = SystemTime::now();
    for partitions in partition_groups {
        let config = consumer_config.clone();
        let topic = String::from(&kafka_topic);
        let storage_clone = storage.clone();
        let sender = s.clone();
        thread::spawn(move || {
            load_partitions_to_db(config, topic, partitions, storage_clone, sender)
        });
    }
    for _ in 0..threads {
        let p = receiver.recv()?;
        info!("partitions: {:?} are ready", p)
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
