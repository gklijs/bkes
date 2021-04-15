use crate::api;
use crate::error::BkesError;
use rdkafka::message::OwnedHeaders;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::util::Timeout;
use rdkafka::ClientConfig;

pub struct KafkaProducer {
    producer: FutureProducer,
    topic: String,
}

impl KafkaProducer {
    pub fn new(topic: String, kafka_brokers: String) -> KafkaProducer {
        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", kafka_brokers)
            .set("message.timeout.ms", "60000")
            .create()
            .expect("Producer creation error");
        KafkaProducer { producer, topic }
    }
    pub async fn send(
        &self,
        key: Vec<u8>,
        record: api::Record,
        order: u32,
    ) -> Result<api::StoreSuccess, BkesError> {
        let headers = OwnedHeaders::new_with_capacity(1);
        let headers = headers.add("order", &order.to_string());
        let fr = FutureRecord {
            topic: &self.topic,
            partition: None,
            payload: Some(&record.value),
            key: Some(&key),
            timestamp: Some(record.timestamp),
            headers: Some(headers),
        };
        let result = self.producer.send(fr, Timeout::Never);
        let (partition, offset) = result.await?;
        Ok(api::StoreSuccess {
            partition,
            offset,
            timestamp: record.timestamp,
        })
    }
}
