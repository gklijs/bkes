use crate::api;
use crate::api::bkes_server::Bkes;
use crate::error::BkesError;
use crate::kafka_producer::KafkaProducer;
use crate::storage::Storage;
use tonic::{Request, Response, Status};

pub struct MyBkes {
    storage: Storage,
    producer: KafkaProducer,
}

impl MyBkes {
    pub(crate) fn new(storage: Storage, topic: String, kafka_brokers: String) -> MyBkes {
        let producer = KafkaProducer::new(topic, kafka_brokers);
        MyBkes { storage, producer }
    }
    async fn produce(
        &self,
        key: Vec<u8>,
        store_result: Result<api::Record, BkesError>,
        order: u32,
    ) -> Result<api::StoreSuccess, BkesError> {
        let record = store_result?;
        self.producer.send(key, record, order).await
    }
}

#[tonic::async_trait]
impl Bkes for MyBkes {
    async fn start(
        &self,
        request: Request<api::StartRequest>,
    ) -> Result<Response<api::StartReply>, Status> {
        let message = request.into_inner();
        let store_result = self.storage.start(message.key.clone(), message.value);
        let reply = match self.produce(message.key, store_result, 0).await {
            Ok(succes) => api::StartReply {
                reply: Some(api::start_reply::Reply::Success(succes)),
            },
            Err(e) => api::StartReply {
                reply: Some(api::start_reply::Reply::Error(e.into_api_error())),
            },
        };
        Ok(Response::new(reply))
    }
    async fn add(
        &self,
        request: Request<api::AddRequest>,
    ) -> Result<Response<api::AddReply>, Status> {
        let message = request.into_inner();
        let store_result = self
            .storage
            .add(message.key.clone(), message.value, message.order);
        let reply = match self.produce(message.key, store_result, message.order).await {
            Ok(success) => api::AddReply {
                reply: Some(api::add_reply::Reply::Success(success)),
            },
            Err(e) => api::AddReply {
                reply: Some(api::add_reply::Reply::Error(e.into_api_error())),
            },
        };
        Ok(Response::new(reply))
    }
    async fn retrieve(
        &self,
        request: Request<api::RetrieveRequest>,
    ) -> Result<Response<api::RetrieveReply>, Status> {
        let message = request.into_inner();
        let result = self.storage.retrieve(message.key);
        let reply = match result {
            Ok(records) => api::RetrieveReply {
                reply: Some(api::retrieve_reply::Reply::Success(records)),
            },
            Err(e) => api::RetrieveReply {
                reply: Some(api::retrieve_reply::Reply::Error(e.into_api_error())),
            },
        };
        Ok(Response::new(reply))
    }
}
