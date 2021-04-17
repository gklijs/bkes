mod api {
    tonic::include_proto!("tech.gklijs.bkes.api");
}
mod error;
mod grpc;
mod kafka_consumer;
mod kafka_producer;
mod storage;

use crate::api::bkes_server::BkesServer;
use crate::error::BkesError;
use crate::grpc::MyBkes;
use crate::kafka_consumer::sync;
use std::env;
use tonic::transport::Server;

#[tokio::main]
async fn main() -> Result<(), BkesError> {
    env_logger::init();
    log::info!("starting up");
    let kafka_topic = env::var("KAFKA_TOPIC").expect("KAFKA_TOPIC env property not set");
    let kafka_brokers = env::var("KAFKA_BROKERS").expect("KAFKA_BROKERS env property not set");
    let storage = sync(kafka_topic.clone(), kafka_brokers.clone())?;
    let my_bkes = MyBkes::new(storage, kafka_topic, kafka_brokers);
    let addr = match env::var("API_PORT") {
        Ok(val) => format!("0.0.0.0:{}", val).parse()?,
        Err(_) => "0.0.0.0:50030".parse()?,
    };
    Server::builder()
        .add_service(BkesServer::new(my_bkes))
        .serve(addr)
        .await?;
    Ok(())
}

#[cfg(test)]
mod test {
    use crate::api;
    use crate::api::bkes_client::BkesClient;
    use tonic::transport::Channel;

    #[tokio::test]
    async fn some_call() {
        let channel = Channel::from_static("http://localhost:50030")
            .connect()
            .await
            .unwrap();

        let mut client = BkesClient::new(channel);
        let request = tonic::Request::new(api::StartRequest {
            key: vec![7],
            value: vec![1],
        });
        let response = client.start(request).await.unwrap();

        println!("RESPONSE={:?}", response);
    }

    #[tokio::test]
    async fn other_call() {
        let channel = Channel::from_static("http://localhost:50030")
            .connect()
            .await
            .unwrap();

        let mut client = BkesClient::new(channel);
        let request = tonic::Request::new(api::AddRequest {
            key: vec![7],
            value: vec![11, 23],
            order: 1u32,
        });
        let response = client.add(request).await.unwrap();

        println!("RESPONSE={:?}", response);
    }

    #[tokio::test]
    async fn final_call() {
        let channel = Channel::from_static("http://localhost:50030")
            .connect()
            .await
            .unwrap();

        let mut client = BkesClient::new(channel);
        let request = tonic::Request::new(api::RetrieveRequest { key: vec![7] });
        let response = client.retrieve(request).await.unwrap();

        println!("RESPONSE={:?}", response);
    }
}
