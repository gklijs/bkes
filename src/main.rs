mod api {
    tonic::include_proto!("tech.gklijs.bkes.api");
}
mod error;
mod grpc;
mod kafka_consumer;
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
    let storage = sync(kafka_topic, kafka_brokers).await?;
    let my_bkes = MyBkes::new(storage);
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
