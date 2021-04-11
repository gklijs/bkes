mod error;
mod storage;

use crate::api::bkes_server::{Bkes, BkesServer};
use crate::storage::Storage;
use std::env;
use tonic::transport::Server;
use tonic::{Request, Response, Status};

pub mod api {
    tonic::include_proto!("tech.gklijs.bkes.api");
}

pub struct MyBkes {
    storage: Storage,
}

impl MyBkes {
    fn new() -> MyBkes {
        let storage = Storage::new();
        MyBkes { storage }
    }
}

#[tonic::async_trait]
impl Bkes for MyBkes {
    async fn start(
        &self,
        request: Request<api::StartRequest>,
    ) -> Result<Response<api::StartReply>, Status> {
        let message = request.into_inner();
        let result = self.storage.start(message.key, message.value);
        let reply = match result {
            Ok(timestamp) => api::StartReply {
                reply: Some(api::start_reply::Reply::Success(api::StoreSuccess {
                    timestamp,
                })),
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
        let result = self.storage.add(message.key, message.value, message.order);
        let reply = match result {
            Ok(timestamp) => api::AddReply {
                reply: Some(api::add_reply::Reply::Success(api::StoreSuccess {
                    timestamp,
                })),
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

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = match env::var("API_PORT") {
        Ok(val) => format!("0.0.0.0:{}", val).parse()?,
        Err(_) => "0.0.0.0:50030".parse()?,
    };
    let my_bkes = MyBkes::new();
    Server::builder()
        .add_service(BkesServer::new(my_bkes))
        .serve(addr)
        .await?;
    Ok(())
}
