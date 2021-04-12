use crate::api;
use crate::api::bkes_server::Bkes;
use crate::storage::Storage;
use tonic::{Request, Response, Status};

pub struct MyBkes {
    storage: Storage,
}

impl MyBkes {
    pub(crate) fn new(storage: Storage) -> MyBkes {
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
