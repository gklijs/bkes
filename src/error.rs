use crate::api;
use core::fmt;
use error::Error;
use prost::Message;
use std::error;
use std::ops::Deref;

#[derive(Debug)]
pub enum BkesError {
    User(String),
    Concurrency(String),
    Server(Box<dyn Error + Send + 'static>),
}

impl fmt::Display for BkesError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            BkesError::User(s) => write!(f, "user error: {}", s),
            BkesError::Concurrency(s) => write!(f, "consistency error: {}", s),
            BkesError::Server(e) => write!(f, "external error: {}", e),
        }
    }
}

impl Error for BkesError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            BkesError::User(_) => Option::None,
            BkesError::Concurrency(_) => Option::None,
            BkesError::Server(e) => e.source(),
        }
    }
}

impl BkesError {
    pub(crate) fn into_api_error(self) -> api::Error {
        let (r#type, error, cause) = match self {
            BkesError::User(error) => (api::error::Type::User as i32, error, String::new()),
            BkesError::Concurrency(error) => {
                (api::error::Type::Concurrency as i32, error, String::new())
            }
            BkesError::Server(e) => (
                api::error::Type::Server as i32,
                e.to_string(),
                match e.source() {
                    None => String::new(),
                    Some(e) => e.to_string(),
                },
            ),
        };
        api::Error {
            r#type,
            error,
            cause,
        }
    }
}

impl From<sled::Error> for BkesError {
    fn from(err: sled::Error) -> BkesError {
        BkesError::Server(Box::new(err))
    }
}

impl From<std::time::SystemTimeError> for BkesError {
    fn from(err: std::time::SystemTimeError) -> BkesError {
        BkesError::Server(Box::new(err))
    }
}

impl From<prost::DecodeError> for BkesError {
    fn from(err: prost::DecodeError) -> BkesError {
        BkesError::Server(Box::new(err))
    }
}

impl From<sled::CompareAndSwapError> for BkesError {
    fn from(err: sled::CompareAndSwapError) -> BkesError {
        match err.current {
            None => BkesError::Concurrency(String::from(
                "Unexpected value updating key, no current value present.",
            )),
            Some(bytes) => match api::StoredRecords::decode(bytes.deref()) {
                Err(_) => BkesError::Concurrency(String::from(
                    "Unexpected value updating key, current value can't be serialized.",
                )),
                Ok(records) => BkesError::Concurrency(format!(
                    "Unexpected value updating key, current value contains {} records.",
                    records.records.len()
                )),
            },
        }
    }
}

impl From<rdkafka::error::KafkaError> for BkesError {
    fn from(err: rdkafka::error::KafkaError) -> BkesError {
        BkesError::Server(Box::new(err))
    }
}

impl From<(rdkafka::error::KafkaError, rdkafka::message::OwnedMessage)> for BkesError {
    fn from(err: (rdkafka::error::KafkaError, rdkafka::message::OwnedMessage)) -> BkesError {
        BkesError::Server(Box::new(err.0))
    }
}

impl From<std::net::AddrParseError> for BkesError {
    fn from(err: std::net::AddrParseError) -> BkesError {
        BkesError::Server(Box::new(err))
    }
}

impl From<tonic::transport::Error> for BkesError {
    fn from(err: tonic::transport::Error) -> BkesError {
        BkesError::Server(Box::new(err))
    }
}

impl From<tokio::task::JoinError> for BkesError {
    fn from(err: tokio::task::JoinError) -> BkesError {
        BkesError::Server(Box::new(err))
    }
}

impl From<sled::transaction::TransactionError> for BkesError {
    fn from(err: sled::transaction::TransactionError) -> BkesError {
        BkesError::Server(Box::new(err))
    }
}

impl From<std::io::Error> for BkesError {
    fn from(err: std::io::Error) -> BkesError {
        BkesError::Server(Box::new(err))
    }
}
