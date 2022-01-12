use super::{PubSub, Result};

use std::marker::PhantomData;
use std::pin::Pin;

use tokio::sync::broadcast;
use tokio_stream::{self as stream, Stream, StreamExt, StreamMap};

pub mod error {
    use super::*;
    pub enum SendError<T> {
        NoTopic,
        SendError(broadcast::error::SendError<(String, T)>),
    }
    impl<T> Into<error::SendError<T>> for broadcast::error::SendError<(String, T)> {
        fn into(self) -> error::SendError<T> {
            SendError::SendError(self)
        }
    }

    pub enum RecvError {
        RecvError(broadcast::error::RecvError),
    }
    impl Into<error::RecvError> for broadcast::error::RecvError {
        fn into(self) -> error::RecvError {
            RecvError::RecvError(self)
        }
    }
}
use error::{RecvError, SendError};

pub struct Sender<T> {
    topic: Option<String>,
    tx: broadcast::Sender<(String, T)>,
}

impl<T: Send> Sender<T> {
    pub(crate) fn new(tx: broadcast::Sender<(String, T)>) -> Self {
        Self { topic: None, tx }
    }

    pub(crate) fn with_topic(self, topic: String) -> Self {
        Self {
            topic: Some(topic),
            ..self
        }
    }

    pub fn send(&self, msg: T) -> std::result::Result<usize, error::SendError<T>> {
        if let Some(ref topic) = self.topic {
            self.tx.send((topic.into(), msg)).map_err(|e| e.into())
        } else {
            Err(SendError::NoTopic)
        }
    }

    pub fn publish(
        &self,
        topic: String,
        msg: T,
    ) -> std::result::Result<usize, error::SendError<T>> {
        self.tx.send((topic, msg)).map_err(|e| e.into())
    }
}

pub struct Receiver<T> {
    map: StreamMap<String, Pin<Box<dyn Stream<Item = T> + Send>>>,
    phantom: PhantomData<T>,
}

impl<T: Send + Clone> Receiver<T> {
    pub fn new() -> Self {
        Self {
            map: StreamMap::new(),
            phantom: PhantomData::default(),
        }
    }

    pub async fn recv(&mut self) -> Option<(String, T)> {
        self.map.next().await
    }
}
