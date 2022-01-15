use super::{BusControl, PubSub, Result};

use std::marker::PhantomData;
use std::pin::Pin;

use tokio::sync::{broadcast, mpsc, oneshot};
use tokio_stream::{self as stream, Stream, StreamExt, StreamMap};

pub mod error {
    use super::*;

    #[derive(Debug)]
    pub enum SendError {
        NoTopic,
        SendError,
    }
    impl<Topic, Event> Into<error::SendError> for mpsc::error::SendError<(Topic, Event)> {
        fn into(self) -> error::SendError {
            SendError::SendError
        }
    }

    pub enum RecvError {
        RecvError,
    }
    impl Into<error::RecvError> for mpsc::error::RecvError {
        fn into(self) -> error::RecvError {
            RecvError::RecvError
        }
    }
}
use error::{RecvError, SendError};

pub struct Publisher<Topic, Event> {
    tx: mpsc::Sender<(Topic, Event)>,
}

impl<Topic: Send, Event: Send> Publisher<Topic, Event> {
    pub(crate) fn new(tx: mpsc::Sender<(Topic, Event)>) -> Self {
        Self { tx }
    }

    pub async fn send(
        &self,
        topic: Topic,
        msg: Event,
    ) -> std::result::Result<(), error::SendError> {
        self.tx.send((topic, msg)).await.map_err(|e| e.into())
    }
}

pub struct BoundPublisher<Event> {
    tx: mpsc::Sender<Event>,
}

impl<Event: Send> BoundPublisher<Event> {
    pub(crate) fn new(tx: mpsc::Sender<Event>) -> Self {
        Self { tx }
    }

    pub async fn send(&self, msg: Event) -> std::result::Result<(), error::SendError> {
        self.tx
            .send(msg)
            .await
            .map_err(|e| error::SendError::SendError)
    }
}

pub struct Subscriber<Topic, Event> {
    map: StreamMap<Topic, Pin<Box<dyn Stream<Item = Event> + Send>>>,

    ctl: mpsc::Sender<BusControl<Topic, Event>>,

    int_tx: mpsc::Sender<(Topic, Event)>,
    rx: mpsc::Receiver<(Topic, Event)>,
}

impl<Topic, Event> Subscriber<Topic, Event>
where
    Event: Clone + Send + 'static,
    Topic: Clone + Send + 'static,
{
    pub fn new(capacity: usize, ctl: mpsc::Sender<BusControl<Topic, Event>>) -> Self {
        let (int_tx, rx) = mpsc::channel(capacity);
        Self {
            map: StreamMap::new(),
            ctl,
            int_tx,
            rx,
        }
    }

    pub(crate) fn add_rx(&mut self, topic: Topic, mut rx: broadcast::Receiver<Event>) {
        let tx = self.int_tx.clone();
        tokio::spawn(async move {
            loop {
                while let Ok(event) = rx.recv().await {
                    let t = topic.clone();
                    let _ = tx.send((t, event)).await;
                }
            }
        });
    }

    pub async fn add_subscription(&mut self, topic: Topic) {
        let (tx, rx): (
            oneshot::Sender<broadcast::Receiver<Event>>,
            oneshot::Receiver<broadcast::Receiver<Event>>,
        ) = oneshot::channel();
        let ctl_msg = BusControl::Subscribe {
            topic: topic.clone(),
            respond_to: tx,
        };

        self.ctl.send(ctl_msg).await;
        let new_rx = rx.await.expect("couldn't create new receiver");
        self.add_rx(topic, new_rx);
    }

    pub async fn recv(&mut self) -> Option<(Topic, Event)> {
        self.rx.recv().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn pub_sends_event() {
        let (tx, mut rx) = mpsc::channel(16);
        let publisher = Publisher::new(tx);

        publisher.send("topic1", 1).await;
        let (topic, event) = rx.recv().await.unwrap();

        assert_eq!(1, event);
    }

    #[tokio::test]
    async fn pub_sends_events() {
        let (tx, mut rx) = mpsc::channel(16);
        let publisher = Publisher::new(tx);

        for i in 0..15 {
            publisher.send("topic1", i).await;
        }

        let mut received = vec![];
        for i in 0..15 {
            let (topic, event) = rx.recv().await.unwrap();
            received.push(event);
        }

        assert_eq!((0usize..15).collect::<Vec<usize>>(), received);
    }

    #[tokio::test]
    async fn recv_recvs_event() {
        let (tx, mut rx) = broadcast::channel(16);
        let (ctl_tx, ctl_rx): (
            mpsc::Sender<BusControl<String, usize>>,
            mpsc::Receiver<BusControl<String, usize>>,
        ) = mpsc::channel(16);
        let mut subscriber = Subscriber::new(16, ctl_tx.clone());
        subscriber.add_rx("topic1".into(), rx);

        tx.send(1);
        let (topic, event) = subscriber.recv().await.unwrap();

        assert_eq!(1, event);
    }

    #[tokio::test]
    async fn recv_recvs_events() {
        let (tx, mut rx) = broadcast::channel(16);
        let (ctl_tx, ctl_rx): (
            mpsc::Sender<BusControl<String, usize>>,
            mpsc::Receiver<BusControl<String, usize>>,
        ) = mpsc::channel(16);
        let mut subscriber = Subscriber::new(16, ctl_tx.clone());
        subscriber.add_rx("topic1".into(), rx);

        for i in 0..15 {
            tx.send(i);
        }

        let mut received = vec![];
        for i in 0..15 {
            let (topic, event) = subscriber.recv().await.unwrap();
            received.push(event);
        }
        assert_eq!((0usize..15).collect::<Vec<usize>>(), received);
    }
}
