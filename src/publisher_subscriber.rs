use super::{BusControl, Filter};

use std::collections::HashMap;
use std::hash::Hash;

use async_stream::stream;
use tokio::sync::{broadcast, mpsc, oneshot};
use tokio_stream::Stream;

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
}

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
            .map_err(|_| error::SendError::SendError)
    }
}

pub struct Subscriber<Topic, Event> {
    ctl: mpsc::Sender<BusControl<Topic, Event>>,

    int_tx: Option<mpsc::Sender<(Topic, Event)>>,
    rx: mpsc::Receiver<(Topic, Event)>,

    filters: HashMap<Topic, Vec<Filter<Event>>>,
}

impl<Topic, Event> Subscriber<Topic, Event>
where
    Event: Clone + Send + 'static,
    Topic: Hash + Eq + Clone + Send + 'static,
{
    pub fn new(capacity: usize, ctl: mpsc::Sender<BusControl<Topic, Event>>) -> Self {
        let (int_tx, rx) = mpsc::channel(capacity);
        Self {
            ctl,
            int_tx: Some(int_tx),
            rx,
            filters: HashMap::new(),
        }
    }

    pub fn shutdown(&mut self) {
        self.int_tx = None;
    }

    pub(crate) fn add_rx(&mut self, topic: Topic, mut rx: broadcast::Receiver<Event>) {
        if let Some(int_tx) = &self.int_tx {
            let tx = int_tx.clone();
            tokio::spawn(async move {
                while let Ok(event) = rx.recv().await {
                    let t = topic.clone();
                    let _ = tx.send((t, event)).await;
                }
                println!("broadcast dropped");
            });
        } else {
            panic!("no internal transmitter");
        }
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

        // TODO: handle this error
        let _ = self.ctl.send(ctl_msg).await;
        let new_rx = rx.await.expect("couldn't create new receiver");
        self.add_rx(topic, new_rx);
    }

    pub fn add_filter(&mut self, topic: Topic, func: Filter<Event>) {
        let filters = self.filters.entry(topic).or_default();
        filters.push(func);
    }

    pub async fn recv(&mut self) -> Option<(Topic, Event)> {
        while let Some((topic, event)) = self.rx.recv().await {
            let filters = self.filters.entry(topic.clone()).or_default();
            if filters.iter().all(|func| func(event.clone())) {
                return Some((topic, event));
            }
        }
        None
    }

    pub async fn stream(&mut self) -> impl Stream<Item = (Topic, Event)> + '_ {
        stream! {
            while let Some((topic, event)) = self.recv().await {
                yield ((topic, event));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn pub_sends_event() {
        let n = 16;
        let (tx, mut rx) = mpsc::channel(n);
        let publisher = Publisher::new(tx);

        publisher.send("topic1", 1).await.unwrap();
        let (_topic, event) = rx.recv().await.unwrap();

        assert_eq!(1, event);
    }

    #[tokio::test]
    async fn pub_sends_events() {
        let n = 16;
        let (tx, mut rx) = mpsc::channel(n);
        let publisher = Publisher::new(tx);

        for i in 0..n {
            publisher.send("topic1", i).await.unwrap();
        }

        let mut received = vec![];
        for _i in 0..n {
            let (_topic, event) = rx.recv().await.unwrap();
            received.push(event);
        }

        assert_eq!((0usize..n).collect::<Vec<usize>>(), received);
    }

    #[tokio::test]
    async fn sub_recvs_event() {
        let n = 16;
        let (tx, rx) = broadcast::channel(n);
        let (ctl_tx, _ctl_rx): (
            mpsc::Sender<BusControl<String, usize>>,
            mpsc::Receiver<BusControl<String, usize>>,
        ) = mpsc::channel(n);
        let mut subscriber = Subscriber::new(n, ctl_tx.clone());
        subscriber.add_rx("topic1".into(), rx);

        tx.send(1).unwrap();
        let (_topic, event) = subscriber.recv().await.unwrap();

        assert_eq!(1, event);
    }

    #[tokio::test]
    async fn sub_recvs_events() {
        let n = 16;
        let (tx, rx) = broadcast::channel(n);
        let (ctl_tx, _ctl_rx): (
            mpsc::Sender<BusControl<String, usize>>,
            mpsc::Receiver<BusControl<String, usize>>,
        ) = mpsc::channel(n);
        let mut subscriber = Subscriber::new(n, ctl_tx.clone());
        subscriber.add_rx("topic1".into(), rx);

        for i in 0..n {
            tx.send(i).unwrap();
        }

        let mut received = vec![];
        for _i in 0..n {
            let (_topic, event) = subscriber.recv().await.unwrap();
            received.push(event);
        }
        assert_eq!((0usize..n).collect::<Vec<usize>>(), received);
    }

    #[tokio::test]
    async fn sub_filters_events() {
        let n = 16;
        let (tx, rx) = broadcast::channel(n);
        let (ctl_tx, _ctl_rx): (
            mpsc::Sender<BusControl<String, usize>>,
            mpsc::Receiver<BusControl<String, usize>>,
        ) = mpsc::channel(n);
        let mut subscriber = Subscriber::new(n, ctl_tx.clone());
        subscriber.add_rx("topic1".into(), rx);
        subscriber.add_filter("topic1".into(), Box::new(|e| e % 2 == 0));

        for i in 0..n {
            println!("sending {}", i);
            tx.send(i).unwrap();
        }
        drop(tx);
        subscriber.shutdown();

        let mut received: Vec<usize> = vec![];
        for _i in 0..n {
            while let Some((_topic, event)) = subscriber.recv().await {
                println!("received {}", event);
                received.push(event);
            }
        }
        let expected = (0usize..n)
            .collect::<Vec<usize>>()
            .iter()
            .map(|x| *x)
            .filter(|x| x % 2 == 0)
            .collect::<Vec<usize>>();
        assert_eq!(expected, received);
    }
}
