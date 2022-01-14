use std::fmt::Debug;
use std::sync::Arc;
use tokio::sync::{broadcast, mpsc, RwLock};

struct Channel<Event> {
    rx: Arc<RwLock<mpsc::Receiver<Event>>>,
    tx: broadcast::Sender<Event>,
}

impl<Event> Channel<Event>
where
    Event: Clone + Debug + Send,
{
    pub fn new(rx: mpsc::Receiver<Event>, tx: broadcast::Sender<Event>) -> Self {
        Self {
            rx: Arc::new(RwLock::new(rx)),
            tx,
        }
    }

    pub async fn run(&self) {
        loop {
            if let Some(event) = self.rx.write().await.recv().await {
                // ignore any send failures
                self.tx.send(event);
            } else {
                break;
            }
        }
    }

    pub fn subscribe(&self) -> broadcast::Receiver<Event> {
        self.tx.subscribe()
    }
}

struct Aggregator<Topic, Event> {
    tx: mpsc::Sender<(Topic, Event)>,
}

impl<Topic, Event> Aggregator<Topic, Event>
where
    Event: Clone + Send + 'static,
    Topic: Clone + Send + 'static,
{
    pub fn new(tx: mpsc::Sender<(Topic, Event)>) -> Self {
        Self { tx }
    }

    pub fn add(&mut self, topic: Topic, mut rx: mpsc::Receiver<Event>) {
        let tx = self.tx.clone();
        tokio::spawn(async move {
            loop {
                while let Some(event) = rx.recv().await {
                    let topic = topic.clone();
                    tx.send((topic, event)).await;
                }
            }
        });
    }

    pub async fn run(&mut self) {}
}
