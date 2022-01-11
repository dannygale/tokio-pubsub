use std::collections::HashMap;
use std::fmt::Debug;

use thiserror::Error;
use tokio::sync::broadcast;

use crate::Result;

#[derive(Error, Debug)]
pub enum PubSubError {
    #[error("this topic already exists")]
    TopicExists(String),
}

struct PubSub<T> {
    topics: HashMap<String, broadcast::Sender<T>>,

    events_rx: broadcast::Receiver<(String, T)>,
    events_tx: broadcast::Sender<(String, T)>,

    capacity: usize,
}

impl<T: Clone + Debug + Send> PubSub<T> {
    pub fn new(capacity: usize) -> Self {
        let (tx, rx) = broadcast::channel(capacity);
        Self {
            topics: HashMap::new(),
            events_rx: rx,
            events_tx: tx,
            capacity,
        }
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }

    pub fn channel(&self) -> broadcast::Sender<(String, T)> {
        self.events_tx.clone()
    }

    pub fn subscribe(&mut self, topic: String) -> broadcast::Receiver<T> {
        if let Some(tx) = self.topics.get(&topic) {
            tx.subscribe()
        } else {
            let (tx, rx) = self
                .create_topic(topic)
                .expect("topic shouldn't have existed");
            rx
        }
    }

    fn create_topic(
        &mut self,
        topic: String,
    ) -> Result<(broadcast::Sender<T>, broadcast::Receiver<T>)> {
        if let Some(tx) = self.topics.get(&topic) {
            return Err(PubSubError::TopicExists(topic))?;
        }

        let (tx, rx) = broadcast::channel(self.capacity);
        self.topics.insert(topic, tx.clone());
        Ok((tx, rx))
    }

    pub async fn run(&mut self) -> Result<()> {
        while let Ok((topic, event)) = self.events_rx.recv().await {
            if let Some(tx) = self.topics.get(&topic) {
                match tx.send(event) {
                    Ok(n) => {
                        // on success, send returns number of receivers sent to.
                        // nothing to do
                    }
                    Err(e) => {
                        // An Err means there were no receivers
                        // drop the topic
                        self.topics.remove(&topic);
                    }
                }
            } else {
                // no topic for this event, therefore no subscribers. do nothing
            }
        }

        Ok(())
    }
}
