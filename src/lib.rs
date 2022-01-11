use std::collections::HashMap;
use std::fmt::Debug;

use thiserror::Error;
use tokio::{
    self,
    sync::{broadcast, oneshot},
    time::{sleep, Duration},
};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

#[derive(Error, Debug)]
pub enum PubSubError {
    #[error("this topic already exists")]
    TopicExists(String),
}

struct PubSub<T> {
    topics: HashMap<String, broadcast::Sender<T>>,

    events_rx: broadcast::Receiver<(String, T)>,
    events_tx: broadcast::Sender<(String, T)>,

    shutdown: broadcast::Receiver<()>,

    capacity: usize,
}

impl<T: Clone + Debug + Send> PubSub<T> {
    pub fn new(capacity: usize, shutdown: broadcast::Receiver<()>) -> Self {
        let (tx, rx) = broadcast::channel(capacity);
        Self {
            topics: HashMap::new(),
            events_rx: rx,
            events_tx: tx,
            shutdown,
            capacity,
        }
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }

    pub fn channel(&self) -> broadcast::Sender<(String, T)> {
        self.events_tx.clone()
    }

    pub fn subscribe(&mut self, topic: &str) -> broadcast::Receiver<T> {
        if let Some(tx) = self.topics.get(topic) {
            tx.subscribe()
        } else {
            let (tx, rx) = self
                .create_topic(topic.to_owned())
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

    pub async fn run(&mut self) {
        loop {
            tokio::select! {
                result = self.events_rx.recv() => {
                    if let Ok((topic, event)) = result {
                        self.process_event(topic, event).await;
                    }
                },
                _ = self.shutdown.recv() => {
                    sleep(Duration::from_millis(100)).await;
                    return
                }
            };
        }
    }

    async fn process_event(&mut self, topic: String, event: T) {
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio_test;

    #[tokio::test]
    async fn sub_recv_event() {
        let (stx, srx) = broadcast::channel(1);
        let mut bus = PubSub::<usize>::new(1, srx);

        let mut sub1 = bus.subscribe("topic1");
        let pub1 = bus.channel();
        tokio::spawn(async move { bus.run().await });

        let send_event = 123;
        println!("sending 123");
        pub1.send(("topic1".into(), send_event.clone()))
            .expect("couldn't send");

        println!("awaiting sub1.recv()");
        let event = sub1.recv().await.unwrap();
        println!("checking equality");
        assert_eq!(event, send_event);

        println!("sending shutdown");
        stx.send(());
    }

    #[tokio::test]
    async fn sub_recv_multiple_events() {
        let (stx, srx) = broadcast::channel(1);
        let mut bus = PubSub::<usize>::new(1, srx);

        let mut sub1 = bus.subscribe("topic1");
        let pub1 = bus.channel();
        tokio::spawn(async move { bus.run().await });

        let send_events = vec![123, 456, 789];
        for e in send_events.iter() {
            pub1.send(("topic1".into(), e.clone()))
                .expect("couldn't send");

            let recv = sub1.recv().await.unwrap();
            assert_eq!(e, &recv);
        }

        println!("sending shutdown");
        stx.send(());
    }

    #[tokio::test]
    async fn sub_recv_queued_events() {
        let (stx, srx) = broadcast::channel(1);
        let mut bus = PubSub::<usize>::new(16, srx);

        let mut sub1 = bus.subscribe("topic1");
        let pub1 = bus.channel();
        tokio::spawn(async move { bus.run().await });

        let send_events = vec![123, 456, 789];
        for e in send_events.iter() {
            pub1.send(("topic1".into(), e.clone()))
                .expect("couldn't send");
        }

        for e in send_events.iter() {
            let recv = sub1.recv().await.unwrap();
            assert_eq!(e, &recv);
        }

        stx.send(());
    }

    #[tokio::test]
    async fn subs_recv_single_event() {
        let (stx, srx) = broadcast::channel(1);
        let mut bus = PubSub::<usize>::new(1, srx);

        let mut sub1 = bus.subscribe("topic1");
        let mut sub2 = bus.subscribe("topic1");
        let pub1 = bus.channel();
        tokio::spawn(async move { bus.run().await });

        let send_event = 123;
        pub1.send(("topic1".into(), send_event.clone()))
            .expect("couldn't send");

        let event = sub1.recv().await.unwrap();
        assert_eq!(event, send_event);
        let event = sub2.recv().await.unwrap();
        assert_eq!(event, send_event);

        stx.send(());
    }

    #[tokio::test]
    async fn subs_recv_multiple_events() {
        let (stx, srx) = broadcast::channel(1);
        let mut bus = PubSub::<usize>::new(1, srx);

        let mut sub1 = bus.subscribe("topic1");
        let mut sub2 = bus.subscribe("topic1");
        let pub1 = bus.channel();
        tokio::spawn(async move { bus.run().await });

        let send_events = vec![123, 456, 789];
        for e in send_events.iter() {
            pub1.send(("topic1".into(), e.clone()))
                .expect("couldn't send");

            let recv = sub1.recv().await.unwrap();
            assert_eq!(e, &recv);
            let recv = sub2.recv().await.unwrap();
            assert_eq!(e, &recv);
        }

        println!("sending shutdown");
        stx.send(());
    }

    #[tokio::test]
    async fn subs_recv_multiple_queued_events() {
        let (stx, srx) = broadcast::channel(1);
        let mut bus = PubSub::<usize>::new(16, srx);

        let mut sub1 = bus.subscribe("topic1");
        let mut sub2 = bus.subscribe("topic1");
        let pub1 = bus.channel();
        tokio::spawn(async move { bus.run().await });

        let send_events = vec![123, 456, 789];
        for e in send_events.iter() {
            pub1.send(("topic1".into(), e.clone()))
                .expect("couldn't send");
        }

        for e in send_events.iter() {
            let recv = sub1.recv().await.unwrap();
            assert_eq!(e, &recv);
        }
        for e in send_events.iter() {
            let recv = sub2.recv().await.unwrap();
            assert_eq!(e, &recv);
        }

        stx.send(());
    }

    #[tokio::test]
    async fn sub_recv_multiple_pubs() {
        let (stx, srx) = broadcast::channel(1);
        let mut bus = PubSub::<usize>::new(16, srx);

        let mut sub1 = bus.subscribe("topic1");
        let pub1 = bus.channel();
        let pub2 = bus.channel();
        tokio::spawn(async move { bus.run().await });

        let mut send_events = vec![123, 456, 789];

        for e in send_events.iter() {
            pub1.send(("topic1".into(), e.clone()))
                .expect("couldn't send");
            pub2.send(("topic1".into(), e.clone()))
                .expect("couldn't send");
        }

        let mut all_events = send_events.clone();
        all_events.append(&mut send_events.clone());

        let mut recv_events = vec![];
        for e in all_events.iter() {
            recv_events.push(sub1.recv().await.unwrap());
        }
        all_events.sort();
        recv_events.sort();

        stx.send(());
    }

    #[tokio::test]
    async fn apollo() {
        #[derive(Debug, Clone, Eq, PartialEq)]
        enum ApolloEvent {
            Ignition,
            Liftoff,
            EagleHasLanded,
            HoustonWeHaveAProblem,
        }

        let (shutdown_tx, shutdown_rx) = broadcast::channel(1);
        let mut bus = PubSub::new(16, shutdown_rx);

        let apollo11 = bus.channel();
        let apollo13 = bus.channel();

        let mut houston1 = bus.subscribe("apollo11");
        let mut houston2 = bus.subscribe("apollo13");

        tokio::spawn(async move { bus.run().await });

        apollo11.send(("apollo11".into(), ApolloEvent::Ignition));
        apollo11.send(("apollo11".into(), ApolloEvent::Liftoff));
        apollo11.send(("apollo11".into(), ApolloEvent::EagleHasLanded));

        apollo13.send(("apollo13".into(), ApolloEvent::Ignition));
        apollo13.send(("apollo13".into(), ApolloEvent::Liftoff));
        apollo13.send(("apollo13".into(), ApolloEvent::HoustonWeHaveAProblem));

        let mut srx = shutdown_tx.subscribe();
        let apollo_future = tokio::spawn(async move {
            let mut apollo11_events = vec![];
            let mut apollo13_events = vec![];
            loop {
                tokio::select! {
                    _ = srx.recv() => return (apollo11_events, apollo13_events),
                    res = houston1.recv() => apollo11_events.push(res.unwrap()),
                    res = houston2.recv() => apollo13_events.push(res.unwrap()),
                }
            }
        });
        sleep(Duration::from_millis(100)).await;
        shutdown_tx.send(());

        let (apollo11_events, apollo13_events) = apollo_future.await.unwrap();

        assert_eq!(
            apollo11_events,
            vec![
                ApolloEvent::Ignition,
                ApolloEvent::Liftoff,
                ApolloEvent::EagleHasLanded
            ]
        );
        assert_eq!(
            apollo13_events,
            vec![
                ApolloEvent::Ignition,
                ApolloEvent::Liftoff,
                ApolloEvent::HoustonWeHaveAProblem
            ]
        );
    }
}
