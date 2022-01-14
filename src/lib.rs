use std::collections::HashMap;
use std::fmt::Debug;
use std::hash::Hash;

//use dashmap::DashMap;
use thiserror::Error;
use tokio::{
    self,
    sync::{broadcast, mpsc},
    time::{sleep, Duration},
};

mod channel;
mod publisher_subscriber;
use channel::*;
pub use publisher_subscriber::{Publisher, Subscriber};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

#[derive(Error, Debug)]
pub enum PubSubError {
    #[error("this topic already exists")]
    TopicExists,
}

struct PubSub<Topic, Event> {
    topics_tx: HashMap<Topic, broadcast::Sender<Event>>,

    events_rx: mpsc::Receiver<(Topic, Event)>,
    events_tx: mpsc::Sender<(Topic, Event)>,

    shutdown: broadcast::Receiver<()>,

    capacity: usize,
}

impl<Topic, Event> PubSub<Topic, Event>
where
    Topic: Clone + Debug + Eq + Hash + Send + 'static,
    Event: Clone + Debug + Send + 'static,
{
    pub fn new(capacity: usize, shutdown: broadcast::Receiver<()>) -> Self {
        let (tx, rx) = mpsc::channel(capacity);
        Self {
            topics_tx: HashMap::new(),
            events_rx: rx,
            events_tx: tx,
            shutdown,
            capacity,
        }
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }

    pub fn channel(&self) -> mpsc::Sender<(Topic, Event)> {
        self.events_tx.clone()
    }

    pub fn bound_sender(&self, topic: Topic) -> mpsc::Sender<Event> {
        let (ext_tx, mut rx) = mpsc::channel(self.capacity());

        // aggregate events onto the main events_rx mpsc receiver
        let tx = self.events_tx.clone();
        tokio::spawn(async move {
            loop {
                while let Some(event) = rx.recv().await {
                    let t = topic.clone();
                    tx.send((t, event)).await;
                }
            }
        });

        ext_tx
    }

    pub fn subscribe(&mut self, topic: Topic) -> broadcast::Receiver<Event> {
        if let Some(tx) = self.topics_tx.get(&topic) {
            tx.subscribe()
        } else {
            let (_tx, rx) = self
                .create_topic(topic)
                .expect("topic shouldn't have existed");
            rx
        }
    }

    fn create_topic(
        &mut self,
        topic: Topic,
    ) -> Result<(broadcast::Sender<Event>, broadcast::Receiver<Event>)> {
        if let Some(_tx) = self.topics_tx.get(&topic) {
            return Err(PubSubError::TopicExists)?;
        }

        let (tx, rx) = broadcast::channel(self.capacity);
        self.topics_tx.insert(topic, tx.clone());
        Ok((tx, rx))
    }

    pub async fn run(&mut self) {
        loop {
            tokio::select! {
                result = self.events_rx.recv() => {
                    if let Some((topic, event)) = result {
                        self.process_event(&topic, event).await;
                    }
                },
                _ = self.shutdown.recv() => {
                    // TODO: propagate to receivers
                    sleep(Duration::from_millis(100)).await;
                    return
                }
            };
        }
    }

    async fn process_event(&mut self, topic: &Topic, event: Event) {
        if let Some(tx) = self.topics_tx.get(topic) {
            match tx.send(event) {
                Ok(_n) => {
                    // on success, send returns number of receivers sent to.
                    // nothing to do
                }
                Err(_e) => {
                    // An Err means there were no receivers
                    // drop the topic
                    self.topics_tx.remove(topic);
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

    #[tokio::test]
    async fn sub_recv_event() {
        let (stx, srx) = broadcast::channel(1);
        let mut bus = PubSub::<String, usize>::new(1, srx);

        let mut sub1 = bus.subscribe("topic1".into());
        let pub1 = bus.channel();
        tokio::spawn(async move { bus.run().await });

        let send_event = 123;
        println!("sending 123");
        pub1.send(("topic1".into(), send_event.clone()))
            .await
            .expect("couldn't send");

        println!("awaiting sub1.recv()");
        let event = sub1.recv().await.unwrap();
        println!("checking equality");
        assert_eq!(event, send_event);

        println!("sending shutdown");
        stx.send(()).unwrap();
    }

    #[tokio::test]
    async fn sub_recv_multiple_events() {
        let (stx, srx) = broadcast::channel(1);
        let mut bus = PubSub::<String, usize>::new(1, srx);

        let mut sub1 = bus.subscribe("topic1".into());
        let pub1 = bus.channel();
        tokio::spawn(async move { bus.run().await });

        let send_events = vec![123, 456, 789];
        for e in send_events.iter() {
            pub1.send(("topic1".into(), e.clone()))
                .await
                .expect("couldn't send");

            let recv = sub1.recv().await.unwrap();
            assert_eq!(e, &recv);
        }

        println!("sending shutdown");
        stx.send(()).unwrap();
    }

    #[tokio::test]
    async fn sub_recv_queued_events() {
        let (stx, srx) = broadcast::channel(1);
        let mut bus = PubSub::<String, usize>::new(16, srx);

        let mut sub1 = bus.subscribe("topic1".into());
        let pub1 = bus.channel();
        tokio::spawn(async move { bus.run().await });

        let send_events = vec![123, 456, 789];
        for e in send_events.iter() {
            pub1.send(("topic1".into(), e.clone()))
                .await
                .expect("couldn't send");
        }

        for e in send_events.iter() {
            let recv = sub1.recv().await.unwrap();
            assert_eq!(e, &recv);
        }

        stx.send(()).unwrap();
    }

    #[tokio::test]
    async fn subs_recv_single_event() {
        let (stx, srx) = broadcast::channel(1);
        let mut bus = PubSub::<String, usize>::new(1, srx);

        let mut sub1 = bus.subscribe("topic1".into());
        let mut sub2 = bus.subscribe("topic1".into());
        let pub1 = bus.channel();
        tokio::spawn(async move { bus.run().await });

        let send_event = 123;
        pub1.send(("topic1".into(), send_event.clone()))
            .await
            .expect("couldn't send");

        let event = sub1.recv().await.unwrap();
        assert_eq!(event, send_event);
        let event = sub2.recv().await.unwrap();
        assert_eq!(event, send_event);

        stx.send(()).unwrap();
    }

    #[tokio::test]
    async fn subs_recv_multiple_events() {
        let (stx, srx) = broadcast::channel(1);
        let mut bus = PubSub::<String, usize>::new(1, srx);

        let mut sub1 = bus.subscribe("topic1".into());
        let mut sub2 = bus.subscribe("topic1".into());
        let pub1 = bus.channel();
        tokio::spawn(async move { bus.run().await });

        let send_events = vec![123, 456, 789];
        for e in send_events.iter() {
            pub1.send(("topic1".into(), e.clone()))
                .await
                .expect("couldn't send");

            let recv = sub1.recv().await.unwrap();
            assert_eq!(e, &recv);
            let recv = sub2.recv().await.unwrap();
            assert_eq!(e, &recv);
        }

        println!("sending shutdown");
        stx.send(()).unwrap();
    }

    #[tokio::test]
    async fn subs_recv_multiple_queued_events() {
        let (stx, srx) = broadcast::channel(1);
        let mut bus = PubSub::<String, usize>::new(16, srx);

        let mut sub1 = bus.subscribe("topic1".into());
        let mut sub2 = bus.subscribe("topic1".into());
        let pub1 = bus.channel();
        tokio::spawn(async move { bus.run().await });

        let send_events = vec![123, 456, 789];
        for e in send_events.iter() {
            pub1.send(("topic1".into(), e.clone()))
                .await
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

        stx.send(()).unwrap();
    }

    #[tokio::test]
    async fn sub_recv_multiple_pubs() {
        let (stx, srx) = broadcast::channel(1);
        let mut bus = PubSub::<String, usize>::new(16, srx);

        let mut sub1 = bus.subscribe("topic1".into());
        let pub1 = bus.channel();
        let pub2 = bus.channel();
        tokio::spawn(async move { bus.run().await });

        let send_events = vec![123, 456, 789];

        for e in send_events.iter() {
            pub1.send(("topic1".into(), e.clone()))
                .await
                .expect("couldn't send");
            pub2.send(("topic1".into(), e.clone()))
                .await
                .expect("couldn't send");
        }

        let mut all_events = send_events.clone();
        all_events.append(&mut send_events.clone());

        let mut recv_events = vec![];
        for _e in all_events.iter() {
            recv_events.push(sub1.recv().await.unwrap());
        }
        all_events.sort();
        recv_events.sort();

        stx.send(()).unwrap();
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
        let mut bus = PubSub::<String, ApolloEvent>::new(16, shutdown_rx);

        let apollo11 = bus.channel();
        let apollo13 = bus.channel();

        let mut houston1 = bus.subscribe("apollo11".into());
        let mut houston2 = bus.subscribe("apollo13".into());

        tokio::spawn(async move { bus.run().await });

        apollo11
            .send(("apollo11".into(), ApolloEvent::Ignition))
            .await
            .unwrap();
        apollo11
            .send(("apollo11".into(), ApolloEvent::Liftoff))
            .await
            .unwrap();
        apollo11
            .send(("apollo11".into(), ApolloEvent::EagleHasLanded))
            .await
            .unwrap();

        apollo13
            .send(("apollo13".into(), ApolloEvent::Ignition))
            .await
            .unwrap();
        apollo13
            .send(("apollo13".into(), ApolloEvent::Liftoff))
            .await
            .unwrap();
        apollo13
            .send(("apollo13".into(), ApolloEvent::HoustonWeHaveAProblem))
            .await
            .unwrap();

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
        shutdown_tx.send(()).unwrap();

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
