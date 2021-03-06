use std::collections::HashMap;
use std::fmt::Debug;
use std::hash::Hash;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll};

use futures::task::noop_waker;

//use dashmap::DashMap;
use thiserror::Error;
use tokio::{
    self,
    sync::{broadcast, mpsc, oneshot},
};

//mod filter;
mod publisher_subscriber;

//use filter::Filter;
pub use publisher_subscriber::{BoundPublisher, Publisher, Subscriber};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;
type Preproc<Event> = Box<dyn Fn(Event) -> Event + Send + 'static>;
type Filter<Event> = Box<dyn Fn(Event) -> bool>;

#[derive(Error, Debug)]
pub enum PubSubError {
    #[error("this topic already exists")]
    TopicExists,
}

pub enum BusControl<Topic, Event> {
    Subscribe {
        topic: Topic,
        respond_to: oneshot::Sender<broadcast::Receiver<Event>>,
    },
    DropTopic {
        topic: Topic,
    },
    SetPreprocessor {
        preproc: Preproc<Event>,
    },
    ClearPreprocessor,
    Shutdown,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum BusEvent<Topic> {
    BusIdle,

    SubscriberCreated(Topic),
    // TODO: SubscriberDropped
    SubscribersDropped(Topic, usize),

    PublisherCreated,
    BoundPublisherCreated(Topic),
    // TODO: PublisherDropped
    PublisherDropped,

    TopicCreated(Topic),
    TopicDropped(Topic),
    NoSubscribers(Topic),

    ShutdownReceived,
    ShutdownCompleted,
    //ReceivedUserEvent(Event),
    //PreprocessedEvent(Event, Event),
    //SentEventToTopic(Topic, Event),
    PreprocessorSet,
    PreprocessorCleared,

    ControlChannelCreated,
}

struct TopicInfo<Event> {
    sender: broadcast::Sender<Event>,
    subscribers: AtomicUsize,
}

impl<Event> TopicInfo<Event> {
    pub fn new(sender: broadcast::Sender<Event>) -> Self {
        Self {
            sender,
            subscribers: AtomicUsize::new(0),
        }
    }
}

pub struct PubSub<Topic, Event> {
    // subscriber channels
    topics_tx: HashMap<Topic, TopicInfo<Event>>,

    // main incoming event bus
    events_tx: mpsc::Sender<(Topic, Event)>,
    events_rx: mpsc::Receiver<(Topic, Event)>,

    // receive a shutdown signal
    // this may be unnecessary -- if all senders to events_rx drop, run will return
    // however, self holds the events_tx sender. How do I handle that?
    shutdown: broadcast::Receiver<()>,

    // channel for receiving control messages
    control_tx: mpsc::Sender<BusControl<Topic, Event>>,
    control_rx: mpsc::Receiver<BusControl<Topic, Event>>,

    // channel for emitting bus events
    meta_tx: broadcast::Sender<BusEvent<Topic>>,

    // sink all user events
    sink_tx: broadcast::Sender<Event>,

    // optional event preprocessor
    // eg - to sequence events
    preproc: Option<Preproc<Event>>,

    capacity: usize,
}

impl<Topic, Event> PubSub<Topic, Event>
where
    Topic: Clone + Debug + Eq + Hash + Send + 'static,
    Event: Clone + Debug + Send + 'static,
{
    pub fn new(capacity: usize, shutdown: broadcast::Receiver<()>) -> Self {
        let (events_tx, events_rx) = mpsc::channel(capacity);
        let (control_tx, control_rx) = mpsc::channel(capacity);
        let (meta_tx, _) = broadcast::channel(capacity);
        let (sink_tx, _) = broadcast::channel(capacity);

        Self {
            topics_tx: HashMap::new(),

            events_tx,
            events_rx,
            control_tx,
            control_rx,
            meta_tx,
            sink_tx,

            shutdown,
            preproc: None,
            capacity,
        }
    }

    fn meta(&self, event: BusEvent<Topic>) {
        let _ = self.meta_tx.send(event.clone());
    }
    fn sink(&self, event: Event) {
        let _ = self.sink_tx.send(event);
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }

    pub fn control_channel(&self) -> mpsc::Sender<BusControl<Topic, Event>> {
        self.meta(BusEvent::ControlChannelCreated);
        self.control_tx.clone()
    }

    pub fn publisher(&self) -> Publisher<Topic, Event> {
        self.meta(BusEvent::PublisherCreated);
        Publisher::new(self.events_tx.clone())
    }

    // TODO: should I push this complexity down into the BoundPublisher struct?
    // Kind of feel like I should. Then there's no special handling on the bus side of things...
    pub fn bind_publisher(&self, topic: Topic) -> BoundPublisher<Event> {
        let (ext_tx, mut rx) = mpsc::channel(self.capacity());

        let t = topic.clone();
        // aggregate events onto the main events_rx mpsc receiver, pre-filling the topic
        let tx = self.events_tx.clone();
        tokio::spawn(async move {
            loop {
                while let Some(event) = rx.recv().await {
                    let send_topic = t.clone();
                    // ignore the result
                    let _ = tx.send((send_topic, event)).await;
                }
                // if we get out of the while loop, the external sender dropped and we're done
            }
        });

        self.meta(BusEvent::BoundPublisherCreated(topic));
        BoundPublisher::new(ext_tx)
    }

    pub fn subscribe_meta(&self) -> broadcast::Receiver<BusEvent<Topic>> {
        self.meta_tx.subscribe()
    }

    pub fn subscribe_sink(&self) -> broadcast::Receiver<Event> {
        self.sink_tx.subscribe()
    }

    pub(crate) fn get_receiver(&mut self, topic: Topic) -> broadcast::Receiver<Event> {
        if let Some(tx) = self.topics_tx.get(&topic) {
            self.meta(BusEvent::SubscriberCreated(topic.clone()));
            tx.sender.subscribe()
        } else {
            let (_tx, rx) = self
                .create_topic(topic.clone())
                .expect("topic shouldn't have existed");
            self.meta(BusEvent::TopicCreated(topic.clone()));
            self.meta(BusEvent::SubscriberCreated(topic.clone()));
            rx
        }
    }

    pub fn subscriber(&mut self, topic: Topic) -> Subscriber<Topic, Event> {
        let ext_rx = self.get_receiver(topic.clone());

        let mut sub = Subscriber::new(self.capacity(), self.control_channel());
        sub.add_rx(topic.clone(), ext_rx);
        sub
    }

    fn create_topic(
        &mut self,
        topic: Topic,
    ) -> Result<(broadcast::Sender<Event>, broadcast::Receiver<Event>)> {
        if let Some(_tx) = self.topics_tx.get(&topic) {
            return Err(PubSubError::TopicExists)?;
        }

        let (tx, rx) = broadcast::channel(self.capacity);
        let topic_info = TopicInfo::new(tx.clone());
        self.topics_tx.insert(topic.clone(), topic_info);
        self.meta(BusEvent::TopicCreated(topic));
        Ok((tx, rx))
    }

    fn drop_topic(&mut self, topic: &Topic) {
        self.meta(BusEvent::TopicDropped(topic.clone()));
        self.topics_tx.remove(topic);
    }

    fn events_empty(&mut self) -> bool {
        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);

        match self.events_rx.poll_recv(&mut cx) {
            Poll::Pending => true,
            _ => false,
        }
    }

    pub async fn run(&mut self) {
        let mut idle = true;
        loop {
            if !idle {
                idle = self.events_empty();
                if idle {
                    self.meta(BusEvent::BusIdle);
                }
            }

            tokio::select! {
                result = self.events_rx.recv() => {
                    if let Some((topic, event)) = result {
                        let event = self.preprocess_event(event);
                        self.process_event(&topic, event).await;
                    } else {
                        // this should never trigger because self holds a sender
                        unreachable!();
                    }
                },
                result = self.control_rx.recv() => {
                    if let Some(ctl_msg) = result {
                        self.process_control_message(ctl_msg).await;
                    } else {
                        unreachable!();
                    }
                }
                _ = self.shutdown.recv() => {
                    // TODO: propagate to receivers
                    self.shutdown().await;
                    return
                }
            };
        }
    }

    #[inline]
    fn preprocess_event(&self, event: Event) -> Event {
        if let Some(ref func) = self.preproc {
            let new_event = func(event.clone());
            //self.meta(BusEvent::PreprocessedEvent(event, new_event.clone()));
            new_event
        } else {
            event
        }
    }

    async fn process_control_message(&mut self, msg: BusControl<Topic, Event>) {
        match msg {
            BusControl::Subscribe { topic, respond_to } => {
                let rx = self.get_receiver(topic);
                let _ = respond_to.send(rx);
            }
            BusControl::DropTopic { topic } => self.drop_topic(&topic),
            BusControl::SetPreprocessor { preproc } => self.set_preprocessor(preproc),
            BusControl::ClearPreprocessor => self.clear_preprocessor(),
            // TODO: is this actually what I want to do here?
            // Note that this will NOT kill any receivers that have been added with
            // Subscriber.add_rx
            BusControl::Shutdown => self.shutdown().await,
        }
    }

    async fn shutdown(&mut self) {
        while let Some((topic, event)) = self.events_rx.recv().await {
            let event = self.preprocess_event(event);
            self.process_event(&topic, event).await;
        }
        let _ = self.meta_tx.send(BusEvent::ShutdownReceived);
        self.events_rx.close();
        self.control_rx.close();
        let _ = self.meta_tx.send(BusEvent::ShutdownCompleted);
    }

    async fn process_event(&mut self, topic: &Topic, event: Event) {
        self.sink(event.clone());
        if let Some(tx) = self.topics_tx.get(topic) {
            match tx.sender.send(event) {
                Ok(n) => {
                    // on success, send returns number of receivers sent to.
                    if tx.subscribers.load(Ordering::Relaxed) != n {
                        self.meta(BusEvent::SubscribersDropped(topic.clone(), n));
                        tx.subscribers.store(n, Ordering::Relaxed);
                    }
                    // nothing to do
                }
                Err(_e) => {
                    // An Err means there were no receivers
                    // drop the topic
                    self.drop_topic(topic)
                }
            }
        } else {
            // no topic for this event, therefore no subscribers. do nothing
            self.meta(BusEvent::NoSubscribers(topic.clone()));
        }
    }

    pub fn set_preprocessor(&mut self, f: Preproc<Event>) {
        self.preproc = Some(f);
        self.meta(BusEvent::PreprocessorSet);
    }

    pub fn clear_preprocessor(&mut self) {
        self.preproc = None;
        self.meta(BusEvent::PreprocessorCleared);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::{sleep, Duration};

    #[tokio::test]
    async fn sub_recv_event() {
        let (stx, srx) = broadcast::channel(1);
        let mut bus = PubSub::<String, usize>::new(1, srx);

        let mut sub1 = bus.get_receiver("topic1".into());
        let pub1 = bus.publisher();
        tokio::spawn(async move { bus.run().await });

        let send_event = 123;
        pub1.send("topic1".into(), send_event.clone())
            .await
            .expect("couldn't send");

        let event = sub1.recv().await.unwrap();
        assert_eq!(event, send_event);

        stx.send(()).unwrap();
    }

    #[tokio::test]
    async fn sub_recv_multiple_events() {
        let (stx, srx) = broadcast::channel(1);
        let mut bus = PubSub::<String, usize>::new(1, srx);

        let mut sub1 = bus.get_receiver("topic1".into());
        let pub1 = bus.publisher();
        tokio::spawn(async move { bus.run().await });

        let send_events = vec![123, 456, 789];
        for e in send_events.iter() {
            pub1.send("topic1".into(), e.clone())
                .await
                .expect("couldn't send");

            let recv = sub1.recv().await.unwrap();
            assert_eq!(e, &recv);
        }

        stx.send(()).unwrap();
    }

    #[tokio::test]
    async fn sub_recv_queued_events() {
        let (stx, srx) = broadcast::channel(1);
        let mut bus = PubSub::<String, usize>::new(16, srx);

        let mut sub1 = bus.get_receiver("topic1".into());
        let pub1 = bus.publisher();
        tokio::spawn(async move { bus.run().await });

        let send_events = vec![123, 456, 789];
        for e in send_events.iter() {
            pub1.send("topic1".into(), e.clone())
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

        let mut sub1 = bus.get_receiver("topic1".into());
        let mut sub2 = bus.get_receiver("topic1".into());
        let pub1 = bus.publisher();
        tokio::spawn(async move { bus.run().await });

        let send_event = 123;
        pub1.send("topic1".into(), send_event.clone())
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

        let mut sub1 = bus.get_receiver("topic1".into());
        let mut sub2 = bus.get_receiver("topic1".into());
        let pub1 = bus.publisher();
        tokio::spawn(async move { bus.run().await });

        let send_events = vec![123, 456, 789];
        for e in send_events.iter() {
            pub1.send("topic1".into(), e.clone())
                .await
                .expect("couldn't send");

            let recv = sub1.recv().await.unwrap();
            assert_eq!(e, &recv);
            let recv = sub2.recv().await.unwrap();
            assert_eq!(e, &recv);
        }

        stx.send(()).unwrap();
    }

    #[tokio::test]
    async fn subs_recv_multiple_queued_events() {
        let (stx, srx) = broadcast::channel(1);
        let mut bus = PubSub::<String, usize>::new(16, srx);

        let mut sub1 = bus.get_receiver("topic1".into());
        let mut sub2 = bus.get_receiver("topic1".into());
        let pub1 = bus.publisher();
        tokio::spawn(async move { bus.run().await });

        let send_events = vec![123, 456, 789];
        for e in send_events.iter() {
            pub1.send("topic1".into(), e.clone())
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

        let mut sub1 = bus.get_receiver("topic1".into());
        let pub1 = bus.publisher();
        let pub2 = bus.publisher();
        tokio::spawn(async move { bus.run().await });

        let send_events = vec![123, 456, 789];

        for e in send_events.iter() {
            pub1.send("topic1".into(), e.clone())
                .await
                .expect("couldn't send");
            pub2.send("topic1".into(), e.clone())
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

        assert_eq!(all_events, recv_events);

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

        let apollo11 = bus.publisher();
        let apollo13 = bus.publisher();

        let mut houston1 = bus.get_receiver("apollo11".into());
        let mut houston2 = bus.get_receiver("apollo13".into());

        tokio::spawn(async move { bus.run().await });

        apollo11
            .send("apollo11".into(), ApolloEvent::Ignition)
            .await
            .unwrap();
        apollo11
            .send("apollo11".into(), ApolloEvent::Liftoff)
            .await
            .unwrap();
        apollo11
            .send("apollo11".into(), ApolloEvent::EagleHasLanded)
            .await
            .unwrap();

        apollo13
            .send("apollo13".into(), ApolloEvent::Ignition)
            .await
            .unwrap();
        apollo13
            .send("apollo13".into(), ApolloEvent::Liftoff)
            .await
            .unwrap();
        apollo13
            .send("apollo13".into(), ApolloEvent::HoustonWeHaveAProblem)
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
        sleep(Duration::from_nanos(0)).await;
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

    #[tokio::test]
    async fn bus_empty() {
        let (_shutdown_tx, shutdown_rx) = broadcast::channel(1);
        let mut bus = PubSub::<String, usize>::new(16, shutdown_rx);

        let mut meta = bus.subscribe_meta();

        let pub1 = bus.publisher();
        pub1.send("topic1".into(), 1).await.unwrap();

        sleep(Duration::from_nanos(0)).await;

        let event = meta.recv().await.unwrap();
        assert_eq!(event, BusEvent::PublisherCreated);
        //let event = meta.recv().await.unwrap();
        //assert_eq!(event, BusEvent::BusIdle);

        bus.shutdown().await;
    }
}
