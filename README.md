# tokio-pubsub

An extensible pub/sub message passing implementation built on top of tokio's `broadcast` and `mpsc` channels

## Features
- Publish/subscribe semantics (topics)
- Dynamically add/remove subscribers 
- Dynamically created topics

Future:
- Advanced rule-based filtering
- Event transformations
- In-band or out-of-band subscribe/unsubscribe
- Extensible for decoupling to REST interfaces, etc.

## Getting Started

```rust
enum ApolloEvent {
    Ignition,
    Liftoff,
    EagleHasLanded,
    HoustonWeHaveAProblem,
}

#[tokio::main]
fn main() -> Result<(), Box<dyn std::error::Error>> {
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

```

## User Guide

TODO: User Guide

## Design

The central message bus has a `sync::mpsc` channel which publishers use to submit a `message` to a `topic`. The bus
holds a `sync::broadcast` channel for each `topic`

Subscriptions are created using the builder pattern. There are several ways to subscribe to a PubSub bus:
- Receive all events: `bus.subscribe()` 
- Receive all events on a topic: `bus.topic(topic_name).subscribe()`
 
### Future:
    - Filter all events: `bus.filter(|e| f(e)).subscribe()`
    - Pre-filter events on a topic: `bus.topic(topic_name).filter(|e| f(e)).subscribe()`
    - Transform events: `bus.transform(|e| f(e))`

    - You can apply a `filter` to the bus as a whole or to a `topic`. You can apply a `transform` to the whole bus, a
      `topic`or a `filter`ed bus/`topic`. 
    - The `filter` closures accept an `Event` and return `bool` to indicate whether to pass the event or not
    - The `transform` closures accept an `Event` and return Result<`Event`>
