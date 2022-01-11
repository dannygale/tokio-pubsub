# tokio-pubsub

An extensible pub/sub message passing implementation built on top of tokio's `broadcast` and `mpsc` channels

## Features
- Publish/subscribe semantics (topics)
- Advanced rule-based filtering
- Dynamically add/remove subscribers 
- Dynamically created topics
- In-band or out-of-band subscribe/unsubscribe
- Extensible for decoupling to REST interfaces, etc.

## Getting Started

```rust
enum MyEvent {
    Ignition,
    Liftoff,
    EagleHasLanded,
    HoustonWeHaveAProblem,
}

#[tokio::main]
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let bus = pubsub::with_capacity(8);
    
    let first_publisher = bus.publisher()?;
    let second_publisher = bus.publisher()?;
    
    let first_subscriber = bus.subscribe()?;
    let second_subscriber = bus.subscribe()?;
    
    
    
    let t1 = tokio::spawn(async move {
            first_publisher.send(MyEvent::Ignitiion)?;
            first_publisher.send(MyEvent::Liftoff)?;
            first_publisher.send(MyEvent::EagleHasLanded)?);
    });
    t1.await;

    let t2 = tokio::spawn(async move {
            second_publisher.send(MyEvent::HoustonWeHaveAProblem)?;
    });
    t2.await;

    assert_eq!(first_subscriber.recv().await?, MyEvent::Ignition);
    assert_eq!(first_subscriber.recv().await?, MyEvent::Liftoff);
    assert_eq!(first_subscriber.recv().await?, MyEvent::EagleHasLanded);
    assert_eq!(first_subscriber.recv().await?, MyEvent::HoustonWeHaveAProblem);
    
    assert_eq!(second_subscriber.recv().await?, MyEvent::Ignition);
    assert_eq!(second_subscriber.recv().await?, MyEvent::Liftoff);
    assert_eq!(second_subscriber.recv().await?, MyEvent::EagleHasLanded);
    assert_eq!(second_subscriber.recv().await?, MyEvent::HoustonWeHaveAProblem);
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
