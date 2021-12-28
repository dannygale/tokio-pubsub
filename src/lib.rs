
use std::collections::{HashMap, BTreeMap, BTreeSet};
use std::hash::Hash;
use std::fmt::Debug;

use chrono::{DateTime, Utc};
use serde::{Serialize, Deserialize};
use thiserror::Error;

//mod sync_bus;
mod tokio_bus;
pub use tokio_bus::*;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

pub trait IsEvent<Id, EventKind> {
    fn sender(&self) -> Id;
    fn kind(&self) -> EventKind;
    fn ts(&self) -> DateTime<Utc>;
    fn dest(&self) -> Option<Id>;
}

#[derive(Error, Debug)]
pub enum EventBusError {
    #[error("Unknown EventSender")]
    UnknownEventSender,

    #[error("Unknown EventReceiver")]
    UnknownEventReceiver,

    #[error("Duplicate EventSender")]
    DuplicateEventSender,

    #[error("Duplicate EventReceiver")]
    DuplicateEventReceiver,

    #[error("Send Error")]
    SendError
}

#[derive(PartialOrd, Ord, PartialEq, Eq, Clone)]
pub enum Subscription<T: Clone> {
    All,
    Kind(T),
    Direct
}

pub(crate) enum BusEvent {
    Error(EventBusError),
}


// TODO: create a BusEvent that wraps real Events but also handles subscribe/etc
// TODO: update to work with Arc<impl EventSender> and Arc<impl EventReceiver> instead of IDs
//

pub trait EventBus {
    type Id;
    type Rx;
    type Tx;
    type EventKind: Clone;
    type Event: IsEvent<Self::Id, Self::EventKind>;

    /// the channel provides the event queue for senders to send events
    fn channel(&self) -> Self::Tx;

    /// subscribe a receiver to a sender for events of a given type, all events from this sender,
    /// or only direct messages
    fn subscribe(&mut self, rx: Self::Id, tx: Self::Id, subscription: Subscription<Self::EventKind>) -> Result<()>;

    /// add a new sender by Id
    fn add_sender(&mut self, id: Self::Id) -> Result<()>;

    /// add a new receiver by Id
    fn add_receiver(&mut self, id: Self::Id, channel: Self::Tx)-> Result<()>;

    /// remove a sender
    fn rm_sender(&mut self, id: Self::Id) -> Result<()>;

    /// remove a receiver
    fn rm_receiver(&mut self, id: Self::Id) -> Result<Self::Tx>;

    /// when sink() is called, an EventBus will start to emit all events to that sink after normal
    /// routing
    fn sink(&mut self) -> Self::Rx;
}

#[derive(Default)]
pub(crate) struct EventSender<Id, EventKind> {
    subscribers: BTreeMap<EventKind, BTreeSet<Id>>,
}

#[derive(Default)]
pub(crate) struct EventReceiver<Id, Tx, EventKind: Clone> {
    channel: Tx,
    subscribed_to: BTreeMap<Id, BTreeSet<Subscription<EventKind>>>
}

impl<Id, Tx, EventKind: Clone> EventReceiver<Id, Tx, EventKind> {
    pub fn new(channel: Tx) -> Self {
        Self { channel, subscribed_to: BTreeMap::new() }
    }
}

