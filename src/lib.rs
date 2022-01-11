use std::fmt::Debug;

use chrono::{DateTime, Utc};
use thiserror::Error;

//mod sync_bus;
mod tokio_bus;
pub use tokio_bus::*;

mod pubsub;
pub use pubsub::*;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

pub trait IsEvent {
    type Id;
    type Kind;

    fn sender(&self) -> Self::Id;
    fn kind(&self) -> Self::Kind;
    fn ts(&self) -> DateTime<Utc>;
    fn dest(&self) -> Option<Self::Id>;
}

#[derive(Error, Debug, Clone)]
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
    SendError,

    #[error("Receiver blocked")]
    ReceiverBlocked,

    #[error("Receiver dropped")]
    ReceiverDropped,
}

impl<Id, Ev, Rx, Filter> From<EventBusError> for BusEventKind<Id, Ev, Rx, Filter> {
    fn from(err: EventBusError) -> BusEventKind<Id, Ev, Rx, Filter> {
        BusEventKind::Error(err)
    }
}

#[derive(Clone)]
pub struct BusEvent<Id, UserEvent, Rx, Filter> {
    ts: DateTime<Utc>,
    event: BusEventKind<Id, UserEvent, Rx, Filter>,
}

// TODO: create a BusEvent that wraps real Events and handles subscribe/etc
#[derive(Clone)]
pub enum BusEventKind<Id, UserEvent, Rx, Filter> {
    Error(EventBusError),
    UserEvent(UserEvent),
    Subscribe { rx: Rx, filter: Filter },
    Unsubscribe { sub_id: Id },
}

impl<Id, Ev, Rx, Filter> From<BusEventKind<Id, Ev, Rx, Filter>> for BusEvent<Id, Ev, Rx, Filter> {
    fn from(bek: BusEventKind<Id, Ev, Rx, Filter>) -> BusEvent<Id, Ev, Rx, Filter> {
        BusEvent {
            ts: Utc::now(),
            event: bek,
        }
    }
}
impl<Id, Ev, Rx, Filter> From<BusEvent<Id, Ev, Rx, Filter>> for BusEventKind<Id, Ev, Rx, Filter> {
    fn from(be: BusEvent<Id, Ev, Rx, Filter>) -> BusEventKind<Id, Ev, Rx, Filter> {
        be.event
    }
}

// TODO: update to work with Arc<impl EventSender> and Arc<impl EventReceiver> instead of IDs
//

pub trait EventBus<Id, Rx, Tx, Kind, Event, Filter> {
    // get a channel on which to send events
    fn channel(&self) -> Tx;

    /// subscribe a receiver to a sender for events matching a filter. Return an Id for the
    /// subscription, which can be used later to unsubscribe
    fn subscribe(&mut self, rx_id: Id, rx_ch: Tx, filter: Filter) -> Result<Id>;

    /// unsubscribe a specific subscription
    fn unsubscribe(&mut self, sub_id: Id) -> Result<()>;

    // drop all subscriptions for a given receiver
    fn drop_rx(&mut self, rx_id: Id) -> Result<()>;

    /// when sink() is called, an EventBus will start to emit all events to that sink after normal
    /// routing
    fn sink(&mut self) -> Rx;
}
