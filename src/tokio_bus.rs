use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fmt::Debug;
use std::hash::Hash;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::{self, error::TrySendError};

use crate::{
    BusEvent, BusEventKind, EventBus, EventBusError, EventReceiver, EventSender, IsEvent, Result,
    Subscription,
};

/// an implementation of `EventBus` that relies on Tokio's `mpsc` channels
pub struct TokioEventBus<Id, Kind: Clone, Event, F> {
    events_rx: mpsc::Receiver<BusEvent<Id, Event, Kind>>,
    events_tx: mpsc::Sender<BusEvent<Id, Event, Kind>>,

    receivers: HashMap<Id, (mpsc::Sender<Event>, Vec<Id>)>,

    subscriptions: HashMap<Id, (Id, F)>,

    sink: Option<mpsc::UnboundedSender<BusEvent<Id, Event, Kind>>>,
}

impl<Id, EventKind, Event, F> TokioEventBus<Id, EventKind, Event, F>
where
    Id: Hash + Default + Sync + Send + Eq + Ord + Clone,
    Event: IsEvent<Id, EventKind> + Sync + Send + Clone + Debug,
    EventKind: Ord + Clone,
    F: Fn(&Event) -> bool,
{
    pub fn new(n: usize) -> Self {
        let (events_tx, events_rx) = mpsc::channel(n);
        Self {
            events_rx,
            events_tx,
            receivers: HashMap::new(),
            subscriptions: HashMap::new(),
            sink: None,
        }
    }

    async fn emit(&self, e: BusEventKind<Id, Event, EventKind>) {
        self.events_tx.send(e.clone().into()).await;
        if let Some(sink) = self.sink.as_ref() {
            sink.send(e.into());
        }
    }

    async fn route_event(&self, event: Event) {
        let subscriptions: Vec<(&Id, &F)> = self
            .subscriptions
            .iter()
            .map(|x| (&x.1 .0, &x.1 .1))
            .collect();

        for (rx_id, filter) in subscriptions.iter() {
            if filter(&event) {
                if let Some((rx, _subs)) = self.receivers.get(&rx_id) {
                    match rx.try_send(event.clone()) {
                        Ok(()) => {}
                        Err(TrySendError::Full(_)) => {
                            self.emit(EventBusError::ReceiverBlocked.into()).await
                        }
                        Err(TrySendError::Closed(_)) => {
                            self.emit(EventBusError::ReceiverDropped.into()).await
                        }
                    }
                } else {
                    self.emit(EventBusError::UnknownEventReceiver.into()).await;
                }
            }
        }
    }

    pub async fn run(&mut self) {
        while let Some(event) = self.events_rx.recv().await {
            match event.event {
                BusEventKind::Error(bus_error) => match bus_error {
                    EventBusError::UnknownEventSender => {}
                    EventBusError::UnknownEventReceiver => {}
                    EventBusError::DuplicateEventSender => {}
                    EventBusError::DuplicateEventReceiver => {}
                    EventBusError::SendError => {}
                    EventBusError::ReceiverBlocked => {}
                    EventBusError::ReceiverDropped => {}
                    _ => unreachable!(),
                },
                BusEventKind::UserEvent(user_event) => self.route_event(user_event).await,
                BusEventKind::Subscribe { rx, tx, sub } => {}
                BusEventKind::Unsubscribe { rx, tx, sub } => {}
            }

            /////////////////////////////////////////////

            /*
            let tx_id = event.sender();
            let kind = event.kind();

            let sender = match self.senders.get(&tx_id) {
                Some(sender) => sender,
                None => return,
            };

            // if a destination is specified, don't send to other subscribers
            if let Some(rx_id) = event.dest() {
                if let Some(rx) = self.receivers.get(&rx_id) {
                    rx.channel.send(event.clone()).await.unwrap();
                }
            } else {
                // no explicit destination -- send to subscribers according to filters
                for (sub_id, (rx_id, filter)) in self.subscriptions.iter() {
                    if filter(&event) {
                        if let Some(rx) = self.receivers.get(&rx_id) {}
                    }
                }

                let mut sent_to = BTreeSet::new();

                // send to receivers of all events from this subscriber
                if let Some(subscribers) = sender.subscribers.get(&Subscription::All) {
                    for rx_id in subscribers.iter() {
                        if let Some(rx) = self.receivers.get(rx_id) {
                            rx.channel.send(event.clone()).await.unwrap();
                            sent_to.insert(rx_id);
                        }
                    }
                }

                // send to receivers of this kind of event from this subscriber
                match sender.subscribers.get(&Subscription::Kind(kind)) {
                    Some(subscribers) => {
                        for rx_id in subscribers.iter() {
                            if !sent_to.contains(&rx_id) {
                                if let Some(rx) = self.receivers.get(rx_id) {
                                    // TODO: replace this unwrap
                                    rx.channel.send(event.clone()).await.unwrap();
                                }
                            }
                        }
                    }
                    None => {}
                }
            }

            // if sink, send to sink too
            if let Some(sink) = self.sink.as_ref() {
                sink.send(event).await;
            }
            */
        }
    }

    fn subscribe2(&mut self, rx: Id, filter: F) -> Result<()> {
        let subs = self.subscriptions.entry(rx).or_default();
        subs.push(filter);
        Ok(())
    }
}

impl<'a, Id, EventKind, Event, F> EventBus for TokioEventBus<Id, EventKind, Event, F>
where
    Id: Serialize + Deserialize<'a> + Hash + Copy + Clone + Ord + Eq + Default,
    Event: IsEvent<Id, EventKind>,
    EventKind: Ord + Clone,
    F: Fn(Event) -> bool,
{
    type Id = Id;
    type Rx = mpsc::Receiver<Event>;
    type Tx = mpsc::Sender<Event>;
    type EventKind = EventKind;
    type Event = Event;

    fn channel(&self) -> Self::Tx {
        self.events_tx.clone()
    }

    fn subscribe(&mut self, rx: Id, tx: Id, sub: Subscription<EventKind>) -> Result<()> {
        let sender = if let Some(sender) = self.senders.get_mut(&tx) {
            sender
        } else {
            Err(EventBusError::UnknownEventSender)?
        };

        let receiver = if let Some(receiver) = self.receivers.get_mut(&rx) {
            receiver
        } else {
            Err(EventBusError::UnknownEventReceiver)?
        };

        sender
            .subscribers
            .entry(sub.clone())
            .or_default()
            .insert(rx);
        receiver.subscribed_to.entry(tx).or_default().insert(sub);

        Ok(())
    }
    fn add_sender(&mut self, id: Id) -> Result<()> {
        if self.senders.contains_key(&id) {
            Err(EventBusError::DuplicateEventSender)?;
        }

        self.senders.insert(
            id,
            EventSender {
                subscribers: BTreeMap::new(),
            },
        );

        Ok(())
    }
    fn add_receiver(&mut self, id: Id, channel: mpsc::Sender<Event>) -> Result<()> {
        if self.receivers.contains_key(&id) {
            Err(EventBusError::DuplicateEventReceiver)?;
        }

        self.receivers.insert(id, EventReceiver::new(channel));

        Ok(())
    }
    fn rm_sender(&mut self, id: Self::Id) -> Result<()> {
        if let Some(sender) = self.senders.remove(&id) {
            for (_kind, rx_ids) in sender.subscribers {
                for rx_id in rx_ids {
                    let rx = self.receivers.get_mut(&rx_id).unwrap();
                    rx.subscribed_to.remove(&id);
                }
            }
            Ok(())
        } else {
            Err(EventBusError::UnknownEventSender)?
        }
    }
    fn rm_receiver(&mut self, id: Self::Id) -> Result<Self::Tx> {
        if let Some(receiver) = self.receivers.remove(&id) {
            for (tx_id, kinds) in receiver.subscribed_to {
                let tx = self.senders.get_mut(&tx_id).unwrap();
                for kind in kinds {
                    let subs = tx.subscribers.get_mut(&kind).unwrap();
                    subs.remove(&id);
                }
            }
            Ok(receiver.channel)
        } else {
            Err(EventBusError::UnknownEventReceiver)?
        }
    }

    fn sink(&mut self) -> Self::Rx {
        let (tx, rx) = mpsc::channel(usize::MAX);
        self.sink = Some(tx);
        rx
    }
}
