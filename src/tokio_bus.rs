use std::collections::{HashMap, BTreeMap, BTreeSet};
use std::hash::Hash;
use std::fmt::Debug;

use tokio::sync::mpsc;
use serde::{Serialize, Deserialize};

use crate::{
    Result, EventBusError, 
    EventBus, EventSender, EventReceiver, IsEvent,
    Subscription,

};

/// an implementation of `EventBus` that relies on Tokio's `mpsc` channels
pub struct TokioEventBus<Id, EventKind: Clone, Event> {
    events_rx: mpsc::Receiver<Event>,
    events_tx: mpsc::Sender<Event>,

    senders: HashMap<Id, EventSender<Id, Subscription<EventKind>>>,
    receivers: HashMap<Id, EventReceiver<Id, mpsc::Sender<Event>, EventKind>>,

    sink: Option<mpsc::Sender<Event>>
}

impl<Id, EventKind, Event> TokioEventBus<Id, EventKind, Event>
where Id: Hash + Default + Sync + Send + Eq + Ord,
      Event: IsEvent<Id, EventKind> + Sync + Send + Clone + Debug,
      EventKind: Ord + Clone
{
    pub fn new(n: usize) -> Self {
        let ( events_tx, events_rx ) = mpsc::channel(n);
        Self { 
            events_rx,
            events_tx,
            senders: HashMap::new(),
            receivers: HashMap::new(),
            sink: None
        }
    }

    pub async fn run(&mut self) {
        while let Some(event) = self.events_rx.recv().await {
            let tx_id = event.sender();
            let kind = event.kind();

            let sender = match self.senders.get(&tx_id) {
                Some(sender) => sender,
                None => return
            };


            // if a destination is specified, don't send to other subscribers
            if let Some(rx_id) = event.dest() {
                if let Some(rx) = self.receivers.get(&rx_id) {
                    rx.channel.send(event.clone()).await.unwrap();
                }
            } else {
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
                    },
                    None => {}
                }
            }

            // if sink, send to sink too
            if let Some(sink) = self.sink.as_ref() {
                sink.send(event).await;
            }
        }
    }
}

impl<'a, Id, EventKind, Event> EventBus for TokioEventBus<Id, EventKind, Event>
where Id: Serialize + Deserialize<'a> + Hash + Copy + Clone + Ord + Eq + Default,
      Event: IsEvent<Id, EventKind>,
      EventKind: Ord + Clone,
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

        let sender = 
            if let Some(sender) = self.senders.get_mut(&tx) {
                sender
            } else {
                Err(EventBusError::UnknownEventSender)?
            };

        let receiver =
            if let Some(receiver) = self.receivers.get_mut(&rx) {
                receiver
            } else {
                Err(EventBusError::UnknownEventReceiver)?
            };

        sender.subscribers.entry(sub.clone()).or_default().insert(rx);
        receiver.subscribed_to.entry(tx).or_default().insert(sub);

        Ok(())
    }
    fn add_sender(&mut self, id: Id) -> Result<()> {
        if self.senders.contains_key(&id) {
            Err(EventBusError::DuplicateEventSender)?;
        }

        self.senders.insert(id, EventSender { subscribers: BTreeMap::new() });

        Ok(())
    }
    fn add_receiver(&mut self, id: Id, channel: mpsc::Sender<Event>) -> Result<()> {
        if self.receivers.contains_key(&id) {
            Err(EventBusError::DuplicateEventReceiver)?;
        }

        self.receivers.insert(id, EventReceiver::new(channel) );

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
        } else { Err(EventBusError::UnknownEventSender)? }
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
        } else { Err(EventBusError::UnknownEventReceiver)?}
    }

    fn sink(&mut self) -> Self::Rx {
        let (tx, rx) = mpsc::channel(usize::MAX);
        self.sink = Some(tx);
        rx
    }
}
