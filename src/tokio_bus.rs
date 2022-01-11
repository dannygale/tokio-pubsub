use std::collections::HashMap;
use std::fmt::Debug;
use std::hash::Hash;

use tokio::sync::mpsc::{self, error::TrySendError};
use uuid::Uuid;

use crate::{BusEvent, BusEventKind, EventBus, EventBusError, IsEvent, Result};

/// an implementation of `EventBus` that relies on Tokio's `mpsc` channels
pub struct TokioEventBus<Id, Event, Rx, Filter> {
    events_rx: mpsc::Receiver<BusEvent<Id, Event, Rx, Filter>>,
    events_tx: mpsc::Sender<BusEvent<Id, Event, Rx, Filter>>,

    receivers: HashMap<Id, (mpsc::Sender<BusEvent<Id, Event, Rx, Filter>>, Vec<Uuid>)>,

    subscriptions: HashMap<Id, (Id, Filter)>,

    sink: Option<mpsc::Sender<BusEvent<Id, Event, Rx, Filter>>>,
}

impl<Id, Event, Rx, F> TokioEventBus<Id, Event, Rx, F>
where
    Id: Hash + Default + Sync + Send + Eq + Ord + Clone,
    Event: IsEvent + Sync + Send + Clone + Debug,
    Rx: Clone,
    F: Fn(&BusEventKind<Id, Event, Rx, F>) -> bool + Clone,
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

    async fn emit(&self, e: BusEventKind<Id, Event, Rx, F>) {
        self.events_tx.send(e.clone().into()).await;
        if let Some(sink) = self.sink.as_ref() {
            sink.send(e.into());
        }
    }

    async fn send_event(
        &self,
        rx: &mpsc::Sender<BusEvent<Id, Event, Rx, F>>,
        event: BusEventKind<Id, Event, Rx, F>,
    ) {
        let event = BusEvent::from(event);
        match rx.try_send(event) {
            Ok(()) => {}
            Err(TrySendError::Full(_)) => self.emit(EventBusError::ReceiverBlocked.into()).await,
            Err(TrySendError::Closed(_)) => self.emit(EventBusError::ReceiverDropped.into()).await,
        }
    }

    async fn route_event(&self, event: BusEventKind<Id, Event, Rx, F>) {
        let subscriptions: Vec<(&Id, &F)> = self
            .subscriptions
            .iter()
            .map(|x| (&x.1 .0, &x.1 .1))
            .collect();

        for (rx_id, filter) in subscriptions.iter() {
            if filter(&event) {
                if let Some((rx, _subs)) = self.receivers.get(&rx_id) {
                    self.send_event(rx, event.clone()).await;
                } else {
                    self.emit(EventBusError::UnknownEventReceiver.into()).await;
                }
            }
        }
    }

    pub async fn run(&mut self) {
        while let Some(event) = self.events_rx.recv().await {
            let e = event.event;
            match e {
                BusEventKind::Error(bus_error) => match bus_error {
                    EventBusError::UnknownEventSender => {}
                    EventBusError::UnknownEventReceiver => {}
                    EventBusError::DuplicateEventSender => {}
                    EventBusError::DuplicateEventReceiver => {}
                    EventBusError::SendError => {}
                    EventBusError::ReceiverBlocked => {}
                    EventBusError::ReceiverDropped => {}
                },
                _ => self.route_event(e).await,
                //BusEventKind::UserEvent(_) => self.route_event(e).await,
                //BusEventKind::Subscribe { rx, filter } => {}
                //BusEventKind::Unsubscribe { sub_id } => {}
            }
        }
    }
}

impl<'a, Kind, Event, Rx, Filter>
    EventBus<
        Uuid,
        mpsc::Receiver<BusEvent<Uuid, Event, Rx, Filter>>,
        mpsc::Sender<BusEvent<Uuid, Event, Rx, Filter>>,
        Kind,
        Event,
        Filter,
    > for TokioEventBus<Uuid, Event, Rx, Filter>
where
    //Id: Serialize + Deserialize<'a> + Hash + Copy + Clone + Ord + Eq + Default,
    Event: IsEvent + Sync + Send + Clone + Debug,
    Rx: Clone,
    Filter: Fn(Event) -> bool + Clone,
{
    fn channel(&self) -> mpsc::Sender<BusEvent<Uuid, Event, Rx, Filter>> {
        self.events_tx.clone()
    }

    fn subscribe(
        &mut self,
        rx_id: Uuid,
        rx_ch: mpsc::Sender<BusEvent<Uuid, Event, Rx, Filter>>,
        filter: Filter,
    ) -> Result<Uuid> {
        let sub_id = Uuid::new_v4();
        self.receivers.insert(rx_id, (rx_ch, vec![sub_id]));
        self.subscriptions.insert(sub_id, (rx_id, filter));
        Ok(sub_id)
    }

    fn unsubscribe(&mut self, sub_id: Uuid) -> Result<()> {
        if let Some((rx_id, _f)) = self.subscriptions.remove(&sub_id) {
            if let Some((rx, subs)) = self.receivers.get_mut(&rx_id) {
            } else {
                //self.emit(EventBusError::UnknownEventReceiver.into());
                Err(EventBusError::UnknownEventReceiver)?;
            }
        }
        Ok(())
    }

    fn drop_rx(&mut self, rx_id: Uuid) -> Result<()> {
        Ok(())
    }

    fn sink(&mut self) -> mpsc::Receiver<BusEvent<Uuid, Event, Rx, Filter>> {
        let (tx, rx) = mpsc::channel(usize::MAX);
        self.sink = Some(tx);
        rx
    }
}
