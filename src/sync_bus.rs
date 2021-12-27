use crate::*;

use std::sync::mpsc;

/// an implementation of `EventBus` that relies on Tokio's `mpsc` channels
pub struct SyncEventBus<Id, Ev> {
    events_rx: mpsc::Receiver<Ev>,
    events_tx: mpsc::Sender<Ev>,

    senders: HashMap<Id, EventSender<Id, Ev>>,
    receivers: HashMap<Id, EventReceiver<Id, mpsc::Sender<Ev>>>,

    sink: mpsc::UnboundedSender<Ev>,
}

impl<Id, Ev> TokioEventBus<Id, Ev>
where Id: Hash + Default + Sync + Send + Eq + Ord,
      Ev: IsEvent<Id> + Sync + Send + Clone + Debug
{
    pub fn new() -> (mpsc::UnboundedReceiver<Ev>, Self) {
        let ( events_tx, events_rx ) = mpsc::channel(128);
        let ( sink_tx, sink_rx ) = mpsc::unbounded_channel();
        (sink_rx, Self { 
            events_rx,
            events_tx,
            senders: HashMap::new(),
            receivers: HashMap::new(),
            sink: sink_tx
        })
    }

    pub async fn run(&mut self) {
        while let Some(event) = self.events_rx.recv().await {
            let tx_id = event.sender();
            let kind = event.kind();

            let sender = match self.senders.get(&tx_id) {
                Some(sender) => sender,
                None => return
            };

            match sender.subscribers.get(&kind) {
                Some(subscribers) => {
                    for rx_id in subscribers.iter() {
                        if let Some(rx) = self.receivers.get(rx_id) {
                            rx.channel.send(event.clone()).await.unwrap();
                        }
                    }
                },
                None => {}
            }

            self.sink.send(event).await.unwrap();
        }
    }
}

impl<'a, Id, Ev> EventBus for TokioEventBus<Id, Ev>
where Id: Serialize + Deserialize<'a> + Hash + Copy + Clone + Ord + Eq + Default,
      Ev: IsEvent<Id>
{
    type ActorId = Id;
    type Rx = mpsc::Receiver<Ev>;
    type Tx = mpsc::Sender<Ev>;
    type Message = Ev;

    fn channel(&self) -> Self::Tx {
        self.events_tx.clone()
    }

    fn subscribe(&mut self, rx: Id, tx: Id, kind: EventKind) -> Result<()> {
        if !self.senders.contains_key(&tx) {
            Err(EventBusError::UnknownEventSender)?;
        }
        if !self.receivers.contains_key(&rx) {
            Err(EventBusError::UnknownEventSender)?;
        }

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

        sender.subscribers.entry(kind).or_default().insert(rx);
        receiver.subscribed_to.entry(tx).or_default().insert(kind);

        Ok(())
    }
    fn add_sender(&mut self, id: Id) -> Result<()> {
        if self.senders.contains_key(&id) {
            Err(EventBusError::DuplicateEventSender)?;
        }

        self.senders.insert(id, EventSender::default());

        Ok(())
    }
    fn add_receiver(&mut self, id: Id, channel: mpsc::Sender<Ev>) -> Result<()> {
        if self.receivers.contains_key(&id) {
            Err(EventBusError::DuplicateEventReceiver)?;
        }

        self.receivers.insert(id, EventReceiver::new(channel) );

        Ok(())
    }
    fn rm_sender(&mut self, id: Self::ActorId) -> Result<()> {
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
    fn rm_receiver(&mut self, id: Self::ActorId) -> Result<Self::Tx> {
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
}
