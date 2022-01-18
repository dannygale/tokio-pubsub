#[derive(Default)]
pub struct Filter<Topic, Event> {
    topic: Option<Topic>,
    func: Option<Preproc<Event>>,
}

impl<Topic, Event> Filter<Topic, Event> {
    pub fn new() -> Self {
        Self {
            topic: None,
            func: None,
        }
    }

    pub fn topic(self, topic: Topic) -> Self {
        Self {
            topic: Some(topic),
            ..self
        }
    }

    pub fn func(self, func: Preproc<Event>) -> Self {
        Self {
            func: Some(func),
            ..self
        }
    }
}
