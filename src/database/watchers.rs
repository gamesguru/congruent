use std::{
	collections::{HashMap, hash_map},
	future::Future,
	pin::Pin,
	sync::Arc,
};

use conduwuit::SyncRwLock;
use event_listener::Event;

type Watcher = SyncRwLock<HashMap<Vec<u8>, Arc<Event>>>;

#[derive(Default)]
pub(crate) struct Watchers {
	watchers: Watcher,
}

impl Watchers {
	pub(crate) fn watch<'a>(
		&'a self,
		prefix: &[u8],
	) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
		let listener = match self.watchers.write().entry(prefix.to_vec()) {
			| hash_map::Entry::Occupied(o) => Arc::clone(o.get()),
			| hash_map::Entry::Vacant(v) => {
				let event = Arc::new(Event::new());
				v.insert(Arc::clone(&event));
				event
			},
		};

		Box::pin(listener.listen())
	}

	pub(crate) fn wake(&self, key: &[u8]) {
		let watchers = self.watchers.read();
		let mut triggered = Vec::new();
		for length in 0..=key.len() {
			if watchers.contains_key(&key[..length]) {
				triggered.push(&key[..length]);
			}
		}

		drop(watchers);

		if !triggered.is_empty() {
			let watchers = self.watchers.write();
			for prefix in triggered {
				if let Some(event) = watchers.get(prefix) {
					event.notify(usize::MAX);
				}
			}
		}
	}
}
