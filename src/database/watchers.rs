use std::{
	collections::{HashMap, hash_map},
	future::Future,
	pin::Pin,
};

use async_channel::{Receiver, Sender, unbounded};
use conduwuit::SyncRwLock;

type Watcher = SyncRwLock<HashMap<Vec<u8>, (Sender<()>, Receiver<()>)>>;

#[derive(Default)]
pub(crate) struct Watchers {
	watchers: Watcher,
}

impl Watchers {
	pub(crate) fn watch<'a>(
		&'a self,
		prefix: &[u8],
	) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
		let rx = match self.watchers.write().entry(prefix.to_vec()) {
			| hash_map::Entry::Occupied(o) => o.get().1.clone(),
			| hash_map::Entry::Vacant(v) => {
				let (tx, rx) = unbounded();
				v.insert((tx, rx.clone()));
				rx
			},
		};

		Box::pin(async move {
			// Tx is never destroyed
			rx.recv().await.expect("channel should still be open");
		})
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
			let mut watchers = self.watchers.write();
			for prefix in triggered {
				if let Some(tx) = watchers.remove(prefix) {
					tx.0.try_send(()).expect("channel should still be open");
				}
			}
		}
	}
}
