use std::{
	collections::{HashMap, HashSet},
	sync::Arc,
};

use conduwuit_core::{Result, matrix::event::Event};
use futures::{StreamExt, pin_mut};
use ruma::{OwnedEventId, RoomId};

use super::Service;

/// Sync reachability callback used by the live overlay.
///
/// The closure is intentionally exact and side-effect free. The homeserver
/// constructs it from a KV-backed snapshot of the room DAG, then the
/// `Reachability` impl uses the cheap topological negative filter first and
/// only consults this closure when it cannot prove non-reachability.
type SlowPath = dyn Fn(&OwnedEventId, &OwnedEventId) -> rezzy::Reach + Send + Sync + 'static;

/// Homeserver-side live overlay for room-DAG reachability.
///
/// This is a drop-in `rezzy::Reachability` implementation. It is deliberately
/// conservative in scope: one topological label map, one exact slow-path
/// closure, no sealed-segment specialization yet.
#[derive(Clone)]
pub struct LiveReachability {
	topo: HashMap<OwnedEventId, u64>,
	slow_path: Arc<SlowPath>,
}

impl LiveReachability {
	/// Build an overlay from a preloaded snapshot.
	///
	/// `topo` is a dense topological ordering where smaller numbers are older.
	/// `parents` is a parent-adjacency snapshot keyed by event ID. The slow
	/// path walks this snapshot backwards from `to` to `from`, which is exact
	/// for the room DAG.
	#[must_use]
	pub fn from_snapshot(
		topo: HashMap<OwnedEventId, u64>,
		parents: HashMap<OwnedEventId, Vec<OwnedEventId>>,
	) -> Self {
		let parents = Arc::new(parents);
		let slow_path: Arc<SlowPath> = {
			let parents = Arc::clone(&parents);
			Arc::new(move |from, to| exact_reachability(from, to, &parents))
		};

		Self { topo, slow_path }
	}

	/// Build an overlay from a caller-provided exact slow path.
	#[must_use]
	pub fn new(topo: HashMap<OwnedEventId, u64>, slow_path: Arc<SlowPath>) -> Self {
		Self { topo, slow_path }
	}

	/// Snapshot the current room DAG from the timeline store and build a live
	/// overlay over it.
	///
	/// The snapshot is intentionally KV-backed and exact. This is the first
	/// adapter layer; sealed segments and batch overrides are left for later.
	pub async fn build(service: &Service, room_id: &RoomId) -> Result<Self> {
		let mut topo = HashMap::new();
		let mut parents = HashMap::new();
		let mut ordinal = 0_u64;

		let stream = service.topo_pdus(room_id, None);
		pin_mut!(stream);
		while let Some(item) = stream.next().await {
			let (_, pdu) = item?;
			ordinal = ordinal.saturating_add(1);
			let event_id = pdu.event_id().to_owned();
			topo.insert(event_id.clone(), ordinal);
			parents
				.insert(event_id, pdu.prev_events().map(ToOwned::to_owned).collect::<Vec<_>>());
		}

		Ok(Self::from_snapshot(topo, parents))
	}
}

impl rezzy::Reachability for LiveReachability {
	type Id = OwnedEventId;

	fn reaches(&self, from: &Self::Id, to: &Self::Id) -> rezzy::Reach {
		if from == to {
			return rezzy::Reach::Yes;
		}

		let Some(&from_topo) = self.topo.get(from) else {
			return rezzy::Reach::Unknown;
		};
		let Some(&to_topo) = self.topo.get(to) else {
			return rezzy::Reach::Unknown;
		};

		// Negative filter: if `from` is not strictly older than `to`, it cannot
		// reach `to` in a forward-only DAG.
		if from_topo >= to_topo {
			return rezzy::Reach::No;
		}

		(self.slow_path)(from, to)
	}
}

/// Build the exact slow path over a snapshot of parent edges.
fn exact_reachability(
	from: &OwnedEventId,
	to: &OwnedEventId,
	parents: &HashMap<OwnedEventId, Vec<OwnedEventId>>,
) -> rezzy::Reach {
	if from == to {
		return rezzy::Reach::Yes;
	}

	// We walk backwards from `to` over parent edges until either we find `from`
	// or exhaust the snapshot.
	let mut stack = vec![to.clone()];
	let mut seen = HashSet::new();

	while let Some(current) = stack.pop() {
		if !seen.insert(current.clone()) {
			continue;
		}

		let Some(prevs) = parents.get(&current) else {
			continue;
		};
		for prev in prevs {
			if prev == from {
				return rezzy::Reach::Yes;
			}
			stack.push(prev.clone());
		}
	}

	rezzy::Reach::No
}

impl Service {
	/// Build a KV-backed live reachability overlay for a room.
	pub async fn build_live_reachability(&self, room_id: &RoomId) -> Result<LiveReachability> {
		LiveReachability::build(self, room_id).await
	}
}

#[cfg(test)]
mod tests {
	use rezzy::Reachability;
	use ruma::owned_event_id;

	use super::*;

	fn reachability() -> LiveReachability {
		let a = owned_event_id!("$a:example.org");
		let b = owned_event_id!("$b:example.org");
		let c = owned_event_id!("$c:example.org");
		let d = owned_event_id!("$d:example.org");

		let mut topo = HashMap::new();
		topo.insert(a.clone(), 1);
		topo.insert(b.clone(), 2);
		topo.insert(c.clone(), 3);
		topo.insert(d.clone(), 4);

		let mut parents = HashMap::new();
		parents.insert(a.clone(), Vec::new());
		parents.insert(b.clone(), vec![a.clone()]);
		parents.insert(c.clone(), vec![b.clone()]);
		parents.insert(d.clone(), vec![b.clone()]);

		LiveReachability::from_snapshot(topo, parents)
	}

	#[test]
	fn topo_filter_and_slow_path_agree_on_chain_edges() {
		let reach = reachability();
		let a = owned_event_id!("$a:example.org");
		let b = owned_event_id!("$b:example.org");
		let c = owned_event_id!("$c:example.org");
		let d = owned_event_id!("$d:example.org");

		assert_eq!(reach.reaches(&a, &c), rezzy::Reach::Yes);
		assert_eq!(reach.reaches(&c, &a), rezzy::Reach::No);
		assert_eq!(reach.reaches(&a, &d), rezzy::Reach::Yes);
		assert_eq!(reach.reaches(&d, &a), rezzy::Reach::No);
		assert_eq!(reach.reaches(&b, &b), rezzy::Reach::Yes);
	}

	#[test]
	fn missing_labels_fall_back_to_unknown() {
		let reach = LiveReachability::new(HashMap::new(), Arc::new(|_, _| rezzy::Reach::Unknown));
		let a = owned_event_id!("$a:example.org");
		let b = owned_event_id!("$b:example.org");

		assert_eq!(reach.reaches(&a, &b), rezzy::Reach::Unknown);
	}
}
