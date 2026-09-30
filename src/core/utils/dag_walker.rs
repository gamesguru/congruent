use std::{collections::HashSet, future::Future};

use ruma::OwnedEventId;

use crate::PduEvent;

/// Result of fetching an event for the DAG walker.
pub enum FetchResult {
	/// The event was found in the timeline.
	Timeline(PduEvent, bool, bool), // pdu, is_rejected, is_soft_failed
	/// The event was found as an outlier.
	Outlier(PduEvent, bool, bool), // pdu, is_rejected, is_soft_failed
	/// The event could not be found locally.
	Missing,
}

/// Statistics and missing events returned from the DAG walk.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct WalkResult {
	pub in_timeline: usize,
	pub in_outlier: usize,
	pub missing: Vec<OwnedEventId>,
	pub rejected: usize,
	pub soft_failed: usize,
}

/// Walks the DAG asynchronously starting from the given seed events.
///
/// `fetch_event` is an async closure that retrieves an event by ID.
pub async fn walk_dag<F, Fut>(seed_events: Vec<OwnedEventId>, mut fetch_event: F) -> WalkResult
where
	F: FnMut(OwnedEventId) -> Fut,
	Fut: Future<Output = FetchResult>,
{
	let mut in_timeline: usize = 0;
	let mut in_outlier: usize = 0;
	let mut missing = Vec::new();

	let mut rejected: usize = 0;
	let mut soft_failed: usize = 0;

	let mut queue = seed_events.clone();
	let mut seen: HashSet<OwnedEventId> = seed_events.into_iter().collect();

	while let Some(event_id) = queue.pop() {
		match fetch_event(event_id.clone()).await {
			| FetchResult::Timeline(pdu, is_rejected, is_soft_failed) => {
				in_timeline = in_timeline.saturating_add(1);
				if is_rejected {
					rejected = rejected.saturating_add(1);
				}
				if is_soft_failed {
					soft_failed = soft_failed.saturating_add(1);
				}
				for auth_id in &pdu.auth_events {
					if seen.insert(auth_id.clone()) {
						queue.push(auth_id.clone());
					}
				}
			},
			| FetchResult::Outlier(pdu, is_rejected, is_soft_failed) => {
				in_outlier = in_outlier.saturating_add(1);
				if is_rejected {
					rejected = rejected.saturating_add(1);
				}
				if is_soft_failed {
					soft_failed = soft_failed.saturating_add(1);
				}
				for auth_id in &pdu.auth_events {
					if seen.insert(auth_id.clone()) {
						queue.push(auth_id.clone());
					}
				}
			},
			| FetchResult::Missing => {
				missing.push(event_id);
			},
		}
	}

	WalkResult {
		in_timeline,
		in_outlier,
		missing,
		rejected,
		soft_failed,
	}
}

#[cfg(test)]
mod tests {
	use std::{
		collections::HashMap,
		sync::{Arc, Mutex},
	};

	use ruma::{EventId, event_id};

	use super::*;

	fn mock_pdu(id: &EventId, auth_events: Vec<OwnedEventId>) -> PduEvent {
		// Minimal PduEvent for testing auth_events traversal
		use ruma::{events::TimelineEventType, uint, user_id};

		PduEvent {
			event_id: id.to_owned(),
			room_id: None,
			sender: user_id!("@test:example.com").to_owned(),
			origin: None,
			origin_server_ts: uint!(0),
			kind: TimelineEventType::RoomMessage,
			content: serde_json::from_str("{}").unwrap(),
			state_key: None,
			prev_events: vec![],
			depth: uint!(0),
			auth_events,
			redacts: None,
			unsigned: None,
			hashes: crate::matrix::pdu::EventHash { sha256: "test".into() },
			signatures: None,
			rejected: false,
		}
	}

	// Helper to abstract the Arc<Mutex<>> boilerplate
	fn mock_fetcher(
		db: Arc<Mutex<HashMap<OwnedEventId, FetchResult>>>,
	) -> impl FnMut(OwnedEventId) -> std::pin::Pin<Box<dyn Future<Output = FetchResult>>> {
		move |id: OwnedEventId| {
			let db = db.clone();
			Box::pin(async move {
				db.lock()
					.unwrap()
					.remove(&id)
					.unwrap_or(FetchResult::Missing)
			})
		}
	}

	#[tokio::test]
	async fn test_clean_dag() {
		let mut db = HashMap::new();
		db.insert(
			event_id!("$1").to_owned(),
			FetchResult::Timeline(
				mock_pdu(event_id!("$1"), vec![event_id!("$2").to_owned()]),
				false,
				false,
			),
		);
		db.insert(
			event_id!("$2").to_owned(),
			FetchResult::Timeline(
				mock_pdu(event_id!("$2"), vec![event_id!("$3").to_owned()]),
				false,
				false,
			),
		);
		db.insert(
			event_id!("$3").to_owned(),
			FetchResult::Timeline(mock_pdu(event_id!("$3"), vec![]), false, false),
		);

		let result =
			walk_dag(vec![event_id!("$1").to_owned()], mock_fetcher(Arc::new(Mutex::new(db))))
				.await;

		assert_eq!(result.in_timeline, 3);
		assert_eq!(result.in_outlier, 0);
		assert!(result.missing.is_empty());
	}

	#[tokio::test]
	async fn test_dag_with_outliers() {
		let mut db = HashMap::new();
		db.insert(
			event_id!("$1").to_owned(),
			FetchResult::Timeline(
				mock_pdu(event_id!("$1"), vec![event_id!("$2").to_owned()]),
				false,
				false,
			),
		);
		db.insert(
			event_id!("$2").to_owned(),
			FetchResult::Outlier(
				mock_pdu(event_id!("$2"), vec![event_id!("$3").to_owned()]),
				false,
				false,
			),
		);
		db.insert(
			event_id!("$3").to_owned(),
			FetchResult::Outlier(mock_pdu(event_id!("$3"), vec![]), false, false),
		);

		let result =
			walk_dag(vec![event_id!("$1").to_owned()], mock_fetcher(Arc::new(Mutex::new(db))))
				.await;

		assert_eq!(result.in_timeline, 1);
		assert_eq!(result.in_outlier, 2);
		assert!(result.missing.is_empty());
	}

	#[tokio::test]
	async fn test_dag_with_holes() {
		let mut db = HashMap::new();
		db.insert(
			event_id!("$1").to_owned(),
			FetchResult::Timeline(
				mock_pdu(event_id!("$1"), vec![
					event_id!("$2").to_owned(),
					event_id!("$3").to_owned(),
				]),
				false,
				false,
			),
		);
		// $2 is missing
		db.insert(
			event_id!("$3").to_owned(),
			FetchResult::Timeline(mock_pdu(event_id!("$3"), vec![]), false, false),
		);

		let result =
			walk_dag(vec![event_id!("$1").to_owned()], mock_fetcher(Arc::new(Mutex::new(db))))
				.await;

		assert_eq!(result.in_timeline, 2);
		assert_eq!(result.missing, vec![event_id!("$2").to_owned()]);
	}

	#[tokio::test]
	async fn test_cyclic_dag() {
		// A -> B -> A
		let fetcher = |id: OwnedEventId| {
			Box::pin(async move {
				if id == event_id!("$A") {
					FetchResult::Timeline(
						mock_pdu(event_id!("$A"), vec![event_id!("$B").to_owned()]),
						false,
						false,
					)
				} else if id == event_id!("$B") {
					FetchResult::Timeline(
						mock_pdu(event_id!("$B"), vec![event_id!("$A").to_owned()]),
						false,
						false,
					)
				} else {
					FetchResult::Missing
				}
			})
		};

		let result = walk_dag(vec![event_id!("$A").to_owned()], fetcher).await;

		assert_eq!(result.in_timeline, 2);
		assert_eq!(result.missing.len(), 0);
	}
}
