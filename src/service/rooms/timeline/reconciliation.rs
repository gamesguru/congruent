use std::collections::BTreeMap;

use conduwuit::{Result, err};
use conduwuit_core::utils::hash::algebraic::element_hash_for_pdu;
use futures::StreamExt;
use ruma::{OwnedEventId, RoomId};

#[derive(Clone, Debug)]
pub struct RoomReconciliationState {
	pub resident: rezzy::reconcile::ResidentKernel,
	pub sorted_h64: Vec<u64>,
	pub h64_to_event_hashes: BTreeMap<u64, Vec<(OwnedEventId, rezzy::reconcile::ElementHash)>>,
	pub known_event_count: u64,
	pub depth_range: [u64; 2],
	pub origin_server_ts_range: [u64; 2],
}

impl RoomReconciliationState {
	fn new() -> Self {
		Self {
			resident: rezzy::reconcile::ResidentKernel::new(),
			sorted_h64: Vec::new(),
			h64_to_event_hashes: BTreeMap::new(),
			known_event_count: 0,
			depth_range: [0, 0],
			origin_server_ts_range: [0, 0],
		}
	}

	fn push_pdu(
		&mut self,
		pdu: &conduwuit_core::matrix::pdu::PduEvent,
		room_version: &ruma::RoomVersionId,
	) -> Result {
		let hash = element_hash_for_pdu(pdu, room_version)?;
		self.resident
			.insert(hash)
			.map_err(|e| err!("failed to update reconciliation resident state: {e:?}"))?;
		self.sorted_h64.push(hash.h64);
		self.h64_to_event_hashes
			.entry(hash.h64)
			.or_default()
			.push((pdu.event_id.clone(), hash));
		self.known_event_count = self.known_event_count.saturating_add(1);
		let depth = u64::from(pdu.depth);
		let ts = u64::from(pdu.origin_server_ts);
		if self.known_event_count == 1 {
			self.depth_range = [depth, depth];
			self.origin_server_ts_range = [ts, ts];
		} else {
			self.depth_range[0] = self.depth_range[0].min(depth);
			self.depth_range[1] = self.depth_range[1].max(depth);
			self.origin_server_ts_range[0] = self.origin_server_ts_range[0].min(ts);
			self.origin_server_ts_range[1] = self.origin_server_ts_range[1].max(ts);
		}
		Ok(())
	}
}

impl super::Service {
	pub async fn reconciliation_state(
		&self,
		room_id: &RoomId,
	) -> Result<RoomReconciliationState> {
		if let Some(state) = self.reconciliation_cache.lock().get_mut(room_id) {
			return Ok(state.clone());
		}

		let state = self.rebuild_reconciliation_state(room_id).await?;
		self.reconciliation_cache
			.lock()
			.insert(room_id.to_owned(), state.clone());
		Ok(state)
	}

	pub async fn try_update_reconciliation_state(
		&self,
		room_id: &RoomId,
		pdu: &conduwuit_core::matrix::pdu::PduEvent,
	) -> Result {
		let room_version = self.services.state.get_room_version(room_id).await?;
		if let Some(state) = self.reconciliation_cache.lock().get_mut(room_id) {
			state.push_pdu(pdu, &room_version)?;
		}
		Ok(())
	}

	async fn rebuild_reconciliation_state(
		&self,
		room_id: &RoomId,
	) -> Result<RoomReconciliationState> {
		let room_version = self.services.state.get_room_version(room_id).await?;
		let mut state = RoomReconciliationState::new();

		let pdus = self.all_pdus(room_id);
		futures::pin_mut!(pdus);
		while let Some((_, pdu)) = pdus.next().await {
			state.push_pdu(&pdu, &room_version)?;
		}

		let outliers = self.db.outlier_pdus(room_id);
		futures::pin_mut!(outliers);
		while let Some(pdu) = outliers.next().await {
			state.push_pdu(&pdu?, &room_version)?;
		}

		state.sorted_h64.sort_unstable();
		Ok(state)
	}
}

#[cfg(test)]
mod tests {
	use ruma::RoomVersionId;
	use serde_json::json;

	use super::*;
	use crate::utils::to_canonical_object;

	fn test_pdu(event_id: &str, room_id: &str, depth: u64, ts: u64) -> conduwuit_core::PduEvent {
		let event_id = event_id.try_into().expect("valid event id");
		let room_id = room_id.try_into().expect("valid room id");
		let json = to_canonical_object(json!({
			"room_id": room_id.as_str(),
			"sender": "@alice:example.com",
			"origin_server_ts": ts,
			"type": "m.room.message",
			"content": {"msgtype": "m.text", "body": "hello"},
			"prev_events": [],
			"depth": depth,
			"auth_events": [],
			"redacts": null,
			"hashes": {"sha256": "0123456789abcdef"},
		}))
		.expect("canonical json");

		conduwuit_core::PduEvent::from_id_val(&event_id, json, Some(room_id.as_ref()))
			.expect("valid pdu")
	}

	#[test]
	fn push_pdu_tracks_counts_and_ranges() {
		let room_version = RoomVersionId::V11;
		let mut state = RoomReconciliationState::new();
		let first = test_pdu("$a:example.com", "!room:example.com", 2, 20);
		let second = test_pdu("$b:example.com", "!room:example.com", 7, 10);

		state.push_pdu(&first, &room_version).expect("first");
		state.push_pdu(&second, &room_version).expect("second");

		assert_eq!(state.known_event_count, 2);
		assert_eq!(state.depth_range, [2, 7]);
		assert_eq!(state.origin_server_ts_range, [10, 20]);
		assert_eq!(state.h64_to_event_hashes.len(), 2);
		assert_eq!(state.resident.accumulator().known_event_count(), 2);
		assert_eq!(state.sorted_h64.len(), 2);
	}
}
