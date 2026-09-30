use std::future::ready;

use conduwuit::{Event, PduCount, Result};
use conduwuit_core::matrix::pdu::PduEvent;
use futures::{StreamExt, pin_mut};
use ruma::{CanonicalJsonObject, EventId, RoomId};

/// Populates `unsigned.prev_content`, `unsigned.prev_sender`, and
/// `unsigned.replaces_state` on a PDU's JSON from the given previous state
/// event. This is idempotent — it first removes any existing values before
/// writing.
pub fn update_unsigned_prev_content(
	pdu_json: &mut CanonicalJsonObject,
	prev_state: &PduEvent,
) -> Result<()> {
	let unsigned = pdu_json.entry("unsigned".to_owned()).or_insert_with(|| {
		ruma::CanonicalJsonValue::Object(std::collections::BTreeMap::default())
	});

	if let ruma::CanonicalJsonValue::Object(unsigned) = unsigned {
		// Idempotently remove old (possibly wrong/missing) fields
		unsigned.remove("prev_content");
		unsigned.remove("prev_sender");
		unsigned.remove("replaces_state");

		let prev_content_value = prev_state.get_content_as_value();

		unsigned.insert(
			"prev_content".to_owned(),
			ruma::CanonicalJsonValue::Object(
				conduwuit_core::utils::to_canonical_object(prev_content_value).map_err(|e| {
					conduwuit::err!(Database(error!(
						"Failed to convert prev_state to canonical JSON: {e}"
					)))
				})?,
			),
		);
		unsigned.insert(
			"prev_sender".to_owned(),
			ruma::CanonicalJsonValue::String(prev_state.sender().to_string()),
		);
		unsigned.insert(
			"replaces_state".to_owned(),
			ruma::CanonicalJsonValue::String(prev_state.event_id().to_string()),
		);
	}

	Ok(())
}

#[conduwuit_macros::implement(super::Service)]
#[tracing::instrument(level = "debug", skip_all)]
pub async fn repair_room_unsigned(&self, room_id: &RoomId) -> Result<usize> {
	let pdus_stream = self
		.pdus(room_id, std::ops::Bound::Excluded(PduCount::min()))
		.filter_map(|r| ready(r.ok()))
		.filter(|(_count, pdu)| ready(pdu.state_key().is_some()))
		.map(|(_count, pdu)| {
			let event_id = pdu.event_id().to_owned();
			let kind = pdu.kind().to_string();
			let state_key = pdu.state_key().unwrap_or_default().to_owned();
			async move {
				// Get the stored JSON
				let pdu_json = self.get_pdu_json(&event_id).await;

				let mut already_has_prev_content = false;
				if let Ok(ref json) = pdu_json {
					if let Some(ruma::CanonicalJsonValue::Object(unsigned)) = json.get("unsigned")
					{
						if unsigned.contains_key("prev_content") {
							already_has_prev_content = true;
						}
					}
				}

				if already_has_prev_content {
					return (event_id, kind, state_key, pdu_json, None, true);
				}

				// Try state snapshot lookup
				let prev_state = if let Ok(ssh) = self
					.services
					.state_accessor
					.pdu_shortstatehash(&event_id)
					.await
				{
					self.services
						.state_accessor
						.state_get(ssh, &kind.clone().into(), &state_key)
						.await
						.ok()
						.filter(|prev| prev.event_id() != event_id)
				} else {
					None
				};

				(event_id, kind, state_key, pdu_json, prev_state, false)
			}
		})
		.buffer_unordered(500);

	pin_mut!(pdus_stream);

	tracing::info!("repair_unsigned: starting streaming state event repair in {room_id}");

	let mut repaired = 0_usize;
	let mut skipped = 0_usize;
	let mut errors = 0_usize;

	while let Some((
		event_id,
		_kind,
		_state_key,
		pdu_json,
		prev_state,
		already_has_prev_content,
	)) = pdus_stream.next().await
	{
		if already_has_prev_content {
			skipped = skipped.saturating_add(1);
			let processed = repaired.saturating_add(skipped).saturating_add(errors);
			if processed.is_multiple_of(1000) {
				tracing::info!(
					"repair_unsigned: {processed} processed ({repaired} repaired, {skipped} \
					 skipped)"
				);
			}
			continue;
		}

		let Ok(mut pdu_json): std::result::Result<CanonicalJsonObject, _> = pdu_json else {
			errors = errors.saturating_add(1);
			continue;
		};

		let unsigned = pdu_json.entry("unsigned".to_owned()).or_insert_with(|| {
			ruma::CanonicalJsonValue::Object(std::collections::BTreeMap::new())
		});

		let ruma::CanonicalJsonValue::Object(unsigned) = unsigned else {
			errors = errors.saturating_add(1);
			continue;
		};

		// If no state snapshot, try replaces_state fallback
		let prev_state = match prev_state {
			| Some(_) => prev_state,
			| None => {
				let replaces = unsigned
					.get("replaces_state")
					.and_then(|v| v.as_str())
					.and_then(|s| <&EventId>::try_from(s).ok())
					.filter(|eid| **eid != event_id);

				match replaces {
					| Some(prev_eid) => self.get_pdu(prev_eid).await.ok(),
					| None => {
						skipped = skipped.saturating_add(1);
						continue;
					},
				}
			},
		};

		// Populate from the previous state event
		if let Some(prev_state) = prev_state {
			if let Err(e) = update_unsigned_prev_content(&mut pdu_json, &prev_state) {
				tracing::warn!(%event_id, "repair_unsigned: failed to update unsigned: {e}");
				errors = errors.saturating_add(1);
				continue;
			}
		}

		// Write back
		let Ok(pdu_id) = self.get_pdu_id(&event_id).await else {
			errors = errors.saturating_add(1);
			continue;
		};

		if let Err(e) = self.replace_pdu(&pdu_id, &pdu_json, &event_id).await {
			tracing::warn!(%event_id, "repair_unsigned: failed to write updated json: {e}");
			errors = errors.saturating_add(1);
		} else {
			repaired = repaired.saturating_add(1);
		}

		let processed = repaired.saturating_add(skipped).saturating_add(errors);
		if processed.is_multiple_of(1000) {
			tracing::info!(
				"repair_unsigned: {processed} processed ({repaired} repaired, {skipped} skipped)"
			);
		}
	}

	tracing::info!(
		"repair_unsigned complete for {room_id}: {repaired} repaired, {skipped} skipped, \
		 {errors} errors"
	);

	Ok(repaired)
}
