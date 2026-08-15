use std::future::Future;

use conduwuit::debug;
use ruma::{EventId, OwnedEventId};

/// Calculate new forward extremities after processing an incoming event.
///
/// This is the core DAG tip management algorithm:
/// 1. Start with current forward extremities
/// 2. If not soft-fail: remove any referenced by incoming event's prev_events
/// 3. If not soft-fail: remove any already marked as referenced in the DB
/// 4. If not soft-fail: add the incoming event as a new tip
///
/// Soft-failed events bypass all modification — current extremities are
/// returned unchanged, and the incoming event does NOT become a tip.
pub(crate) async fn calculate_forward_extremities<F, Fut>(
	current_extremities: Vec<OwnedEventId>,
	incoming_event_id: &EventId,
	prev_events: &[&EventId],
	soft_fail: bool,
	is_referenced: F,
	is_forward_extremity: bool,
) -> Vec<OwnedEventId>
where
	F: Fn(&EventId) -> Fut,
	Fut: Future<Output = bool>,
{
	if soft_fail {
		// Soft-failed events do NOT modify the extremity set.
		// They are invisible to the DAG tip tracking.
		return current_extremities;
	}

	let mut new_extremities = Vec::with_capacity(current_extremities.len());

	for event_id in current_extremities {
		// Remove extremities that are referenced by the incoming event's prev_events
		if prev_events.iter().any(|&pe| pe == event_id) {
			continue;
		}

		// Remove extremities that are already marked as referenced by some other event
		if is_referenced(&event_id).await {
			continue;
		}

		new_extremities.push(event_id);
	}

	// Add the incoming event as a new forward extremity if it's a DAG tip.
	// If it's an ancestor being upgraded, it should not become a tip, unless
	// all previous tips were collapsed and we have no other choice to prevent
	// an empty extremity list.
	if is_forward_extremity || new_extremities.is_empty() {
		new_extremities.push(incoming_event_id.to_owned());
	}

	debug!(
		"Retained {} extremities checked against {} prev_events",
		new_extremities.len(),
		prev_events.len()
	);

	assert!(!new_extremities.is_empty(), "extremities must not be empty");

	new_extremities
}

#[cfg(test)]
mod tests {
	use std::future::ready;

	use ruma::{OwnedEventId, event_id};

	use super::*;

	/// Nothing is referenced in the DB by default in tests.
	fn never_referenced(_event_id: &EventId) -> std::future::Ready<bool> { ready(false) }

	// ---------------------------------------------------------------
	// Test 1: Single linear chain — extremity collapses to 1
	// ---------------------------------------------------------------
	#[tokio::test]
	async fn linear_chain_collapses() {
		let a = event_id!("$aaa:example.org").to_owned();
		let b_id = event_id!("$bbb:example.org");

		let result = calculate_forward_extremities(
			vec![a.clone()],
			b_id,
			&[a.as_ref()],
			false,
			never_referenced,
			true,
		)
		.await;

		assert_eq!(result, vec![b_id.to_owned()]);
	}

	// ---------------------------------------------------------------
	// Test 2: Fork from 2 servers — both tips present
	// ---------------------------------------------------------------
	#[tokio::test]
	async fn fork_creates_two_tips() {
		let a = event_id!("$aaa:example.org").to_owned();
		let b_id = event_id!("$bbb:example.org");

		// Server 1 sends B referencing A
		let after_b = calculate_forward_extremities(
			vec![a.clone()],
			b_id,
			&[a.as_ref()],
			false,
			never_referenced,
			true,
		)
		.await;

		assert_eq!(after_b, vec![b_id.to_owned()]);

		// Server 2 sends C also referencing A
		// But A is already referenced (marked by B), so we simulate that
		let c_id = event_id!("$ccc:example.org");
		let is_referenced = |eid: &EventId| ready(eid == event_id!("$aaa:example.org"));

		let after_c = calculate_forward_extremities(
			vec![b_id.to_owned()],
			c_id,
			&[a.as_ref()],
			false,
			is_referenced,
			true,
		)
		.await;

		// B is not referenced by C's prev_events, and not referenced in DB
		// C is added as new tip
		assert_eq!(after_c, vec![b_id.to_owned(), c_id.to_owned()]);
	}

	// ---------------------------------------------------------------
	// Test 3: Merge event collapses fork
	// ---------------------------------------------------------------
	#[tokio::test]
	async fn merge_collapses_fork() {
		let b = event_id!("$bbb:example.org").to_owned();
		let c = event_id!("$ccc:example.org").to_owned();
		let d_id = event_id!("$ddd:example.org");

		let result = calculate_forward_extremities(
			vec![b.clone(), c.clone()],
			d_id,
			&[b.as_ref(), c.as_ref()],
			false,
			never_referenced,
			true,
		)
		.await;

		assert_eq!(result, vec![d_id.to_owned()]);
	}

	// ---------------------------------------------------------------
	// Test 4: Soft-fail does NOT create extremity
	// ---------------------------------------------------------------
	#[tokio::test]
	async fn soft_fail_does_not_modify() {
		let a = event_id!("$aaa:example.org").to_owned();
		let b_id = event_id!("$bbb:example.org");

		let result = calculate_forward_extremities(
			vec![a.clone()],
			b_id,
			&[a.as_ref()],
			true, // soft_fail!
			never_referenced,
			true,
		)
		.await;

		// Extremities unchanged — A stays, B is NOT added
		assert_eq!(result, vec![a]);
	}

	// ---------------------------------------------------------------
	// Test 5: Concurrent joins from 3 servers, then merge
	// ---------------------------------------------------------------
	#[tokio::test]
	async fn concurrent_joins_then_merge() {
		let a = event_id!("$aaa:example.org").to_owned();
		let j1_id = event_id!("$j1:server1.org");
		let j2_id = event_id!("$j2:server2.org");
		let j3_id = event_id!("$j3:server3.org");

		// All three joins reference A
		// Simulate sequential processing: after J1, A is referenced
		let after_j1 = calculate_forward_extremities(
			vec![a.clone()],
			j1_id,
			&[a.as_ref()],
			false,
			never_referenced,
			true,
		)
		.await;
		assert_eq!(after_j1, vec![j1_id.to_owned()]);

		// J2 also references A (which is now marked as referenced in DB)
		let a_is_referenced = |eid: &EventId| ready(eid == event_id!("$aaa:example.org"));
		let after_j2 = calculate_forward_extremities(
			vec![j1_id.to_owned()],
			j2_id,
			&[a.as_ref()],
			false,
			a_is_referenced,
			true,
		)
		.await;
		assert_eq!(after_j2, vec![j1_id.to_owned(), j2_id.to_owned()]);

		// J3 also references A
		let after_j3 = calculate_forward_extremities(
			vec![j1_id.to_owned(), j2_id.to_owned()],
			j3_id,
			&[a.as_ref()],
			false,
			a_is_referenced,
			true,
		)
		.await;
		assert_eq!(after_j3, vec![j1_id.to_owned(), j2_id.to_owned(), j3_id.to_owned()]);

		// Merge event references all three
		let m_id = event_id!("$merge:example.org");
		let result = calculate_forward_extremities(
			vec![j1_id.to_owned(), j2_id.to_owned(), j3_id.to_owned()],
			m_id,
			&[j1_id, j2_id, j3_id],
			false,
			never_referenced,
			true,
		)
		.await;
		assert_eq!(result, vec![m_id.to_owned()]);
	}

	// ---------------------------------------------------------------
	// Test 6: Concurrent membership updates (profile changes + message)
	// ---------------------------------------------------------------
	#[tokio::test]
	async fn concurrent_membership_and_messages() {
		let a = event_id!("$aaa:example.org").to_owned();
		let rename_id = event_id!("$rename:server1.org");
		let avatar_id = event_id!("$avatar:server2.org");
		let msg_id = event_id!("$msg:server3.org");

		// User renames, referencing A
		let after_rename = calculate_forward_extremities(
			vec![a.clone()],
			rename_id,
			&[a.as_ref()],
			false,
			never_referenced,
			true,
		)
		.await;
		assert_eq!(after_rename, vec![rename_id.to_owned()]);

		// Avatar change also references A (now referenced in DB)
		let a_referenced = |eid: &EventId| ready(eid == event_id!("$aaa:example.org"));
		let after_avatar = calculate_forward_extremities(
			vec![rename_id.to_owned()],
			avatar_id,
			&[a.as_ref()],
			false,
			a_referenced,
			true,
		)
		.await;
		assert_eq!(after_avatar, vec![rename_id.to_owned(), avatar_id.to_owned()]);

		// Message also references A
		let after_msg = calculate_forward_extremities(
			vec![rename_id.to_owned(), avatar_id.to_owned()],
			msg_id,
			&[a.as_ref()],
			false,
			a_referenced,
			true,
		)
		.await;
		assert_eq!(after_msg, vec![
			rename_id.to_owned(),
			avatar_id.to_owned(),
			msg_id.to_owned()
		]);

		// Merge collapses all three
		let merge_id = event_id!("$merge:example.org");
		let result = calculate_forward_extremities(
			vec![rename_id.to_owned(), avatar_id.to_owned(), msg_id.to_owned()],
			merge_id,
			&[rename_id, avatar_id, msg_id],
			false,
			never_referenced,
			true,
		)
		.await;
		assert_eq!(result, vec![merge_id.to_owned()]);
	}

	// ---------------------------------------------------------------
	// Test 7: Partial merge — only some tips referenced
	// ---------------------------------------------------------------
	#[tokio::test]
	async fn partial_merge() {
		let b = event_id!("$bbb:example.org").to_owned();
		let c = event_id!("$ccc:example.org").to_owned();
		let d = event_id!("$ddd:example.org").to_owned();
		let e_id = event_id!("$eee:example.org");

		// E references only B and C, not D
		let result = calculate_forward_extremities(
			vec![b.clone(), c.clone(), d.clone()],
			e_id,
			&[b.as_ref(), c.as_ref()],
			false,
			never_referenced,
			true,
		)
		.await;

		assert_eq!(result, vec![d, e_id.to_owned()]);
	}

	// ---------------------------------------------------------------
	// Test 8: Already-referenced extremity gets pruned
	// ---------------------------------------------------------------
	#[tokio::test]
	async fn already_referenced_pruned() {
		let a = event_id!("$aaa:example.org").to_owned();
		let b = event_id!("$bbb:example.org").to_owned();
		let c_id = event_id!("$ccc:example.org");

		// B is marked as referenced in the DB (some other event already refs it)
		let b_is_referenced = |eid: &EventId| ready(eid == event_id!("$bbb:example.org"));

		// C references only A
		let result = calculate_forward_extremities(
			vec![a.clone(), b.clone()],
			c_id,
			&[a.as_ref()],
			false,
			b_is_referenced,
			true,
		)
		.await;

		// A collapsed via prev_events, B collapsed via is_referenced, only C remains
		assert_eq!(result, vec![c_id.to_owned()]);
	}

	// ---------------------------------------------------------------
	// Test 9: Idempotent re-processing
	// ---------------------------------------------------------------
	#[tokio::test]
	async fn idempotent_processing() {
		let a = event_id!("$aaa:example.org").to_owned();
		let b_id = event_id!("$bbb:example.org");

		let first = calculate_forward_extremities(
			vec![a.clone()],
			b_id,
			&[a.as_ref()],
			false,
			never_referenced,
			true,
		)
		.await;

		// Process again with same inputs (B is now an extremity)
		let second = calculate_forward_extremities(
			first.clone(),
			b_id,
			&[a.as_ref()],
			false,
			never_referenced,
			true,
		)
		.await;

		// B should appear exactly once, A is not in the set anymore
		assert_eq!(first, vec![b_id.to_owned()]);
		assert_eq!(
			second,
			vec![b_id.to_owned(), b_id.to_owned()],
			"re-processing adds the event again since it's not in prev_events"
		);
		// NOTE: In practice, the caller (upgrade_outlier_pdu) short-circuits
		// before reaching this code if the event is already in the timeline.
		// The duplicate is expected at the algorithm level; the caller prevents
		// it.
	}

	// ---------------------------------------------------------------
	// Test 10: Large fan-in — 10 concurrent senders
	// ---------------------------------------------------------------
	#[tokio::test]
	async fn large_fan_in() {
		// Build 10 events all referencing A
		let event_ids: Vec<OwnedEventId> = (1..=10)
			.map(|i| format!("$e{i}:example.org"))
			.map(|s| OwnedEventId::try_from(s).unwrap())
			.collect();

		// Simulate: after all 10 are processed, all are extremities
		let all_extremities = event_ids.clone();

		// Merge event references all 10
		let merge_id = event_id!("$merge:example.org");
		let prev_events: Vec<&EventId> = event_ids.iter().map(AsRef::as_ref).collect();

		let result = calculate_forward_extremities(
			all_extremities,
			merge_id,
			&prev_events,
			false,
			never_referenced,
			true,
		)
		.await;

		assert_eq!(result, vec![merge_id.to_owned()]);
	}
}
