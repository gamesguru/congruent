use std::{collections::BTreeMap, time::Instant};

use conduwuit::{
	Err, Event, PduEvent, Result, debug::INFO_SPAN_LEVEL, defer, implement,
	utils::continue_exponential_backoff_secs, warn,
};
use ruma::{CanonicalJsonValue, EventId, MilliSecondsSinceUnixEpoch, RoomId, ServerName};
use tracing::debug;

#[implement(super::Service)]
#[allow(clippy::type_complexity)]
#[allow(clippy::too_many_arguments)]
#[tracing::instrument(
	name = "prev",
	level = INFO_SPAN_LEVEL,
	skip_all,
	fields(%prev_id),
)]
pub(super) async fn handle_prev_pdu<'a, Pdu>(
	&self,
	origin: &'a ServerName,
	event_id: &'a EventId,
	room_id: &'a RoomId,
	eventid_info: Option<(PduEvent, BTreeMap<String, CanonicalJsonValue>)>,
	create_event: &'a Pdu,
	first_ts_in_room: MilliSecondsSinceUnixEpoch,
	prev_id: &'a EventId,
) -> Result
where
	Pdu: Event + Send + Sync,
{
	// Check for disabled again because it might have changed
	if self.services.metadata.is_disabled(room_id).await {
		return Err!(Request(Forbidden(debug_warn!(
			"Federaton of room {room_id} is currently disabled on this server. Request by \
			 origin {origin} and event ID {event_id}"
		))));
	}

	if let Some((time, tries)) = self
		.services
		.globals
		.bad_event_ratelimiter
		.read()
		.get(prev_id)
	{
		// Exponential backoff
		const MIN_DURATION: u64 = 5 * 60;
		const MAX_DURATION: u64 = 60 * 60 * 24;
		if continue_exponential_backoff_secs(MIN_DURATION, MAX_DURATION, time.elapsed(), *tries) {
			debug!(
				?tries,
			duration = ?time.elapsed(),
			"Backing off from prev_event"
			);
			return Ok(());
		}
	}

	let (pdu, json) = match eventid_info {
		| Some(eventid_info) => eventid_info,
		| None => {
			// `fetch_prev` only returns entries for prev_events it actually went
			// and fetched -- it pre-filters anything `pdu_exists` already knows
			// about (see its `still_needed` filter), and that check matches
			// *outliers* too, not just timeline events. So `None` here doesn't
			// mean "already handled", it can also mean "already known, but only
			// ever stored as an outlier" -- e.g. pulled in earlier purely to
			// satisfy some other event's auth chain. Left alone, that event
			// stays permanently invisible to `/messages`: nothing else in this
			// call tree will ever revisit it. Recover the stored outlier and
			// promote it ourselves; `upgrade_outlier_to_timeline_pdu` already
			// no-ops if it turns out to be a timeline event after all.
			if self.services.timeline.non_outlier_pdu_exists(prev_id).await {
				return Ok(());
			}
			if self.services.pdu_metadata.is_event_rejected(prev_id).await {
				// Read-only check here: whether we go on to actually retry is
				// still gated by the further early-returns below (outlier JSON
				// missing, unparsable, or too old), and we must not clear the
				// rejection marker unless we're actually about to hand the event
				// to `upgrade_outlier_to_timeline_pdu` for reprocessing. Clearing
				// it here and then bailing out below would leave the event
				// looking "accepted" (`is_event_accepted` is just
				// `!is_event_rejected`) to every other caller without ever having
				// re-evaluated it.
				let retry_worthy = self
					.services
					.pdu_metadata
					.get_rejection_reason(prev_id)
					.await
					.is_some_and(|reason| {
						crate::rooms::pdu_metadata::is_retryable_rejection_reason(&reason)
					});
				if !retry_worthy {
					return Ok(());
				}
			}
			let Ok(json) = self.services.timeline.get_outlier_pdu_json(prev_id).await else {
				return Ok(());
			};
			let Ok(pdu) = PduEvent::from_id_val(prev_id, json.clone(), Some(room_id)) else {
				warn!("Stored outlier {prev_id} failed to parse back into a PduEvent");
				return Ok(());
			};
			(pdu, json)
		},
	};

	// Skip old events
	if pdu.origin_server_ts() < first_ts_in_room {
		return Ok(());
	}

	// We're now committed to actually reprocessing `prev_id` via
	// `upgrade_outlier_to_timeline_pdu` below, so this is the right point to
	// clear a retryable rejection marker -- atomically with the decision to
	// retry, not before it (see the comment above). If the event turned out
	// not to be marked rejected at all, this is a no-op.
	self.services
		.pdu_metadata
		.take_retry_if_rejection_retryable(prev_id)
		.await;

	let start_time = Instant::now();
	self.federation_handletime
		.write()
		.insert(room_id.into(), ((*prev_id).to_owned(), start_time));

	defer! {{
		if self.services.server.running() {
			self.federation_handletime
				.write()
				.remove(room_id);
		}
	}};

	// Keep the large upgrade future out of handle_prev_pdu's own future.
	Box::pin(self.upgrade_outlier_to_timeline_pdu(
		pdu,
		json,
		create_event,
		origin,
		room_id,
		false,
		false,
		false,
	))
	.await?;

	debug!(
		elapsed = ?start_time.elapsed(),
		"Handled prev_event",
	);

	Ok(())
}
