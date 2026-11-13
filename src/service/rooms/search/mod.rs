use std::{collections::HashSet, ops::Bound, sync::Arc};

use conduwuit::{
	PduEvent, Result, debug_warn, implement,
	matrix::event::{Event, Matches},
};
use futures::{Stream, StreamExt};
use slipstream::{
	RoomId, UserId, api::client::search::search_events::v3::Criteria, events::TimelineEventType,
};

use crate::{Dep, rooms};

pub struct Service {
	services: Services,
}

struct Services {
	state_accessor: Dep<rooms::state_accessor::Service>,
	timeline: Dep<rooms::timeline::Service>,
	pdu_metadata: Dep<rooms::pdu_metadata::Service>,
}

#[derive(Clone, Debug)]
pub struct RoomQuery<'a> {
	pub room_id: &'a RoomId,
	pub user_id: Option<&'a UserId>,
	pub criteria: &'a Criteria,
	pub limit: usize,
	pub skip: usize,
}

const WORD_MAX_LEN: usize = 50;

impl crate::Service for Service {
	fn build(args: crate::Args<'_>) -> Result<Arc<Self>> {
		Ok(Arc::new(Self {
			services: Services {
				state_accessor: args
					.depend::<rooms::state_accessor::Service>("rooms::state_accessor"),
				timeline: args.depend::<rooms::timeline::Service>("rooms::timeline"),
				pdu_metadata: args.depend::<rooms::pdu_metadata::Service>("rooms::pdu_metadata"),
			},
		}))
	}

	fn name(&self) -> &str { crate::service::make_name(std::module_path!()) }
}

#[implement(Service)]
pub async fn search_pdus<'a>(
	&'a self,
	query: &'a RoomQuery<'a>,
	sender_user: &'a UserId,
) -> Result<(usize, impl Stream<Item = PduEvent> + Send + 'a)> {
	let tokens: Vec<String> = tokenize(&query.criteria.search_term).collect();
	let filter = &query.criteria.filter;
	let mut count = 0_usize;
	let mut skipped = 0_usize;
	let mut results = Vec::new();

	let mut pdus = std::pin::pin!(
		self.services
			.timeline
			.pdus_rev(query.room_id, Bound::Unbounded)
	);

	while let Some(item) = pdus.next().await {
		let Ok((_pdu_count, pdu)) = item else {
			continue;
		};

		if !matches_search(&pdu, &tokens) {
			continue;
		}

		count = count.saturating_add(1);

		// The page is full and the skip is satisfied: later matches only add to
		// the total, so don't pay for filter/visibility checks on them.
		if skipped >= query.skip && results.len() >= query.limit {
			continue;
		}

		if pdu.is_redacted() || !filter.matches(&pdu) {
			continue;
		}

		let Some(room_id) = pdu.room_id_or_hash() else {
			continue;
		};
		let Some(user_id) = query.user_id else {
			continue;
		};
		if !self
			.services
			.state_accessor
			.user_can_see_event(user_id, &room_id, pdu.event_id())
			.await
		{
			continue;
		}

		if skipped < query.skip {
			skipped = skipped.saturating_add(1);
			continue;
		}

		results.push(pdu);
	}

	let pdus = futures::stream::iter(results)
		.map(move |mut pdu| {
			pdu.set_unsigned(query.user_id);

			pdu
		})
		.then(async move |mut pdu| {
			if let Err(e) = self
				.services
				.pdu_metadata
				.add_bundled_aggregations_to_pdu(sender_user, &mut pdu)
				.await
			{
				debug_warn!("Failed to add bundled aggregations: {e}");
			}
			pdu
		});

	Ok((count, pdus))
}

/// Test whether a PDU's message body contains every search token.
///
/// Search reads the event bodies directly, so there is no inverted index to
/// keep in sync. Only room messages are searched, matching the old index's
/// scope.
fn matches_search(pdu: &PduEvent, tokens: &[String]) -> bool {
	if tokens.is_empty() || pdu.kind != TimelineEventType::RoomMessage {
		return false;
	}

	let content = pdu.get_content_as_value();
	let Some(body) = content.get("body").and_then(|body| body.as_str()) else {
		return false;
	};

	body_matches(body, tokens)
}

fn body_matches(body: &str, tokens: &[String]) -> bool {
	if tokens.is_empty() {
		return false;
	}

	let words: HashSet<String> = tokenize(body).collect();
	tokens.iter().all(|token| words.contains(token))
}

/// Splits a string into tokens used for search.
///
/// This may be used to tokenize both message bodies and search queries.
fn tokenize(body: &str) -> impl Iterator<Item = String> + Send + '_ {
	body.split_terminator(|c: char| !c.is_alphanumeric())
		.filter(|s| !s.is_empty())
		.filter(|word| word.len() <= WORD_MAX_LEN)
		.map(str::to_lowercase)
}

#[cfg(test)]
mod tests {
	use super::{body_matches, tokenize};

	#[test]
	fn search_requires_all_query_tokens() {
		let tokens = tokenize("Hello World").collect::<Vec<_>>();
		assert!(body_matches("hello, WORLD!", &tokens));
		assert!(!body_matches("hello there", &tokens));
	}

	#[test]
	fn empty_query_matches_nothing() {
		assert!(!body_matches("anything", &[]));
	}
}
