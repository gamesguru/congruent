use axum::extract::State;
use conduwuit::{Event, Result};
use futures::StreamExt;
use ruma::{OwnedEventId, api::federation::event::event_relationships};

use super::AccessCheck;
use crate::{
	Ruma,
	msc2836::{self, Params, Requester},
};

/// # `POST /_matrix/federation/unstable/event_relationships`
///
/// Walks the `m.relationship` DAG from an anchor event, restricted to
/// events this server is participating in (no further outbound federation
/// requests are made, to avoid routing loops between servers).
///
/// An implementation of [MSC2836](https://github.com/matrix-org/matrix-spec-proposals/pull/2836)
pub(crate) async fn get_event_relationships_route(
	State(services): State<crate::State>,
	body: Ruma<event_relationships::unstable::Request>,
) -> Result<event_relationships::unstable::Response> {
	let origin = body.origin();

	if let Some(room_id) = &body.room_id {
		AccessCheck {
			services: &services,
			origin,
			room_id,
			event_id: None,
		}
		.check()
		.await?;
	}

	let params = Params::defaulted(msc2836::DefaultedParams {
		event_id: body.event_id.clone(),
		room_id: body.room_id.clone(),
		max_depth: body.max_depth,
		max_breadth: body.max_breadth,
		limit: body.limit,
		depth_first: body.depth_first,
		recent_first: body.recent_first,
		include_parent: body.include_parent,
		include_children: body.include_children,
		direction: body.direction.clone(),
	});

	let (events, limited) =
		Box::pin(msc2836::resolve(&services, Requester::Federation(origin), params)).await?;

	let mut raw_events = Vec::with_capacity(events.len());
	let mut auth_chain_ids = std::collections::HashSet::<OwnedEventId>::new();
	for pdu in &events {
		raw_events.push(msc2836::to_raw_json_with_children(&services, pdu).await);
		if let Some(room_id) = pdu.room_id_or_hash() {
			let chain: Vec<OwnedEventId> = services
				.rooms
				.auth_chain
				.event_ids_iter(&room_id, std::iter::once(pdu.event_id()))
				.filter_map(|r| async move { r.ok() })
				.collect()
				.await;
			auth_chain_ids.extend(chain);
		}
	}

	let mut auth_chain = Vec::with_capacity(auth_chain_ids.len());
	for event_id in auth_chain_ids {
		if let Ok(pdu) = services.rooms.timeline.get_pdu(&event_id).await {
			auth_chain.push(
				services
					.sending
					.convert_to_outgoing_federation_event(pdu.to_canonical_object())
					.await,
			);
		}
	}

	Ok(event_relationships::unstable::Response {
		events: raw_events,
		next_batch: None,
		limited,
		auth_chain,
	})
}
