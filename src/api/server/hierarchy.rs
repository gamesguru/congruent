use axum::extract::State;
use conduwuit::{
	Err, Result, info,
	utils::stream::{BroadbandExt, IterStream},
};
use conduwuit_service::rooms::spaces::{
	Identifier, SummaryAccessibility, get_parent_children_via,
};
use futures::{FutureExt, StreamExt};
use ruma::api::federation::space::get_hierarchy;

use crate::Ruma;

/// # `GET /_matrix/federation/v1/hierarchy/{roomId}`
///
/// Gets the space tree in a depth-first manner to locate child rooms of a given
/// space.
pub(crate) async fn get_hierarchy_route(
	State(services): State<crate::State>,
	body: Ruma<get_hierarchy::v1::Request>,
) -> Result<get_hierarchy::v1::Response> {
	if !services.rooms.metadata.exists(&body.room_id).await {
		return Err!(Request(NotFound("Room does not exist.")));
	}

	if !services
		.rooms
		.state_cache
		.server_is_participant(services.globals.server_name(), &body.room_id)
		.await
	{
		info!(
			origin = body.origin().as_str(),
			"Refusing to serve state for room we aren't participating in"
		);
		return Err!(Request(NotFound("This server is not participating in that room.")));
	}

	info!(
		origin = body.origin().as_str(),
		room_id = %body.room_id,
		suggested_only = body.suggested_only,
		"Serving hierarchy request"
	);

	let room_id = &body.room_id;
	let suggested_only = body.suggested_only;
	let ref identifier = Identifier::ServerName(body.origin());
	match services
		.rooms
		.spaces
		.get_summary_and_children_local(room_id, identifier)
		.await?
	{
		| None => Err!(Request(NotFound("The requested room was not found"))),

		| Some(SummaryAccessibility::Inaccessible) => {
			Err!(Request(NotFound("The requested room is inaccessible")))
		},

		| Some(SummaryAccessibility::Accessible(room)) => {
			let (children, inaccessible_children) =
				get_parent_children_via(&room, suggested_only)
					.into_iter()
					.stream()
					.broad_filter_map(|(child, _via)| async move {
						match services
							.rooms
							.spaces
							.get_summary_and_children_local(&child, identifier)
							.await
							.ok()?
						{
							// Room not known locally or inaccessible — report as
							// inaccessible so the querying server can try the child's
							// resident server.
							| None | Some(SummaryAccessibility::Inaccessible) =>
								Some((None, Some(child))),

							| Some(SummaryAccessibility::Accessible(summary)) =>
								Some((Some(summary), None)),
						}
					})
					.unzip()
					.map(|(children, inaccessible_children): (Vec<_>, Vec<_>)| {
						(
							children.into_iter().flatten().map(Into::into).collect(),
							inaccessible_children.into_iter().flatten().collect(),
						)
					})
					.await;

			Ok(get_hierarchy::v1::Response { room, children, inaccessible_children })
		},
	}
}
