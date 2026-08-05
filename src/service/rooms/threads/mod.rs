use std::{collections::BTreeMap, sync::Arc};

use conduwuit_core::{
	Event, Result, err,
	matrix::pdu::{PduCount, PduEvent, PduId, RawPduId},
	utils::{
		ReadyExt,
		stream::{TryIgnore, WidebandExt},
	},
};
use conduwuit_database::{Deserialized, Interfix, Json, Map};
use futures::{Stream, StreamExt};
use ruma::{
	CanonicalJsonValue, EventId, OwnedEventId, OwnedRoomId, OwnedUserId, RoomId, UserId,
	api::client::threads::get_threads::v1::IncludeThreads,
	events::relation::{BundledThread, RelationType},
	uint,
};
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::{Dep, globals, rooms, rooms::short::ShortRoomId};

pub struct Service {
	db: Data,
	services: Services,
}

struct Services {
	globals: Dep<globals::Service>,
	short: Dep<rooms::short::Service>,
	timeline: Dep<rooms::timeline::Service>,
}

pub(super) struct Data {
	threadid_userids: Arc<Map>,
	userroomthread_subscription: Arc<Map>,
}

/// Maximum relation hops walked when resolving thread membership.
const MAX_THREAD_HOPS: usize = 3;

#[derive(Deserialize)]
struct ExtractThreadRelation {
	#[serde(rename = "m.relates_to")]
	relates_to: ThreadRelation,
}

#[derive(Deserialize)]
struct ThreadRelation {
	rel_type: RelationType,
	event_id: OwnedEventId,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ThreadSubscription {
	pub subscribed: bool,
	pub automatic: bool,
	pub bump_stamp: u64,
	pub last_unsubscribed: u64,
}

impl crate::Service for Service {
	fn build(args: crate::Args<'_>) -> Result<Arc<Self>> {
		Ok(Arc::new(Self {
			db: Data {
				threadid_userids: args.db["threadid_userids"].clone(),
				userroomthread_subscription: args.db["userroomthread_subscription"].clone(),
			},
			services: Services {
				globals: args.depend::<globals::Service>("globals"),
				short: args.depend::<rooms::short::Service>("rooms::short"),
				timeline: args.depend::<rooms::timeline::Service>("rooms::timeline"),
			},
		}))
	}

	fn name(&self) -> &str { crate::service::make_name(std::module_path!()) }
}

impl Service {
	pub async fn get_thread_id<E>(&self, event: &E) -> Option<OwnedEventId>
	where
		E: Event + Sync,
	{
		let mut relates_to = event
			.get_content::<ExtractThreadRelation>()
			.ok()?
			.relates_to;

		for _ in 0..MAX_THREAD_HOPS {
			if relates_to.rel_type == RelationType::Thread {
				return Some(relates_to.event_id);
			}

			relates_to = self
				.services
				.timeline
				.get_pdu(&relates_to.event_id)
				.await
				.ok()?
				.get_content::<ExtractThreadRelation>()
				.ok()?
				.relates_to;
		}

		None
	}

	pub async fn get_thread_id_for_event(&self, event_id: &EventId) -> Option<OwnedEventId> {
		let pdu = self.services.timeline.get_pdu(event_id).await.ok()?;
		self.get_thread_id(&pdu).await
	}

	pub async fn thread_event_count(&self, event_id: &EventId) -> Result<PduCount> {
		self.services.timeline.get_pdu_count(event_id).await
	}

	pub async fn thread_root_exists(&self, room_id: &RoomId, thread_id: &EventId) -> bool {
		self.services
			.timeline
			.get_pdu(thread_id)
			.await
			.is_ok_and(|pdu| pdu.room_id == room_id)
	}

	pub async fn get_subscription(
		&self,
		user_id: &UserId,
		room_id: &RoomId,
		thread_id: &EventId,
	) -> Option<ThreadSubscription> {
		let key = (user_id, room_id, thread_id);
		self.db
			.userroomthread_subscription
			.qry(&key)
			.await
			.deserialized()
			.ok()
	}

	pub async fn put_subscription(
		&self,
		user_id: &UserId,
		room_id: &RoomId,
		thread_id: &EventId,
		automatic: bool,
	) -> Result<ThreadSubscription> {
		let previous = self
			.db
			.userroomthread_subscription
			.qry(&(user_id, room_id, thread_id))
			.await
			.deserialized()
			.unwrap_or(ThreadSubscription {
				subscribed: false,
				automatic: false,
				bump_stamp: 0,
				last_unsubscribed: 0,
			});

		let subscription = ThreadSubscription {
			subscribed: true,
			automatic,
			bump_stamp: self.services.globals.next_count()?,
			last_unsubscribed: previous.last_unsubscribed,
		};
		self.db
			.userroomthread_subscription
			.put((user_id, room_id, thread_id), Json(&subscription));

		Ok(subscription)
	}

	pub fn delete_subscription(
		&self,
		user_id: &UserId,
		room_id: &RoomId,
		thread_id: &EventId,
	) -> Result<ThreadSubscription> {
		let count = self.services.globals.next_count()?;
		let subscription = ThreadSubscription {
			subscribed: false,
			automatic: false,
			bump_stamp: count,
			last_unsubscribed: count,
		};
		self.db
			.userroomthread_subscription
			.put((user_id, room_id, thread_id), Json(&subscription));

		Ok(subscription)
	}

	pub async fn subscriptions_since(
		&self,
		user_id: &UserId,
		since: u64,
	) -> BTreeMap<OwnedRoomId, BTreeMap<OwnedEventId, ThreadSubscription>> {
		let prefix = (user_id, Interfix);
		self.db
			.userroomthread_subscription
			.stream_prefix(&prefix)
			.ignore_err()
			.ready_filter_map(
				|(key, subscription): (
					(&UserId, OwnedRoomId, OwnedEventId),
					Json<ThreadSubscription>,
				)| {
					let subscription = subscription.0;
					(subscription.subscribed && subscription.bump_stamp > since).then_some((
						key.1,
						key.2,
						subscription,
					))
				},
			)
			.ready_fold(
				BTreeMap::new(),
				|mut subscriptions, (room_id, thread_id, subscription)| {
					subscriptions
						.entry(room_id)
						.or_default()
						.insert(thread_id, subscription);
					subscriptions
				},
			)
			.await
	}

	pub async fn add_to_thread<E>(&self, root_event_id: &EventId, event: &E) -> Result
	where
		E: Event + Send + Sync,
	{
		let root_id = self
			.services
			.timeline
			.get_pdu_id(root_event_id)
			.await
			.map_err(|e| {
				err!(Request(InvalidParam("Invalid event_id in thread message: {e:?}")))
			})?;

		let root_pdu = self
			.services
			.timeline
			.get_pdu_from_id(&root_id)
			.await
			.map_err(|e| err!(Request(InvalidParam("Thread root not found: {e:?}"))))?;

		let mut root_pdu_json = self
			.services
			.timeline
			.get_pdu_json_from_id(&root_id)
			.await
			.map_err(|e| err!(Request(InvalidParam("Thread root pdu not found: {e:?}"))))?;

		if let CanonicalJsonValue::Object(unsigned) = root_pdu_json
			.entry("unsigned".to_owned())
			.or_insert_with(|| CanonicalJsonValue::Object(BTreeMap::default()))
		{
			if let Some(mut relations) = unsigned
				.get("m.relations")
				.and_then(|r| r.as_object())
				.and_then(|r| r.get("m.thread"))
				.and_then(|relations| {
					serde_json::from_value::<BundledThread>(relations.clone().into()).ok()
				}) {
				// Thread already existed
				relations.count = relations.count.saturating_add(uint!(1));
				relations.latest_event = event.to_format();

				let content = serde_json::to_value(relations).expect("to_value always works");

				unsigned.insert(
					"m.relations".to_owned(),
					json!({ "m.thread": content })
						.try_into()
						.expect("thread is valid json"),
				);
			} else {
				// New thread
				let relations = BundledThread {
					latest_event: event.to_format(),
					count: uint!(1),
					current_user_participated: true,
				};

				let content = serde_json::to_value(relations).expect("to_value always works");

				unsigned.insert(
					"m.relations".to_owned(),
					json!({ "m.thread": content })
						.try_into()
						.expect("thread is valid json"),
				);
			}

			self.services
				.timeline
				.replace_pdu(&root_id, &root_pdu_json, root_event_id)
				.await?;
		}

		let mut users = Vec::new();
		match self.get_participants(&root_id).await {
			| Ok(userids) => {
				users.extend_from_slice(&userids);
			},
			| _ => {
				users.push(root_pdu.sender().to_owned());
			},
		}
		users.push(event.sender().to_owned());

		self.update_participants(&root_id, &users)
	}

	pub async fn threads_until<'a>(
		&'a self,
		user_id: &'a UserId,
		room_id: &'a RoomId,
		shorteventid: PduCount,
		_inc: &'a IncludeThreads,
	) -> Result<impl Stream<Item = (PduCount, PduEvent)> + Send + 'a> {
		let shortroomid: ShortRoomId = self.services.short.get_shortroomid(room_id).await?;

		let current: RawPduId = PduId {
			shortroomid,
			shorteventid: shorteventid.saturating_sub(1),
		}
		.into();

		let stream = self
			.db
			.threadid_userids
			.rev_raw_keys_from(&current)
			.ignore_err()
			.map(RawPduId::from)
			.ready_take_while(move |pdu_id| pdu_id.shortroomid() == shortroomid.to_be_bytes())
			.wide_filter_map(move |pdu_id| async move {
				let mut pdu = self.services.timeline.get_pdu_from_id(&pdu_id).await.ok()?;

				let pdu_id: PduId = pdu_id.into();
				pdu.as_mut_pdu().set_unsigned(Some(user_id));

				Some((pdu_id.shorteventid, pdu))
			});

		let threads = stream.collect::<Vec<_>>().await;
		let mut ordered = Vec::with_capacity(threads.len());
		for (root_count, pdu) in threads {
			let unsigned = pdu.get_unsigned_as_value();
			let latest_event_id = unsigned
				.get("m.relations")
				.and_then(|relations| relations.get("m.thread"))
				.and_then(|thread| thread.get("latest_event"))
				.and_then(|event| event.get("event_id"))
				.and_then(|event_id| event_id.as_str())
				.and_then(|event_id| EventId::parse(event_id).ok());
			let latest_count = match latest_event_id {
				| Some(event_id) => self
					.services
					.timeline
					.get_pdu_count(event_id)
					.await
					.unwrap_or(root_count),
				| None => root_count,
			};
			ordered.push((latest_count, pdu));
		}
		ordered.sort_by_key(|(count, _)| std::cmp::Reverse(*count));

		Ok(futures::stream::iter(ordered))
	}

	pub(super) fn update_participants(
		&self,
		root_id: &RawPduId,
		participants: &[OwnedUserId],
	) -> Result {
		let users = participants
			.iter()
			.map(|user| user.as_bytes())
			.collect::<Vec<_>>()
			.join(&[0xFF][..]);

		self.db.threadid_userids.insert(root_id, &users);

		Ok(())
	}

	pub(super) async fn get_participants(&self, root_id: &RawPduId) -> Result<Vec<OwnedUserId>> {
		self.db.threadid_userids.get(root_id).await.deserialized()
	}
}
