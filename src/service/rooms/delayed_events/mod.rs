mod submission_queue;

use std::{
	collections::{BTreeMap, HashSet},
	fmt::Write,
	sync::{
		Arc,
		atomic::{AtomicBool, AtomicUsize, Ordering},
	},
	time::{Duration, SystemTime},
};

use async_trait::async_trait;
use conduwuit::{
	Err, Error, Result, debug_warn, err, error,
	utils::{self, stream::WidebandExt},
};
use database::{Deserialized, Json, Map};
use futures::{StreamExt, join};
use http::StatusCode;
use loole::Sender;
use slipstream::{
	MilliSecondsSinceUnixEpoch, OwnedEventId, OwnedRoomId, OwnedUserId, RoomId, UserId,
	api::client::error::{ErrorKind, StandardErrorBody},
	codec::{Deserialize as CodecDeserialize, Serialize as CodecSerialize},
	events::TimelineEventType,
	sswire::Raw,
};

#[derive(Clone, Copy, Debug)]
pub enum UpdateAction {
	Restart,
	Send,
	Cancel,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DelayedEventStatus {
	Scheduled,
	Send,
	Cancel,
	Error,
}

impl DelayedEventStatus {
	#[must_use]
	pub const fn as_str(self) -> &'static str {
		match self {
			| Self::Scheduled => "scheduled",
			| Self::Send => "send",
			| Self::Cancel => "cancel",
			| Self::Error => "error",
		}
	}
}

impl CodecSerialize for DelayedEventStatus {
	fn to_json(&self) -> slipstream::json::Value { self.as_str().to_json() }
}

/// Opaque event content, stored transparently as its JSON value.
#[derive(Clone, Debug)]
pub struct AnyTimelineEventContent(pub slipstream::json::Value);

impl CodecSerialize for AnyTimelineEventContent {
	fn to_json(&self) -> slipstream::json::Value { self.0.clone() }
}

impl CodecDeserialize for AnyTimelineEventContent {
	fn from_json(value: &slipstream::json::Value) -> Result<Self, slipstream::codec::DeError> {
		Ok(Self(value.clone()))
	}
}

#[derive(Clone, Debug)]
pub struct DelayedEventData {
	/// The ID of the delayed event.
	pub delay_id: String,

	/// The ID of the room that the delayed event was scheduled to be sent in.
	pub room_id: OwnedRoomId,

	/// The event type of the delayed event.
	pub event_type: TimelineEventType,

	/// The State Key if the event is a state event, nothing otherwise
	pub state_key: Option<String>,

	/// The event content to send.
	pub content: Raw<AnyTimelineEventContent>,

	/// The duration that the server should wait before sending this event
	/// (encoded as milliseconds).
	pub delay: Duration,

	/// The timestamp when the delayed event was scheduled or last restarted.
	pub running_since: MilliSecondsSinceUnixEpoch,

	/// The error that prevented the delayed event from being sent.
	/// Present only for finalized events that were cancelled due to an error.
	pub error: Option<StandardErrorBody>,

	/// The event_id this event got when it was sent.
	/// Present only for events that were sent successfully.
	pub event_id: Option<OwnedEventId>,

	/// The timestamp when the event was finalized.
	/// Present only for events that were finalized (sent, failed to send, or
	/// cancelled).
	pub finalized_ts: Option<MilliSecondsSinceUnixEpoch>,
}

slipstream::codec_struct! {
	DelayedEventData {
		delay_id: String = ("delay_id"),
		room_id: OwnedRoomId = ("room_id"),
		event_type: TimelineEventType = ("type"),
		state_key: Option<String> = ("state_key", omit),
		content: Raw<AnyTimelineEventContent> = ("content"),
		delay: Duration = ("delay", omit),
		running_since: MilliSecondsSinceUnixEpoch = ("running_since"),
		error: Option<StandardErrorBody> = ("error", omit),
		event_id: Option<OwnedEventId> = ("event_id", omit),
		finalized_ts: Option<MilliSecondsSinceUnixEpoch> = ("finalised_ts", omit),
	}
}

impl DelayedEventData {
	/// Create a new delayed event data object with the given parameters
	#[must_use]
	pub fn new(
		delay_id: String,
		room_id: OwnedRoomId,
		event_type: TimelineEventType,
		state_key: Option<String>,
		content: Raw<AnyTimelineEventContent>,
		delay: Duration,
		running_since: MilliSecondsSinceUnixEpoch,
	) -> Self {
		Self {
			delay_id,
			room_id,
			event_type,
			state_key,
			delay,
			running_since,
			content,
			error: None,
			event_id: None,
			finalized_ts: None,
		}
	}

	/// Returns the status indicated by this delayed event data.
	#[must_use]
	pub fn status(&self) -> DelayedEventStatus {
		if self.finalized_ts.is_none() {
			DelayedEventStatus::Scheduled
		} else if self.event_id.is_some() {
			DelayedEventStatus::Send
		} else if self.error.is_some() {
			DelayedEventStatus::Error
		} else {
			DelayedEventStatus::Cancel
		}
	}
}
use submission_queue::SubmissionQueue;
use tokio::sync::Mutex;

use crate::{Dep, rooms};

const DELAY_ID_SIZE: usize = 32;

struct Data {
	delayid_scheduleddelayedevent: Arc<Map>,
	delayid_finalizeddelayedevent: Arc<Map>,
	userroomdelayid: Arc<Map>,
}

struct Services {
	timeline: Dep<rooms::timeline::Service>,
	state: Dep<rooms::state::Service>,
}

pub struct Service {
	db: Data,
	services: Services,

	// Set of scheduled event to ensure each event is only finalized once
	// Only the process which removes the delay_id from this set may finalize the event
	scheduled_events: Mutex<HashSet<String>>,

	// Queue of scheduled events to know what the next event to be submitted is
	submission_queue: Mutex<SubmissionQueue>,
	submission_queue_sender: Sender<(SystemTime, String)>,

	interrupt_requested: AtomicBool,
	mem_usage: AtomicUsize,
}

#[async_trait]
impl crate::Service for Service {
	fn build(args: crate::Args<'_>) -> Result<Arc<Self>> {
		let (sender, receiver) = loole::unbounded();

		Ok(Arc::new(Self {
			db: Data {
				delayid_scheduleddelayedevent: args.db["delayid_scheduleddelayedevent"].clone(),
				delayid_finalizeddelayedevent: args.db["delayid_finalizeddelayedevent"].clone(),
				userroomdelayid: args.db["userroomdelayid"].clone(),
			},

			services: Services {
				timeline: args.depend::<rooms::timeline::Service>("rooms::timeline"),
				state: args.depend::<rooms::state::Service>("rooms::state"),
			},

			scheduled_events: Mutex::new(HashSet::new()),
			submission_queue: Mutex::new(SubmissionQueue::new(receiver)),
			submission_queue_sender: sender,
			interrupt_requested: AtomicBool::new(false),
			mem_usage: AtomicUsize::new(0),
		}))
	}

	async fn worker(self: Arc<Self>) -> Result<()> {
		Box::pin(submission_queue::worker(&self)).await
	}

	fn interrupt(&self) { self.interrupt_requested.store(true, Ordering::Relaxed); }

	fn name(&self) -> &str { crate::service::make_name(std::module_path!()) }

	async fn memory_usage(&self, out: &mut (dyn Write + Send)) -> Result {
		let mut mem_usage = self.mem_usage.load(Ordering::Relaxed);
		mem_usage = mem_usage.saturating_add(
			self.scheduled_events
				.lock()
				.await
				.len()
				.saturating_mul(DELAY_ID_SIZE.saturating_add(size_of::<String>())),
		);
		writeln!(out, "{mem_usage} bytes")?;

		Ok(())
	}
}

#[derive(Debug)]
pub struct ScheduledDelayedEvent {
	pub event_type: TimelineEventType,
	pub state_key: Option<String>,
	pub content: Raw<AnyTimelineEventContent>,
	pub user_id: OwnedUserId,
	pub room_id: OwnedRoomId,
	pub running_since: SystemTime,
	pub delay: Duration,
}

/// Encodes a `Duration` like serde does: `{"secs":_,"nanos":_}`.
fn duration_to_json(value: &Duration) -> slipstream::json::Value {
	let mut object = slipstream::ObjectBuilder::new();
	object.field("secs", &value.as_secs());
	object.field("nanos", &value.subsec_nanos());
	object.finish()
}

fn duration_from_json(
	value: &slipstream::json::Value,
) -> Result<Duration, slipstream::codec::DeError> {
	let secs = value
		.get("secs")
		.ok_or_else(|| slipstream::codec::DeError::expected("duration secs"))?;
	let nanos = value
		.get("nanos")
		.ok_or_else(|| slipstream::codec::DeError::expected("duration nanos"))?;
	Ok(Duration::new(u64::from_json(secs)?, u32::from_json(nanos)?))
}

/// Encodes a `SystemTime` like serde does:
/// `{"secs_since_epoch":_,"nanos_since_epoch":_}`.
fn system_time_to_json(value: &SystemTime) -> slipstream::json::Value {
	let since = value
		.duration_since(SystemTime::UNIX_EPOCH)
		.unwrap_or_default();
	let mut object = slipstream::ObjectBuilder::new();
	object.field("secs_since_epoch", &since.as_secs());
	object.field("nanos_since_epoch", &since.subsec_nanos());
	object.finish()
}

fn system_time_from_json(
	value: &slipstream::json::Value,
) -> Result<SystemTime, slipstream::codec::DeError> {
	let secs = value
		.get("secs_since_epoch")
		.ok_or_else(|| slipstream::codec::DeError::expected("secs_since_epoch"))?;
	let nanos = value
		.get("nanos_since_epoch")
		.ok_or_else(|| slipstream::codec::DeError::expected("nanos_since_epoch"))?;
	SystemTime::UNIX_EPOCH
		.checked_add(Duration::new(u64::from_json(secs)?, u32::from_json(nanos)?))
		.ok_or_else(|| slipstream::codec::DeError::expected("time"))
}

impl CodecSerialize for ScheduledDelayedEvent {
	fn to_json(&self) -> slipstream::json::Value {
		let mut obj = slipstream::json::Object::new();
		obj.insert("event_type".into(), self.event_type.to_json());
		obj.insert("state_key".into(), self.state_key.to_json());
		obj.insert("content".into(), self.content.to_json());
		obj.insert("user_id".into(), self.user_id.to_json());
		obj.insert("room_id".into(), self.room_id.to_json());
		obj.insert("running_since".into(), system_time_to_json(&self.running_since));
		obj.insert("delay".into(), duration_to_json(&self.delay));
		slipstream::json::Value::Object(obj)
	}
}

impl CodecDeserialize for ScheduledDelayedEvent {
	fn from_json(value: &slipstream::json::Value) -> Result<Self, slipstream::codec::DeError> {
		let input = slipstream::endpoint::Input::new(&[], &[], Some(value));
		let field = |key: &str| {
			value
				.get(key)
				.ok_or_else(|| slipstream::codec::DeError::expected(key))
		};
		Ok(Self {
			event_type: input.body("event_type")?,
			state_key: input.body_or_default("state_key")?,
			content: input.body("content")?,
			user_id: input.body("user_id")?,
			room_id: input.body("room_id")?,
			running_since: system_time_from_json(field("running_since")?)?,
			delay: duration_from_json(field("delay")?)?,
		})
	}
}

impl ScheduledDelayedEvent {
	fn into_data(self, delay_id: String) -> DelayedEventData {
		DelayedEventData::new(
			delay_id,
			self.room_id,
			self.event_type,
			self.state_key,
			self.content,
			self.delay,
			MilliSecondsSinceUnixEpoch::from_system_time(self.running_since)
				.expect("Should be a valid time"),
		)
	}
}

#[derive(Debug)]
pub struct FinalizedDelayedEvent {
	pub event: ScheduledDelayedEvent,
	pub error: Option<(StandardErrorBody, u16)>,
	pub event_id: Option<OwnedEventId>,
	pub finalized_ts: MilliSecondsSinceUnixEpoch,
}

impl CodecSerialize for FinalizedDelayedEvent {
	fn to_json(&self) -> slipstream::json::Value {
		let mut obj = slipstream::json::Object::new();
		obj.insert("event".into(), self.event.to_json());
		obj.insert("error".into(), self.error.to_json());
		obj.insert("event_id".into(), self.event_id.to_json());
		obj.insert("finalized_ts".into(), self.finalized_ts.to_json());
		slipstream::json::Value::Object(obj)
	}
}

impl CodecDeserialize for FinalizedDelayedEvent {
	fn from_json(value: &slipstream::json::Value) -> Result<Self, slipstream::codec::DeError> {
		let input = slipstream::endpoint::Input::new(&[], &[], Some(value));
		Ok(Self {
			event: input.body("event")?,
			error: input.body_or_default("error")?,
			event_id: input.body_or_default("event_id")?,
			finalized_ts: input.body("finalized_ts")?,
		})
	}
}

impl FinalizedDelayedEvent {
	fn into_data(self, delay_id: String) -> DelayedEventData {
		let mut data = self.event.into_data(delay_id);
		data.error = self.error.map(|(e, _)| e);
		data.event_id = self.event_id;
		data.finalized_ts = Some(self.finalized_ts);
		data
	}

	fn outcome(&self) -> DelayedEventStatus {
		if self.event_id.is_some() {
			DelayedEventStatus::Send
		} else if self.error.is_some() {
			DelayedEventStatus::Error
		} else {
			DelayedEventStatus::Cancel
		}
	}
}

impl Service {
	/// check the outcome of an event which has been finalized compared to a
	/// given action Return success if the outcome matches the action, and the
	/// appropriate error otherwise
	async fn check_finalized_event_outcome(
		&self,
		delay_id: &String,
		action: UpdateAction,
	) -> Result<()> {
		let mut finalized_event = self.db.delayid_finalizeddelayedevent.get(delay_id).await;

		if finalized_event.is_err() {
			// There are no synchronization primitives ensuring that the finalized event has
			// made it to the database before we look for it. A best-effort answer is
			// acceptable here, so we simply wait a bit and look again.
			smol::Timer::after(Duration::from_millis(200)).await;
			finalized_event = self.db.delayid_finalizeddelayedevent.get(delay_id).await;
		}

		let finalized_event: FinalizedDelayedEvent = finalized_event?.deserialized()?;

		match (action, finalized_event.outcome()) {
			| (UpdateAction::Send, DelayedEventStatus::Send)
			| (UpdateAction::Cancel, DelayedEventStatus::Cancel) => Ok(()),
			| (UpdateAction::Send, DelayedEventStatus::Error) => match finalized_event.error {
				| Some((StandardErrorBody { kind, .. }, status_code)) => {
					let status_code = StatusCode::from_u16(status_code).map_err(|_| {
						err!(Request(Unknown(
							"The event was already sent, and failed for an unknown reason."
						)))
					})?;
					Err(Error::Request(
						kind,
						"The event was already sent, and failed.".into(),
						status_code,
					))
				},
				| _ => Err!(Request(Unknown(
					"The event was already sent, and failed for an unknown reason."
				))),
			},
			| _ => Err!(Request(Unknown(
				"The finalized event outcome does not match the requested action."
			))),
		}
	}

	async fn finalize_delayed_event(
		&self,
		delay_id: &String,
		event: ScheduledDelayedEvent,
		action: UpdateAction,
		leave_scheduled_on_error: bool,
	) -> Result<()> {
		if !self.scheduled_events.lock().await.remove(delay_id.as_str()) {
			// Someone else has already finalized this event
			return self.check_finalized_event_outcome(delay_id, action).await;
		}

		let timestamp = MilliSecondsSinceUnixEpoch::now();
		let relation = (event.user_id.clone(), event.room_id.clone(), delay_id.clone());

		let result = match action {
			| UpdateAction::Send => {
				let state_lock = self
					.services
					.state
					.mutex
					.lock::<RoomId>(&event.room_id)
					.await;

				let mut unsigned = BTreeMap::new();
				unsigned.insert("delay_id".to_owned(), delay_id.clone().into());

				let result = match &event.state_key {
					| Some(state_key) =>
						Box::pin(self.services.timeline.send_state_event_for_key_helper(
							&event.user_id,
							&event.room_id,
							&state_lock,
							&event.event_type.to_string().into(),
							&event.content.cast(),
							state_key,
							Some(timestamp),
							Some(unsigned),
						))
						.await,
					| None =>
						Box::pin(self.services.timeline.send_message_event_helper(
							&event.user_id,
							&event.room_id,
							&state_lock,
							&event.event_type.to_string().into(),
							&event.content.cast(),
							None,
							Some(timestamp),
							Some(unsigned),
						))
						.await,
				};

				match result {
					| Ok(event_id) => {
						self.db.delayid_finalizeddelayedevent.put(
							delay_id,
							Json(FinalizedDelayedEvent {
								event,
								error: None,
								event_id: Some(event_id),
								finalized_ts: timestamp,
							}),
						);
						Ok(())
					},
					| Err(error) => {
						if leave_scheduled_on_error {
							// In case of error on a manual send, return the error and leave the
							// event scheduled
							self.scheduled_events.lock().await.insert(delay_id.clone());
							return Err(error);
						}
						let recorded_error = match &error {
							| Error::Request(kind, message, status_code) => (
								StandardErrorBody {
									kind: kind.clone(),
									message: message.to_string(),
								},
								status_code.as_u16(),
							),
							| _ => (
								StandardErrorBody {
									kind: ErrorKind::Unknown,
									message: format!("{error}"),
								},
								409,
							),
						};
						self.db.delayid_finalizeddelayedevent.put(
							delay_id,
							Json(FinalizedDelayedEvent {
								event,
								error: Some(recorded_error),
								event_id: None,
								finalized_ts: timestamp,
							}),
						);
						Err(error)
					},
				}
			},
			| UpdateAction::Cancel => {
				self.db.delayid_finalizeddelayedevent.put(
					delay_id,
					Json(FinalizedDelayedEvent {
						event,
						error: None,
						event_id: None,
						finalized_ts: timestamp,
					}),
				);
				Ok(())
			},
			| _ => panic!("this method should only be called for the Send or Cancel action"),
		};

		self.db.delayid_scheduleddelayedevent.remove(delay_id);
		self.db.userroomdelayid.del(&relation);

		result
	}

	/// Add a delayed event to the queue
	/// It will be submitted when it's timer runs out, unless modified before
	#[allow(clippy::arithmetic_side_effects)]
	pub async fn queue_delayed_event(&self, event: ScheduledDelayedEvent) -> Result<String> {
		let delay_id = utils::random_string(DELAY_ID_SIZE);

		let submission_time = event.running_since + event.delay;
		let relation = (event.user_id.clone(), event.room_id.clone(), delay_id.clone());

		self.db
			.delayid_scheduleddelayedevent
			.put(&delay_id, Json(event));

		self.db.userroomdelayid.put(&relation, ());

		self.scheduled_events.lock().await.insert(delay_id.clone());

		if let Err(_err) = self
			.submission_queue_sender
			.send((submission_time, delay_id.clone()))
		{
			self.db.delayid_scheduleddelayedevent.remove(&delay_id);
			self.db.userroomdelayid.del(&relation);
			self.scheduled_events.lock().await.remove(delay_id.as_str());
			return Err!(Request(Unknown(debug_error!(
				"Server was unable to process delayed event request (worker not running)."
			))));
		}

		Ok(delay_id)
	}

	pub async fn update_delayed_event(
		&self,
		delay_id: String,
		action: UpdateAction,
	) -> Result<()> {
		let Ok(event) = self.db.delayid_scheduleddelayedevent.get(&delay_id).await else {
			return self.check_finalized_event_outcome(&delay_id, action).await;
		};
		let mut event: ScheduledDelayedEvent = event.deserialized()?;

		match action {
			| UpdateAction::Restart => {
				event.running_since = SystemTime::now();
				self.db
					.delayid_scheduleddelayedevent
					.put(&delay_id, Json(event));
				Ok(())
			},
			| UpdateAction::Send | UpdateAction::Cancel =>
				Box::pin(self.finalize_delayed_event(&delay_id, event, action, true)).await,
		}
	}

	/// Get a delayed event, scheduled or finalized, with a given ID
	pub async fn get_delayed_event(
		&self,
		sender_user: &UserId,
		delay_id: String,
	) -> Result<DelayedEventData> {
		let (scheduled, finalized) = join!(
			self.db.delayid_scheduleddelayedevent.get(&delay_id),
			self.db.delayid_finalizeddelayedevent.get(&delay_id)
		);

		match (scheduled, finalized) {
			| (_, Ok(event)) => {
				let finalized: FinalizedDelayedEvent = event.deserialized()?;
				if finalized.event.user_id != sender_user {
					return Err!(Request(Forbidden(
						"You are not authorized to view this delayed event."
					)));
				}
				Ok(finalized.into_data(delay_id))
			},
			| (Ok(event), _) => {
				let scheduled: ScheduledDelayedEvent = event.deserialized()?;
				if scheduled.user_id != sender_user {
					return Err!(Request(Forbidden(
						"You are not authorized to view this delayed event."
					)));
				}
				Ok(scheduled.into_data(delay_id))
			},
			| _ => Err!(Request(NotFound("No delayed event with this delay_id was found"))),
		}
	}

	/// get all scheduled delayed events for a user
	pub async fn get_user_scheduled_delayed_events(
		&self,
		user_id: &UserId,
		room_id: Option<&RoomId>,
	) -> Vec<DelayedEventData> {
		let mut prefix = Vec::from(user_id.as_bytes());
		if let Some(room_id) = room_id {
			prefix.push(0xFF);
			prefix.extend_from_slice(room_id.as_bytes());
		}

		self.db
			.userroomdelayid
			.keys_prefix(&prefix)
			.wide_filter_map(async |key| {
				let (_, _, delay_id): (OwnedUserId, OwnedRoomId, String) = key.ok()?;
				Some(
					self.db
						.delayid_scheduleddelayedevent
						.get(&delay_id)
						.await
						.ok()?
						.deserialized::<ScheduledDelayedEvent>()
						.ok()?
						.into_data(delay_id),
				)
			})
			.collect()
			.await
	}

	/// send the event with the given delay_id if it's time has indeed expired.
	/// if the event timer has been reset and the event should not be submitted
	/// yet, return the new submission time
	///
	/// Errors should be logged and then ignored. This method is not on an API
	/// path that can return an error.
	#[allow(clippy::arithmetic_side_effects)]
	async fn send_event_if_ready(&self, delay_id: String) -> Option<(SystemTime, String)> {
		let event = self
			.db
			.delayid_scheduleddelayedevent
			.get(&delay_id)
			.await
			.inspect_err(|err| {
				debug_warn!(%delay_id, "Event was not found (database error: {err}).\
					If the event was updated via the management endpoint, this is probably normal.");
			})
			.ok()?;
		let event: ScheduledDelayedEvent = event
			.deserialized()
			.inspect_err(
				|err| error!(%delay_id, %err, "Invalid delayed event data found in the database."),
			)
			.ok()?;

		let submission_time = event.running_since + event.delay;

		if submission_time <= SystemTime::now() {
			let _ = Box::pin(self.finalize_delayed_event(
				&delay_id,
				event,
				UpdateAction::Send,
				false,
			))
			.await
			.inspect_err(|err| error!(%delay_id, %err, "Error encountered submitting event."));
			None
		} else {
			Some((submission_time, delay_id))
		}
	}
}

database::codec_value_impls!(ScheduledDelayedEvent, FinalizedDelayedEvent);
