//! Adapter traits between the database's serde-driven (de)serializer and
//! types that do not implement serde.
//!
//! Slipstream's types are serde-free and the orphan rule forbids implementing
//! serde's traits for them here, so every key, prefix and value passed to a
//! [`Map`](crate::Map) is first projected onto a serde-compatible
//! representation: identifiers become `&str`, composite keys become tuples of
//! those projections, and codec-encoded values become raw bytes. The byte
//! encoding on disk is unchanged.

use std::collections::BTreeSet;

use conduwuit::{Error, Result, matrix::StateKey};
use serde::{Deserialize, Serialize, Serializer};
use slipstream::codec;

use crate::{Cbor, Ignore, IgnoreAll, Interfix, Json, Separator};

/// A value that can be written as (part of) a database key or value.
pub trait DbKey {
	/// The serde-serializable projection of `Self`.
	type Ser<'a>: Serialize
	where
		Self: 'a;

	fn db_ser(&self) -> Self::Ser<'_>;
}

/// A value that can be read from (part of) a database key or value.
pub trait DbDe<'a>: Sized {
	/// The serde-deserializable representation that is converted into `Self`.
	type De: Deserialize<'a>;

	/// # Errors
	///
	/// Returns an error if the stored representation is not a valid `Self`.
	fn from_de(de: Self::De) -> Result<Self>;
}

/// A [`DbDe`] that owns its data.
pub trait DbDeOwned: for<'a> DbDe<'a> {}
impl<T> DbDeOwned for T where T: for<'a> DbDe<'a> {}

impl<T: DbKey + ?Sized> DbKey for &T {
	type Ser<'a>
		= T::Ser<'a>
	where
		Self: 'a;

	fn db_ser(&self) -> Self::Ser<'_> { (**self).db_ser() }
}

impl<T: DbKey + ?Sized> DbKey for Box<T> {
	type Ser<'a>
		= T::Ser<'a>
	where
		Self: 'a;

	fn db_ser(&self) -> Self::Ser<'_> { (**self).db_ser() }
}

/// Types that are already serde-serializable and project onto themselves.
macro_rules! key_leaf {
	($($t:ty),* $(,)?) => {$(
		impl DbKey for $t {
			type Ser<'a> = &'a $t where Self: 'a;

			fn db_ser(&self) -> Self::Ser<'_> { self }
		}
	)*};
}

/// Types that are already serde-deserializable and read as themselves.
macro_rules! de_leaf {
	($($t:ty),* $(,)?) => {$(
		impl DbDe<'_> for $t {
			type De = $t;

			fn from_de(de: Self::De) -> Result<Self> { Ok(de) }
		}
	)*};
}

key_leaf!(
	str,
	String,
	[u8],
	[u32],
	[u64],
	bool,
	u8,
	u16,
	u32,
	u64,
	u128,
	usize,
	i8,
	i16,
	i32,
	i64,
	i128,
	isize,
	(),
	Interfix,
	Separator,
);
de_leaf!(
	String,
	bool,
	u8,
	u16,
	u32,
	u64,
	u128,
	usize,
	i8,
	i16,
	i32,
	i64,
	i128,
	isize,
	(),
	Ignore,
	IgnoreAll
);

impl<'a> DbDe<'a> for &'a str {
	type De = &'a str;

	fn from_de(de: Self::De) -> Result<Self> { Ok(de) }
}

impl<'a> DbDe<'a> for &'a [u8] {
	type De = &'a [u8];

	fn from_de(de: Self::De) -> Result<Self> { Ok(de) }
}

impl<'a> DbDe<'a> for std::borrow::Cow<'a, str> {
	type De = Self;

	fn from_de(de: Self::De) -> Result<Self> { Ok(de) }
}

impl<T: Serialize> DbKey for Cbor<T> {
	type Ser<'a>
		= &'a Self
	where
		Self: 'a;

	fn db_ser(&self) -> Self::Ser<'_> { self }
}

impl<'a, T: Deserialize<'a>> DbDe<'a> for Cbor<T> {
	type De = Self;

	fn from_de(de: Self::De) -> Result<Self> { Ok(de) }
}

impl<T: DbKey> DbKey for Option<T> {
	type Ser<'a>
		= Option<T::Ser<'a>>
	where
		Self: 'a;

	fn db_ser(&self) -> Self::Ser<'_> { self.as_ref().map(DbKey::db_ser) }
}

impl<'a, T: DbDe<'a>> DbDe<'a> for Option<T> {
	type De = Option<T::De>;

	fn from_de(de: Self::De) -> Result<Self> { de.map(T::from_de).transpose() }
}

impl<T: Serialize> DbKey for Vec<T> {
	type Ser<'a>
		= &'a Self
	where
		Self: 'a;

	fn db_ser(&self) -> Self::Ser<'_> { self }
}

impl<'a, T: Deserialize<'a>> DbDe<'a> for Vec<T> {
	type De = Self;

	fn from_de(de: Self::De) -> Result<Self> { Ok(de) }
}

impl<T: codec::Serialize + Ord> DbKey for BTreeSet<T> {
	type Ser<'a>
		= RawBytes
	where
		Self: 'a;

	fn db_ser(&self) -> Self::Ser<'_> { RawBytes(codec::to_string(self).into_bytes()) }
}

impl<'a, T: codec::Deserialize + Ord> DbDe<'a> for BTreeSet<T> {
	type De = RawBytesDe<'a>;

	fn from_de(de: Self::De) -> Result<Self> { from_json_slice(de.0) }
}

impl<T: Serialize, const N: usize> DbKey for conduwuit::arrayvec::ArrayVec<T, N> {
	type Ser<'a>
		= &'a Self
	where
		Self: 'a;

	fn db_ser(&self) -> Self::Ser<'_> { self }
}

impl<'a, T: Deserialize<'a>, const N: usize> DbDe<'a> for conduwuit::arrayvec::ArrayVec<T, N> {
	type De = Self;

	fn from_de(de: Self::De) -> Result<Self> { Ok(de) }
}

macro_rules! tuple_impls {
	($(($($t:ident $i:tt),+))+) => {$(
		impl<$($t: DbKey),+> DbKey for ($($t,)+) {
			type Ser<'a> = ($($t::Ser<'a>,)+) where Self: 'a;

			fn db_ser(&self) -> Self::Ser<'_> { ($(self.$i.db_ser(),)+) }
		}

		impl<'a, $($t: DbDe<'a>),+> DbDe<'a> for ($($t,)+) {
			type De = ($($t::De,)+);

			fn from_de(de: Self::De) -> Result<Self> { Ok(($($t::from_de(de.$i)?,)+)) }
		}
	)+};
}

tuple_impls! {
	(A 0)
	(A 0, B 1)
	(A 0, B 1, C 2)
	(A 0, B 1, C 2, D 3)
	(A 0, B 1, C 2, D 3, E 4)
	(A 0, B 1, C 2, D 3, E 4, F 5)
}

/// Raw bytes that serialize without any framing.
pub struct RawBytes(pub Vec<u8>);

impl Serialize for RawBytes {
	fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
		serializer.serialize_bytes(&self.0)
	}
}

/// Raw bytes that deserialize from the remainder of the record.
pub struct RawBytesDe<'a>(pub &'a [u8]);

impl<'de> Deserialize<'de> for RawBytesDe<'de> {
	fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
		struct Visit;
		impl<'de> serde::de::Visitor<'de> for Visit {
			type Value = RawBytesDe<'de>;

			fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
				f.write_str("raw bytes")
			}

			fn visit_borrowed_bytes<E: serde::de::Error>(
				self,
				v: &'de [u8],
			) -> Result<Self::Value, E> {
				Ok(RawBytesDe(v))
			}
		}
		deserializer.deserialize_bytes(Visit)
	}
}

/// Implement [`DbKey`] and [`DbDe`] for types stored as codec-encoded JSON.
#[macro_export]
macro_rules! codec_value_impls {
	($($t:ty),* $(,)?) => {$(
		impl $crate::dbkey::DbKey for $t {
			type Ser<'a> = $crate::dbkey::RawBytes where Self: 'a;

			fn db_ser(&self) -> Self::Ser<'_> {
				$crate::dbkey::RawBytes(::slipstream::codec::to_string(self).into_bytes())
			}
		}

		impl<'a> $crate::dbkey::DbDe<'a> for $t {
			type De = $crate::dbkey::RawBytesDe<'a>;

			fn from_de(de: Self::De) -> ::conduwuit::Result<Self> {
				let text = ::std::str::from_utf8(de.0)
					.map_err(|e| ::conduwuit::Error::SerdeDe(e.to_string().into()))?;
				::slipstream::codec::from_str(text)
					.map_err(|e| ::conduwuit::Error::SerdeDe(e.to_string().into()))
			}
		}
	)*};
}

codec_value_impls!(
	conduwuit::matrix::Pdu,
	slipstream::json::Value,
	std::collections::BTreeMap<String, slipstream::json::Value>,
	slipstream::device::Device,
	slipstream::pusher::Pusher,
	slipstream::filter::FilterDefinition,
	slipstream::federation_api::discovery::ServerSigningKeys,
);

impl<T: Serialize, const N: usize> DbKey for [T; N]
where
	[T; N]: Serialize,
{
	type Ser<'a>
		= &'a [T; N]
	where
		Self: 'a;

	fn db_ser(&self) -> Self::Ser<'_> { self }
}

/// Values stored as codec-encoded JSON.
impl<T: codec::Serialize> DbKey for Json<T> {
	type Ser<'a>
		= RawBytes
	where
		Self: 'a;

	fn db_ser(&self) -> Self::Ser<'_> { RawBytes(codec::to_string(&self.0).into_bytes()) }
}

impl<'a, T: codec::Deserialize> DbDe<'a> for Json<T> {
	type De = RawBytesDe<'a>;

	fn from_de(de: Self::De) -> Result<Self> {
		let text = std::str::from_utf8(de.0).map_err(|e| Error::SerdeDe(e.to_string().into()))?;
		codec::from_str(text)
			.map(Json)
			.map_err(|e| Error::SerdeDe(e.to_string().into()))
	}
}

/// Raw Slipstream JSON values are stored directly as their UTF-8 JSON bytes.
impl<T> DbKey for slipstream::sswire::Raw<T> {
	type Ser<'a>
		= RawBytes
	where
		Self: 'a;

	fn db_ser(&self) -> Self::Ser<'_> { RawBytes(self.0.as_bytes().to_vec()) }
}

impl<'a, T> DbDe<'a> for slipstream::sswire::Raw<T> {
	type De = RawBytesDe<'a>;

	fn from_de(de: Self::De) -> Result<Self> {
		let text = std::str::from_utf8(de.0).map_err(|e| Error::SerdeDe(e.to_string().into()))?;
		Ok(Self(text.to_owned(), core::marker::PhantomData))
	}
}

/// Identifiers are stored as their plain string form.
macro_rules! id_impls {
	($($t:ty),* $(,)?) => {$(
		impl DbKey for $t {
			type Ser<'a> = &'a str where Self: 'a;

			fn db_ser(&self) -> Self::Ser<'_> { self.as_str() }
		}

		impl<'a> DbDe<'a> for $t {
			type De = &'a str;

			fn from_de(de: Self::De) -> Result<Self> {
				<$t>::parse(de).map_err(|e| Error::SerdeDe(e.to_string().into()))
			}
		}
	)*};
}

id_impls!(
	slipstream::OwnedEventId,
	slipstream::OwnedRoomId,
	slipstream::OwnedRoomAliasId,
	slipstream::OwnedServerName,
	slipstream::OwnedUserId,
	slipstream::OwnedRoomOrAliasId,
	slipstream::OwnedDeviceId,
	slipstream::OwnedTransactionId,
	slipstream::OwnedMxcUri,
);

impl<A, K> DbKey for slipstream::OwnedKeyId<A, K> {
	type Ser<'a>
		= &'a str
	where
		Self: 'a;

	fn db_ser(&self) -> Self::Ser<'_> { self.as_str() }
}

impl<'a, A, K> DbDe<'a> for slipstream::OwnedKeyId<A, K> {
	type De = &'a str;

	fn from_de(de: Self::De) -> Result<Self> {
		Self::parse(de).map_err(|e| Error::SerdeDe(e.to_string().into()))
	}
}

impl DbKey for slipstream::OneTimeKeyAlgorithm {
	type Ser<'a>
		= &'a str
	where
		Self: 'a;

	fn db_ser(&self) -> Self::Ser<'_> { self.as_str() }
}

impl<'a> DbDe<'a> for slipstream::OneTimeKeyAlgorithm {
	type De = &'a str;

	fn from_de(de: Self::De) -> Result<Self> { Ok(Self::from(de)) }
}

impl DbKey for StateKey {
	type Ser<'a>
		= &'a str
	where
		Self: 'a;

	fn db_ser(&self) -> Self::Ser<'_> { self.as_str() }
}

impl DbKey for slipstream::http_headers::ContentDisposition {
	type Ser<'a>
		= String
	where
		Self: 'a;

	fn db_ser(&self) -> Self::Ser<'_> { self.to_string() }
}

impl<'a> DbDe<'a> for StateKey {
	type De = &'a str;

	fn from_de(de: Self::De) -> Result<Self> { Ok(Self::from(de)) }
}

impl DbKey for slipstream::RoomVersionId {
	type Ser<'a>
		= &'a str
	where
		Self: 'a;

	fn db_ser(&self) -> Self::Ser<'_> { self.as_str() }
}

impl<'a> DbDe<'a> for slipstream::RoomVersionId {
	type De = &'a str;

	fn from_de(de: Self::De) -> Result<Self> {
		de.parse()
			.map_err(|e| Error::SerdeDe(format!("invalid room version: {e}").into()))
	}
}

macro_rules! event_type_impls {
	($($t:ty),* $(,)?) => {$(
		impl DbKey for $t {
			type Ser<'a> = &'a str where Self: 'a;

			fn db_ser(&self) -> Self::Ser<'_> { self.as_str() }
		}

		impl<'a> DbDe<'a> for $t {
			type De = &'a str;

			fn from_de(de: Self::De) -> Result<Self> { Ok(<$t>::from(de)) }
		}
	)*};
}

event_type_impls!(
	slipstream::events::TimelineEventType,
	slipstream::events::StateEventType,
	slipstream::events::MessageLikeEventType,
	slipstream::events::GlobalAccountDataEventType,
	slipstream::events::RoomAccountDataEventType,
);

impl DbKey for slipstream::Mxc<'_> {
	type Ser<'a>
		= String
	where
		Self: 'a;

	fn db_ser(&self) -> Self::Ser<'_> { self.to_string() }
}

/// Decode a codec-encoded JSON value stored as UTF-8 bytes.
///
/// # Errors
///
/// Returns an error if `bytes` are not valid UTF-8 JSON for `T`.
pub fn from_json_slice<T: codec::Deserialize>(bytes: &[u8]) -> Result<T> {
	let text = std::str::from_utf8(bytes).map_err(|e| Error::SerdeDe(e.to_string().into()))?;
	codec::from_str(text).map_err(|e| Error::SerdeDe(e.to_string().into()))
}
