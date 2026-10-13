//! Metadata engine backed by redb.

pub(crate) mod redb;

pub(crate) use redb::RedbEngine as Engine;
