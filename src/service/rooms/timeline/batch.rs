use std::sync::Arc;
use conduwuit::{Result, PduEvent};
use ruma::OwnedEventId;
use futures::{Stream, StreamExt};
use database::{Qry, Deserialized};
use crate::rooms::timeline::data::Data;

impl Data {
    pub fn multi_get_pdus<'a, S>(
        &'a self,
        event_ids: S,
    ) -> impl Stream<Item = Result<PduEvent>> + Send + 'a
    where
        S: Stream<Item = OwnedEventId> + Send + 'a,
    {
        event_ids.map(|_| Err(conduwuit::err!(Database("not implemented"))))
    }
}
