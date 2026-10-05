# Slipstream needs

- Add `api::client::relations::event_relationships::unstable` for
  `POST /_matrix/client/unstable/event_relationships`. Do not reuse the
  federation MSC2836 endpoint: its metadata and authentication scheme differ.
