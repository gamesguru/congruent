# TODO: Assert DAG Topological Order in Client Timelines

Currently, our complex DAG resilience and pathology tests (like `TestDAGPathologyAndFederationResilience` in `dag_resilience_test.go`) do not strictly verify the final output rendered to the client timeline via the `/_matrix/client/v3/rooms/{roomID}/messages` endpoint.

## The Problem

While we verify that:

1. `alice` successfully receives the `mergeTip` event via `/sync`.
2. `/backfill` successfully returns some PDUs.
3. `alice` can still successfully send an event (`"after outlier"`) at the very end.

We **do not** currently fetch the room's timeline via `/messages` to assert that Conduwuit's topological sorter has correctly ordered all of the pathologically intertwined events.

## Action Items

Add a final assertion block to `dag_resilience_test.go` (and similar complex DAG stress tests) to verify the rendered timeline order.

For example, we should query `/_matrix/client/v3/rooms/{roomID}/messages?dir=b&limit=20` and strictly assert that the topological order returned by Conduwuit perfectly matches the expected logical sequence:

- `"outlier child"`
- `"carrier for outlier"`
- `"merge event"`
- `"after outlier"`

Checking this ensures that not only does the database and federation logic remain intact under stress, but that the client-facing presentation of the DAG is perfectly resolved.
