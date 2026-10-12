# Matrix Federation Routing & Server Reputation

## The Problem

Matrix homeservers (including Synapse and Conduwuit) currently rely on naive iteration when resolving missing events or fetching auth chains. When a room contains missing events, the server iterates through a list of participating servers in the room.

This leads to significant performance degradation ("federation lag") when:

1. The first few servers in the list are offline, timing out, or under DDoS.
2. The servers are online but have highly fragmented, incomplete views of the DAG (Directed Acyclic Graph) for that specific room.
3. The server wastes seconds or minutes linearly backing off and retrying useless endpoints before finally hitting a healthy, well-connected server (like `matrix.org`).

## The Solution: Reputation-Based Routing

To optimize federation, we need to abandon naive iteration in favor of an **algorithmic routing system** that scores servers based on historical performance and dynamically balances exploration vs. exploitation.

### Core Metrics (The Fitness Function)

Each server's reputation should be modeled by a blend of global and per-room metrics:

#### 1. Global Metrics (Server Health)

- **Latency (ms):** Moving average of response times for federation API endpoints.
- **Success Rate (%):** Ratio of `200 OK` vs `4xx`/`5xx` or timeouts.
- **Uptime/Availability:** Tracks if the server is currently reachable.

#### 2. Per-Room Metrics (DAG Comprehensiveness)

- **Depth of Knowledge:** How often this specific server successfully provides missing events or complete auth chains for _this specific room_. (A server might be globally fast, but useless for a niche room it barely participates in).

## Algorithmic Approaches

### 1. Simulated Annealing (Dynamic Exploration)

Matrix federation faces a classic "Explore vs. Exploit" dilemma. We want to exploit known-good servers, but we also must explore unknown servers in case the known-good ones lack the specific event we need.

**Mechanism:**

- **Energy State (Cost):** Inversely proportional to the server's Reputation Score.
- **Temperature (T):** Defines the urgency of the request.
    - **Low Temperature (Foreground):** When a user actively opens an app or attempts to send a message, $T$ drops to near-zero. The algorithm strictly _exploits_ the highest-ranked servers to guarantee the lowest possible latency for the user.
    - **High Temperature (Background):** During background syncs, retry loops, or routine catch-ups, $T$ increases. The algorithm _explores_ by randomly selecting lower-ranked or unknown servers. This acts as a background probe, updating the reputation database without impacting user experience.

### 2. Multi-Armed Bandit / ELO Rating

Alternatively, servers can be modeled as "arms" in a Multi-Armed Bandit problem using **Upper Confidence Bound (UCB)**.

- Servers with high success rates are picked often.
- Servers that haven't been tried recently get an artificial "uncertainty bonus" ensuring they are occasionally re-tested in case they recovered from an outage.

## Database Implementation

This requires adding a `federation_reputation` column family or table to the database:

```rust
struct ServerReputation {
    success_count: u64,
    failure_count: u64,
    average_latency_ms: u32,
    last_contacted: UnixTimestamp,
}

struct RoomServerReputation {
    server_name: ServerName,
    room_id: RoomId,
    auth_chain_successes: u32,
}
```

When a federation request is required, the `resolver` service queries the `reputation` service, sorts the candidate servers based on the current Temperature and UCB, and attempts the fetch.

## Future Possibilities

- **Probing:** A background worker could periodically send `GET /_matrix/federation/v1/version` to known servers to passively update their latency and uptime scores without waiting for an active user request.
- **Gossip:** Servers could theoretically share their reputation tables over federation, allowing new homeservers to bootstrap their routing tables instantly.
