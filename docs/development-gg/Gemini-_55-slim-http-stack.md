> From: https://gemini.google.com/app/8f89f7c2089050fc

# you asked

lightest weight stack to build an efficient, fast-to-compile api in rust? No
bloated things. i think even h2, reqwuest, and axum are bloaty. tokio is bloaty.
etc. Best idea/input?

---

# gemini response

If you consider Tokio, Hyper, and Axum bloated, the standard `async/await`
ecosystem (with its state machines, work-stealing runtimes, and massive
dependency trees) is the root cause.

To get lightning-fast compiles (under 3 seconds from scratch) and tiny binaries,
your best bet is to drop `async` entirely and rely on modern OS threads, or
switch to a strict zero-dependency framework.

Here are the lightest, fastest-to-compile stacks for an expert Rust environment:

### 1. The Synchronous "No-Async" Stack (Most Practical)

Modern Linux thread spawning takes ~20µs. Unless you are dealing with C10k
websocket connections, a blocking thread-pool is incredibly efficient and strips
hundreds of dependencies out of your `Cargo.toml`.

- **Server:** `tiny_http`. It is a pure-Rust, blocking HTTP/1.1 server. It has
  almost zero dependencies (just `log` and `ascii`), uses standard `std::net`,
  and compiles in ~2 seconds.
- **Router:** `rouille`. Built on top of `tiny_http`, it adds simple macro-based
  routing and basic middleware without sacrificing compile times.
- **Client (Reqwest alternative):** `ureq` or `minreq`. Both are blocking HTTP
  clients. `minreq` is virtually dependency-free, while `ureq` is highly
  optimized but compiles in a fraction of the time `reqwest` takes.

### 2. The "Zero-Dependency" Frameworks (Lightest Possible)

If you want routing but absolutely refuse to pull in third-party utility crates,
a few frameworks are built entirely on `std`.

- **`humphrey`:** A blazing-fast, strictly zero-dependency HTTP server. It
  handles its own threading and routing using only the standard library. Compile
  times are near-instantaneous.
- **`webio`:** Another zero-dependency framework engineered specifically to
  prevent massive dependency trees. It provides raw `TcpStream` access and
  Go-style multi-threading using native OS threads.

### 3. The Bare Metal Stack (Roll Your Own)

If you want absolute control over memory and allocations, write the TCP loop
yourself.

- **Parser:** `httparse`. This is the zero-copy, zero-allocation C-like Rust
  parser that actually powers `hyper` under the hood—but without the async
  runtime attached. It compiles in milliseconds.
- **Networking:** `std::net::TcpListener`.
- **Concurrency:** `std::thread::spawn` or a lightweight pool like `threadpool`.
  You read the TCP stream, pass the buffer to `httparse`, and write the raw
  `&str` response back.

### 4. The `io_uring` Route (High IOPS, No Tokio)

Since you are already working with `liburing` (based on your recent environment
setups), you can achieve extreme async performance without the Tokio bloat by
using a thread-per-core architecture.

- **Stack:** `glommio`. It completely bypasses the multi-threaded `Send + Sync`
  overhead of Tokio by pinning tasks to a single core using Linux `io_uring`. It
  compiles much faster than Tokio and is highly cache-efficient for I/O-heavy
  workloads.

---

# you asked

so are 3 and 4 complementary or mutually exclusive? Can you sketch the best
possible approach tailored to me?

---

# gemini response

They are 100% complementary. Option 4 (`io_uring`) is purely a high-throughput
I/O multiplexer, while Option 3 (`httparse`) is purely an in-memory HTTP
byte-slice parser.

By combining them, you get the absolute pinnacle of zero-bloat, bare-metal Rust:
the kernel pushes packets directly into userspace memory, and `httparse` parses
them in-place with zero allocations and zero copies. There are no futures, no
state machines, and no work-stealing overhead.

Because you are bypassing the async ecosystem entirely, this stack compiles from
scratch in about 2 to 3 seconds.

Here is the blueprint for a shared-nothing, thread-per-core HTTP server
utilizing modern Linux kernel features.

### The Crate Diet

- **`io-uring`:** The official, thin Rust bindings for `liburing`. It contains
  no event loop and makes no architectural decisions for you.
- **`httparse`:** A pure state-machine parser. It takes a `&[u8]` and returns an
  array of header slices.
- **`libc` / `core_affinity`:** To pin threads to specific physical CPU cores.

### The Architecture: Thread-Per-Core Multishot

Standard asynchronous servers loop over `accept()` and `read()`. On modern Linux
kernels (5.19+ and 6.0+), `io_uring` provides **multishot** operations that
eliminate this loop. You issue a single system call to tell the kernel to
continuously accept connections or read data forever until you cancel it.

**1. Initialization & Core Pinning** Spawn one standard OS thread per physical
CPU core. Pin each thread to its designated core using `libc::sched_setaffinity`
to ensure L1/L2 cache locality. Do not share state, atomic variables, or
channels across these threads.

**2. Setup the Buffer Rings** On each thread, initialize an `IoUring` instance.
Instead of passing an empty buffer with every read request, register a Provided
Buffer Ring (`IORING_REGISTER_PBUF_RING`). This allows you to hand the kernel a
pool of memory up front. When network data arrives, the kernel claims a buffer,
fills it, and drops a Completion Queue Entry (CQE) telling you which buffer it
used.

**3. Multishot Accept** Create a standard `std::net::TcpListener`. Submit a
single `IORING_OP_MULTISHOT_ACCEPT` Submission Queue Entry (SQE). Every time a
new client connects, the kernel will generate a CQE with the new file
descriptor. You never have to manually re-arm the accept loop.

**4. Multishot Receive** For every new file descriptor returned by the accept
CQE, submit a single `IORING_OP_RECV_MULTISHOT` SQE linked to your provided
buffer ring. As clients send HTTP requests, the kernel writes the raw bytes
directly into your ring buffers and alerts your thread via the completion queue.

**5. Zero-Copy Parsing** When you receive a read CQE, take the raw byte slice
from the buffer ring and pass it directly to `httparse::Request::parse`.
`httparse` does not allocate memory; it simply iterates over the slice and
returns pointers to the method, path, and headers.

**6. Routing and Response** Since the path is returned as a `&str`, route the
request using a simple `match` statement or a static `phf` (perfect hash
function) map. Construct your HTTP response headers statically. Submit the
response back to the client using `IORING_OP_SEND_ZC` (zero-copy send) or a
standard vectored write. Once the send completes, recycle the buffer back into
the provided buffer ring.

---

# repo notes (continuwuity)

The conclusion above does not transfer directly to this repository:

- axum and reqwest are already gone. Outbound federation uses the hyper-util
  legacy client with futures-rustls (ring); inbound uses hyper +
  `MinimalRouter`.
- Duplicate dependencies are minimal (mostly feature-unification double builds
  such as `serde_core`, `libc`, `either`, `toml`).
- Build time is dominated by existing project dependencies (ring/mimalloc C
  builds, hickory, redb, rocksdb, ruma, serde), not hyper/tokio.
- The httparse + io_uring design is a separate experimental server (keep-alive,
  chunking, partial reads, TLS, h2, Unix sockets, timeouts would all be
  re-implemented). It is not a practical replacement for the production HTTP
  stack.

Immediate work is dependency-feature cleanup (e.g. unused `tower-http` features)
driven by `cargo build --timings` and `cargo bloat` measurements, not adopting
the rewrite.
