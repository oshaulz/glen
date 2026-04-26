# Glen

An embedded document database for Nim — durable, concurrent, and built for low-latency in-process workloads.

```nim
let db = newGlenDB("./mydb")
db.put("users", "u1", VObject())
echo db.get("users", "u1")
```

> **Status:** beta (0.3.0). The on-disk format is versioned (WAL v2, snapshot v1) and tested across reopen / replay / replication. Expect minor API churn until 1.0.

---

## What you get

| Capability | Details |
|---|---|
| **Storage** | Per-collection snapshots + segmented WAL with FNV-1a record checksums and `GLENWAL2` headers |
| **Recovery** | Snapshots loaded first, then WAL replayed; corrupt segment tails are tolerated |
| **Cache** | Sharded LRU with per-shard locks, byte-budgeted, hot-touch promotion |
| **Concurrency** | Striped per-collection RW-locks; multi-collection transactions acquire stripes in sorted order to avoid deadlock |
| **Transactions** | Optimistic, version-checked at commit; `csOk` / `csConflict` / `csInvalid` results |
| **Indexes** | Single-field equality, composite equality, single-field range scans (CritBitTree backing, asc/desc, limit) |
| **Subscriptions** | Document-level, field-path, and field-delta callbacks; stream-encodable for IPC |
| **Replication** | Multi-master, transport-agnostic; HLC last-write-wins; per-peer cursors persisted to `peers.state` |
| **Validation** | Zod-style schema DSL — `zString().minLen(...)`, `zInt().gte(...)`, `zEnum(...)`, `zobject:` blocks |
| **Codec** | Compact tagged binary, varuint + zigzag, configurable size limits |

No external runtime dependencies. Pure Nim ≥ 1.6.

---

## Install

```
nimble install https://github.com/oshaulz/glen
```

or, in your `.nimble`:

```nim
requires "glen >= 0.2.0"
```

---

## Quick tour

### CRUD

```nim
import glen/glen, glen/types, glen/db

let db = newGlenDB("./mydb")

var alice = VObject()
alice["name"] = VString("Alice")
alice["age"]  = VInt(30)

db.put("users", "u1", alice)
echo db.get("users", "u1")          # {age: 30, name: "Alice"}

db.delete("users", "u1")
assert db.get("users", "u1").isNil

# Batch — single lock acquisition, batched WAL append
db.putMany("users", @[("u2", VString("a")), ("u3", VString("b"))])
db.deleteMany("users", @["u2", "u3"])
```

### Borrowed reads (zero-copy)

`get` clones the returned value so callers can't tamper with cached state. When that clone is wasted — read-only hot loops — use the borrowed variants:

```nim
let v = db.getBorrowed("users", "u1")        # do not mutate v
for (id, v) in db.getBorrowedMany("users", @["u1", "u2"]): discard
for (id, v) in db.getBorrowedAll("users"): discard
```

### Optimistic transactions

```nim
import glen/txn

let t = db.beginTxn()
discard db.get("items", "i1", t)     # records the read version
t.stagePut(Id(collection: "items", docId: "i1"), VString("updated"))

let res = db.commit(t)
case res.status
of csOk:       echo "applied"
of csConflict: echo "version moved under us — retry"
of csInvalid:  echo res.message
```

Transactions spanning multiple collections lock all touched stripes write-exclusively in sorted order — no deadlock, no cross-collection skew.

### Indexes and queries

```nim
db.createIndex("users", "byName",    "name")          # equality
db.createIndex("users", "byNameAge", "name,age")      # composite equality
db.createIndex("users", "byAge",     "age")           # rangeable

for (id, _) in db.findBy("users", "byName", VString("Alice"), limit = 10):
  echo id

# Composite equality
for (id, _) in db.findBy("users", "byNameAge",
    VArray(@[VString("Alice"), VInt(30)])):
  echo id

# Range, ascending, inclusive bounds
for (id, _) in db.rangeBy("users", "byAge",
    minVal = VInt(25), maxVal = VInt(40),
    inclusiveMin = true, inclusiveMax = true,
    limit = 0, asc = true):
  echo id
```

### Subscriptions

Three flavours, pick the granularity you need:

```nim
# Whole-document
let h = db.subscribe("users", "u1", proc(id: Id; v: Value) =
  echo "doc changed: ", id, " -> ", v)

# Specific field path — fires only when that path's value differs (deep-eq)
let hf = db.subscribeField("users", "u1", "profile.age",
  proc(id: Id; path: string; oldV, newV: Value) =
    echo path, ": ", oldV, " -> ", newV)

# Field delta — string-aware: emits {kind: "append", added: " world"}
# for incremental string growth, "set"/"replace"/"delete" otherwise
let hd = db.subscribeFieldDelta("logs", "x1", "text",
  proc(id: Id; path: string; delta: Value) = echo delta)
```

Each variant has a `subscribe*Stream` companion that frames events into a `std/streams` Stream using the binary codec — handy for piping to a socket or IPC channel.

### Schema validation

```nim
import glen/validators

let UserSchema = zobject:
  name:   zString().trim().minLen(2).maxLen(64)
  age:    zInt().gte(0).lte(150)
  email:  zString().trim().minLen(3)
  role:   zEnum(["admin", "member", "guest"]).default("member")
  active: zBool().default(true)

let res = UserSchema.parse(doc)
if not res.ok:
  for issue in res.issues:
    echo describePath(issue.path), ": ", issue.message
else:
  db.put("users", id, doc)        # res.value also holds the coerced doc
```

Validators coerce, fill defaults, and report path-tagged issues. There's no automatic gating on `put` — wrap your writes in a helper if you want every mutation validated (see `examples/crud_validated.nim`).

### Multi-master replication

Glen ships the data plane only — bring your own transport (HTTP, gRPC, NATS, files-on-a-USB-stick). Three primitives:

```nim
# Sender
let (nextCursor, batch) = db.exportChanges(
  since = lastSentToPeer,
  includeCollections = @["users", "orders"])    # optional allowlist
# transport.send(peer, batch)
db.setPeerCursor("peerB", nextCursor)            # persisted to peers.state

# Receiver — idempotent (changeId) + LWW (HLC)
db.applyChanges(received)
```

Conflict resolution uses a hybrid logical clock: `(wallMillis, counter, nodeId)`. After receiving a remote change Glen advances its local HLC, so future writes always sort after the freshest thing it's seen.

`gcReplLog()` trims the in-memory change log up to `min(peerCursors)` — call it periodically once peers have ack'd.

---

## Persistence model

```
mydb/
├── glen.wal.0          ← active append-only segment
├── glen.wal.1
├── users.snap          ← per-collection snapshot
├── orders.snap
├── node.id             ← stable node identifier (auto-generated)
└── peers.state         ← persisted per-peer replication cursors
```

**Recovery order on `newGlenDB`:**
1. Load every `*.snap` into the in-memory tables.
2. Replay every WAL segment in order, applying puts/deletes and rebuilding indexes.
3. Restore replication metadata (HLC, changeId per doc) so LWW conflict resolution stays correct across restarts.

**Compaction.** `db.compact()` writes a fresh snapshot for each collection and resets the WAL to segment 0. Snapshot writes are atomic — temp file + `rename(2)` (POSIX) or temp + remove + move (Windows).

### WAL sync policies

| Mode | Behaviour | Use when |
|---|---|---|
| `wsmAlways` | `flushFile` after every record | strict durability, low write rate |
| `wsmInterval` *(default)* | flush every `flushEveryBytes` (default 8 MiB) | typical workloads |
| `wsmNone` | rely on the OS page cache | bulk imports, throwaway databases |

```nim
let db = newGlenDB("./mydb", walSync = wsmInterval, walFlushEveryBytes = 8 * 1024 * 1024)
db.setWalSync(wsmAlways)        # change at runtime
```

---

## Configuration

Most knobs are constructor args, but everything also reads from environment variables for ops that don't want to recompile:

| Env var | Default | Maps to |
|---|---|---|
| `GLEN_NODE_ID` | random | stable replication node identity |
| `GLEN_WAL_SYNC` | `interval` | `always` / `interval` / `none` |
| `GLEN_WAL_FLUSH_BYTES` | 8 MiB | bytes between fsyncs in `interval` mode |
| `GLEN_CACHE_CAP_BYTES` | 64 MiB | total LRU budget |
| `GLEN_CACHE_SHARDS` | 16 | LRU shard count (per-shard locks) |
| `GLEN_MAX_STRING_OR_BYTES` | 16 MiB | codec safety cap |
| `GLEN_MAX_ARRAY_LEN` | 1,000,000 | codec safety cap |
| `GLEN_MAX_OBJECT_FIELDS` | 1,000,000 | codec safety cap |

```nim
let db = newGlenDBFromEnv("./mydb")    # reads all of the above
```

### Tuning

- **Stripe count** (`lockStripesCount`, default 32): bump if you have a lot of collections accessed in parallel and write contention dominates.
- **Cache shards** (`cacheShards`): set ≥ number of writer threads to minimise per-shard contention.
- **Cache capacity**: Glen estimates value byte-size and evicts LRU-style; size it at 1–2× your hot working set.

### Concurrency notes

A single `GlenDB` is safe to share across threads. Concurrent writes to
different collections, concurrent first-writes to brand-new collections,
and concurrent `createIndex`/`put` on disjoint collections are all fine —
Glen wraps per-collection state in a ref behind a struct-rwlock.

The one build-time requirement: **compile multi-threaded callers with
`--mm:atomicArc -d:useMalloc`.** ORC's default refcounter and cycle
collector are not thread-safe across cross-thread Value refs and will
crash in `unregisterCycle` at shutdown. Glen's value graph is acyclic, so
atomicArc has no functional downside. Single-threaded callers can keep
`--mm:orc` (the default).

---

## Performance

Apple M5, `-d:release`, ORC + `-O3`.

**Single-threaded** (`nimble bench_release`):

```
BENCH puts:               270k ops/s
BENCH gets (cloned):      1.67M ops/s
BENCH gets (borrowed):    20M ops/s
BENCH getMany:            28k batches/s  ×100 docs ≈ 2.8M doc reads/s
BENCH txn commits:        333k ops/s
```

The borrowed-read path skips the defensive `clone()` and is appropriate for
read-only hot loops. Use it where you can.

**Multi-threaded contention** (`nimble bench_concurrent`, atomicArc + `-d:useMalloc`):

```
disjoint-write-only   4w/0r ×50k =>  214k ops/s   (low stripe contention)
disjoint-mixed-rw     4w/4r ×50k =>  426k ops/s
shared-write-only     4w/0r ×50k =>  174k ops/s   (max stripe contention)
shared-mixed-rw       4w/4r ×50k =>  406k ops/s
read-heavy-shared     1w/8r ×50k =>  2.1M ops/s
```

---

## Worked example: local-first todos

`examples/todo_sync.nim` is a small CLI that uses Glen's replication API to
sync todos across two processes via a shared mailbox directory. Build it
once, run it twice:

```bash
nim c -d:release --path:src examples/todo_sync.nim

# terminal 1
./examples/todo_sync --db ./node-a add "buy milk"
./examples/todo_sync --db ./node-a add "ship glen 0.3"
./examples/todo_sync --db ./node-a push ./mailbox

# terminal 2
./examples/todo_sync --db ./node-b pull ./mailbox
./examples/todo_sync --db ./node-b list             # sees both todos
./examples/todo_sync --db ./node-b complete <id>
./examples/todo_sync --db ./node-b push ./mailbox

# terminal 1 picks up the completion
./examples/todo_sync --db ./node-a pull ./mailbox
./examples/todo_sync --db ./node-a list
```

The mailbox is just a directory of `.batch` files containing
codec-encoded change sets. `pull` is idempotent — running it twice
applies nothing the second time, because every change carries a
`changeId` that Glen recognises on apply.

A second, smaller example with strict schema validation lives in
`examples/crud_validated.nim`.

## Architecture, in one paragraph

A `GlenDB` is a `ref` holding `collection -> docId -> Value` tables under striped RW-locks, a sharded LRU cache fronting reads, a `WriteAheadLog` that owns the on-disk segment files, a `SubscriptionManager` keyed by `collection:docId`, and `IndexesByName` per collection backed by `CritBitTree` for ordered keys. Every mutation: assign a replication change record under `replLock` (incrementing seq + advancing local HLC), append to the WAL (write-ahead, before in-memory state), apply to the table, update indexes and cache, and finally fan out subscriptions outside the locks. Transactions defer all of this until `commit`, which acquires every touched stripe in sorted order, validates recorded read versions against current versions, and applies the staged writes batched into a single `appendMany` WAL call.

---

## Testing

```
nimble test            # debug, all 31 cases
nimble test_release    # ORC + -O3
nimble bench_release   # benchmarks only
```

Suites cover: basic CRUD, WAL replay & corrupt-tail tolerance, snapshot round-trip, compaction, transactions, subscriptions (doc / field / field-delta / streaming), cache eviction, codec fuzzing, soak tests with periodic compaction-and-reopen, indexed soak under churn, multi-master export/apply with idempotency, LWW convergence, durability across reopen, filter include/exclude, and validator schemas.

---

## Roadmap

- Auto-compaction (size-based + time-based triggers)
- Query layer: filters, projections, cursor pagination
- Secondary derived indexes (computed fields)
- Optional zstd page compression for snapshots
- Native async transport adapters for replication

---

## License

MIT. See `LICENSE`.
