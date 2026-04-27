# Core API — `glen/db`

The base document API: open / close, CRUD, batches, transactions, indexes,
subscriptions, replication, schema validation.

## Opening a database

```nim
import glen/db, glen/types

let db = newGlenDB("./mydb")
# ... use ...
db.close()
```

### Constructor signature

```nim
proc newGlenDB*(dir: string;
                cacheCapacity = 64*1024*1024;
                cacheShards = 16;
                walSync: WalSyncMode = wsmInterval;
                walFlushEveryBytes = 8*1024*1024;
                lockStripesCount = 32;
                spillableMode = false;
                hotDocCap = 0;
                maxDirtyDocs = 0;
                compactWalBytes = 0;
                compactIntervalMs = 0;
                compactDirtyCount = 0): GlenDB
```

| Arg | Default | Notes |
|---|---|---|
| `dir` | — | directory; created if absent |
| `cacheCapacity` | 64 MiB | total LRU byte budget |
| `cacheShards` | 16 | LRU shard count (per-shard locks) |
| `walSync` | `wsmInterval` | `wsmAlways` / `wsmInterval` / `wsmNone` |
| `walFlushEveryBytes` | 8 MiB | flush every N bytes in interval mode |
| `lockStripesCount` | 32 | per-collection stripe locks |
| `spillableMode` | `false` | mmap snapshot, lazy fault — see [spillable-mode.md](../spillable-mode.md) |
| `hotDocCap` | 0 | spill-mode LRU cap on `cs.docs.len` (`0` = unbounded) |
| `maxDirtyDocs` | 0 | spill-mode in-flight cap (`0` = unbounded) |
| `compactWalBytes` | 0 | auto-compact when WAL total ≥ N bytes (`0` = off) |
| `compactIntervalMs` | 0 | auto-compact when ≥ N ms since last compact (`0` = off) |
| `compactDirtyCount` | 0 | auto-compact when summed `cs.dirty.len` ≥ N (`0` = off; spill-mode only) |

### Auto-compaction triggers

`db.compact()` rewrites every collection's snapshot and resets the WAL. Long-
running services need this to happen automatically; the three trigger knobs
above let you opt in. They're all checked at the end of every mutation
(`put`, `delete`, `putMany`, `deleteMany`, `commit`, `applyChanges`) — first
trigger to fire wins, and a re-entrancy guard prevents two threads from
double-compacting. Set any to `0` to disable that trigger; with all three at
`0` (the default) compaction stays manual.

## Configuration

Most knobs are constructor args; `newGlenDBFromEnv` reads the same settings
from environment variables for ops that don't want to recompile.

```nim
let db = newGlenDBFromEnv("./mydb")
```

| Env var | Default | Maps to |
|---|---|---|
| `GLEN_NODE_ID` | random | stable replication node identity |
| `GLEN_WAL_SYNC` | `interval` | `walSync` (`always` / `interval` / `none`) |
| `GLEN_WAL_FLUSH_BYTES` | 8 MiB | `walFlushEveryBytes` |
| `GLEN_CACHE_CAP_BYTES` | 64 MiB | `cacheCapacity` |
| `GLEN_CACHE_SHARDS` | 16 | `cacheShards` |
| `GLEN_MAX_STRING_OR_BYTES` | 16 MiB | codec safety cap |
| `GLEN_MAX_ARRAY_LEN` | 1,000,000 | codec safety cap |
| `GLEN_MAX_OBJECT_FIELDS` | 1,000,000 | codec safety cap |
| `GLEN_COMPACT_WAL_BYTES` | 0 | `compactWalBytes` |
| `GLEN_COMPACT_INTERVAL_MS` | 0 | `compactIntervalMs` |
| `GLEN_COMPACT_DIRTY_COUNT` | 0 | `compactDirtyCount` |

## CRUD

```nim
db.put(collection, docId, value: Value)
db.get(collection, docId): Value             # cloned
db.getBorrowed(collection, docId): Value     # ref; do not mutate
db.delete(collection, docId)
db.currentVersion(collection, docId): uint64
```

Batch:

```nim
db.putMany(collection, items: openArray[(string, Value)])
db.deleteMany(collection, docIds: openArray[string])
db.getMany(collection, docIds): seq[(string, Value)]            # cloned
db.getBorrowedMany(collection, docIds): seq[(string, Value)]
db.getAll(collection): seq[(string, Value)]
db.getBorrowedAll(collection): seq[(string, Value)]
```

Streaming variants — bounded memory, one Value at a time:

```nim
for (id, doc) in db.getAllStream("things"): ...
for (id, doc) in db.getBorrowedAllStream("things"): ...
for (id, doc) in db.getManyStream("things", ids): ...
for (id, doc) in db.getBorrowedManyStream("things", ids): ...
```

See [spillable-mode.md#streaming-iterators](../spillable-mode.md#streaming-iterators).

## Transactions (OCC)

```nim
import glen/txn

let t = db.beginTxn()
discard db.get("items", "i1", t)         # records read version
t.stagePut(Id(collection: "items", docId: "i1"), VString("new"))
t.stageDelete("items", "i2")

case db.commit(t).status
of csOk:       echo "applied"
of csConflict: echo "version moved — retry"
of csInvalid:  echo "rolled back"
```

Multi-collection commits acquire all touched stripes in sorted order — no
deadlock. See [concurrency.md](../concurrency.md).

## Indexes

Equality and single-field range, persisted in `indexes.manifest`:

```nim
db.createIndex(collection, name, fieldPath: string)
db.dropIndex(collection, name)

for (id, doc) in db.findBy(collection, indexName, keyValue, limit = 0): ...
for (id, doc) in db.rangeBy(collection, indexName,
                            minVal, maxVal,
                            inclusiveMin = true, inclusiveMax = true,
                            limit = 0, asc = true): ...

# Streaming variants:
for (id, doc) in db.findByStream(...): ...
for (id, doc) in db.rangeByStream(...): ...
```

`fieldPath` is a comma-separated path expression: `"name"`, `"profile.age"`,
or `"name,profile.age"` for composite equality.

Spatial indexes have their own surface — see [api/spatial.md](spatial.md).

## Subscriptions

```nim
# Whole-document
let h = db.subscribe(collection, docId,
  proc(id: Id; v: Value) = echo id, " -> ", v)

# Specific field path
let hf = db.subscribeField(collection, docId, "profile.age",
  proc(id: Id; path: string; oldV, newV: Value) =
    echo path, ": ", oldV, " -> ", newV)

# Field delta — emits {kind: "append", added: " world"} for incremental
# string growth, "set"/"replace"/"delete" otherwise
let hd = db.subscribeFieldDelta(collection, docId, "text",
  proc(id: Id; path: string; delta: Value) = echo delta)

# Unsubscribe
db.unsubscribe(h)
db.unsubscribeField(hf)
db.unsubscribeFieldDelta(hd)
```

Streaming companions write framed events to a `std/streams` stream — useful
for IPC:

```nim
import std/streams
let s = newFileStream("events.bin", fmWrite)
db.subscribeStream(coll, docId, s)
db.subscribeFieldStream(coll, docId, "field", s)
db.subscribeFieldDeltaStream(coll, docId, "field", s)
```

## Replication

Multi-master, transport-agnostic. See
[concurrency.md#replication](../concurrency.md#replication) for the model.

```nim
type ReplExportCursor* = uint64

proc exportChanges*(db: GlenDB;
                    since: ReplExportCursor;
                    includeCollections: seq[string] = @[];
                    excludeCollections: seq[string] = @[]):
                    (ReplExportCursor, seq[ReplChange])

proc applyChanges*(db: GlenDB; changes: openArray[ReplChange])

proc setPeerCursor*(db: GlenDB; peerId: string; seq: uint64)
proc getPeerCursor*(db: GlenDB; peerId: string): uint64
proc gcReplLog*(db: GlenDB)        # trim every cs.replLog up to min(peerCursors);
                                   # setPeerCursor calls this automatically
```

Conflict resolution is HLC last-write-wins; idempotent via `changeId`.

## Schema validation

Zod-style DSL in `glen/validators`:

```nim
import glen/validators

let UserSchema = zobject:
  name:   zString().trim().minLen(2).maxLen(64)
  age:    zInt().gte(0).lte(150)
  email:  zString().trim().minLen(3)
  role:   zEnum(["admin", "member", "guest"]).default("member")
  active: zBool().default(true)

let res = UserSchema.parse(doc)
if res.ok:
  db.put("users", id, res.value)     # res.value carries coerced doc
else:
  for issue in res.issues:
    echo describePath(issue.path), ": ", issue.message
```

There's no automatic gating on `put` — wrap your writes in a helper if you
want every mutation validated. Example: `examples/crud_validated.nim`.

## Maintenance

```nim
db.snapshotAll()       # write *.snap atomically; WAL untouched
db.compact()           # snapshot + .gri/.gpi dump + WAL reset
db.cacheStats(): CacheStats
db.setWalSync(mode, flushEveryBytes = 0)
db.close()
```

`compact()` writes snapshot v3 by default — see [storage.md](../storage.md).

## Borrowed reads — caveats

`getBorrowed*` returns the in-memory `Value` ref directly:

- **Do not mutate**. Glen shares the same ref across cache, cs.docs, and your
  call site.
- Safe to read concurrently.
- Cheaper than `get` (no clone). Use in tight read-only hot loops.

If you mutate a borrowed value, you'll silently corrupt the cache and other
callers' views. Use `get` (cloned) when in doubt.

## Vector index (HNSW)

Approximate k-nearest-neighbour search over per-document embedding fields.
The graph is in-memory (HNSW) and persisted to a `.vri` binary dump on
`compact()`; reopen auto-loads the dump rather than re-inserting every doc.

```nim
db.createVectorIndex("docs", "by_emb", embeddingField = "emb", dim = 384,
                     metric = vmCosine)

let q: seq[float32] = ...   # query embedding
let hits = db.findNearestVector("docs", "by_emb", q, k = 10)
for (id, dist) in hits: echo id, " @ ", dist
```

Knobs (defaults in parens): `M` (16, neighbours per layer), `efConstruction`
(200, candidate pool during insert), `efSearch` (64, candidate pool during
query). Distance metrics: `vmCosine` (vectors are unit-normalised on insert),
`vmL2`, `vmDot`. All metrics are "smaller = closer" for ranking.

`findNearestVectorStream` yields `(docId, distance, doc)` triples for the
k nearest, fetching docs lazily so spill-mode stays bounded.

## Higher-level query API

Method-chain composer over a collection. Predicates: `whereEq`, `whereNe`,
`whereLt(e)`, `whereGt(e)`, `whereIn`, `whereContains`. Result shaping:
`orderByField`, `limitN`, `afterCursor`. Returns `seq[(id, doc-clone)]`.

```nim
var q = db.query("users")
let page = q.whereEq("status", VString("active"))
            .whereGte("age", VInt(18))
            .orderByField("age")
            .limitN(20)
            .run()
let next = nextCursor(page)
```

The planner picks at most one equality / `in` predicate that an existing
single-field index covers, walks its candidate IDs, and post-filters with
the remaining predicates. Without a covering index, it falls back to a
collection scan. There are no joins, aggregations, or group-bys — by design.

## See also

- [Architecture](../architecture.md) — how db.nim wires the subsystems
- [Concurrency](../concurrency.md) — locks, OCC validation, replication
- [Spillable mode](../spillable-mode.md) — `spillableMode = true`
- [api/spatial.md](spatial.md) — geo / polygon indexes
- [api/timeseries.md](timeseries.md) — TSDB + tilestack (standalone engines)
- [api/numeric.md](numeric.md) — Vector / Matrix
