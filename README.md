# Glen

An embedded document database for Nim with first-class spatial, temporal, and numeric primitives — durable, concurrent, and built for low-latency in-process workloads.

```nim
let db = newGlenDB("./mydb")
db.put("users", "u1", VObject())
echo db.get("users", "u1")
```

> **Status:** beta (0.4.1). On-disk formats are versioned and tested across reopen / replay / replication: WAL v2, snapshot **v2** (with v1 read-back compat), GRI/GPI v1, GTS/TTS v1. Expect minor API churn until 1.0.

---

## What you get

### Core

| Capability | Details |
|---|---|
| **Storage** | Per-collection snapshots + segmented WAL with FNV-1a record checksums and `GLENWAL2` headers |
| **Recovery** | Snapshots loaded first, then WAL replayed; corrupt segment tails are tolerated |
| **Cache** | Sharded LRU with per-shard locks, byte-budgeted, hot-touch promotion |
| **Concurrency** | Striped per-collection RW-locks; multi-collection transactions acquire stripes in sorted order to avoid deadlock |
| **Transactions** | Optimistic, version-checked at commit; `csOk` / `csConflict` / `csInvalid` results |
| **Indexes** | Equality (single + composite) and single-field range scans (CritBitTree backing, asc/desc, limit). **Persisted** in `indexes.manifest`; auto-rebuilt on reopen |
| **Subscriptions** | Document-level, field-path, and field-delta callbacks; stream-encodable for IPC |
| **Replication** | Multi-master, transport-agnostic; HLC last-write-wins; per-peer cursors persisted to `peers.state` |
| **Validation** | Zod-style schema DSL — `zString().minLen(...)`, `zInt().gte(...)`, `zEnum(...)`, `zobject:` blocks |
| **Codec** | Compact tagged binary, varuint + zigzag, configurable size limits |

### Spatial, temporal, numeric

| Capability | Details |
|---|---|
| **Geospatial index** | R-tree (Guttman linear split) with STR bulk-load. Bbox / KNN / radius queries. Both planar (Euclidean degrees) and geographic (haversine metres) metrics. Persisted to `.gri` |
| **Polygon index** | R-tree of MBRs + exact ray-cast point-in-polygon. `findPolygonsContaining`, `findPointsInPolygon`. Persisted to `.gpi` |
| **Time-series engine** | Gorilla-style chunked column store: delta-of-delta timestamps + XOR float encoding. ~1–3 bits/sample on smooth data, with min/max chunk metadata for range scans |
| **Linear algebra** | `Vector` and row-major `Matrix` with `dot`/`norm`/`matmul`/`transpose`/`cosine`/etc. Stored inside any document as nested `VFloat` arrays |
| **GeoMesh** | A 2-D raster of values (or per-cell vectors) tied to a geographic bbox. Sample at `(lon, lat)`. Compact `vkBytes` storage |
| **Tile time-stacks** | Spatially-tiled, Gorilla-compressed `(time, row, col, channel)` rasters. Designed for radar reflectivity / weather / model probability fields. ~10–30× compression on sparse-but-smooth fields; one-cell deep histories without reading the rest |

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

## Spatial, temporal, and numeric extensions

### Geospatial: points, bboxes, KNN, radius

```nim
import glen/db, glen/types, glen/geo

db.put("places", "sf",  placeDoc(-122.42, 37.77))
db.put("places", "oak", placeDoc(-122.27, 37.80))
db.put("places", "la",  placeDoc(-118.24, 34.05))

# STR-bulk-loaded R-tree on two numeric fields.
db.createGeoIndex("places", "byLoc", lonField = "lon", latField = "lat")

# Bounding box
for (id, doc) in db.findInBBox("places", "byLoc",
                               minLon = -123.0, minLat = 37.0,
                               maxLon = -122.0, maxLat = 38.0):
  echo id

# K-nearest, geographic metric (haversine metres)
for (id, metres, doc) in db.findNearest("places", "byLoc",
                                        lon = -122.42, lat = 37.77, k = 3,
                                        metric = gmGeographic):
  echo id, " @ ", metres, "m"

# Radius (haversine post-filter)
for (id, metres, _) in db.findWithinRadius("places", "byLoc",
                                           -122.42, 37.77,
                                           radiusMeters = 50_000.0):
  echo id, " ", metres / 1000.0, "km"
```

The R-tree is **persisted** (manifest + binary `.gri` dump on `compact()`), so reopens skip the bulk-rebuild scan. WAL replay applies on top of the loaded tree, so post-compact mutations are reflected correctly.

### Polygons: zone membership and "points in polygon"

```nim
# Polygons live as VArray of [lon, lat] pairs on a doc field.
proc poly(verts: openArray[(float64, float64)]): Value = ...
db.put("zones", "z1", VObject(...).withField("shape", poly([...])))
db.createPolygonIndex("zones", "byShape", "shape")

# Which zones contain (lon, lat)?
for (id, _) in db.findPolygonsContaining("zones", "byShape",
                                         x = -122.42, y = 37.77):
  echo id

# Which indexed points fall inside a polygon?
let p = Polygon(vertices: @[(0.0, 0.0), (10.0, 0.0), (10.0, 10.0), (0.0, 10.0)])
for (id, _) in db.findPointsInPolygon("places", "byLoc", p):
  echo id
```

R-tree pre-filter on bounding boxes + exact ray-cast point-in-polygon test. Persisted to `.gpi`.

### Time-series: Gorilla-encoded scalar streams

`glen/timeseries` is a separate engine, file-per-series, optimised for one workload: many `(timestamp, float64)` per second per series, near-monotonic timestamps, slowly-changing values (sensor data, metrics, prices).

```nim
import glen/timeseries

let cpu = openSeries("./metrics/cpu.gts")
cpu.append(nowMillis(), 0.42)
cpu.append(nowMillis(), 0.51)

for (ts, v) in cpu.range(fromMs, toMs): echo ts, " ", v
for (ts, v) in cpu.latest(100): echo ts, " ", v

cpu.dropBlocksBefore(cutoffMs)   # retention by full block
cpu.close()
```

Delta-of-delta on timestamps + XOR on float values; chunks carry min/max metadata so range queries skip whole chunks via the in-memory block index. Typical ~1–3 bits/sample on smooth data. Torn-tail tolerated on reopen.

### Linear algebra: `Vector` and `Matrix`

```nim
import glen/linalg

let a = vec(1.0, 2.0, 3.0)
let b = vec(4.0, 5.0, 6.0)
echo a + b, dot(a, b), norm(a), cosine(a, b)

let M = matFromRows(@[@[0.1, 0.2], @[0.3, 0.4]])
let y = M * a[0..1]              # matrix-vector
let WtW = transpose(M) * M       # matrix-matrix (cache-friendly ikj)

# Embeddings live inside ordinary documents as nested VFloat arrays:
var face = VObject()
face["embedding"] = toValue(vec(0.1, 0.2, 0.3, 0.4))
db.put("faces", "f1", face)
let (ok, e) = readVector(db.get("faces", "f1")["embedding"])
```

No new `ValueKind`; round-trips through the existing codec, snapshot, and replication paths.

### GeoMesh: a raster pinned to a bbox

```nim
import glen/geomesh

var mesh = newGeoMesh(
  bbox(-122.5, 37.5, -122.0, 38.0),
  rows = 64, cols = 64, channels = 3,
  labels = @["rain", "snow", "clear"])
for r in 0 ..< mesh.rows:
  for c in 0 ..< mesh.cols:
    mesh.setCell(r, c, llmInferProbabilities(r, c))

var doc = VObject()
doc["model"]    = VString("rain-v3")
doc["forecast"] = mesh.toValue()    # data packed as vkBytes
db.put("predictions", "today-utc-12", doc)

# later — sample at a point
let stored = db.get("predictions", "today-utc-12")
let (ok, m) = readGeoMesh(stored["forecast"])
let probs = m.vectorAt(-122.42, 37.77)        # @[0.18, 0.0, 0.82]
```

`row 0 = top of bbox`, `col 0 = left`. Single-doc, atomic, replicates and subscribes like any other document. A 1000×1000×5 mesh is ~40 MB on disk via `vkBytes`.

### Tile time-stacks: rasters that change over time

```nim
import glen/tilestack

let stack = newTileStack("./radar/KMUX",
  bbox       = bbox(-122.7, 36.7, -120.7, 38.7),
  rows = 200, cols = 200, channels = 1,
  tileSize = 64, chunkSize = 128,
  labels = @["dbz"])

# ingest one frame every 5 minutes
stack.appendFrame(scanTimeMs, mesh)

# show me the storm at 12:34 — reassembled from per-tile chunks
let (ok, frame) = stack.readFrame(scanTimeMs)

# what was reflectivity right over my house for the last hour?
# decodes only the one tile that owns the cell
let history = stack.readPointHistory(myLon, myLat,
                                     fromMs = nowMs - 3_600_000,
                                     toMs   = nowMs)
```

Each tile is an append-only file of Gorilla-encoded chunks (one chunk = `chunkSize` frames). Per-cell-per-channel streams are independent, so a chunk holds `tileSize² × channels` parallel XOR-encoded series sharing one timestamp stream. Compression: ~10–30× on radar-like sparse fields, ~2–3× on fully-varying smooth ones.

**When to use which:**

| Use case | Pick |
|---|---|
| Latest scan / animate last hour / alerting on new frame | **frame-per-doc** with a `GeoMesh` field + range index on `tsMillis` |
| Long archive, point histories, disk cost matters | **TileStack** |
| Single sensor stream | **Series** (`glen/timeseries`) |
| Dense grid of model output, queried by location | **GeoMesh** in a doc, bbox stored as polygon for spatial lookup |

Mix freely — frame-per-doc for the hot 24h, TileStack for the long tail.

---

## Persistence model

```
mydb/
├── glen.wal.0          ← active append-only segment
├── glen.wal.1
├── users.snap          ← per-collection snapshot
├── orders.snap
├── node.id             ← stable node identifier (auto-generated)
├── peers.state         ← persisted per-peer replication cursors
├── indexes.manifest    ← persisted definitions of equality / geo / polygon indexes
├── places.byLoc.gri    ← R-tree binary dump for a geo index (written by compact())
└── zones.byShape.gpi   ← R-tree binary dump for a polygon index
```

Plus, when used:
- `<dir>/<series>.gts` — Gorilla time-series files (one per `Series`)
- `<stackDir>/manifest.tsm` + `<stackDir>/tile_<r>_<c>.tts` — tile time-stack files

**Recovery order on `newGlenDB`:**
1. Load every `*.snap` into the in-memory tables.
2. Read `indexes.manifest`; for each spatial index, try to load its `.gri` / `.gpi` binary dump (skip on missing/CRC mismatch).
3. Replay every WAL segment in order, applying puts/deletes and **incrementally** updating any installed indexes — so `.gri`-loaded trees pick up post-compact mutations correctly.
4. For any manifest entry whose dump didn't load, bulk-build the index from the now-loaded docs.
5. Restore replication metadata (HLC, changeId per doc) so LWW conflict resolution stays correct across restarts.

**Compaction.** `db.compact()` writes a fresh snapshot for each collection, dumps every spatial index as `.gri` / `.gpi`, and resets the WAL to segment 0. Snapshot writes are atomic — temp file + `rename(2)` (POSIX) or temp + remove + move (Windows).

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

Apple M5, `-d:release`, ORC + `-O3`. Reproduce with the corresponding `nimble`
task; numbers are best-of-a-few-runs for stability.

### Core CRUD (`nimble bench_release`)

```
puts:                270k ops/s
gets (cloned):       1.67M ops/s
gets (borrowed):     20M ops/s
getMany:             28k batches/s  ×100 docs ≈ 2.8M doc reads/s
txn commits:         333k ops/s
```

The borrowed-read path skips the defensive `clone()` and is appropriate for
read-only hot loops. Use it where you can.

### Multi-threaded contention (`nimble bench_concurrent`, atomicArc + `-d:useMalloc`)

```
disjoint-write-only   4w/0r ×50k =>  214k ops/s   (low stripe contention)
disjoint-mixed-rw     4w/4r ×50k =>  426k ops/s
shared-write-only     4w/0r ×50k =>  174k ops/s   (max stripe contention)
shared-mixed-rw       4w/4r ×50k =>  406k ops/s
read-heavy-shared     1w/8r ×50k =>  2.1M ops/s
```

### Geospatial (`nimble bench_geo`)

Raw R-tree (in-memory, no GlenDB):

```
bulkLoad (STR):    100k entries  =>  1.6M entries/s
bulkLoad (STR):     1M entries   =>  2.3M entries/s
insert (Guttman): 100k ops       =>  1.9M ops/s
searchBBox (5°):   10k queries   =>  345k q/s   (100k pts, ~159 hits/query)
searchBBox (5°):    1k queries   =>   30k q/s   (1M pts, ~1592 hits/query)
nearest k=10:      10k queries   =>  123k q/s
nearestGeo k=10:   10k queries   =>   93k q/s   (haversine bbox lower-bound)
```

GlenDB-integrated geo index (100k docs):

```
put (no index):                   255k docs/s
createGeoIndex (STR bulk-build):  862k docs/s    (just builds the tree)
put (with active geo index):      242k docs/s    (~5% overhead vs no index)
findWithinRadius 100km:            69k q/s
findNearest planar k=10:           63k q/s
findNearest geographic k=10:       55k q/s
```

Polygons (50k docs, ~3°-square axis-aligned shapes):

```
put polygons:                     99k docs/s
createPolygonIndex (STR):        943k docs/s
findPolygonsContaining:           46k q/s        (R-tree prefilter + ray-cast)
```

Index persistence:

```
reopen with .gri present:        765 ms          (load + WAL replay)
compact (snapshot + .gri dump):  182 ms
reopen with corrupt .gri:        411 ms          (CRC fail → bulk-rebuild)
```

### Time-series (`nimble bench_timeseries`)

Gorilla scalar TSDB, 1M samples per series:

| Value pattern | Append rate | Bits/sample on disk |
|---|---|---|
| Constant | **100M samples/s** | 2.11 |
| Regular cadence | 19M samples/s | 14.13 |
| Smooth (sin) | 4.4M samples/s | 59.60 |
| Noisy | 4.5M samples/s | 59.28 |

```
open (scan all chunk headers):  9 ms for 1M-sample file
range (random window):          1.3k q/s        (avg 546 samples returned)
latest n=100:                   901k q/s        ← 60× faster (decoded-chunk LRU)
latest n=1000:                  115k q/s        ← 115× faster
```

Tile time-stack (radar-shaped sparse field):

| Geometry | Append | Compression | bits/cell | Point-history | readFrame |
|---|---|---|---|---|---|
| 200×200, 200 frames | 3125 frames/s | **24.9×** | 2.57 | **1166 q/s** | **362 q/s** |
| 512×512, 64 frames | 432 frames/s | 21.2× | 3.02 | **844 q/s** | **94 q/s** |

The decoded-chunk LRU (`glen/chunkcache.nim`) drives the read-side wins —
repeated `latest` / `readPointHistory` / `readFrame` calls reuse the same
decoded chunks instead of re-running the bit-unpacking loop. Sized at 64
chunks per series and 16 chunks per tile by default; tunable via the
`decodedChunkCacheSize` constructor arg.

---

## Working-set spill (datasets bigger than RAM)

Glen ships a **spillable mode** that opt-in lets you operate on databases
larger than memory. When enabled, the on-disk snapshot is `mmap`'d and
documents are faulted into RAM only on first access. Cold non-dirty docs are
evicted under a configurable cap; the next read reads them straight back from
the mapped region.

```nim
# Open a multi-GB database with at most ~10k hot docs in RAM.
let db = newGlenDB("./big.glen",
                   spillableMode = true,
                   hotDocCap     = 10_000)

let doc = db.get("events", "2026-04-26T12:34:56")     # faults from mmap
db.put("events", "newKey", value)                      # stays hot until compact()
db.delete("events", "old-key")                         # tombstoned, persisted on compact()
db.compact()    # writes a fresh snapshot v2, swaps the mmap, clears tombstones
db.close()
```

**Mechanics:**
- Snapshot **v2** (`GLENSNP2`): header + per-doc index + body. The index lets
  any doc be located in O(1) without scanning the file.
- `compact()` always writes v2 going forward (eager and spill modes). The
  reader auto-detects v1 vs v2; legacy v1 files load eagerly as before.
- Mutations stay in memory and the WAL until the next `compact()`. Until
  then they cannot be evicted (the eviction loop only drops non-dirty entries).
- Tombstones (`cs.deleted`) make `get` return nil for snapshot-only docs
  removed since the last compaction.

**When this matters**: archives larger than RAM, query workloads that touch a
small fraction of the dataset, edge devices with constrained memory. For a
fully-loaded working set, eager mode (the default) is faster — no fault path,
no mmap deref, no LRU bookkeeping.

**Caveats** (current implementation):
- Multi-doc transactions (`commit`) and replication-apply (`applyChanges`)
  paths are best-effort in spillable mode: docs they touch get faulted in
  and stick in cs.docs. Mostly fine but if you hit OOM with very large
  in-flight txn batches, drop the batch size.
- The doc index itself lives in RAM (~30–50 bytes/doc). A 100M-doc DB
  carries a ~4 GB resident index even in spill mode. Paged indexes (B+ tree
  on disk) would lift this ceiling and remain a future addition.

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

A `GlenDB` is a `ref` holding `collection -> docId -> Value` tables under striped RW-locks, a sharded LRU cache fronting reads, a `WriteAheadLog` that owns the on-disk segment files, a `SubscriptionManager` keyed by `collection:docId`, `IndexesByName` per collection backed by `CritBitTree` for ordered keys, and `GeoIndexesByName` / `PolygonIndexesByName` per collection backed by R-trees with STR bulk-load. Every mutation assigns a replication change record under `replLock` (incrementing seq + advancing local HLC), appends to the WAL (write-ahead, before in-memory state), applies to the table, updates the equality / geo / polygon indexes and cache, then fans out subscriptions outside the locks. Transactions defer all of this until `commit`, which acquires every touched stripe in sorted order, validates recorded read versions against current versions, and applies the staged writes batched into a single `appendMany` WAL call. The spatial / temporal / numeric extensions (`geo`, `timeseries`, `linalg`, `geomesh`, `tilestack`) are independent modules: the first three integrate with the document model (geo / polygon indexes register hooks; vectors and matrices ride along inside `Value`s as nested arrays), while `timeseries` and `tilestack` are standalone storage engines using their own files but sharing the bit-packing primitives in `glen/bitpack`.

---

## Testing

```
nimble test            # debug, full suite (currently 130 cases)
nimble test_release    # ORC + -O3
nimble bench_release   # single-threaded benchmark
nimble bench_concurrent  # multi-threaded contention benchmark (atomicArc)
```

Suites cover: basic CRUD, WAL replay & corrupt-tail tolerance, snapshot round-trip, compaction, transactions, subscriptions (doc / field / field-delta / streaming), cache eviction, codec fuzzing, soak tests with periodic compaction-and-reopen, indexed soak under churn, multi-master export/apply with idempotency, LWW convergence, durability across reopen, filter include/exclude, validator schemas, R-tree bbox / KNN / radius queries, polygon point-in-polygon / find-points-in-polygon / projection metrics, index manifest persistence (auto-rebuild on reopen) and binary `.gri`/`.gpi` round-trip, Gorilla TSDB encoding round-trips and torn-tail recovery, retention via `dropBlocksBefore`, vector / matrix arithmetic and Value serialization, GeoMesh cell math and packed-bytes round-trip, tile time-stack encode/decode, point-history extraction, multi-tile reassembly, and compression sanity on radar-shaped fields.

---

## Roadmap

- Auto-compaction (size-based + time-based triggers)
- Query layer: filters, projections, cursor pagination
- Secondary derived indexes (computed fields)
- Optional zstd page compression for snapshots and tile chunks
- Native async transport adapters for replication
- Vector index (HNSW or IVF) for nearest-neighbour queries on stored embeddings
- Bilinear interpolation in `GeoMesh.sampleAt`
- Per-cell offset table in tile chunks (faster point-history without decoding all streams)
- SIMD bit-decode (AVX-512 PEXT/PDEP, ARM NEON) for the Gorilla unpack hot path
- Paged on-disk doc index for spill mode (lift the in-memory index ceiling)
- Parallel replication export (single export currently runs under one log lock)

---

## License

MIT. See `LICENSE`.
