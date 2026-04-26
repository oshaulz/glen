## Glen: An Embedded Document Database for Nim

### TL;DR

Glen is a fast, embedded, single-process document database for Nim. The core
holds documents in memory for low-latency access, persists every mutation
through a write-ahead log, and periodically snapshots per collection for
fast recovery. It offers optimistic transactions, document- and field-level
subscriptions, equality / range / spatial / polygon indexes, a sharded LRU
cache, multi-master replication via Hybrid Logical Clocks, and a Zod-style
schema DSL.

On top of that base, Glen ships first-class spatial primitives (R-tree-backed
geo and polygon indexes with planar / geographic distance metrics), a
Gorilla-encoded scalar time-series engine, lightweight linear algebra,
a bbox-anchored raster type (`GeoMesh`), and a tile time-stack engine for
compressed storage of rasters that evolve through time (radar, weather,
LLM-emitted probability fields).

Snapshot v3 lays the doc index on disk via mmap so working sets bigger than
RAM are workable; `spillableMode` + `hotDocCap` + streaming iterators give a
predictable memory floor for arbitrarily large datasets.

It is designed for local-first apps, edge / embedded services, single-node
real-time workloads, and increasingly for geospatial / temporal systems with
low-millisecond query budgets.

---

## 1. Overview and Goals

Glen aims to provide:

- Low-latency reads and writes via in-memory data structures fronted by a
  sharded cache
- Durability via WAL + snapshot recovery, tolerant of mid-write crashes
- Optimistic transactions over multi-collection writes
- Real-time subscriptions at document and field granularity
- Equality, range, geo, and polygon indexes
- Multi-master replication with HLC last-write-wins, transport-agnostic
- Spillable storage for datasets that don't fit in RAM
- A small, clear core that is easy to reason about and embed

Non-goals (for now):

- Distributed consensus (Paxos/Raft); Glen's replication is eventually
  consistent under HLC LWW
- Cross-process / multi-writer concurrency with strict durability semantics
- Heavy SQL-like query planning
- True ACID across processes

---

## 2. Data Model

- **Collections**: named buckets of documents.
- **Document Id**: `Id` holds `(collection: string, docId: string, version: uint64)`.
- **Value**: variant supporting `null`, `bool`, `int64`, `float64`, `string`,
  `bytes`, `array`, `object`, and a typed `id` reference.
- **Versioning**: each doc has a monotonically increasing version, used for
  OCC and replication conflict resolution.
- **HLC**: Hybrid Logical Clock `(wallMillis, counter, nodeId)` attached to
  every mutation; drives multi-master last-write-wins.

---

## 3. Architecture

### 3.1 In-Memory State

A `GlenDB` is a `ref` carrying:

- `collections: Table[string, CollectionStore]` under a structural RW-lock.
- An array of `lockStripesCount` per-collection RW-locks (default 32),
  hashed by collection name.
- A sharded LRU `db.cache` (default 16 shards, 64 MiB byte-budget).
- A `SubscriptionManager` keyed by `collection:docId`.
- A `WriteAheadLog` owning the on-disk segment files.
- Replication state: `replSeq`, local `Hlc`, in-memory change log, per-peer
  cursors.
- An `indexManifestPath` recording persisted index definitions.

Each `CollectionStore` carries:

- `docs`, `versions` — the in-memory hot working set (eager) or hot subset
  (spill).
- `indexes`, `geoIndexes`, `polygonIndexes` — secondary indexes.
- `replMetaHlc`, `replMetaChangeId` — per-doc replication metadata for LWW.
- `snapshot` — optional mmap'd `*.snap` file (active in spillable mode).
- `dirty`, `deleted`, `hotDocCap`, `maxDirtyDocs` — spill-mode bookkeeping.

### 3.2 Sharded LRU Cache

- Reduces read latency and lock contention. Sized by total byte budget,
  split across `numShards` shards keyed by `hash(collection:docId)`.
- Each shard has its own lock and LRU list; concurrent reads on different
  hot keys don't contend.
- Metrics via `db.cacheStats()`: per-shard hits, misses, puts, evictions.

### 3.3 Write-Ahead Log

- Segmented files: `glen.wal.0`, `glen.wal.1`, …
- Header: `GLENWAL2` (magic + version) on first write to a fresh segment.
- Record format: `varuint bodyLen | uint32 fnv1a | body`. Body is a
  codec-encoded mutation (put or delete with version, hlc, changeId,
  originNode).
- Sync policies: `wsmAlways` (fsync per record) / `wsmInterval` (fsync per
  N bytes; default 8 MiB) / `wsmNone` (rely on OS buffering).
- Batched `appendMany` minimises flushes for multi-record commits.
- Replay tolerates a torn final record per segment; subsequent segments still
  consulted.

### 3.4 Snapshots

Three on-disk versions exist; readers auto-detect.

- **v1 (legacy)**: `varuint numDocs` + concatenated `(idLen, id, valLen, value)`
  records. No magic. Stream-decoded.
- **v2**: header + per-doc index entries (idLen + id + bodyOffset + bodyLength)
  + body section. Loaded by reading the entire index into a `Table` —
  O(n) at open, RAM-resident.
- **v3 (default; written by `compact()`)**: a paged index. Header + bodies
  (sorted by docId) + entries (variable-size, sorted) + offsets table
  (`docCount × uint64`, sorted). Lookups binary-search the offsets table
  through the OS page cache via mmap; only the pages actually touched stay
  resident.

Atomic write: temp file + `rename(2)` (POSIX) or temp + remove + move
(Windows).

`compact()` writes a fresh v3 snapshot for every collection and resets the
WAL. After compaction, replay starts from the snapshot, not from the
beginning of time.

### 3.5 Transactions (OCC)

- `db.beginTxn()` returns a `Txn` (state `tsActive`).
- `db.get(…, t)` records the observed version into `t.readVersions`.
- `t.stagePut(id, value)` / `t.stageDelete(coll, id)` build the write set.
- `db.commit(t)`:
  1. Pre-flight `maxDirtyDocs` check (spill-mode guardrail).
  2. Acquire every touched stripe (writes ∪ reads) in **sorted order** —
     deadlock-free.
  3. Validate recorded read versions against current. Mismatch → `csConflict`.
  4. Build WAL records; assign repl seq + HLC under `replLock`.
  5. Apply writes to `cs.docs`, `versions`, cache, indexes (eq + geo + poly).
  6. Batch-append WAL via `appendMany`.
  7. Release stripes; notify subscribers outside locks.

`csInvalid` is returned if the txn is no longer active or the dirty-budget
check failed.

### 3.6 Subscriptions

- **Document-level** — receive new value on every change.
- **Field-path** — fire only when the value at a specific dotted path differs
  (deep equality).
- **Field-delta** — string-aware: emits `{kind: "append", added: " world"}`
  for incremental string growth, `"set"` / `"replace"` / `"delete"` otherwise.
- **Streaming companions** — write framed events to a `std/streams` Stream
  via the binary codec, useful for IPC.

Callbacks fire after locks are released; they can issue their own DB calls
without deadlock.

### 3.7 Equality / Range Indexes

- Single-field equality, composite equality, single-field range. Backed by
  CritBitTree for ordered keys; `Table[string, HashSet[string]]` for postings.
- O(log n) maintenance per insert/update/delete (no full rebuild on delete).
- Range scans are O(log n + m); ascending iterates the critbit directly,
  descending materialises keys for reverse traversal.
- **Persistence**: `indexes.manifest` (text file) records every index
  definition. On reopen the manifest drives reconstruction. Equality indexes
  bulk-rebuild from docs; spatial indexes try the matching `.gri`/`.gpi`
  binary dump (CRC mismatch → silent fallback to bulk-rebuild). Users no
  longer need to re-call `createIndex` after restart.

### 3.8 Codec

- Compact tagged binary, varuint + zigzag, configurable size limits via
  env vars (`GLEN_MAX_STRING_OR_BYTES`, `GLEN_MAX_ARRAY_LEN`,
  `GLEN_MAX_OBJECT_FIELDS`).
- Used by WAL records, snapshot bodies, and replication change payloads.

### 3.9 Geospatial indexes (R-tree)

- Two index types: `GeoIndex` (point bboxes derived from two numeric fields)
  and `PolygonIndex` (polygon-shape MBRs plus the per-doc polygon cached for
  exact tests).
- Backing: a memory-resident R-tree, fanout 16. **STR (Sort-Tile-Recursive)**
  bulk-load on `createGeoIndex` for tight MBRs over an existing collection;
  **Guttman linear split** for incremental insert/delete.
- KNN: best-first traversal with a min-heap on bbox-to-query lower bound
  (Hjaltason & Samet). Two metric modes:
  - `gmPlanar` — squared Euclidean over raw degrees; trivially fast.
  - `gmGeographic` — haversine bbox lower bound (clamp into bbox, haversine
    to the clamped point); result distances in metres.
- Polygon containment: bbox prefilter + exact ray-cast. `findPointsInPolygon`
  reverses direction (bbox-of-polygon query against a points geo index, then
  ray-cast).
- Persistence: `<collection>.<name>.gri` (points) / `.gpi` (polygons).
  Format: magic + version + kind + entries (idLen + id + bbox) + (for
  polygons) per-id vertex lists + FNV-1a32 trailer. Loaded **before** WAL
  replay so replay hooks update the loaded tree incrementally.

### 3.10 Time-series engine (Gorilla)

- Standalone module (`glen/timeseries`), not part of the document model. One
  `Series` per file: append-only, designed for many `(int64 ts, float64 value)`
  per second.
- File: 16-byte header + sequence of chunks. Chunk header (40 B):
  `payloadBytes`, `count`, `startTs`, `endTs`, `minVal`, `maxVal` →
  bit-packed payload → FNV-1a32 trailer.
- Encoding: timestamps via delta-of-delta with 1/9/12/16/36-bit prefix codes;
  values via XOR with leading/trailing meaningful-bit-block reuse (Gorilla).
  ~1–3 bits/sample on smooth data, sub-1 bit on constant inputs.
- Active block buffer in memory until `blockSize` samples or `flush()`. Read
  paths cache decoded chunks via `glen/chunkcache` so repeated `latest` /
  `range` queries hit a Table instead of re-running bit-decoding (60–115×
  speedup on tail-of-snapshot reads).
- Recovery: per-chunk CRC; torn tail truncated on reopen.
- Retention: `dropBlocksBefore(cutoffMs)` rewrites surviving blocks
  byte-for-byte (no re-encoding).

### 3.11 Linear algebra

- `Vector = seq[float64]` (idiomatic). `Matrix` row-major flat
  (`rows`, `cols`, `data: seq[float64]`) for cache-friendly `matmul`
  (ikj loop, skip-on-zero).
- Operations: `add`/`sub`/`scale`/`dot`/`norm`/`normalize`/`euclidean`/
  `cosine`/`hadamard` for vectors; `matmul`/`transpose`/`matvec`/`trace`
  for matrices; in-place variants for hot loops; `+`/`-`/`*` operator sugar.
- Storage: `toValue` / `readVector` / `readMatrix` round-trip through plain
  `VArray`-of-`VFloat`. No new `ValueKind` — vectors and matrices ride along
  inside ordinary documents and inherit codec, snapshot, replication, and
  subscription support for free.

### 3.12 GeoMesh — bbox-anchored 2-D raster

- One immutable raster pinned to a geographic `BBox` with
  `(rows, cols, channels)`. Convention: `row 0` = top of bbox (max lat),
  `col 0` = left (min lon).
- In-memory: `seq[float64]` flat row-major channel-interleaved at
  `data[(row*cols + col)*channels + ch]`.
- API: `cellAt(lon, lat) → (inBounds, row, col)`,
  `cellCenter(row, col) → (lon, lat)`, `valueAt`/`vectorAt`,
  `setCell` / `[r, c, ch]`.
- Storage: serialised `data` field uses `vkBytes` (raw little-endian
  float64). A 1000×1000×5 mesh is ~40 MB on disk vs hundreds of MB if
  stored as nested `VArray`s.

### 3.13 Tile time-stack

- Standalone module (`glen/tilestack`). Use case: a 2-D raster that evolves
  through time — radar reflectivity sweeps, weather model output, animated
  probability fields from an LLM, satellite image time series.
- Storage: a directory containing a text `manifest.tsm` (bbox, dims,
  tile/chunk size, labels) plus one append-only `tile_<r>_<c>.tts` per
  spatial tile.
- Chunk encoding: a single shared timestamp stream (DoD), then
  `tileSize² × channels` independent Gorilla streams. Each cell-channel has
  its own `XorState`.
- `appendFrame(tsMillis, mesh)` decomposes a `GeoMesh` into per-tile slices
  and appends each to its tile's active buffer. Auto-flush on
  `chunkSize`-frames threshold; out-of-order ts forces a chunk flush so
  per-chunk monotonicity holds.
- `readFrame(tsMillis)` reassembles a full mesh by gathering each tile's
  data at that ts; `readPointHistory(lon, lat, fromMs, toMs)` decodes only
  the **single tile** that owns the cell — that's the workload tiling
  exists to make fast.
- Read paths cache decoded chunks via `glen/chunkcache` (4–6× speedup on
  repeated `readPointHistory`/`readFrame`).
- Compression: ~10–30× on radar-like sparse fields, ~2–3× on fully-varying
  smooth fields.

### 3.14 Shared bit-packing primitives

`glen/bitpack` factors out the encode/decode primitives reused by
`timeseries` and `tilestack`: `BitWriter`/`BitReader` (MSB-first big-endian
bit packing), `zigzag`/`unzigzag`, `encodeDoD`/`decodeDoD` with 1/9/12/16/36
total-bit prefix codes, `encodeXor`/`decodeXor` with `XorState`, and
`fnv1a32`. The DoD prefix table observes that the 7-bit zigzag slot covers
`[-64, 63]` (not `[-63, 64]`); the XOR reuse path uses `high(uint64)`
instead of `(1 shl 64) - 1` to avoid undefined behaviour when the prior
fresh block had `leading == 0 && trailing == 0`.

### 3.15 Schema validation (`glen/validators`)

- Zod-style DSL: `zString().minLen(2)`, `zInt().gte(0).lte(150)`, `zBool()`,
  `zEnum([…])`, with `zobject:` blocks for nested schemas.
- `parse(value)` returns `(ok, coercedValue, issues[])`. Issues carry
  path-tagged messages.
- No automatic gating on `put` — wrap your writes in a helper if you want
  every mutation validated.

### 3.16 Spillable mode

When `newGlenDB(..., spillableMode = true)`:

- Snapshot is opened via `mmap` on the v3 file. The doc index is on disk;
  only the 40-byte header reads at open.
- `cs.docs` starts empty; reads fault docs in via a binary-search lookup
  on the mmap'd offsets table.
- `hotDocCap` (default 0 = unbounded) caps `cs.docs.len`; over-cap reads
  evict cold non-dirty entries, faulted back from the snapshot on next
  access.
- `dirty` / `deleted` track in-memory mutations that haven't been folded
  into the snapshot yet. They cannot be evicted (the value lives only in
  cs.docs + WAL until the next compact).
- `maxDirtyDocs` (default 0 = unbounded) caps the dirty set. Multi-doc
  operations (`commit`, `applyChanges`, `putMany`, `deleteMany`) check
  before applying any writes; overflow returns `csInvalid` (commit) or
  raises `ValueError` (others).
- `compact()` rewrites the snapshot from `materializeAllDocs()` (snapshot
  reads + dirty cs.docs entries + skip tombstones), swaps the mmap, clears
  dirty/deleted.

### 3.17 Streaming iterators

For bulk reads with bounded memory: every `getAll` / `getMany` / `findBy` /
`rangeBy` / `findInBBox` / `findNearest` / `findWithinRadius` /
`findPolygonsContaining` / `findPolygonsIntersecting` /
`findPointsInPolygon` has a `*Stream` companion that yields one
`(id, value)` at a time. The iterator captures the matching ID set under a
brief lock, releases it, then re-acquires per yield to bypass-load each
value (without populating cs.docs). Mid-iteration deletes are tolerated.

Combined with `spillableMode + hotDocCap`, this gives a memory floor of
"one Value at a time + ~30 B per ID for the upfront list."

---

## 4. Concurrency Model

- **structLock** (RW): protects only the outer `collections` map.
- **stripe locks** (RW × `lockStripesCount`): protect per-collection state.
  Stripe selection: `abs(hash(collection)) % stripeCount`.
- **replLock** (mutex): replication seq counter + local HLC + in-memory
  change log.

Multi-stripe operations acquire stripes in ascending sorted order →
deadlock-free. Global ops (`snapshotAll`, `compact`, `close`) acquire every
stripe + structLock read.

Cache and subscription manager carry their own internal locking and aren't
shared with these.

### Borrowed Reads

Standard reads (`get`, `getMany`, `getAll`) return deep clones. Borrowed
APIs (`getBorrowed`, `getBorrowedMany`, `getBorrowedAll`,
`getBorrowedAllStream`, `getBorrowedManyStream`) return refs for speed.
**Borrowed values are read-only**; mutating them silently corrupts the
cache and other callers' views.

### Multi-threaded compilation

`--mm:atomicArc -d:useMalloc --threads:on`. ORC's cycle collector is not
thread-safe across cross-thread `Value` refs. Glen's `Value` graph is
acyclic, so atomicArc has no functional downside. Single-threaded callers
keep `--mm:orc` (default).

---

## 5. Replication

Multi-master, transport-agnostic. Each mutation gets a tuple under
`replLock`:

- `seq` — local monotonic counter; drives the export cursor.
- `hlc = (wallMillis, counter, nodeId)` — Hybrid Logical Clock.
- `changeId = "$seq:$nodeId"` — globally unique; makes apply idempotent.
- `originNode` — for filtering / topology.

`exportChanges(since: cursor, includeCollections, excludeCollections)`
returns a (newCursor, changes) pair from the in-memory log.
`applyChanges(changes)` is idempotent via `changeId` and resolves conflicts
via HLC last-write-wins; older changes are dropped, newer ones win. After
apply, the local HLC advances past the highest seen value.

`gcReplLog()` trims the in-memory log up to `min(peerCursors)`. Peer
cursors persist to `peers.state` (debounced; flushed on close).

Glen ships the data plane only — bring your own transport (HTTP, gRPC,
NATS, mailbox dir, USB stick).

---

## 6. Durability, Recovery, and Compaction

- WAL durability tunes via `walSync` policy.
- **Recovery sequence**:
  1. Load every `*.snap` (eager: decode all entries; spill: just mmap +
     read header).
  2. Read `indexes.manifest`. For each spatial entry, try loading the
     matching `.gri`/`.gpi` binary dump. CRC mismatch → silent fallback
     to bulk-rebuild post-replay.
  3. Replay every WAL segment in order — each record advances repl seq,
     restores per-doc HLC + changeId, and incrementally updates any
     loaded indexes.
  4. Bulk-rebuild any equality / geo / polygon index that didn't have a
     dump loaded.
  5. Restore peer cursors from `peers.state`.
- **Compaction**: `db.compact()` writes fresh v3 snapshots, dumps every
  `geoIndex`/`polygonIndex` to `.gri`/`.gpi`, and resets the WAL. Atomic
  rename of each snapshot file. In spillable mode, also closes and
  reopens the mmap and clears dirty/deleted.

Backup: snapshot-based — call `snapshotAll`, copy `*.snap`, `*.gri`,
`*.gpi`, `indexes.manifest`, `peers.state`, `glen.wal.*` to your target.
Restore by placing them in a directory and opening with `newGlenDB`.

---

## 7. Operational Guidance

### 7.1 Files and Layout

```
mydb/
├── glen.wal.0           ← active WAL segment
├── glen.wal.1           ← rotated segments
├── users.snap           ← per-collection snapshot (v3 by default)
├── orders.snap
├── node.id              ← stable replication node id
├── peers.state          ← persisted per-peer cursors
├── indexes.manifest     ← persisted index definitions
├── places.byLoc.gri     ← R-tree binary dump for geo index
└── zones.byShape.gpi    ← R-tree binary dump for polygon index
```

Plus, when used:

- `<dir>/<series>.gts` — Gorilla scalar TSDB files
- `<stackDir>/manifest.tsm` + `<stackDir>/tile_<r>_<c>.tts` — tile time-stack

### 7.2 Configuration

Constructor: `newGlenDB(dir; cacheCapacity, cacheShards, walSync,
walFlushEveryBytes, lockStripesCount, spillableMode, hotDocCap,
maxDirtyDocs)`.

Env-driven: `newGlenDBFromEnv(dir)` reads `GLEN_NODE_ID`,
`GLEN_WAL_SYNC`, `GLEN_WAL_FLUSH_BYTES`, `GLEN_CACHE_CAP_BYTES`,
`GLEN_CACHE_SHARDS`, plus codec safety caps.

Runtime: `db.setWalSync(mode, flushEveryBytes)`,
`db.cache.adjustCapacity(newCap)`.

### 7.3 Compaction Policy

Periodically call `db.compact()` or trigger when `db.wal.totalSize()`
crosses a threshold. After compact, the WAL is empty and the snapshot
reflects live state.

---

## 8. Best Use Cases

- Local-first / offline-first apps needing fast embedded storage and
  real-time updates.
- Edge services where a single-process database suffices.
- Real-time dashboards and reactive UIs via subscriptions.
- Geospatial / temporal pipelines: per-station radar archives, sensor /
  metric time series, weather model output, LLM-emitted probability rasters.
- Single-node analytics with multi-million doc working sets (eager) or
  arbitrarily large archives (spill).
- Multi-master edge replicas synchronising via mailbox dirs / occasional
  WAN windows.
- Testing / prototyping where simple APIs and strong defaults matter.

---

## 9. When Not to Use Glen

- Distributed systems requiring strong consistency / consensus.
- Heavy multi-process writers with strict ACID.
- Complex ad-hoc query planning (joins, aggregations across collections,
  cost-based optimization).
- Workloads dominated by random snapshot lookups against billions of docs
  with no caching whatsoever — the v3 paged index handles this gracefully
  but a dedicated KV engine (RocksDB et al.) will still win on raw cold-key
  throughput.

---

## 10. Application Patterns

- **Event sourcing**: store events as docs; subscribe for materialised
  views; periodic `compact()` to bound replay time.
- **CQRS**: equality / range / spatial indexes power the query side;
  writes flow via `commit` with OCC.
- **Streaming UIs**: `subscribeField` / `subscribeFieldDelta` for reactive
  state propagation; streaming companions for IPC to non-Nim consumers.
- **Edge / IoT**: persist locally; ship batched changes upstream via
  `exportChanges` whenever a transport is available.
- **Geospatial real-time + archive**: frame-per-doc + GeoMesh for the hot
  24h, TileStack for the long tail.
- **Local-first sync**: each device runs a Glen, exchanges change batches
  through any transport (USB stick, HTTP POST, mailbox dir).

---

## 11. API Highlights

- **Creation**: `newGlenDB(dir; …, spillableMode, hotDocCap, maxDirtyDocs)`,
  `newGlenDBFromEnv(dir)`.
- **Basic Ops**: `put`, `get`, `getMany`, `getAll`, `delete`.
- **Batch**: `putMany`, `deleteMany`.
- **Borrowed**: `getBorrowed`, `getBorrowedMany`, `getBorrowedAll`.
- **Streaming iterators**: `getAllStream`, `getManyStream`, `findByStream`,
  `rangeByStream`, `findInBBoxStream`, `findNearestStream`,
  `findWithinRadiusStream`, `findPolygonsContainingStream`, …
- **Transactions**: `beginTxn`, `get(…, t)`, `getMany(…, t)`, `commit`.
- **Indexes**: `createIndex`/`dropIndex`/`findBy`/`rangeBy`,
  `createGeoIndex`/`dropGeoIndex`/`findInBBox`/`findNearest`/
  `findWithinRadius`, `createPolygonIndex`/`dropPolygonIndex`/
  `findPolygonsContaining`/`findPolygonsIntersecting`/`findPointsInPolygon`.
- **Subscriptions**: `subscribe`, `subscribeField`, `subscribeFieldDelta`,
  stream-companions, `unsubscribe*`.
- **Replication**: `exportChanges`, `applyChanges`, `setPeerCursor`,
  `getPeerCursor`, `gcReplLog`.
- **Validation**: `zobject:`, `zString`/`zInt`/`zBool`/`zEnum` builders,
  `parse`.
- **Maintenance**: `snapshotAll`, `compact`, `close`.
- **Standalone engines**: `glen/timeseries` (`openSeries`, `append`,
  `range`, `latest`, `dropBlocksBefore`); `glen/tilestack` (`newTileStack`,
  `appendFrame`, `readFrame`, `readFrameRange`, `readPointHistory`).
- **Numeric**: `glen/linalg` (`Vector`, `Matrix`, `add`, `dot`, `cosine`,
  `matmul`, …); `glen/geomesh` (`newGeoMesh`, `cellAt`, `valueAt`,
  `vectorAt`, `toValue`/`readGeoMesh`).

---

## 12. Benchmarks and Methodology

The repo includes:

- `tests/test_bench.nim` — core CRUD micro-benchmarks.
- `tests/test_bench_concurrent.nim` — multi-threaded contention
  (`atomicArc + -d:useMalloc`).
- `tests/test_bench_geo.nim` — R-tree, geo / polygon indexes, persistence.
- `tests/test_bench_timeseries.nim` — Gorilla TSDB + tile time-stacks.
- `tests/test_snapshot_v3.nim` — open / lookup / iteration on a 100k-doc
  v3 snapshot.

Numbers vary by hardware, sync mode, cache size / shards. For realistic
sizing prefer application-level benchmarks. See `docs/performance.md` for
representative figures on Apple M5.

---

## 13. Roadmap

- Auto-compaction (size- and time-based triggers).
- Higher-level query layer: filters, projections, cursor pagination.
- Secondary derived indexes (computed fields).
- zstd page compression for snapshots and tile chunks.
- Vector index (HNSW or IVF) for nearest-neighbour over stored embeddings.
- Per-cell offset table in tile chunks (skip-decode for `readPointHistory`).
- Bilinear interpolation in `GeoMesh.sampleAt`.
- SIMD bit-decode (AVX-512 PEXT/PDEP, ARM NEON) for the Gorilla unpack
  hot path.
- Parallel replication export.
- Encryption at rest; integrity beyond per-record checksums.
- Native async transport adapters for replication.

---

## 14. Security and Safety Notes

- Borrowed values are shared references; treat them as immutable.
- WAL `wsmNone` and large `wsmInterval` increase the window for recent-write
  loss on crash. Default is `wsmInterval` at 8 MiB.
- Snapshot writes are atomic via temp + rename; ensure the host filesystem
  matches the documented behaviour for your platform.
- The `maxDirtyDocs` guardrail converts pathological multi-doc operations
  into clear `csInvalid` / `ValueError` returns instead of OOM.
- The doc index in v3 is mmap-backed; a corrupted snapshot can mislead
  binary search into out-of-range reads. Headers carry section offsets that
  are bounds-checked on open; on-disk integrity beyond that is up to the
  filesystem (and any zstd / encryption layer added in the future).

---

## 15. Conclusion

Glen provides a compact, high-performance embedded datastore with pragmatic
durability, optimistic transactions, subscriptions, indexes, replication,
and first-class spatial / temporal / numeric primitives. It scales from
tightly-bounded edge devices to single-node archives much larger than RAM
via spillable mode and the v3 paged on-disk index. It fits best where
single-process latency matters, simplicity is valued over distributed
complexity, and the working set — when fully resident — fits in memory.
