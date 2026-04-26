## Glen: An Embedded Document Database for Nim

### TL;DR
Glen is a fast, embedded, single-process document database for Nim. It stores data in-memory for speed, persists mutations via a write-ahead log (WAL), periodically takes snapshots per collection for fast recovery, offers optimistic transactions (OCC), field and document subscriptions, indexes for equality and simple ranges, and a sharded LRU cache. On top of that base, Glen adds first-class spatial primitives (R-tree-backed geo and polygon indexes, with both planar and geographic — haversine — distance metrics), a Gorilla-style time-series engine, lightweight linear algebra, a bbox-anchored raster type (`GeoMesh`), and a tiled time-stack engine for compressed storage of rasters that evolve through time (radar / model output / probability fields). It is designed for local-first apps, edge/embedded services, tests, analytics dashboards, single-node real-time workloads, and increasingly geospatial/temporal systems with low-millisecond query budgets.

---

## 1. Overview and Goals
Glen aims to provide:
- Low-latency reads/writes with in-memory data structures
- Durability via WAL + snapshot recovery
- Lightweight optimistic transactions
- Real-time subscriptions at document and field granularity
- Simple indexes (equality, single-field ranges)
- Small, clear core that is easy to reason about and embed

Non-goals (for now):
- Distributed consensus or replication
- Cross-process concurrency/multiprocess durability guarantees
- Heavy SQL-like query planning

---

## 2. Data Model
- Collections: Named buckets of documents.
- Document Id: `Id` holds `(collection: string, docId: string, version: uint64)`.
- Value: Variant type supporting null, bool, int, float, string, bytes, array, object, and id.
- Versioning: Each doc has a monotonically increasing version; used for OCC and subscriptions.

---

## 3. Architecture

### 3.1 In-Memory State
- `collections`: `Table[string, Table[string, Value]]`
- `versions`: `Table[string, Table[string, uint64]]`
- `indexes`: `Table[string, IndexesByName]`
- Synchronization: Reader-prefer `RwLock` with per-collection striped locks.
  - Stripe selection: `hash(collection) % N` where N is `lockStripesCount` (configurable)
  - Multi-collection ops (e.g., commit) lock stripes in ascending order to avoid deadlocks
  - Global ops (snapshot/compact/close) lock all stripes

### 3.2 Sharded LRU Cache
- Purpose: Reduce read latency and lock contention.
- Design: `numShards` shards keyed by hash of `collection:docId`, each with its own lock and LRU list.
- Capacity: Global capacity split across shards.
- Metrics: Per-shard and aggregate hits, misses, puts, evictions via `cache.stats()`.

### 3.3 Write-Ahead Log (WAL)
- Segmented files: `glen.wal.0`, `glen.wal.1`, ...
- Record format: `varuint bodyLen | uint32 checksum | body`.
- Body encoding: `varuint recordType | collection | docId | version | valueLen(+value)`.
- Sync policies:
  - `wsmAlways`: fsync after each append.
  - `wsmInterval`: fsync after `flushEveryBytes` written (default); balances throughput and durability window.
  - `wsmNone`: rely on OS buffering.
- Batching: Commits build multiple `WalRecord`s and append them via `appendMany` to minimize flushes.
- Concurrency: WAL appends are guarded by an internal mutex so concurrent striped writers are safe.
- Rotation: New segment when max size exceeded; durable magic/version header on first write.
- Recovery: On open, replay all segments after loading snapshots; tail corruption tolerated.

### 3.4 Snapshots
- Per-collection snapshot file: `collection.snap`.
- Atomic write via temp file + rename.
- Recovery: Load snapshots, then replay WAL to the latest state.
- Compaction: `db.compact()` snapshots all collections and resets the WAL to bound replay time.

### 3.5 Transactions (OCC)
- Begin: `db.beginTxn()` creates a `Txn` (active state).
- Read-tracking: `db.get(..., t: Txn)` records the version observed.
- Staging: `t.stagePut(id, value)` / `t.stageDelete(collection, docId)`.
- Commit:
  1) Validate all observed versions against current versions; conflict → rollback.
  2) Acquire write locks for involved collection stripes (sorted order); compute new versions; build WAL records; update in-memory state and cache; reindex.
  3) Batch-append WAL; release locks; notify subscribers afterwards.

### 3.6 Subscriptions
- Document-level: Subscribers receive new Value for a `(collection, docId)` on change.
- Field-level: Subscribers can target field paths; delta callbacks available.
- Stream APIs: Write framed events to a stream for integration with external consumers.

### 3.7 Indexes
- Equality and range (single-field) support; composite equality via serialized composite keys.
- Internals: `Table[string, HashSet[string]]` for postings; `CritBitTree` for ordered range keys.
  - Ordered key maintenance is O(log n) for insert/update/delete. Deletes use `excl` on the critbit tree (no full rebuild).
  - Range scans are O(log n + m), where m is the number of results returned. Ascending scans iterate the critbit tree directly; descending scans materialize keys temporarily for reverse traversal.
- Maintenance: On put/delete/commit, indexes update by (re)indexing affected docs.
- **Persistence**: an `indexes.manifest` text file records every index definition. On reopen, the manifest drives index reconstruction: spatial indexes try a binary `.gri`/`.gpi` dump first (written at `compact()`); equality indexes and any spatial entry without a dump rebuild from the in-memory docs after WAL replay. Users no longer need to re-call `createIndex` after restart.

### 3.8 Codec
- Value encode/decode used by WAL and snapshots. Supports all `Value` kinds.

### 3.9 Geospatial indexes (R-tree)
- Two index types: `GeoIndex` (point bboxes derived from two numeric fields) and `PolygonIndex` (polygon-shape bboxes plus the per-doc polygon for exact tests).
- Backing data structure: a memory-resident R-tree with fanout 16 (default). Bulk-loaded with **STR** (Sort-Tile-Recursive) for tight MBRs on `createGeoIndex` over an existing collection; **Guttman linear split** for incremental insert/delete on subsequent mutations.
- KNN traversal: best-first with a min-heap on bbox-to-point lower-bound (Hjaltason & Samet). Two metric modes:
  - `gmPlanar` — squared Euclidean distance over raw coords (degrees); fast, fine for short ranges.
  - `gmGeographic` — haversine-on-bbox lower bound (clamp query lon/lat into the bbox, then haversine to the clamped point). Result distances in metres.
- Polygon containment: bbox prefilter via the R-tree, then exact ray-cast (Crossing Number) test. `findPointsInPolygon` reverses the direction — bbox-of-polygon query against a points geo index, then ray-cast over candidates.
- Persistence: a per-index binary dump (`<collection>.<name>.gri` for points, `.gpi` for polygons) is written at `compact()`. Format: magic `GLENGRI1` + version + kind + entries `(idLen, id, bbox)` + (for polygons) per-id vertex lists + FNV-1a32 trailer. On reopen, the dump is loaded **before** WAL replay so the existing replay hooks update the loaded tree incrementally; bad CRC or missing dump silently falls back to bulk-rebuild from docs.

### 3.10 Time-series engine (Gorilla)
- Standalone module (`glen/timeseries`); not part of the document model. One `Series` = one append-only file, designed for many `(int64 ts, float64 value)` per second per series.
- File layout: 16-byte header, then sequence of chunks. Each chunk: 40-byte header (`payloadBytes`, `count`, `startTs`, `endTs`, `minVal`, `maxVal`), bit-packed payload, FNV-1a32 trailer.
- Encoding: timestamps via delta-of-delta with 1/9/12/16-bit prefix codes; values via XOR with leading/trailing meaningful-bit-block reuse (Gorilla). Result is ~1–3 bits/sample on smooth data, sub-1 bit on constant or perfectly-cadenced inputs.
- Active block buffer: in-memory until it reaches `blockSize` samples or `flush()` is called. The append fast path is amortised O(1) (seq push); only one chunk encode per `blockSize` samples.
- Reads: range scans iterate the in-memory block index, decoding only chunks whose `[startTs, endTs]` intersects the query. Latest-N walks chunks back-to-front. Active block is read directly without bit decoding.
- Recovery: per-chunk CRC; torn tail (mid-flush crash) is silently truncated on reopen.
- Retention: `dropBlocksBefore(cutoffMs)` rewrites the file streaming surviving blocks byte-for-byte (no re-encoding).

### 3.11 Linear algebra
- `Vector = seq[float64]` (idiomatic Nim). `Matrix` is row-major flat (`rows`, `cols`, `data: seq[float64]`) for cache-friendly `matmul` (ikj loop order, skip-on-zero).
- Operations: `add`/`sub`/`scale`/`dot`/`norm`/`normalize`/`euclidean`/`cosine`/`hadamard` for vectors; `matmul`/`transpose`/`matvec`/`trace` for matrices; in-place variants for hot loops; `+`/`-`/`*` operator sugar.
- Storage: helpers `toValue` / `readVector` / `readMatrix` round-trip through plain `VArray`-of-`VFloat`. No new `ValueKind` — vectors and matrices ride along inside ordinary documents and inherit codec, snapshot, replication, and subscription support for free.

### 3.12 GeoMesh — bbox-anchored 2-D raster
- One immutable raster pinned to a geographic `BBox` with `(rows, cols, channels)`. Cell ordering: `row 0` = top of bbox (max lat), `col 0` = left (min lon).
- In-memory: `seq[float64]` flat row-major, channel-interleaved at `data[(row*cols + col)*channels + ch]`.
- API: `cellAt(lon, lat) → (inBounds, row, col)`, `cellCenter(row, col) → (lon, lat)`, `valueAt`/`vectorAt` for point sampling, `setCell` for batch writes.
- Storage: serialised as a `VObject` whose `data` field is `vkBytes` (raw little-endian float64). A 1000×1000×5 mesh is ~40 MB on disk vs hundreds of MB if stored as nested `VArray`s.

### 3.13 Tile time-stack
- Standalone module (`glen/tilestack`). Use case: a 2-D raster that evolves through time — radar reflectivity sweeps, weather model output, animated probability fields from an LLM, satellite image time series.
- Storage: a directory containing a text `manifest.tsm` (bbox, dims, tile/chunk size, labels) plus one append-only `tile_<r>_<c>.tts` file per tile. Each tile file is a sequence of chunks identical in spirit to the time-series engine: 40-byte chunk header + bit-packed payload + FNV-1a32 trailer.
- Chunk encoding: a single shared timestamp stream (DoD), then `tileSize² × channels` independent Gorilla streams (each cell-channel has its own `XorState` so streams can in principle be decoded independently — the current implementation decodes the whole chunk on access).
- `appendFrame(tsMillis, mesh)` decomposes a full `GeoMesh` into per-tile slices and appends each to its tile's active buffer; tiles auto-flush once their buffer reaches `chunkSize`. Out-of-order timestamps force a chunk flush so each chunk stays monotonic.
- `readFrame(tsMillis)` reassembles a full mesh by gathering each tile's data at that ts (active buffer or chunk decode); `readPointHistory(lon, lat, fromMs, toMs, channel)` decodes only the **single tile** that owns the cell — that's the workload tiling exists to make fast.
- Compression: ~10–30× on radar-like sparse fields (most cells stable, a moving storm region varies slowly), ~2–3× on fully-varying smooth fields. Worst case is fully-random per-cell-per-frame, where Gorilla degenerates to nearly raw float storage; that workload is unsuitable for tile stacks.
- Recovery: torn-chunk tail tolerated via per-chunk CRC, identical to the time-series engine.

### 3.14 Shared bit-packing primitives
- `glen/bitpack` factors out the encode/decode primitives reused by `timeseries` and `tilestack`: `BitWriter`/`BitReader` (MSB-first big-endian bit packing), `zigzag`/`unzigzag`, `encodeDoD`/`decodeDoD` with 0/9/12/16/36-bit prefix codes, `encodeXor`/`decodeXor` with `XorState`, and `fnv1a32`. The DoD prefix table corrects a subtle off-by-one (zigzag of `64` doesn't fit in 7 bits, so the 7-bit slot covers `[-64, 63]` not `[-63, 64]`); the XOR reuse path uses `high(uint64)` instead of `(1 shl 64) - 1` to avoid undefined behaviour when the prior fresh block had `leading == 0 && trailing == 0`.

---

## 4. Concurrency Model
- Core state protected by a reader-prefer `RwLock`.
- Cache is sharded; each shard has its own lock; drastically reduces contention for hot reads.
- Per-collection striped locks isolate contention between different collections; multi-collection ops lock multiple stripes with deadlock-safe ordering.
- Transactions are optimistic: conflicts are detected at commit by comparing versions.

### Borrowed Reads
- Standard reads (`get`, `getMany`, `getAll`) return deep clones.
- Borrowed APIs (`getBorrowed`, `getBorrowedMany`, `getBorrowedAll`) return references for speed.
- Important: Borrowed Values are read-only; callers must not mutate them. They may be shared across threads through the cache.

---

## 5. Durability, Recovery, and Compaction
- Durability relies on WAL policy:
  - `wsmAlways` → strongest; lowest throughput.
  - `wsmInterval` → default; tunable window (e.g., 8–64 MiB) balances throughput and loss window.
  - `wsmNone` → fastest; crash may lose recent writes buffered by OS.
- Recovery: Load each `collection.snap`, then `replay(dir)` all WAL segments. Tail corruption is tolerated (records are length+checksum delimited).
- Compaction: Call `db.compact()` periodically or when `wal.totalSize()` exceeds a threshold. This writes snapshots and then truncates the WAL via `wal.reset()`.

---

## 6. Performance Guide

### 6.1 Quick Wins
- Use `wsmInterval` with a larger `walFlushEveryBytes` (8–64 MiB) for higher write throughput.
- Increase `cacheCapacity` and `cacheShards` if memory and cores allow.
- Prefer batch APIs (`getMany`) to amortize locks.
- Use borrowed reads in hot paths when safe to avoid clones.

### 6.2 Transaction Path Optimizations
- Batched WAL appends reduce flushes and syscalls.
- Avoid string parsing: transaction write keys are `(collection, docId)` tuples.
- Pre-encode values outside critical sections if you extend the API for external batched puts.

### 6.3 Indexing Strategy
- Add equality indexes for frequent filters; use single-field range indexes for ordered scans.
- Avoid frequent churn on high-cardinality indexes unless necessary.

### 6.4 Locking Strategy
- Reader-prefer RW lock benefits read-heavy workloads.
- Use striped locks (default 32) to reduce global contention under mixed workloads; tune `lockStripesCount` to core count.

### 6.5 Monitoring
- Expose and monitor `db.cacheStats()` to track hit rates and shard imbalances.
- Track `wal.totalSize()` to schedule compaction.

---

## 7. Operational Guidance

### 7.1 Files and Layout
- Database directory contains `*.snap` (one per collection) and WAL segments `glen.wal.N`.

### 7.2 Configuration
- Constructor: `newGlenDB(dir; cacheCapacity, cacheShards, walSync, walFlushEveryBytes, lockStripesCount)`.
- Runtime WAL policy: `db.setWalSync(mode, flushEveryBytes)`.
- Cache capacity can be adjusted at runtime: `cache.adjustCapacity(newCap)`.

### 7.3 Backup and Restore
- Snapshot-based backup: call `snapshotAll`, copy `*.snap` and WAL segments.
- Restore by placing snapshots and WAL files in the data directory; Glen replays WAL on open.

### 7.4 Compaction Policy
- Periodically call `db.compact()` or trigger it when WAL crosses a size threshold.

---

## 8. Best Use Cases
- Local-first or offline-first applications needing fast embedded storage and real-time updates.
- Edge services where a single-process database suffices.
- Real-time dashboards; reactive UIs via subscriptions.
- Analytics or caching layers with small-to-medium datasets fitting in memory.
- Single-node event logging or event sourcing with periodic snapshotting.
- Testing/prototyping environments that need strong performance and simple APIs.

---

## 9. When Not to Use Glen
- Distributed systems requiring replication, failover, or consensus.
- Data sizes far exceeding available memory (Glen keeps active working set in RAM).
- Heavy multi-process writers or cross-process concurrency with strict durability semantics.
- Complex ad-hoc queries requiring advanced indexing or query planning.

---

## 10. Application Patterns
- Event Sourcing: Store events as docs; periodically materialize snapshots; truncate WAL.
- CQRS: Use equality/range indexes to power query side; apply writes via OCC.
- Streaming Updates: Subscribe to hot documents/fields; feed to UI or downstream.
- IoT/Edge: Local persistence with intermittent compaction; ship snapshots upstream if needed.

---

## 11. API Highlights
- Creation: `newGlenDB(dir; cacheCapacity, cacheShards, walSync, walFlushEveryBytes, lockStripesCount)`
- Basic Ops: `put`, `get`, `getMany`, `getAll`, `delete`
- Batch Ops: `putMany`, `deleteMany`
- Borrowed Reads: `getBorrowed`, `getBorrowedMany`, `getBorrowedAll` (do not mutate)
- Transactions: `beginTxn`, `get(..., t)`, `getMany(..., t)`, `commit`, `rollback`
- Indexes: `createIndex`, `dropIndex`, `findBy`, `rangeBy`
- Subscriptions: `subscribe`, `unsubscribe`, field/stream/delta variants
- Maintenance: `snapshotAll`, `compact`, `close`
- Tuning/Introspection: `setWalSync`, `cacheStats()`, `wal.totalSize()`

---

## 12. Benchmarks and Methodology
The repo includes `tests/test_bench.nim` which runs puts/gets/getMany/txn commit micro-benchmarks. Numbers vary by machine and configuration (sync mode, cache size/shards). For realistic workloads, prefer application-level benchmarks.

---

## 13. Roadmap Ideas
- Per-collection/striped locks to reduce contention further.
- Background/online compaction and incremental snapshots.
- Compression (e.g., zstd) for WAL and snapshots.
- Range iterators and borrowed-result query APIs.
- Encryption at rest; integrity metadata beyond checksums.
- Replication plugins or changefeed export.

---

## 14. Security and Safety Notes
- Borrowed Values are shared references; treat them as immutable.
- WAL `wsmNone` and large `wsmInterval` increase risk of recent-write loss on crash.
- Snapshots replace files atomically; ensure the host filesystem semantics match the documented behavior for your platform.

---

## 15. Conclusion
Glen provides a compact, high-performance embedded datastore with pragmatic durability, optimistic transactions, subscriptions, and indexing. It fits best where single-process latency matters, the working set largely fits in memory, and simplicity is valued over distributed complexity.


