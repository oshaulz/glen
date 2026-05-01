# Performance

Apple M5, `-d:release`, ORC + `-O3`. Reproduce with the corresponding
`nimble` task; All values should be treated as the order of magnitude you'll see, not exact constants.

## Core CRUD (`nimble bench_release`)

```
puts:                380k ops/s
gets (cloned):       1.28M ops/s
gets (borrowed):     9.1M ops/s
getMany:             21k batches/s   ×100 docs ≈ 2.1M doc reads/s
txn commits:         357k ops/s
```

The borrowed-read path skips the defensive `clone()` and is appropriate for
read-only hot loops.

## Batched writes (`nimble bench_release`)

Three batched-write paths exist: `putMany` (single collection, multi-doc),
`deleteMany` (single collection, multi-id), and `commit` over a `Txn` with
multiple `stagePut`/`stageDelete` calls (multi-collection). All three
amortize WAL flush + lock acquisition across the batch.

```
putMany     batch=10:    210k docs/s   (~21k batches/s)
putMany     batch=100:   270k docs/s   (~2.7k batches/s)
putMany     batch=1000:  180k docs/s   (~180 batches/s)
deleteMany  batch=100:   350k docs/s
deleteMany  batch=1000:  350k docs/s
commit (writes/txn=10):     220k docs/s  (~22k commits/s)
commit (writes/txn=100):    130k docs/s  (~1.3k commits/s)
commit (writes/txn=1000):   210k docs/s  (~210 commits/s)
```

Batches in the 10–100 range tend to win over single-doc `put` once you
factor in the lock + WAL framing overhead per call. Past ~1000 docs per
batch the per-doc rate drops back as the in-flight `walRecs` and `pending`
seqs grow and you start crossing the WAL flush threshold mid-batch.
`deleteMany` scales flatter because there's no value-clone cost per record.

## Multi-threaded contention (`nimble bench_concurrent`, `--mm:atomicArc -d:useMalloc`)

```
disjoint-write-only   4w/0r ×200k =>  320k ops/s   (low stripe contention)
disjoint-mixed-rw     4w/4r ×200k =>  640k ops/s
shared-write-only     4w/0r ×200k =>  188k ops/s   (max stripe contention)
shared-mixed-rw       4w/4r ×200k =>  440k ops/s
read-heavy-shared     1w/8r ×200k =>  2.5M ops/s
```

## Geospatial (`nimble bench_geo`)

Raw R-tree (in-memory, no GlenDB):

```
bulkLoad (STR):    100k entries  =>  2.7M entries/s
bulkLoad (STR):     1M entries   =>  2.1M entries/s
insert (Guttman): 100k ops       =>  1.72M ops/s
searchBBox (5°):   50k queries   =>  318k q/s   (100k pts, ~159 hits/query)
searchBBox (5°):    5k queries   =>   30k q/s   (1M pts, ~1594 hits/query)
nearest k=10:      50k queries   =>  111k q/s   (100k pts)
nearest k=10:       5k queries   =>   85k q/s   (1M pts)
nearestGeo k=10:   50k queries   =>   83k q/s   (haversine bbox lower-bound)
```

GlenDB-integrated geo index, eager mode (100k docs):

```
put (no index):                   358k docs/s
createGeoIndex (STR bulk-build):  1.05M docs/s
put (with active geo index):      329k docs/s
findWithinRadius 100km:            89k q/s
findNearest planar k=10:           70k q/s
findNearest geographic k=10:       59k q/s    (~1.2× slower than planar; trig-bound)
```

Polygons, eager mode (50k docs, ~3°-square axis-aligned shapes):

```
put polygons:                    214k docs/s
createPolygonIndex (STR):        962k docs/s
findPolygonsContaining:           70k q/s    (R-tree prefilter + ray-cast)
```

Index persistence:

```
reopen with .gri present:        752 ms      (load + WAL replay)
compact (snapshot + .gri dump):  398 ms
reopen with corrupt .gri:        771 ms      (CRC fail → bulk-rebuild)
```

The compact time reflects v4's per-collection dictionary build (two
pre-passes counting key + `(field, value)` frequencies). On workloads
with significant repetition the disk savings repay this many times over;
on workloads with near-unique values everywhere (the geo bench's lat/lon
floats) it's pure overhead — pass `keyDictThreshold = 0,
valueDictThreshold = 0` to opt out.

## Vector index (HNSW, `tests/test_vectorindex.nim`)

```
recall@10 on 1k 16-d L2 vectors:  ≥ 0.90 (M=16, efC=100, efS=50)
recall@5  on  500 32-d cosine:    ≥ 0.60 (M=16, efC=200, efS=64)
persistence round-trip:           single .vri dump, identical results on reopen
```

Distance metrics: cosine (vectors unit-normalised on insert), L2, dot.
Soft-delete semantics — `unindexDoc` drops the docId mapping but leaves
the graph node; the graph is rebuilt cleanly on `compact()`.

## Time-series (`nimble bench_timeseries`)

Gorilla scalar TSDB, 1M samples per series:

| Value pattern | Append rate | Bits/sample on disk |
|---|---|---|
| Constant | **83M samples/s** | 2.11 |
| Regular cadence | **43M samples/s** | 14.13 |
| Smooth (sin) | **23M samples/s** | 59.60 |
| Noisy | **23M samples/s** | 59.28 |

The 4–5× lift on smooth/noisy data comes from a register-aware
`writeBitsU64` that packs whole bytes per call instead of looping bit-by-bit
through `writeBit`.

```
open  (1M-sample file, 1.7 MB):   <1 ms
open  (10M-sample file, 17 MB):    2 ms
range (random window):           ~10k q/s   (avg 550 samples returned)
range (warm, RW default cache):  >995k q/s  (100-sample windows, RO/RW bench)
range (warm, RO + mmap):         >1.0M q/s
latest n=100:                     ~5.6M q/s
latest n=1000:                    ~877k q/s
```

The headline range-query lift comes from three changes that landed
together:

* `scanBlocks` walks just the 40-byte block headers instead of CRC-ing
  every payload — a 10 GB series opens in seconds (the rest of the per-
  block work moves to read time in `readBlockAt`).
* The decoded-chunk LRU stores `ref DecodedChunk` (parallel
  `seq[int64]` / `seq[float64]`) — a hit is a refcount bump rather than
  a 64 KB deep copy. Within-chunk lookups binary-search the timestamp
  column.
* Read-only series additionally `mmap` the file; per-block reads are
  `copyMem` from the mapped region, no syscalls.

Sharded time-series (`glen/sharded`, time-bucketed by day):

```
append (100 samples/day × 365 days, daily shards):  ~580k samples/s
                                                    (365 shards on disk)
dropBefore (60 days × 1000/day, drop 59 days):      ~3 ms total
                                                    (constant-time per shard:
                                                     close handle + removeFile)
```

Sharded retention scales with **shard count**, not file size. The
equivalent monolithic `dropBlocksBefore` is bounded by the surviving
data volume — fast on small files, painful at 10 GB. The append rate is
lower than monolithic because each shard takes a separate file flush;
batches that stay within one shard see throughput closer to the
monolithic numbers.

`rangeIn(bbox, t0, t1)` only opens shards whose geohash cell intersects
the bbox **and** whose time bucket overlaps `[t0, t1]`, so query cost
scales with the queried slice, not the archive.

Tile time-stack — append, warm-cache reads (default `fillRadarFrame`):

| Geometry | Append | Compression | bits/cell | Point-history | readFrame |
|---|---|---|---|---|---|
| 200×200, 200 frames | **4,255 frames/s** | **79.5×** | 0.81 | 1,255 q/s | 371 q/s |
| 512×512, 64 frames | **736 frames/s** | **106×** | 0.60 | 912 q/s | 111 q/s |

The compression jump (24.9× → 79.5× on 200×200; 21.2× → 106× on 512×512)
comes from the constant-chunk RLE path: chunks where every cell stays
identical across all frames now collapse to "one float64 + timestamps"
instead of full per-cell Gorilla streams.

Tile time-stack — sparse vs dense cold-decode (cache reopened per query):

| Workload | Sparse (radar, 99.5% zero cells) | Dense (every cell varies) |
|---|---|---|
| readFrame (cold) | **181 q/s** | 6 q/s |
| readPointHistory (cold) | **678 q/s** | 32 q/s |
| Disk size | 805 KB (80× compression) | 59.8 MB (1.1× compression) |
| Append throughput | 3,333 fps | 105 fps |

The 22–27× spread between sparse and dense is `decodeXorRun`'s clz
zero-run-skip earning its keep — bulk-skipping runs of zero-XOR cells in
one instruction instead of per-bit reads.

## Snapshot v4 — paged on-disk doc index + dictionaries

```
v3 → v4 keys-only file size:    1000-doc structured collection saves ~5–10%
v4 + value-dict size:           additional ~5–10% on enum-heavy fields
open (just the 56-B header + dict): 0.05 ms for 100k docs
random lookup rate:             ~250k q/s (mmap + binary search + dict resolve)
resident index RAM at open:     0 bytes (index lives on OS page cache)
```

v4 carries an optional pair of per-snapshot dictionaries — keys (every
object-field name across the collection) and per-field string values
(status enums, region codes). Both are recomputed on every `compact()`.
v3 readers reject v4 (different magic); v4 readers handle v3 transparently.

Layout details, threshold knobs, and back-compat in
[storage.md#snapshot-v4](storage.md#snapshot-v4--key--value-dictionaries).

## Tradeoffs

### Mode tradeoffs

| Workload | Best mode | Why |
|---|---|---|
| Working set fits in RAM | eager | hot reads via `cs.docs`; no fault path |
| Dataset > RAM | spillable | only mode that works at all |
| Query touches small fraction | spillable + `hotDocCap` | predictable memory floor |
| Large bulk reads | streaming iterators (`*Stream`) | one Value at a time |

### Index tradeoffs

| Workload | Best index | Why |
|---|---|---|
| Equality on a field | `createIndex` | CritBitTree-backed; O(log n) maintenance |
| Range scans on a single sorted field | `createIndex` (rangeable) | same backing |
| Spatial point queries | `createGeoIndex` | R-tree, STR bulk-load, KNN best-first |
| "Which zone contains this point?" | `createPolygonIndex` | bbox prefilter + ray-cast |
| Vector / embedding NN search | `createVectorIndex` | HNSW; `.vri` graph dump on compact |

### Storage engine tradeoffs

| Workload | Engine |
|---|---|
| Heterogeneous documents | `glen/db` (core) |
| Single-stream metric / sensor / float values | `glen/timeseries` (Gorilla) |
| Multi-billion-sample stream with retention or bbox filters | `glen/sharded` (time + geo partitioning) |
| Dense raster pinned to a bbox | `glen/geomesh` (in a doc) |
| Raster that evolves through time (radar / weather) | `glen/tilestack` |
| Embeddings (per-doc field) | `glen/linalg` (Vector inside a doc) |
| Approximate KNN over embeddings (collection-wide) | `createVectorIndex` (HNSW) |

## Reproducing

The `tests/` directory contains all the benches:

```
nimble bench_release        # core CRUD
nimble bench_concurrent     # multi-threaded contention (atomicArc + -d:useMalloc)
nimble bench_geo            # R-tree, geo, polygon, persistence
nimble bench_timeseries     # Gorilla TSDB + tilestack, sparse-vs-dense
nimble bench_bitpack        # BitReader / clz / Simple-8b microbench
```

The v3/v4 numbers come from stress tests in `tests/test_snapshot_v3.nim`
and `tests/test_snapshot_v4.nim`, both part of `nimble test`.

## What's not optimized (yet)

- **AVX-512 PEXT/PDEP path for `decodeXor`/`decodeDoD`** — variable-length
  prefix codes; requires careful CPUID feature-check + scalar fallback.
  Untestable on this arm64 dev box.
- **Per-cell offset table in tile chunks** — would let `readPointHistory`
  decode just the queried cell's stream instead of the full chunk. Needs a
  chunk format change.
- **zstd compression for snapshots and tile chunks** — pure-Nim true zstd
  doesn't exist; gating optional dep behind `-d:glenZstd` is the obvious
  path but the policy decision is open.
- **Encryption at rest** — `nimcrypto` (pure-Nim) covers AES-GCM +
  ChaCha20-Poly1305 + Argon2id. Threat-model + key-management design
  not yet drafted.
- **TSDB block format extension to use Simple-8b for timestamps** — the
  codec exists and is wired into `tilestack`; TSDB blocks still use the
  legacy interleaved DoD-and-XOR layout, which would need restructuring.
- **Persistent block-index sidecar for `glen/timeseries`** — `scanBlocks`
  now walks only headers (cheap), but a `<name>.gtx` index dumped on
  flush would skip even that on open. Useful for billion-sample series
  that are reopened often.
- **Sharded `latest(n)` / iteration helpers** — `glen/sharded` exposes
  `range` / `rangeIn` / `dropBefore` but not `latest` or `items`; today
  callers fan out manually via `shardKeysOnDisk()`.

None of these are blockers for current workloads; they're constant-factor
improvements where you'd notice.
