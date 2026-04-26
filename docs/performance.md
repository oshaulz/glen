# Performance

Apple M5, `-d:release`, ORC + `-O3`. Reproduce with the corresponding
`nimble` task; numbers are best-of-a-few-runs for stability. All values
should be treated as the order of magnitude you'll see, not exact constants.

## Core CRUD (`nimble bench_release`)

```
puts:                270k ops/s
gets (cloned):       1.67M ops/s
gets (borrowed):     20M ops/s
getMany:             28k batches/s  ×100 docs ≈ 2.8M doc reads/s
txn commits:         333k ops/s
```

The borrowed-read path skips the defensive `clone()` and is appropriate for
read-only hot loops.

## Multi-threaded contention (`nimble bench_concurrent`, `--mm:atomicArc -d:useMalloc`)

```
disjoint-write-only   4w/0r ×50k =>  214k ops/s   (low stripe contention)
disjoint-mixed-rw     4w/4r ×50k =>  426k ops/s
shared-write-only     4w/0r ×50k =>  174k ops/s   (max stripe contention)
shared-mixed-rw       4w/4r ×50k =>  406k ops/s
read-heavy-shared     1w/8r ×50k =>  2.1M ops/s
```

## Geospatial (`nimble bench_geo`)

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

GlenDB-integrated geo index, eager mode (100k docs):

```
put (no index):                   238k docs/s
createGeoIndex (STR bulk-build):  813k docs/s
put (with active geo index):      211k docs/s
findWithinRadius 100km:            81k q/s
findNearest planar k=10:           64k q/s
findNearest geographic k=10:       28k q/s    (~2× slower than planar; trig-bound)
```

Polygons, eager mode (50k docs, ~3°-square axis-aligned shapes):

```
put polygons:                    127k docs/s
createPolygonIndex (STR):        893k docs/s
findPolygonsContaining:           48k q/s    (R-tree prefilter + ray-cast)
```

Index persistence:

```
reopen with .gri present:        866 ms      (load + WAL replay)
compact (snapshot + .gri dump):  200 ms
reopen with corrupt .gri:        475 ms      (CRC fail → bulk-rebuild)
```

## Time-series (`nimble bench_timeseries`)

Gorilla scalar TSDB, 1M samples per series:

| Value pattern | Append rate | Bits/sample on disk |
|---|---|---|
| Constant | **100M samples/s** | 2.11 |
| Regular cadence | 19M samples/s | 14.13 |
| Smooth (sin) | 4.4M samples/s | 59.60 |
| Noisy | 4.5M samples/s | 59.28 |

```
open (scan all chunk headers):  9 ms for 1M-sample file
range (random window):          1.3k q/s    (avg 546 samples returned)
latest n=100:                   901k q/s    ← 60× vs no chunk cache
latest n=1000:                  115k q/s    ← 115× vs no chunk cache
```

Tile time-stack (radar-shaped sparse field):

| Geometry | Append | Compression | bits/cell | Point-history | readFrame |
|---|---|---|---|---|---|
| 200×200, 200 frames | 3125 frames/s | **24.9×** | 2.57 | **1166 q/s** | **362 q/s** |
| 512×512, 64 frames | 432 frames/s | 21.2× | 3.02 | **844 q/s** | **94 q/s** |

The decoded-chunk LRU (`glen/chunkcache`) drives the read-side wins.

## Snapshot v3 — paged on-disk doc index (`tests/test_snapshot_v3.nim`)

```
v3 snapshot file size:         ~3.9 MB     (100k docs; ~36 bytes/doc + bodies)
open (just the 40-B header):   0.03 ms     ← O(1), independent of doc count
random lookup rate:            2.3M q/s    (mmap + binary search)
resident index RAM at open:    0 bytes     ← OS page cache only
```

v2 vs v3 comparison for the cold-snapshot-lookup path:

| Metric | v2 (in-memory) | v3 (paged mmap) |
|---|---|---|
| Open time, 100k docs | hundreds of ms | **0.03 ms** |
| Resident index footprint | ~36 bytes/doc | **0 bytes** |
| Cold lookup latency | ~100 ns | ~430 ns |
| Iteration order | insertion | sorted lex |
| Maximum DB size | bounded by RAM | bounded by disk |

For workloads that read mostly through `cs.docs` / `db.cache` (the hot path),
v2 and v3 are indistinguishable. v3 trades 4–5× higher cold-fault latency for
sub-ms opens and removed RAM ceiling.

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
| Vector / embedding NN search | (not yet) | future: HNSW or IVF |

### Storage engine tradeoffs

| Workload | Engine |
|---|---|
| Heterogeneous documents | `glen/db` (core) |
| Single-stream metric / sensor / float values | `glen/timeseries` (Gorilla) |
| Dense raster pinned to a bbox | `glen/geomesh` (in a doc) |
| Raster that evolves through time (radar / weather) | `glen/tilestack` |
| Embeddings | `glen/linalg` (Vector inside a doc) |

## Reproducing

The `tests/` directory contains all the benches:

```
nimble bench_release        # core CRUD
nimble bench_concurrent     # multi-threaded contention (atomicArc + -d:useMalloc)
nimble bench_geo            # R-tree, geo, polygon, persistence
nimble bench_timeseries     # Gorilla TSDB + tilestack
```

The v3 numbers come from the stress test in `tests/test_snapshot_v3.nim`,
which is part of `nimble test`.

## What's not optimized (yet)

- **SIMD bit-decode** — Gorilla unpack runs scalar; AVX-512 PEXT/PDEP or
  ARM NEON could push it 5–10×. Platform-specific work; not yet shipped.
- **Parallel replication export** — `exportChanges` runs single-threaded
  under `replLock`. Refactorable, not yet done.
- **Per-cell offset table in tile chunks** — would let `readPointHistory`
  decode just the relevant cell streams instead of full chunks.
- **Bilinear interpolation in `GeoMesh.sampleAt`** — currently nearest cell.

These are tracked in the [README roadmap](../README.md). None are blockers
for current workloads; they're constant-factor improvements where you'd notice.
