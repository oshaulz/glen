# Performance

Apple M5, `-d:release`, ORC + `-O3`. Reproduce with the corresponding
`nimble` task; All values should be treated as the order of magnitude you'll see, not exact constants.

## Core CRUD (`nimble bench_release`)

```
puts:                320k ops/s
gets (cloned):       1.35M ops/s
gets (borrowed):     9.1M ops/s
getMany:             20k batches/s   ×100 docs ≈ 2.0M doc reads/s
txn commits:         333k ops/s
```

The borrowed-read path skips the defensive `clone()` and is appropriate for
read-only hot loops.

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
bulkLoad (STR):    100k entries  =>  2.6M entries/s
bulkLoad (STR):     1M entries   =>  2.2M entries/s
insert (Guttman): 100k ops       =>  1.75M ops/s
searchBBox (5°):   50k queries   =>  325k q/s   (100k pts, ~159 hits/query)
searchBBox (5°):    5k queries   =>   31k q/s   (1M pts, ~1594 hits/query)
nearest k=10:      50k queries   =>  112k q/s   (100k pts)
nearest k=10:       5k queries   =>   82k q/s   (1M pts)
nearestGeo k=10:   50k queries   =>   85k q/s   (haversine bbox lower-bound)
```

GlenDB-integrated geo index, eager mode (100k docs):

```
put (no index):                   334k docs/s
createGeoIndex (STR bulk-build):  1.03M docs/s
put (with active geo index):      312k docs/s
findWithinRadius 100km:            84k q/s
findNearest planar k=10:           67k q/s
findNearest geographic k=10:       57k q/s    (~1.2× slower than planar; trig-bound)
```

Polygons, eager mode (50k docs, ~3°-square axis-aligned shapes):

```
put polygons:                    181k docs/s
createPolygonIndex (STR):        847k docs/s
findPolygonsContaining:           63k q/s    (R-tree prefilter + ray-cast)
```

Index persistence:

```
reopen with .gri present:        865 ms      (load + WAL replay)
compact (snapshot + .gri dump):  433 ms
reopen with corrupt .gri:        720 ms      (CRC fail → bulk-rebuild)
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
| Constant | **100M samples/s** | 2.11 |
| Regular cadence | **31.3M samples/s** | 14.13 |
| Smooth (sin) | 4.65M samples/s | 59.60 |
| Noisy | 4.72M samples/s | 59.28 |

```
open (scan all chunk headers):  10 ms for 1M-sample file
range (random window):          8.2k q/s    (avg 550 samples returned)  ← was 1.3k pre-bitpack
latest n=100:                   855k q/s
latest n=1000:                  108k q/s
```

The **6.3× boost on `range`** vs the pre-bitpack baseline comes from the
batched 64-bit `BitReader` + hardware `clz`/`ctz` (`__builtin_clzll` /
`__builtin_ctzll`) replacing scalar bit-shift loops. See
[storage.md](storage.md#bit-decode-hot-path).

Tile time-stack — append, warm-cache reads (default `fillRadarFrame`):

| Geometry | Append | Compression | bits/cell | Point-history | readFrame |
|---|---|---|---|---|---|
| 200×200, 200 frames | **4,255 frames/s** | **79.5×** | 0.81 | 1,263 q/s | 372 q/s |
| 512×512, 64 frames | **736 frames/s** | **106×** | 0.60 | 918 q/s | 113 q/s |

The compression jump (24.9× → 79.5× on 200×200; 21.2× → 106× on 512×512)
comes from the constant-chunk RLE path: chunks where every cell stays
identical across all frames now collapse to "one float64 + timestamps"
instead of full per-cell Gorilla streams.

Tile time-stack — sparse vs dense cold-decode (cache reopened per query):

| Workload | Sparse (radar, 99.5% zero cells) | Dense (every cell varies) |
|---|---|---|
| readFrame (cold) | **188 q/s** | 7 q/s |
| readPointHistory (cold) | **680 q/s** | 33 q/s |
| Disk size | 805 KB (80× compression) | 59.8 MB (1.1× compression) |
| Append throughput | 4,444 fps | 110 fps |

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

None of these are blockers for current workloads; they're constant-factor
improvements where you'd notice.
