# Glen

An embedded document database for Nim with first-class spatial, temporal, and
numeric primitives — durable, concurrent, in-process.

```nim
import glen/db, glen/types

let db = newGlenDB("./mydb")
var alice = VObject()
alice["name"] = VString("Alice")
db.put("users", "u1", alice)
echo db.get("users", "u1")
```

> **Status:** beta (0.5.0). On-disk formats are versioned (WAL v2; snapshot v4
> with v1/v2/v3 read-back; GRI/GPI v1; VRI v1; GTS v1; TTS v2 with v1 read-back).
> Expect minor API churn until 1.0.

## What Glen is very good at

- **Time-series of dense numeric data.** Gorilla-encoded scalar TSDB hits
  100M samples/s appends on constant streams, and cold random-window
  range queries run at ~8.5k q/s on 1M-sample series. `bits/sample`
  drops to ~2 on flat values, ~14 on regular-cadence counters.
- **Raster-over-time** — radar reflectivity, gridded weather model
  output, animated heatmaps. The tile time-stack engine compresses
  sparse fields **80–106×** vs raw and decodes a single cell's history
  out of a chunk without touching the rest.
- **Spatial queries** — R-tree with STR bulk-load (2.4M entries/s), KNN
  with planar or geographic (haversine) metrics, polygon point-in.
  Indexes survive restarts via binary `.gri` / `.gpi` dumps.
- **Approximate vector search** — HNSW index over a per-document
  embedding field, with cosine / L2 / dot metrics. Cosine vectors are
  unit-normalised on insert; the graph dumps to `.vri` on `compact()`
  so reopen doesn't re-insert.
- **Embedded use cases.** Local desktop apps, edge devices, single-process
  services, CLI tools that need real persistence. No daemon, no network,
  no auth surface — `db.close()` and the directory is the whole state.
- **Datasets bigger than RAM.** Spillable mode mmaps the snapshot's
  paged on-disk index; lookups page in the relevant body and nothing
  else. 100k-doc snapshots open in 0.05 ms with 0 bytes of resident
  index RAM.
- **Mixed-schema document workloads.** Snapshot v4's per-collection key
  + value dictionaries auto-compress repeated field names and
  low-cardinality enums (status / region / severity codes) — typically
  5–15% smaller files on structured records, with rare keys staying
  inline so unstructured docs aren't penalised.

It's deliberately **not** trying to be a network database, a SQL engine,
an OLAP warehouse, or a multi-region primary store with strong global
consistency. It's a single-process embedded library that happens to
compress timeseries, radar, vectors, and structured documents
unusually well.

## What's inside

| | |
|---|---|
| **Core** | Document model + WAL + snapshots, optimistic transactions, sharded LRU cache, striped per-collection RW-locks, multi-master replication (HLC, LWW), schema validation, auto-compaction triggers |
| **Querying** | Method-chain query layer with predicate filters, orderBy, limit, opaque cursor pagination; planner picks single-field indexes when available, full-scan fallback otherwise |
| **Indexes** | Equality + range (CritBitTree), geo R-tree (planar or haversine KNN, polygon point-in), HNSW vector index with cosine / L2 / dot metrics |
| **Temporal** | Gorilla-encoded scalar time-series engine; tile time-stacks for compressed raster-over-time storage with constant-chunk RLE + Simple-8b timestamp codec |
| **Numeric** | Vector + Matrix primitives, GeoMesh (raster pinned to a bbox) |
| **Storage** | Snapshot v4 with per-collection key + value dictionaries (typically 5–15% smaller files on structured / enum-heavy data); paged on-disk doc index for spillable mode |
| **Scale** | Spillable mode with mmap'd snapshot — datasets bigger than RAM; streaming iterators for bounded-memory bulk reads |

No external runtime dependencies. Pure Nim ≥ 1.6.

## Install

```
nimble install https://github.com/oshaulz/glen
```

or in a `.nimble` file:

```nim
requires "https://github.com/oshaulz/glen >= 0.5.0"
```

## Documentation

Start here:

- **[Getting started](docs/getting-started.md)** — install, basic usage, common patterns
- **[Architecture](docs/architecture.md)** — system overview, components, data flow

Topical:

- **[Storage and recovery](docs/storage.md)** — WAL, snapshot formats (v1/v2/v3)
- **[Concurrency model](docs/concurrency.md)** — striped RW locks, optimistic transactions, multi-master replication
- **[Spillable mode](docs/spillable-mode.md)** — datasets bigger than RAM, streaming iterators, paged on-disk index
- **[Performance](docs/performance.md)** — benchmarks, tradeoffs, what to expect

API reference:

- **[Core](docs/api/core.md)** — `db.put` / `get` / `delete`, batch ops, transactions, equality / range indexes, subscriptions, replication, schema validation
- **[Spatial](docs/api/spatial.md)** — geo points (R-tree), polygons, GeoMesh
- **[Time-series](docs/api/timeseries.md)** — Gorilla TSDB, tile time-stacks
- **[Numeric](docs/api/numeric.md)** — `Vector`, `Matrix`, ops

For an architectural deep-dive, see [WHITEPAPER.md](WHITEPAPER.md).
For agent / contributor guidance, see [AGENT_GUIDE.md](AGENT_GUIDE.md).

## License

MIT. See [LICENSE](LICENSE).
