# Glen

A document database for Nim that runs **inside your app**. No daemon to
launch, no port to open, no schema to declare up front — open a directory
and start writing documents. Glen is good at the tricky things most
embedded databases punt on: time-series, geographic data, raster grids,
and vector search.

```nim
import glen/glen

let db = newGlenDB("./mydb")
db.put("users", "u1", %*{"name": "Alice", "age": 30})
echo db.get("users", "u1")

# Declarative schema, validator, and indexes in one block
schema users:
  fields:
    name:  zString().trim().minLen(2)
    age:   zInt().gte(0).lte(150)
    email: zString().trim().minLen(3)
  indexes:
    byEmail: equality "email"

registerUsersSchema(db)

# Block query DSL with native Nim operators
let admins = query(db, "users"):
  where:
    role == "admin"
    age >= 30
  orderBy: name asc
  limit: 10

# Transactions with automatic conflict retry
let res = txn(db, retries = 3):
  let acc = txn.get("accounts", srcId)
  txn.put("accounts", srcId, %*{"balance": acc["balance"].i - amount})
```

The verbose procedural API (`VObject() / v["k"] = VString(..)`,
`q.whereEq(..).run()`) is still available for fine-grained control — see
**[DSL guide](docs/dsl.md)** for the full mapping.

> **Status:** beta (0.5.0). All on-disk formats are versioned and
> backwards-compatible — old data keeps working when you upgrade.
> Expect minor API churn until 1.0.

## When Glen is the right pick

- **You're building a desktop app, CLI tool, or edge device** that needs
  real persistence without standing up a separate server. Glen lives in
  your process; closing the app closes the database.
- **You're storing a lot of numbers that change over time** — sensor
  readings, metric streams, financial ticks. Glen's time-series engine
  packs them down to a few bits per sample when values are smooth or
  repeat.
- **You're working with geographic data** — points, polygons, "what's
  near here," "which zone is this in." Spatial indexes are built in,
  with both flat-plane and real-Earth (great-circle) distance.
- **You're storing raster data over time** — weather model output,
  radar sweeps, animated heatmaps. Glen has a dedicated engine for
  compressing rasters that evolve frame by frame, often **80–100×
  smaller** than raw on sparse fields, and you can query a single
  pixel's history without decoding the whole grid.
- **You're using embeddings** for semantic search or recommendations.
  Glen has built-in approximate nearest-neighbour search (HNSW) so you
  don't need to bolt on a separate vector database.
- **Your dataset is larger than RAM.** Glen can keep most of it on
  disk and pull only what you query into memory, with no manual tuning.
- **You want multiple devices to sync** — Glen has multi-master
  replication built in, with last-writer-wins conflict resolution.

It's deliberately **not** a network database, a SQL engine, or a
multi-region primary store with strong global consistency. It's a
library you embed, that happens to be unusually good at numbers,
geography, and vectors.

## What's inside

| | |
|---|---|
| **Documents** | Schema-flexible document model — store any nested JSON-ish value. Crash-safe writes, transactions, change subscriptions, optional schema validation |
| **Queries** | Chainable filter API — `where` / `orderBy` / `limit` / cursor-based pagination. Indexes used automatically when available |
| **Indexes** | Equality and range lookups, spatial (points + polygons), and approximate vector / nearest-neighbour search |
| **Time-series** | Compact storage for streams of (timestamp, value) and for grids that evolve over time. Range queries, point-in-time history, last-N reads |
| **Math** | Vectors, matrices, basic linear algebra; raster meshes pinned to a geographic bounding box |
| **Sync** | Multi-master replication with conflict resolution. Each device has a stable identity; cursors track per-peer progress |
| **Scale** | Datasets larger than RAM via memory-mapped storage. Streaming iterators for bulk reads that don't blow up memory |
| **DSL** | `%*` value literals, `query` / `txn` / `schema` / `watch` / `sync` blocks, and a `db["users"]` collection proxy that supports `for id, doc in users` iteration |

Pure Nim ≥ 1.6. No external runtime dependencies.

## Install

```
nimble install https://github.com/oshaulz/glen
```

or in a `.nimble` file:

```nim
requires "https://github.com/oshaulz/glen >= 0.5.0"
```

## Documentation

New here? Start with:

- **[Getting started](docs/getting-started.md)** — install, your first
  database, common patterns
- **[Architecture](docs/architecture.md)** — how the pieces fit together

Going deeper:

- **[Storage and recovery](docs/storage.md)** — how data gets written
  to disk and survives crashes
- **[Concurrency model](docs/concurrency.md)** — how Glen handles
  parallel reads and writes safely
- **[Spillable mode](docs/spillable-mode.md)** — for datasets larger
  than RAM
- **[Performance](docs/performance.md)** — benchmarks and tradeoffs

API reference (you'll want these once you start writing code):

- **[DSL guide](docs/dsl.md)** — `%*` literals, `query`, `txn`,
  `schema`, `watch`, `sync`, `Collection` proxy
- **[Core](docs/api/core.md)** — basic CRUD, transactions, indexes,
  queries, subscriptions, replication
- **[Spatial](docs/api/spatial.md)** — geographic points and polygons
- **[Time-series](docs/api/timeseries.md)** — sensor streams and rasters
- **[Numeric](docs/api/numeric.md)** — vectors and matrices

For a deeper architectural read, see [WHITEPAPER.md](WHITEPAPER.md).
For contributors, see [AGENT_GUIDE.md](AGENT_GUIDE.md).

## License

MIT. See [LICENSE](LICENSE).
