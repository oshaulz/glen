# Glen DSL

A set of small Nim macros and helpers that wrap Glen's procedural API in
denser, more declarative syntax. Every DSL form compiles down to existing
`db.put` / `db.query` / `db.beginTxn` / `db.subscribe` / etc. calls — you
can mix DSL and procedural code freely, and you can always read the
generated AST with `expandMacros`.

```nim
import glen/glen          # everything: DSL and procedural API
# or, fine-grained:
import glen/dsl           # just the DSL surface
```

## Quick tour

```nim
import glen/glen

let db = newGlenDB("./mydb")

# Schema, validator, and indexes in one block
glenSchema users:
  fields:
    name:  zString().trim().minLen(2).maxLen(64)
    age:   zInt().gte(0).lte(150)
    email: zString().trim().minLen(3)
    role:  zEnum(["admin", "member"]).default("member")
  indexes:
    byEmail: equality "email"
    byAge:   range    "age"

registerUsersSchema(db)

# Collection proxy + JSON-ish literals
let users = db["users"]
users.put("u1", %*{"name": "Alice", "age": 30, "email": "alice@example.com"})

# Block query with native operators
let admins = glenQuery(db, "users"):
  where:
    role == "admin"
    age >= 30
  orderBy: name asc
  limit: 10

# Transaction with auto-retry on conflict
let res = glenTxn(db, retries = 3):
  let acc = txn.get("accounts", srcId)
  txn.put("accounts", srcId, %*{"balance": acc["balance"].i - amount})

# Reactive subscriptions, all closed together
let scope = glenWatch(db):
  doc "users", "u1":
    echo $id, " -> ", $newValue
  collection "users":
    audit.log(id)
defer: scope.close()
```

## `%*` — Value literals

Build a `Value` from a JSON-ish literal. Mirrors `std/json`'s `%*`.

```nim
let u = %*{
  "name": "Alice",
  "age": 30,
  "tags": ["admin", "ops"],
  "addr": { "city": "NYC", "zip": 10001 }
}
```

| Syntax           | Result                                     |
|------------------|--------------------------------------------|
| `%*"abc"`        | `VString("abc")`                           |
| `%*42`           | `VInt(42)`                                 |
| `%*3.14`         | `VFloat(3.14)`                             |
| `%*true`         | `VBool(true)`                              |
| `%*nil`          | `VNull()`                                  |
| `%*[a, b, c]`    | `VArray(@[a, b, c])`                       |
| `%*{"k": v}`     | `VObject` with field assignments           |

For non-literal expressions (variables, function calls), the macro emits a
runtime `toValue(expr)` call. Built-in `toValue` overloads cover `string`,
`bool`, `SomeInteger`, `SomeFloat`, `seq[byte]`, `seq[T]`, `openArray[T]`,
and `Value` (identity). Add your own:

```nim
proc toValue*(p: Point): Value =
  %*{"x": p.x, "y": p.y}
```

Object keys may be string literals (`"name":`) or bare identifiers
(`name:`). Both produce the same key string.

## Collection proxy

`db["users"]` returns a thin `Collection` handle that forwards to the
underlying `db.put` / `db.get` / `db.delete` / `getBorrowedAllStream`
procs. Iteration uses Glen's borrowed-stream so it stays bounded-memory in
spillable mode.

```nim
let users = db["users"]
users.put("u1", %*{"name": "Alice"})
echo users.get("u1")
"u1" in users         # contains
for id, doc in users: # streams, doesn't materialise
  process(doc)
users.createIndex("byEmail", "email")
```

The proxy is value-typed, so `let users = db["users"]` is essentially free
— it's just a `(GlenDB, string)` pair.

## `glenQuery:` — query block

Block syntax over the `query / whereEq / orderByField / limitN` builder.
Predicates use natural Nim operators on dotted-path identifiers.

```nim
let active = glenQuery(db, "users"):
  where:
    status == "active"
    age >= 30
    role in ["admin", "member"]
    name.contains("li")
    addr.city != "Brooklyn"   # dotted paths supported
  orderBy:
    age desc
    name asc
  limit: 50
  after: prevCursor
```

| Operator                | Builder call              |
|-------------------------|---------------------------|
| `field == v`            | `whereEq(field, toValue(v))`        |
| `field != v`            | `whereNe(field, toValue(v))`        |
| `field <  v`            | `whereLt(field, toValue(v))`        |
| `field <= v`            | `whereLte(field, toValue(v))`       |
| `field >  v`            | `whereGt(field, toValue(v))`        |
| `field >= v`            | `whereGte(field, toValue(v))`       |
| `field in [a, b, c]`    | `whereIn(field, @[toValue(a), …])`  |
| `field.contains("xy")`  | `whereContains(field, "xy")`        |

### Sections

| Section    | Meaning                                                        |
|------------|----------------------------------------------------------------|
| `where:`   | Conjunction of predicates (one per line)                       |
| `orderBy:` | One or more `field [asc|desc]` lines (default `asc`)           |
| `limit: N` | Page size (`0` = unlimited)                                    |
| `after: c` | Resume from opaque cursor returned by `nextCursor(rows)`       |

### Returning a builder instead of running

```nim
var q = glenQueryBuilder(db, "users"):
  where:
    age >= 30
for id, doc in q.runStream():
  process(doc)
```

### Geo and vector

Index-aware searches don't fit the predicate algebra cleanly — they
depend on a named index, not a field comparison — so they're exposed as
direct procs, not sections inside `where:`:

```nim
# point index "byLoc" already created with createGeoIndex(...)
for (id, distMeters, doc) in db.near("stores", "byLoc",
                                     lon = -73.98, lat = 40.75,
                                     radiusMeters = 2000.0):
  echo id, " is ", distMeters, " m away"

let knn = db.nearest("stores", "byLoc", -73.98, 40.75, k = 10)

let bbox = db.inBBox("stores", "byLoc",
                     -74.0, 40.7, -73.9, 40.8)

# vector index "byEmbedding" already created with createVectorIndex(...)
let similar = db.nearestVector("docs", "byEmbedding", queryVec, k = 10)
```

## `glenTxn:` — transactions with retries

```nim
let res = glenTxn(db, retries = 3):
  let src = txn.get("accounts", srcId)
  let dst = txn.get("accounts", dstId)
  txn.put("accounts", srcId, %*{"balance": src["balance"].i - amount})
  txn.put("accounts", dstId, %*{"balance": dst["balance"].i + amount})

if not res.ok:
  echo "transfer failed: ", res.message
```

The `txn` helper is injected into the body and exposes:

| Proc                                       | Behaviour                                    |
|--------------------------------------------|----------------------------------------------|
| `txn.get(coll, id) -> Value`               | Records read version for OCC                 |
| `txn.getMany(coll, ids)`                   | Same, batched                                |
| `txn.getAll(coll)`                         | Same, full collection                        |
| `txn.put(coll, id, value)`                 | Stages a put                                 |
| `txn.delete(coll, id)`                     | Stages a delete                              |

### Retry semantics

| Outcome                | Behaviour                                         |
|------------------------|---------------------------------------------------|
| `csOk`                 | Loop exits, returns the result                    |
| `csConflict`           | Body re-runs (up to `retries` more times)         |
| `csInvalid`            | Loop exits, returns the result                    |
| Exception in body      | Caught; surfaces as `csInvalid` with the message  |

A bare `glenTxn(db): body` form runs once with no retries.

## `glenSchema:` — collection schema, validator, and indexes

```nim
glenSchema users:
  fields:
    name:  zString().trim().minLen(2).maxLen(64)
    age:   zInt().gte(0).lte(150)
    email: zString().trim().minLen(3)
    role:  zEnum(["admin", "member"]).default("member")
  indexes:
    byEmail:  equality "email"
    byAge:    range    "age"
    byLoc:    geo      "addr.lon", "addr.lat"
    region:   polygon  "regionShape"
    byEmbed:  vector   "embedding", 384, vmCosine
```

Generates four symbols, all prefixed with the schema name:

| Symbol                      | Purpose                                   |
|-----------------------------|-------------------------------------------|
| `usersCollection: string`   | The collection name (`"users"` here)      |
| `usersSchema: Schema[…]`    | The validator (uses the existing `zobject`) |
| `registerUsersSchema(db)`   | Creates every declared index on `db`       |
| `validateUsers(v) -> ValidationResult[…]` | Thin alias over `usersSchema.parse(v)` |

Index kinds:

| Kind                              | Notes                                                |
|-----------------------------------|------------------------------------------------------|
| `equality "field"`                | Equality + range scans (single-field indexes are rangeable in current Glen) |
| `range "field"`                   | Same engine as `equality`; intent marker             |
| `geo "lonField", "latField"`      | R-tree over a (lon, lat) pair                         |
| `polygon "field"`                 | R-tree over polygon MBRs                              |
| `vector "field", dim [, metric]`  | HNSW; `metric` defaults to `vmCosine`                 |

`registerUsersSchema(db)` is idempotent: indexes that already exist (per
the on-disk manifest) are not rebuilt.

## `glenWatch:` — declarative subscriptions

```nim
let scope = glenWatch(db):
  doc "users", "u1":
    echo $id, " -> ", $newValue

  field "users", "u1", "email":
    audit.email(id, oldValue, newValue)

  fieldDelta "posts", "p1", "body":
    echo "delta event: ", $deltaEvent

  collection "users":
    statsd.increment("users.changes")

# later
scope.close()    # unsubscribes everything
```

| Handler                          | Injected names in body                          |
|----------------------------------|-------------------------------------------------|
| `doc coll, docId`                | `id: Id`, `newValue: Value`                     |
| `field coll, docId, fieldPath`   | `id: Id`, `path: string`, `oldValue, newValue: Value` |
| `fieldDelta coll, docId, fieldPath` | `id: Id`, `path: string`, `deltaEvent: Value` (kind ∈ "set"/"delete"/"append"/"replace") |
| `collection coll`                | `id: Id`, `newValue: Value` — fires for any docId in the collection |

`collection` is a wildcard: every put or delete in the named collection
fires the handler. Useful for audit logs, cache invalidation, derived
data, and stats counters.

The returned `WatchScope` collects every handle. `scope.close()` is
idempotent.

## `glenSync:` — replication supervisor

A small wrapper over `exportChanges` / `applyChanges` / `setPeerCursor`
that lets you declare your replication topology in one place.

```nim
let sync = glenSync(db):
  peer "node-b":
    transport: httpTransport          # SyncTransport ref you provide
    intervalMs: 5000
    collections: ["users", "posts"]   # @[] = all collections
    direction: sdBidirectional        # or sdPushOnly / sdPullOnly

# Drive from your event loop or timer
while running:
  sync.tick()
```

A `SyncTransport` is anything implementing the two-way exchange:

```nim
method exchange*(t: MyTransport;
                 outgoing: seq[ReplChange]): seq[ReplChange] =
  myWire.send(outgoing)
  myWire.receive()
```

The supervisor keeps per-peer cursors and calls `setPeerCursor` so the
in-memory replication log can be GC'd. An `InMemoryTransport` is provided
for tests and demos:

```nim
let tA = InMemoryTransport()
let tB = InMemoryTransport()
tA.peerOf = tB; tB.peerOf = tA
```

`tickAll()` forces every peer to exchange immediately, ignoring intervals
— useful for shutdown drains and test assertions.

## Mixing DSL and procedural code

The DSL is purely additive. Every DSL block expands to a sequence of
calls into the existing API, so:

* You can use `%*` literals everywhere `Value` is expected — including
  inside `db.put`, `txn.put`, vector queries, etc.
* You can chain `db.query(...).whereEq(...).run()` next to a
  `glenQuery:` block in the same module.
* You can register schema indexes via `registerUsersSchema(db)` and
  still call `db.dropIndex` / `db.createIndex` directly.

Run `expandMacros: glenQuery(db, "users"): ...` to see the desugared
form when debugging.

## Versioning

The DSL ships under `glen/dsl/*` and re-exports through `glen/glen`.
DSL macros are part of the 0.5+ public API; their grammar is stable but
may grow new sections (joins, aggregations) in future minor versions.
Existing forms will keep compiling.
