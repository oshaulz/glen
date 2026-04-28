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
schema users:
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
let admins = query(db, "users"):
  where:
    role == "admin"
    age >= 30
  orderBy: name asc
  limit: 10

# Transaction with auto-retry on conflict
let res = txn(db, retries = 3):
  let acc = txn.get("accounts", srcId)
  txn.put("accounts", srcId, %*{"balance": acc["balance"].i - amount})

# Reactive subscriptions, all closed together
let scope = watch(db):
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

## Engine proxies — `db.series` / `db.tiles`

The same `db`-rooted shape extends to Glen's specialised engines, so
they don't feel like a separate library. Each one opens or creates a
file/dir under conventional paths inside `<db.dir>`.

```nim
# Time-series — Gorilla-encoded (timestamp, value) streams
let s = db.series("temp.sensor1")          # <db.dir>/series/temp.sensor1.gts
s.append(nowMillis(), 22.5)
for (ts, v) in s.range(t0, t1): plot(ts, v)
s.close()

# Auto-close form
withSeries(db, "temp.sensor1") do:
  for (ts, v) in s.range(t0, t1): plot(ts, v)

# Tile-stacks — rasters evolving over time
let stack = db.tiles("radar", bbox(-180.0, -90.0, 180.0, 90.0),
                     rows = 360, cols = 720, channels = 1)
stack.appendFrame(nowMillis(), mesh)
let history = stack.readPointHistory(-73.98, 40.75, t0, t1)
stack.close()

# Or attach to an existing stack with auto-close
withTiles(db, "radar") do:
  let frames = s.readFrameRange(t0, t1)
```

| Helper                          | Path                                       |
|---------------------------------|--------------------------------------------|
| `db.series(name, ...)`          | `<db.dir>/series/<name>.gts`               |
| `db.tiles(name, bbox, ...)`     | `<db.dir>/tiles/<name>/` (creates manifest)|
| `db.openTiles(name)`            | `<db.dir>/tiles/<name>/` (must exist)      |
| `seriesExists(db, name)` / `tilesExists(db, name)` | filesystem probe         |
| `withSeries(db, name): body`    | binds `s` in body, closes on exit          |
| `withTiles(db, name): body`     | binds `s` in body, closes on exit          |

Names may contain dots (`"temp.sensor1"`) — they're treated as filename
components, not directory separators. Lifetime is the caller's
responsibility: `withSeries` / `withTiles` cover the common scoped-use
case; for long-lived handles, hold the returned ref and call `.close()`.
Don't open the same series twice concurrently.

## `query:` — query block

Block syntax over the `query / whereEq / orderByField / limitN` builder.
Predicates use natural Nim operators on dotted-path identifiers.

```nim
let active = query(db, "users"):
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

| Section     | Meaning                                                                  |
|-------------|--------------------------------------------------------------------------|
| `where:`    | Conjunction of predicates (one per line)                                 |
| `orderBy:`  | One or more `field [asc|desc]` lines (default `asc`)                     |
| `limit: N`  | Page size (`0` = unlimited)                                              |
| `after: c`  | Resume from opaque cursor returned by `nextCursor(rows)`                 |
| `select:`   | Project each row to listed fields (one per line)                         |
| `near:`     | Geo-radius prefilter against a named index (replaces the candidate set)  |
| `similar:`  | Vector kNN prefilter against an HNSW index                               |
| `count: ()` | Reduction → `int`                                                        |
| `first: ()` | Reduction → `(ok: bool, id: string, value: Value)`                       |
| `exists: ()`| Reduction → `bool`                                                       |

`count:` / `first:` / `exists:` are mutually exclusive with each other
but compose with `where:` / `orderBy:` / `limit:` / `near:` / `similar:`.
`near:` and `similar:` are mutually exclusive (both replace the candidate
set), and they're incompatible with `orderBy:` / `after:` (results come
back sorted by distance from the index).

### Returning a builder instead of running

```nim
var q = queryBuilder(db, "users"):
  where:
    age >= 30
for id, doc in q.runStream():
  process(doc)
```

### Projections — `select:`

Each row is rewritten to a `VObject` that contains only the listed
field paths. Dotted paths are flattened to their leaf segment as the key
(`addr.city` ends up under `"city"`); to keep the nested shape, project
the parent (`addr`) instead. Missing fields are skipped, not stored as
`VNull`.

```nim
let names = query(db, "users"):
  where: role == "admin"
  select:
    name
    age
    addr.city
# names: seq[(string, Value)] where each Value is { "name": ..., "age": ..., "city": ... }
```

### Reductions — `count:` / `first:` / `exists:`

Terminal sections that change the macro's return type. Each takes an
empty `()` arg list to disambiguate from a section header.

```nim
let n = query(db, "users"):
  where: status == "active"
  count: ()                        # n: int

let hit = query(db, "users"):
  where: age >= 30
  orderBy: age desc
  first: ()                        # hit: (ok: bool, id: string, value: Value)
if hit.ok: echo hit.value["name"].s

let any = query(db, "users"):
  where: role == "godmode"
  exists: ()                       # any: bool
```

### Geo and vector — `near:` and `similar:`

Index-aware searches sit alongside `where:`, not inside it: they replace
the candidate set with the index's k-NN / radius results, then `where:`
post-filters those candidates. Both take a paren-tuple of args because
Nim's grammar doesn't allow a comma-list after a colon.

```nim
# Point index "byLoc" already created with createGeoIndex(...)
let nearby = query(db, "stores"):
  near: ("byLoc", -73.98, 40.75, 2000.0)   # (indexName, lon, lat, radiusMeters)
  where: open == true                       # post-filter
  limit: 20

# Vector index "byEmbed" already created with createVectorIndex(...)
let similar = query(db, "docs"):
  similar: ("byEmbed", queryVec, 10)        # (indexName, queryVector, k)
  where: published == true
```

`near:` returns docs sorted by haversine distance ascending. `similar:`
returns docs in HNSW's approximate-nearest order. Both compose with
`select:` and reductions.

If you want raw lookups (no post-filter, no DSL), the underlying procs
are still exposed:

```nim
for (id, distMeters, doc) in db.near("stores", "byLoc", -73.98, 40.75, 2000.0):
  echo id, " is ", distMeters, " m away"

let knn   = db.nearest("stores", "byLoc", -73.98, 40.75, k = 10)
let bbox  = db.inBBox("stores", "byLoc", -74.0, 40.7, -73.9, 40.8)
let pairs = db.nearestVector("docs", "byEmbed", queryVec, k = 10)
```

## `txn:` — transactions with retries

```nim
let res = txn(db, retries = 3):
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

A bare `txn(db): body` form runs once with no retries.

## `schema:` — typed records, validator, indexes, migrations

```nim
schema users:
  version: 2
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
  migrations:
    0 -> 1:
      if doc["role"].isNil:
        doc["role"] = VString("member")
    1 -> 2:
      let legacy = doc["fullName"]
      if not legacy.isNil:
        doc["name"] = legacy
```

The example above generates the following symbols, all prefixed with the
schema name:

| Symbol                                | Purpose                                                       |
|---------------------------------------|---------------------------------------------------------------|
| `type Users* = object`                | Typed record. Each field's static type comes from its schema (`zString()` → `string`, `zInt()` → `int64`, `zArray(zInt())` → `seq[int64]`, `zBool()` → `bool`, etc.) |
| `usersCollection: string`             | The collection name (`"users"` here)                          |
| `usersSchemaVersion: int`             | Current declared version (`2`)                                |
| `usersSchema: Schema[Users]`          | Validator that parses a `Value` into a `Users`                |
| `parseUsers(v) -> ValidationResult[Users]` | Run the validator                                        |
| `validateUsers(v)`                    | Alias for `parseUsers` (compat with non-typed code)           |
| `toValue(u: Users) -> Value`          | Encode a typed record back into a `Value`                     |
| `getUsers(db, id) -> (bool, Users)`   | Typed read; `ok=false` for missing / invalid docs             |
| `putUsers(db, id, u: Users)`          | Typed write (calls `toValue` and `db.put`)                    |
| `registerUsersSchema(db)`             | Creates every declared index on `db`                          |
| `migrateUsers(db)`                    | Walks the collection and replays every declared migration up to `usersSchemaVersion` |

### Typed records

```nim
let alice = Users(name: "Alice", age: 30, email: "alice@example.com",
                  role: "admin")
putUsers(db, "u1", alice)

let (ok, fetched) = getUsers(db, "u1")
if ok: echo fetched.name

# Untyped CRUD still works on the same docs:
db.put(usersCollection, "u2", %*{"name": "Bob", "age": 25,
                                  "email": "bob@x.io", "role": "member"})

# Validate an arbitrary Value before storing it:
let res = parseUsers(someUntypedValue)
if not res.ok:
  for issue in res.issues:
    echo describePath(issue.path), ": ", issue.message
```

Field types are inferred via `schemaValueType` on each schema expression,
so adding refinements (`.minLen(2)`, `.gte(0)`) doesn't change the field
type. `Option[T]` falls out of `optional()` / `nullable()` automatically.

### Optional fields

Three builders cover the "may not be there" cases. Pick by intent:

| Builder                | Static type   | **Field absent**             | **Explicit `null`**         |
|------------------------|---------------|------------------------------|-----------------------------|
| `optional(zX())`       | `Option[X]`   | ✅ parses to `none(X)`        | ✅ parses to `none(X)`       |
| `nullable(zX())`       | `Option[X]`   | ❌ validation error           | ✅ parses to `none(X)`       |
| `zX().default(v)`      | `X`           | ✅ parses to `v`              | ✅ parses to `v`             |
| `zX()` (bare)          | `X`           | ❌ validation error           | ❌ validation error          |

`optional` and `nullable` differ only on absence: `optional` is
"the key may be missing", `nullable` is "the key must be present but
the value may be `null`". Use whichever matches your wire format.

```nim
schema posts:
  fields:
    title: zString()                          # required, non-null
    body:  optional(zString())                # may be missing → none
    draft: zBool().default(false)             # missing → false
    tags:  zArray(zString()).default(@[])     # missing → []
```

### Extra fields not declared in the schema

Glen's schema is **strip-on-parse, preserve-on-store**:

| Path                                                        | Extra fields            |
|-------------------------------------------------------------|-------------------------|
| `db.put` / `db.get` / `Collection` / `%*` (untyped)         | **preserved** as-is     |
| `parseUsers` → `Users` → `toValue` (typed round-trip)       | **dropped** silently    |
| `putUsers(db, id, u: Users)` (typed write)                  | **dropped** (only schema fields are written) |
| `getUsers(db, id) -> (bool, Users)` (typed read)            | dropped *from the typed view*; the on-disk doc still has them |

Glen stores the whole `Value` blob untouched, so the database itself
never loses extras. They only get filtered out when you go through the
typed gate. Practical implications:

- **Internal metadata, caches, third-party annotations** — write them
  via `db.put` of a `%*{...}` literal that includes both the schema
  fields and the extras. The schema validator can still verify the
  schema fields without touching the rest.
- **Typed-mutate-write loops** — `getUsers` → mutate → `putUsers`
  silently drops extras. If you need to preserve them, use the
  untyped path: `let v = db.get(...)`; mutate fields on `v`; `db.put`
  it back.
- **Mix freely** — read typed for app logic (`getUsers`), and at the
  same time read untyped (`db.get`) when you need the full document.
  They see the same bytes on disk.

There is no "strict mode" today that errors on unknown keys; if you
need that, run `parseUsers` for validation but check the input
`Value`'s `obj` table for unexpected keys yourself.

### Index kinds

| Kind                              | Notes                                                |
|-----------------------------------|------------------------------------------------------|
| `equality "field"`                | Equality + range scans (single-field indexes are rangeable in current Glen) |
| `range "field"`                   | Same engine as `equality`; intent marker             |
| `geo "lonField", "latField"`      | R-tree over a (lon, lat) pair                         |
| `polygon "field"`                 | R-tree over polygon MBRs                              |
| `vector "field", dim [, metric]`  | HNSW; `metric` defaults to `vmCosine`                 |

`registerUsersSchema(db)` is idempotent: indexes that already exist (per
the on-disk manifest) are not rebuilt.

### Migrations

`version: N` declares the schema's current version. `migrations:` is a
block of `from -> to: <body>` steps. Inside each body the `doc: Value`
is mutated in place; you can read fields, set new ones, delete others,
and so on. The macro stamps the doc with `_v: int` so re-running
`migrateUsers(db)` is idempotent.

```nim
schema accounts:
  version: 2
  fields:
    name:    zString()
    balance: zInt()
    role:    zString()
  migrations:
    0 -> 1:
      if doc["role"].isNil:
        doc["role"] = VString("member")
    1 -> 2:
      let legacy = doc["credit"]
      if not legacy.isNil:
        doc["balance"] = legacy
```

Run `migrateAccounts(db)` once at startup (or after deployment).
Documents already at the current version are left alone.

## `liveQuery:` — reactive query

A query whose result set updates as the underlying collection changes.
Subscribes once at the collection level and emits diff events
(added / updated / removed) to registered callbacks.

```nim
let live = liveQuery(db, "users"):
  where:
    role == "admin"
    age >= 30

let h = live.onChange(proc (ev: LiveQueryEvent) =
  case ev.kind
  of lqRefilled: ui.upsert(ev.id, ev.newValue)   # initial seed
  of lqAdded:    ui.upsert(ev.id, ev.newValue)
  of lqUpdated:  ui.upsert(ev.id, ev.newValue)
  of lqRemoved:  ui.remove(ev.id)
)

echo live.len           # current matched count
let snap = live.snapshot()   # one-shot read of the matched set

live.offChange(h)
live.close()
```

Predicate algebra is the same as `query:` — `==`, `!=`, `<`, `<=`, `>`,
`>=`, `in`, `.contains`. `orderBy:` and `limit:` are not supported on
live queries (they'd require maintaining a sorted heap and recomputing
window membership on every change); if you need ordered/paged live
state, build it on top of the diff stream.

| Event kind   | When fired                                                | `oldValue` | `newValue` |
|--------------|-----------------------------------------------------------|------------|------------|
| `lqRefilled` | Once per matching doc when `onChange` is registered       | nil        | current    |
| `lqAdded`    | Doc newly enters the result set                           | nil        | new        |
| `lqUpdated`  | Doc was in the set, still matches, value changed          | previous   | new        |
| `lqRemoved`  | Doc left the set (no longer matches, or was deleted)      | previous   | nil        |

## `watch:` — declarative subscriptions

```nim
let scope = watch(db):
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

## `sync:` — replication supervisor

A small wrapper over `exportChanges` / `applyChanges` / `setPeerCursor`
that lets you declare your replication topology in one place.

```nim
let sync = sync(db):
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
  `query:` block in the same module.
* You can register schema indexes via `registerUsersSchema(db)` and
  still call `db.dropIndex` / `db.createIndex` directly.

Run `expandMacros: query(db, "users"): ...` to see the desugared
form when debugging.

## Versioning

The DSL ships under `glen/dsl/*` and re-exports through `glen/glen`.
DSL macros are part of the 0.5+ public API; their grammar is stable but
may grow new sections (joins, aggregations) in future minor versions.
Existing forms will keep compiling.
