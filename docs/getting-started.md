# Getting started

## Install

```
nimble install https://github.com/oshaulz/glen
```

or in a `.nimble`:

```nim
requires "https://github.com/oshaulz/glen >= 0.5.0"
```

Pure Nim, no external runtime deps. Glen requires Nim ≥ 1.6.

## Hello world

```nim
import glen/db, glen/types

let db = newGlenDB("./mydb")

var alice = VObject()
alice["name"] = VString("Alice")
alice["age"]  = VInt(30)

db.put("users", "u1", alice)
echo db.get("users", "u1")           # {age: 30, name: "Alice"}

db.delete("users", "u1")
assert db.get("users", "u1").isNil
db.close()
```

`Value` is a sum type covering `null`, `bool`, `int64`, `float64`, `string`,
`bytes`, `array`, `object`, and a typed `id` reference. Construct with the
`V*` helpers (`VString`, `VInt`, `VObject`, `VArray`, `VBytes`, …).

## Compiling

Single-threaded:

```
nim c -d:release --mm:orc --passC:-O3 --path:src your_file.nim
```

Multi-threaded callers must use `atomicArc` (ORC's cycle collector isn't
thread-safe across cross-thread `Value` refs):

```
nim c -d:release -d:useMalloc --mm:atomicArc --passC:-O3 --threads:on --path:src your_file.nim
```

Glen's `Value` graph is acyclic, so `atomicArc` has no functional downside.

## Common patterns

### Batch writes

`putMany` / `deleteMany` take a single stripe write lock and batch the WAL
append. Use them in tight loops:

```nim
db.putMany("users", @[("u2", VString("a")), ("u3", VString("b"))])
db.deleteMany("users", @["u2", "u3"])
```

### Borrowed reads (zero-copy)

`db.get` clones to keep callers from mutating cached state. For read-only hot
loops, use the borrowed variants:

```nim
let v = db.getBorrowed("users", "u1")        # do not mutate v
for (id, v) in db.getBorrowedMany("users", @["u1", "u2"]): discard
for (id, v) in db.getBorrowedAll("users"): discard
```

### Optimistic transactions

```nim
import glen/txn

let t = db.beginTxn()
discard db.get("items", "i1", t)      # records read version
t.stagePut(Id(collection: "items", docId: "i1"), VString("new"))

case db.commit(t).status
of csOk:       echo "applied"
of csConflict: echo "version moved under us — retry"
of csInvalid:  echo "rolled back"
```

Multi-collection commits acquire all touched stripes in sorted order — no
deadlock, no cross-collection skew. See [concurrency](concurrency.md) for the
locking model.

### Indexes and queries

```nim
db.createIndex("users", "byName", "name")
for (id, doc) in db.findBy("users", "byName", VString("Alice")):
  echo id

db.createIndex("users", "byAge", "age")     # rangeable
for (id, _) in db.rangeBy("users", "byAge",
    minVal = VInt(25), maxVal = VInt(40)):
  echo id
```

Indexes are persisted in `indexes.manifest`; auto-rebuilt on reopen.

### Subscriptions

```nim
let h = db.subscribe("users", "u1", proc(id: Id; v: Value) =
  echo "doc changed: ", id, " -> ", v)

let hf = db.subscribeField("users", "u1", "profile.age",
  proc(id: Id; path: string; oldV, newV: Value) =
    echo path, ": ", oldV, " -> ", newV)
```

See [api/core.md](api/core.md) for the full subscription surface (field-delta,
streaming companions for IPC).

### Datasets bigger than RAM

```nim
let db = newGlenDB("./big.glen",
                   spillableMode = true,
                   hotDocCap     = 10_000)

# Streaming iterator — bounded memory floor
for (id, doc) in db.getAllStream("events"):
  process(id, doc)
```

See [spillable-mode.md](spillable-mode.md).

## Where to go next

- [Architecture](architecture.md) — components, data flow, design
- [Spatial API](api/spatial.md) — R-tree, polygons, GeoMesh
- [Time-series API](api/timeseries.md) — Gorilla TSDB, tile time-stacks
- [Performance](performance.md) — what to expect on a real machine
