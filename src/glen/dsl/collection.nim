## Collection proxy — a thin handle on (db, collection name) that lets you
## use Nim's `for` loop and bracket access against a Glen collection.
##
##   let users = db["users"]            # or: collection(db, "users")
##   users.put("u1", %*{"name": "Alice"})
##   for id, doc in users:              # streams without loading whole set
##     process(doc)
##
## All operations forward to the existing `db.put / db.get / db.delete /
## getBorrowedAllStream` procs — this is pure ergonomics.

import glen/types
import glen/db as glendb

type
  Collection* = object
    db*: glendb.GlenDB
    name*: string

proc collection*(db: glendb.GlenDB; name: string): Collection {.inline.} =
  Collection(db: db, name: name)

proc `[]`*(db: glendb.GlenDB; name: string): Collection {.inline.} =
  ## `db["users"]` — sugar for `collection(db, "users")`.
  Collection(db: db, name: name)

# ---- CRUD ----

proc put*(c: Collection; id: string; value: Value) {.inline.} =
  c.db.put(c.name, id, value)

proc add*(c: Collection; value: Value): string {.inline.} =
  ## Insert under a fresh ULID-style id and return it. See `db.add`.
  c.db.add(c.name, value)

proc get*(c: Collection; id: string): Value {.inline.} =
  c.db.get(c.name, id)

proc getBorrowed*(c: Collection; id: string): Value {.inline.} =
  c.db.getBorrowed(c.name, id)

proc delete*(c: Collection; id: string) {.inline.} =
  c.db.delete(c.name, id)

proc contains*(c: Collection; id: string): bool {.inline.} =
  not c.db.getBorrowed(c.name, id).isNil

proc getAll*(c: Collection): seq[(string, Value)] {.inline.} =
  c.db.getAll(c.name)

proc putMany*(c: Collection; items: openArray[(string, Value)]) {.inline.} =
  c.db.putMany(c.name, items)

proc deleteMany*(c: Collection; ids: openArray[string]) {.inline.} =
  c.db.deleteMany(c.name, ids)

# ---- Iteration ----
#
# Streams the whole collection without materialising the result set —
# spillable mode safe.

iterator items*(c: Collection): (string, Value) =
  for r in c.db.getBorrowedAllStream(c.name):
    yield r

iterator pairs*(c: Collection): (string, Value) =
  for r in c.db.getBorrowedAllStream(c.name):
    yield r

# ---- Query bridge ----

proc query*(c: Collection): glendb.GlenQuery {.inline.} =
  ## Start a query builder against this collection. Chains the existing
  ## `whereEq` / `whereGte` / etc. procs.
  c.db.query(c.name)

# ---- Index management ----

proc createIndex*(c: Collection; name: string; fieldPath: string) {.inline.} =
  c.db.createIndex(c.name, name, fieldPath)

proc dropIndex*(c: Collection; name: string) {.inline.} =
  c.db.dropIndex(c.name, name)
