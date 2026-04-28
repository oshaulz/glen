## Worked example of the Glen DSL surface.
##
## Build:  nim c -r --path:src examples/dsl_tour.nim

import std/[os, strformat]
import glen/glen

let dir = getCurrentDir() / "dsl_tour_db"
removeDir(dir)
let db = newGlenDB(dir)

# ---------- schema (typed records + migrations) ----------------------------

schema users:
  version: 1
  fields:
    name:  zString().trim().minLen(2).maxLen(64)
    age:   zInt().gte(0).lte(150)
    email: zString().trim().minLen(3)
    role:  zEnum(["admin", "member", "guest"]).default("member")
  indexes:
    byEmail: equality "email"
    byRole:  equality "role"
    byAge:   range    "age"
  migrations:
    0 -> 1:
      if doc["role"].isNil:
        doc["role"] = VString("member")

registerUsersSchema(db)

# ---------- typed put/get with the generated `Users` record ---------------

putUsers(db, "u1", Users(
  name: "Alice", age: 30'i64, email: "alice@example.com", role: "admin"))
putUsers(db, "u2", Users(
  name: "Bob", age: 25'i64, email: "bob@example.com", role: "member"))

# Mix typed and untyped CRUD freely.
let users = db["users"]
users.put("u3", %*{
  "name": "Carol", "age": 41, "email": "carol@example.com", "role": "admin"
})

# Migrate any pre-existing rows that lacked `role`.
migrateUsers(db)

let (ok, fetched) = getUsers(db, "u1")
if ok: echo &"typed read u1: {fetched.name} ({fetched.role})"

# Validation:
let res = validateUsers(users.get("u1"))
echo &"validate u1 ok={res.ok}"

# Streaming iteration over the collection:
for id, doc in users:
  echo &"  {id}: {doc[\"name\"].s} ({doc[\"role\"].s})"

# ---------- query ------------------------------------------------------

let admins = query(db, "users"):
  where:
    role == "admin"
    age >= 30
  orderBy: name asc
  limit: 5

echo "admins >=30:"
for (id, doc) in admins:
  echo &"  {id} {doc[\"name\"].s}"

# ---------- liveQuery (reactive) ------------------------------------------

var liveAdds = 0
var liveRemoves = 0
let liveAdmins = liveQuery(db, "users"):
  where:
    role == "admin"

discard liveAdmins.onChange(proc (ev: LiveQueryEvent) =
  case ev.kind
  of lqAdded:    inc liveAdds
  of lqRemoved:  inc liveRemoves
  else: discard
)

echo &"liveAdmins seeded with {liveAdmins.len} admins"
users.put("u5", %*{
  "name": "Erin", "age": 33, "email": "erin@example.com", "role": "admin"
})
users.put("u3", %*{
  "name": "Carol", "age": 41, "email": "carol@example.com", "role": "guest"
})
echo &"after edits: live count={liveAdmins.len}, +{liveAdds} -{liveRemoves}"
liveAdmins.close()

# ---------- watch ------------------------------------------------------

var changeCount = 0
let scope = watch(db):
  collection "users":
    inc changeCount

users.put("u4", %*{
  "name": "Dave", "age": 28, "email": "dave@example.com"
})
users.delete("u4")
echo &"observed {changeCount} user changes"
scope.close()

# ---------- txn --------------------------------------------------------

users.put("counter", %*{"n": 0})
let txnRes = txn(db, retries = 3):
  let cur = txn.get("users", "counter")
  txn.put("users", "counter", %*{"n": cur["n"].i + 1})

echo &"counter txn: status={txnRes.status} n={users.get(\"counter\")[\"n\"].i}"

# ---------- sync (in-memory pair) --------------------------------------

let dirB = getCurrentDir() / "dsl_tour_db_b"
removeDir(dirB)
let dbB = newGlenDB(dirB)

let tA = InMemoryTransport()
let tB = InMemoryTransport()
tA.peerOf = tB; tB.peerOf = tA

let syncA = sync(db):
  peer "B":
    transport: tA
    intervalMs: 0

let syncB = sync(dbB):
  peer "A":
    transport: tB
    intervalMs: 0

syncA.tickAll()
syncB.tickAll()
echo &"replicated {dbB.getAll(\"users\").len} users to peer B"

db.close()
dbB.close()
