## Worked example of the Glen DSL surface.
##
## Build:  nim c -r --path:src examples/dsl_tour.nim

import std/[os, strformat]
import glen/glen

let dir = getCurrentDir() / "dsl_tour_db"
removeDir(dir)
let db = newGlenDB(dir)

# ---------- schema -----------------------------------------------------

schema users:
  fields:
    name:  zString().trim().minLen(2).maxLen(64)
    age:   zInt().gte(0).lte(150)
    email: zString().trim().minLen(3)
    role:  zEnum(["admin", "member", "guest"]).default("member")
  indexes:
    byEmail: equality "email"
    byRole:  equality "role"
    byAge:   range    "age"

registerUsersSchema(db)

# ---------- %* literals + Collection proxy ---------------------------------

let users = db["users"]
users.put("u1", %*{
  "name": "Alice", "age": 30, "email": "alice@example.com", "role": "admin"
})
users.put("u2", %*{
  "name": "Bob", "age": 25, "email": "bob@example.com", "role": "member"
})
users.put("u3", %*{
  "name": "Carol", "age": 41, "email": "carol@example.com", "role": "admin"
})

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
