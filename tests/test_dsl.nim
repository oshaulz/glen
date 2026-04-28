## Tests for the Glen DSL surface (literal, query, txn, schema, watch,
## sync, collection proxy).

import std/[unittest, os, strutils]
import glen/glen
import glen/types, glen/db as glendb, glen/txn as glentxn
import glen/dsl

proc freshDir(name: string): string =
  result = getTempDir() / name
  removeDir(result)
  createDir(result)

# ---- %* literal macro ------------------------------------------------------

suite "dsl: %* value literal":
  test "scalar primitives":
    let s = %*"hello"
    check s.kind == vkString and s.s == "hello"
    let i = %*42
    check i.kind == vkInt and i.i == 42'i64
    let f = %*3.5
    check f.kind == vkFloat and f.f == 3.5
    let b = %*true
    check b.kind == vkBool and b.b
    let n = %*nil
    check n.kind == vkNull

  test "object and nested":
    let v = %*{
      "name": "Alice",
      "age": 30,
      "addr": { "city": "NYC", "zip": 10001 },
      "tags": ["admin", "ops"]
    }
    check v.kind == vkObject
    check v["name"].s == "Alice"
    check v["age"].i == 30
    check v["addr"].kind == vkObject
    check v["addr"]["city"].s == "NYC"
    check v["tags"].kind == vkArray
    check v["tags"].arr.len == 2
    check v["tags"].arr[0].s == "admin"

  test "interpolated variables via toValue":
    let name = "Bob"
    let age = 41
    let v = %*{"name": name, "age": age}
    check v["name"].s == "Bob"
    check v["age"].i == 41

# ---- Collection proxy ------------------------------------------------------

suite "dsl: Collection proxy":
  test "bracket access, put/get/delete, iteration":
    let dir = freshDir("glen_dsl_collection")
    let db = newGlenDB(dir)
    let users = db["users"]
    users.put("u1", %*{"name": "Alice", "age": 30})
    users.put("u2", %*{"name": "Bob", "age": 25})
    check users.get("u1")["name"].s == "Alice"
    check "u1" in users
    var ids: seq[string] = @[]
    for id, doc in users:
      ids.add(id)
      check doc.kind == vkObject
    check ids.len == 2
    users.delete("u1")
    check not (users.contains("u1"))
    db.close()

# ---- glenQuery -------------------------------------------------------------

suite "dsl: glenQuery block":
  test "where + orderBy + limit":
    let dir = freshDir("glen_dsl_query")
    let db = newGlenDB(dir)
    db.put("u", "1", %*{"name": "alice", "age": 30, "status": "active"})
    db.put("u", "2", %*{"name": "bob",   "age": 25, "status": "inactive"})
    db.put("u", "3", %*{"name": "carol", "age": 40, "status": "active"})
    db.put("u", "4", %*{"name": "dave",  "age": 35, "status": "active"})
    let rows = glenQuery(db, "u"):
      where:
        status == "active"
        age >= 30
      orderBy: age desc
      limit: 2
    check rows.len == 2
    check rows[0][1]["age"].i == 40
    check rows[1][1]["age"].i == 35
    db.close()

  test "in predicate and contains":
    let dir = freshDir("glen_dsl_query_in")
    let db = newGlenDB(dir)
    db.put("u", "1", %*{"name": "alice",  "role": "admin"})
    db.put("u", "2", %*{"name": "bob",    "role": "guest"})
    db.put("u", "3", %*{"name": "carol",  "role": "member"})
    db.put("u", "4", %*{"name": "alfred", "role": "admin"})
    let rows = glenQuery(db, "u"):
      where:
        role in ["admin", "member"]
        name.contains("al")
      orderBy: name asc
    check rows.len == 2
    # alphabetical: "alfred" < "alice" so 4 sorts before 1
    check rows[0][0] == "4"
    check rows[1][0] == "1"
    db.close()

# ---- glenTxn ---------------------------------------------------------------

suite "dsl: glenTxn":
  test "ok path commits":
    let dir = freshDir("glen_dsl_txn_ok")
    let db = newGlenDB(dir)
    db.put("a", "1", %*{"balance": 100})
    db.put("a", "2", %*{"balance": 0})
    let res = glenTxn(db, retries = 3):
      let src = txn.get("a", "1")
      let dst = txn.get("a", "2")
      txn.put("a", "1", %*{"balance": src["balance"].i - 25})
      txn.put("a", "2", %*{"balance": dst["balance"].i + 25})
    check res.status == glentxn.csOk
    check db.get("a", "1")["balance"].i == 75
    check db.get("a", "2")["balance"].i == 25
    db.close()

  test "exception in body returns csInvalid":
    let dir = freshDir("glen_dsl_txn_invalid")
    let db = newGlenDB(dir)
    let res = glenTxn(db):
      raise newException(ValueError, "boom")
    check res.status == glentxn.csInvalid
    check res.message.contains("boom")
    db.close()

# ---- glenSchema ------------------------------------------------------------

glenSchema users:
  fields:
    name:  zString().trim().minLen(2).maxLen(64)
    age:   zInt().gte(0).lte(150)
    email: zString().trim().minLen(3)
  indexes:
    byEmail: equality "email"
    byAge:   range    "age"

suite "dsl: glenSchema":
  test "register creates indexes; validate parses docs":
    let dir = freshDir("glen_dsl_schema")
    let db = newGlenDB(dir)
    registerUsersSchema(db)
    db.put(usersCollection, "u1",
      %*{"name": "Alice", "age": 30, "email": "alice@example.com"})
    db.put(usersCollection, "u2",
      %*{"name": "Bob", "age": 41, "email": "bob@example.com"})
    let rows = db.findBy(usersCollection, "byEmail",
      VString("alice@example.com"))
    check rows.len == 1
    check rows[0][0] == "u1"
    let bad = validateUsers(%*{"name": "X", "age": 999, "email": "x"})
    check not bad.ok
    let good = validateUsers(%*{"name": "Carol", "age": 25, "email": "carol@example.com"})
    check good.ok
    db.close()

# ---- glenWatch -------------------------------------------------------------

suite "dsl: glenWatch":
  test "doc and collection handlers fire; close unsubscribes":
    let dir = freshDir("glen_dsl_watch")
    let db = newGlenDB(dir)
    var docHits = 0
    var collHits = 0
    let scope = glenWatch(db):
      doc "users", "u1":
        inc docHits
      collection "users":
        inc collHits
    db.put("users", "u1", %*{"v": 1})  # both fire
    db.put("users", "u2", %*{"v": 2})  # only collection fires
    db.put("posts", "p1", %*{"v": 3})  # neither fires
    check docHits == 1
    check collHits == 2
    scope.close()
    db.put("users", "u1", %*{"v": 99})
    check docHits == 1     # unchanged after close
    check collHits == 2
    db.close()

# ---- glenSync --------------------------------------------------------------

suite "dsl: glenSync":
  test "two in-memory peers round-trip a write":
    let dirA = freshDir("glen_dsl_sync_a")
    let dirB = freshDir("glen_dsl_sync_b")
    let dbA = newGlenDB(dirA)
    let dbB = newGlenDB(dirB)
    let tA = InMemoryTransport()
    let tB = InMemoryTransport()
    tA.peerOf = tB
    tB.peerOf = tA

    let syncA = glenSync(dbA):
      peer "B":
        transport: tA
        intervalMs: 0
    let syncB = glenSync(dbB):
      peer "A":
        transport: tB
        intervalMs: 0

    dbA.put("notes", "n1", %*{"text": "hello from A"})
    syncA.tickAll()    # push to B's inbox
    syncB.tickAll()    # B pulls from its inbox, then pushes its own (empty)
    let rec = dbB.get("notes", "n1")
    check not rec.isNil
    check rec["text"].s == "hello from A"

    dbB.put("notes", "n2", %*{"text": "hello from B"})
    syncB.tickAll()
    syncA.tickAll()
    let rec2 = dbA.get("notes", "n2")
    check not rec2.isNil
    check rec2["text"].s == "hello from B"
    dbA.close(); dbB.close()
