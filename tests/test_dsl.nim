## Tests for the Glen DSL surface (literal, query, txn, schema, watch,
## sync, collection proxy).

import std/[unittest, os, strutils, options]
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

# ---- query -------------------------------------------------------------

suite "dsl: query block":
  test "where + orderBy + limit":
    let dir = freshDir("glen_dsl_query")
    let db = newGlenDB(dir)
    db.put("u", "1", %*{"name": "alice", "age": 30, "status": "active"})
    db.put("u", "2", %*{"name": "bob",   "age": 25, "status": "inactive"})
    db.put("u", "3", %*{"name": "carol", "age": 40, "status": "active"})
    db.put("u", "4", %*{"name": "dave",  "age": 35, "status": "active"})
    let rows = query(db, "u"):
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
    let rows = query(db, "u"):
      where:
        role in ["admin", "member"]
        name.contains("al")
      orderBy: name asc
    check rows.len == 2
    # alphabetical: "alfred" < "alice" so 4 sorts before 1
    check rows[0][0] == "4"
    check rows[1][0] == "1"
    db.close()

# ---- txn ---------------------------------------------------------------

suite "dsl: txn":
  test "ok path commits":
    let dir = freshDir("glen_dsl_txn_ok")
    let db = newGlenDB(dir)
    db.put("a", "1", %*{"balance": 100})
    db.put("a", "2", %*{"balance": 0})
    let res = txn(db, retries = 3):
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
    let res = txn(db):
      raise newException(ValueError, "boom")
    check res.status == glentxn.csInvalid
    check res.message.contains("boom")
    db.close()

# ---- schema ------------------------------------------------------------

schema users:
  fields:
    name:  zString().trim().minLen(2).maxLen(64)
    age:   zInt().gte(0).lte(150)
    email: zString().trim().minLen(3)
  indexes:
    byEmail: equality "email"
    byAge:   range    "age"

suite "dsl: schema":
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

  test "typed records: putUsers / getUsers round-trip":
    let dir = freshDir("glen_dsl_schema_typed")
    let db = newGlenDB(dir)

    # Build a typed record directly. Each field has its zod-derived static
    # type: name=string, age=int64, email=string.
    let alice = Users(name: "Alice", age: 30'i64,
                      email: "alice@example.com")
    putUsers(db, "u1", alice)

    let (ok, fetched) = getUsers(db, "u1")
    check ok
    check fetched.name == "Alice"
    check fetched.age == 30
    check fetched.email == "alice@example.com"

    let (missingOk, _) = getUsers(db, "u404")
    check not missingOk
    db.close()

  test "typed records: parseUsers reports issues for bad input":
    let res = parseUsers(%*{"name": "X", "age": 999, "email": "x"})
    check not res.ok
    check res.issues.len > 0

  test "typed records: toValue produces a VObject with all fields":
    let v = toValue(Users(name: "Bob", age: 25'i64, email: "bob@x.io"))
    check v.kind == vkObject
    check v["name"].s == "Bob"
    check v["age"].i == 25
    check v["email"].s == "bob@x.io"

# ---- optional/nullable/default semantics --------------------------------
# Pinning down the documented contract from docs/dsl.md so the typed
# round-trip and missing-key behaviour can't regress silently.

schema posts:
  fields:
    title: zString().minLen(1)
    body:  optional(zString())              # absent OK, null OK
    note:  nullable(zString())              # null OK, absent NOT OK
    tags:  zArray(zString()).default(@[])   # missing → []

suite "dsl: schema optional/nullable/default":
  test "optional accepts absent and null":
    let absent = parsePosts(%*{"title": "t", "note": nil})
    check absent.ok
    check absent.value.body.isNone
    check absent.value.tags.len == 0
    let nullExpl = parsePosts(%*{"title": "t", "body": nil, "note": nil})
    check nullExpl.ok
    check nullExpl.value.body.isNone

  test "nullable rejects absent but accepts explicit null":
    let absent = parsePosts(%*{"title": "t"})
    check not absent.ok   # missing nullable field -> error
    let nullExpl = parsePosts(%*{"title": "t", "note": nil})
    check nullExpl.ok
    check nullExpl.value.note.isNone

  test "default fills in missing values":
    let r = parsePosts(%*{"title": "t", "note": nil})
    check r.ok
    check r.value.tags.len == 0   # defaulted to @[]

suite "dsl: schema extras (strip vs preserve)":
  test "typed round-trip strips undeclared fields":
    let raw = %*{"title": "keep", "note": nil, "tags": ["a"],
                 "extra": "drop me", "nested": {"deep": 1}}
    let parsed = parsePosts(raw)
    check parsed.ok
    let back = toValue(parsed.value)
    check back["extra"].isNil
    check back["nested"].isNil
    check back["title"].s == "keep"

  test "raw db.put/get preserves undeclared fields":
    let dir = freshDir("glen_dsl_extras_raw")
    let db = newGlenDB(dir)
    let raw = %*{"title": "keep", "extra": "survives", "nested": {"deep": 1}}
    db.put(postsCollection, "p1", raw)
    let fetched = db.get(postsCollection, "p1")
    check fetched["extra"].s == "survives"
    check fetched["nested"]["deep"].i == 1
    db.close()

# ---- schema migrations -----------------------------------------------------

schema accounts:
  version: 2
  fields:
    name:    zString()
    balance: zInt()
    role:    zString()
  migrations:
    0 -> 1:
      # Schema bump: introduce `role` defaulting to "member".
      if doc["role"].isNil:
        doc["role"] = VString("member")
    1 -> 2:
      # Rename "credit" → "balance" if the legacy field is still there.
      let legacy = doc["credit"]
      if not legacy.isNil:
        doc["balance"] = legacy

suite "dsl: schema migrations":
  test "migrateAccounts upgrades pre-existing docs to current version":
    let dir = freshDir("glen_dsl_schema_migrate")
    let db = newGlenDB(dir)
    # Pre-populate with v0 (no role) and v1 (legacy credit field).
    db.put(accountsCollection, "a1",
      %*{"name": "Alice", "credit": 100, "_v": 1})
    db.put(accountsCollection, "a2",
      %*{"name": "Bob", "balance": 50})  # _v missing → treated as 0
    migrateAccounts(db)
    let a1 = db.get(accountsCollection, "a1")
    check a1["balance"].i == 100
    check a1["_v"].i == 2
    let a2 = db.get(accountsCollection, "a2")
    check a2["role"].s == "member"
    check a2["_v"].i == 2
    db.close()

# ---- liveQuery -------------------------------------------------------------

suite "dsl: liveQuery":
  test "seeds initial set, fires diffs on add/update/remove":
    let dir = freshDir("glen_dsl_live")
    let db = newGlenDB(dir)

    # Pre-populate so we get an initial replay.
    db.put("u", "1", %*{"name": "alice", "age": 30, "role": "admin"})
    db.put("u", "2", %*{"name": "bob",   "age": 25, "role": "guest"})

    let live = liveQuery(db, "u"):
      where:
        role == "admin"
        age >= 30

    var refilled, added, updated, removed: int
    let h = live.onChange(proc (ev: LiveQueryEvent) =
      case ev.kind
      of lqRefilled: inc refilled
      of lqAdded:    inc added
      of lqUpdated:  inc updated
      of lqRemoved:  inc removed
    )

    # alice (id=1) is initially in the set; refilled fires once.
    check refilled == 1
    check live.len == 1

    # New admin → lqAdded
    db.put("u", "3", %*{"name": "carol", "age": 40, "role": "admin"})
    check added == 1
    check live.len == 2

    # alice age changes but still matches → lqUpdated
    db.put("u", "1", %*{"name": "alice", "age": 31, "role": "admin"})
    check updated == 1

    # alice demoted → lqRemoved
    db.put("u", "1", %*{"name": "alice", "age": 31, "role": "guest"})
    check removed == 1
    check live.len == 1

    # carol deleted → lqRemoved
    db.delete("u", "3")
    check removed == 2
    check live.len == 0

    # After offChange + close, no further events fire.
    live.offChange(h)
    live.close()
    db.put("u", "4", %*{"name": "dave", "age": 50, "role": "admin"})
    check added == 1   # unchanged
    db.close()

  test "snapshot returns the current matched set":
    let dir = freshDir("glen_dsl_live_snapshot")
    let db = newGlenDB(dir)
    db.put("u", "1", %*{"role": "admin"})
    db.put("u", "2", %*{"role": "guest"})
    db.put("u", "3", %*{"role": "admin"})
    let live = liveQuery(db, "u"):
      where: role == "admin"
    let snap = live.snapshot()
    check snap.len == 2
    var ids: seq[string] = @[]
    for (id, _) in snap: ids.add(id)
    check "1" in ids and "3" in ids
    live.close()
    db.close()

  test "multiple onChange callbacks fan out independently":
    let dir = freshDir("glen_dsl_live_multicb")
    let db = newGlenDB(dir)
    let live = liveQuery(db, "u"):
      where: role == "admin"
    var aHits, bHits: int
    let hA = live.onChange(proc (ev: LiveQueryEvent) =
      if ev.kind == lqAdded: inc aHits)
    let hB = live.onChange(proc (ev: LiveQueryEvent) =
      if ev.kind == lqAdded: inc bHits)
    db.put("u", "1", %*{"role": "admin"})
    check aHits == 1 and bHits == 1
    # offChange one — the other must keep firing.
    live.offChange(hA)
    db.put("u", "2", %*{"role": "admin"})
    check aHits == 1 and bHits == 2
    discard hB
    live.close()
    db.close()

  test "late-registered callback sees current matched set":
    # The replay-on-onChange contract: a callback registered after some
    # mutations should see exactly the *currently* matching docs as
    # lqRefilled, not the original baseline plus a stream of diffs.
    let dir = freshDir("glen_dsl_live_late")
    let db = newGlenDB(dir)
    db.put("u", "1", %*{"role": "admin"})
    let live = liveQuery(db, "u"):
      where: role == "admin"
    db.put("u", "1", %*{"role": "guest"})   # u1 leaves
    db.put("u", "2", %*{"role": "admin"})   # u2 joins
    var seenIds: seq[string] = @[]
    discard live.onChange(proc (ev: LiveQueryEvent) =
      if ev.kind == lqRefilled: seenIds.add(ev.id))
    check seenIds == @["2"]   # u1 should NOT replay
    live.close()
    db.close()

  test "in / .contains predicates":
    let dir = freshDir("glen_dsl_live_preds")
    let db = newGlenDB(dir)
    db.put("u", "1", %*{"name": "alfred",  "role": "admin"})
    db.put("u", "2", %*{"name": "bob",     "role": "guest"})
    db.put("u", "3", %*{"name": "alice",   "role": "member"})
    let live = liveQuery(db, "u"):
      where:
        role in ["admin", "member"]
        name.contains("al")
    check live.len == 2   # alfred + alice
    db.put("u", "4", %*{"name": "alvin", "role": "admin"})
    check live.len == 3
    db.put("u", "1", %*{"name": "alfred", "role": "guest"})  # leaves on `in`
    check live.len == 2
    db.put("u", "3", %*{"name": "bob", "role": "admin"})     # leaves on `contains`
    check live.len == 1
    live.close()
    db.close()

  test "transaction commits fire events for every staged put":
    let dir = freshDir("glen_dsl_live_txn")
    let db = newGlenDB(dir)
    let live = liveQuery(db, "u"):
      where: role == "admin"
    var addHits = 0
    discard live.onChange(proc (ev: LiveQueryEvent) =
      if ev.kind == lqAdded: inc addHits)
    let t = db.beginTxn()
    t.stagePut(Id(collection: "u", docId: "1", version: 0'u64),
               %*{"role": "admin"})
    t.stagePut(Id(collection: "u", docId: "2", version: 0'u64),
               %*{"role": "admin"})
    t.stagePut(Id(collection: "u", docId: "3", version: 0'u64),
               %*{"role": "guest"})  # filtered out
    let res = db.commit(t)
    check res.status == glentxn.csOk
    check addHits == 2
    check live.len == 2
    live.close()
    db.close()

  test "empty initial collection — first put fires lqAdded":
    let dir = freshDir("glen_dsl_live_empty")
    let db = newGlenDB(dir)
    let live = liveQuery(db, "newcoll"):
      where: kind == "valid"
    check live.len == 0
    var refilled, added: int
    discard live.onChange(proc (ev: LiveQueryEvent) =
      case ev.kind
      of lqRefilled: inc refilled
      of lqAdded:    inc added
      else: discard)
    check refilled == 0   # nothing to replay
    db.put("newcoll", "x1", %*{"kind": "valid"})
    check added == 1
    check live.len == 1
    live.close()
    db.close()

  test "predicate that never matches stays empty":
    let dir = freshDir("glen_dsl_live_nomatch")
    let db = newGlenDB(dir)
    db.put("u", "1", %*{"role": "admin"})
    let live = liveQuery(db, "u"):
      where: role == "godmode"   # nothing matches
    var anyEvent = 0
    discard live.onChange(proc (ev: LiveQueryEvent) = inc anyEvent)
    check anyEvent == 0
    db.put("u", "2", %*{"role": "admin"})
    db.put("u", "3", %*{"role": "guest"})
    check anyEvent == 0
    check live.len == 0
    live.close()
    db.close()

  test "close() is idempotent":
    let dir = freshDir("glen_dsl_live_close2")
    let db = newGlenDB(dir)
    let live = liveQuery(db, "u"):
      where: role == "admin"
    live.close()
    live.close()   # second close must not crash
    check live.len == 0
    db.close()

# ---- watch -------------------------------------------------------------

suite "dsl: watch":
  test "doc and collection handlers fire; close unsubscribes":
    let dir = freshDir("glen_dsl_watch")
    let db = newGlenDB(dir)
    var docHits = 0
    var collHits = 0
    let scope = watch(db):
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

# ---- sync --------------------------------------------------------------

suite "dsl: sync":
  test "two in-memory peers round-trip a write":
    let dirA = freshDir("glen_dsl_sync_a")
    let dirB = freshDir("glen_dsl_sync_b")
    let dbA = newGlenDB(dirA)
    let dbB = newGlenDB(dirB)
    let tA = InMemoryTransport()
    let tB = InMemoryTransport()
    tA.peerOf = tB
    tB.peerOf = tA

    let syncA = sync(dbA):
      peer "B":
        transport: tA
        intervalMs: 0
    let syncB = sync(dbB):
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
