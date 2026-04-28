## Tests for the Glen DSL surface (literal, query, txn, schema, watch,
## sync, collection proxy).

import std/[unittest, os, strutils, options, sets]
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

  test "putUsersMany / getUsersMany — schemaless-key form":
    let dir = freshDir("glen_dsl_bulk_typed")
    let db = newGlenDB(dir)
    let written = putUsersMany(db, [
      ("u1", Users(name: "Alice", age: 30'i64, email: "a@x.io")),
      ("u2", Users(name: "Bob",   age: 25'i64, email: "b@x.io")),
      ("u3", Users(name: "Carol", age: 41'i64, email: "c@x.io"))
    ])
    check written == @["u1", "u2", "u3"]
    let rows = getUsersMany(db, ["u1", "u3", "u404"])
    check rows.len == 2   # u404 missing → silently dropped
    var names: seq[string] = @[]
    for (id, u) in rows: names.add(u.name)
    check "Alice" in names and "Carol" in names
    db.close()

# ---- add() — auto-generated ids -------------------------------------------

suite "dsl: add (auto-generated ULID-style ids)":
  test "db.add inserts and returns a fresh sortable id":
    let dir = freshDir("glen_dsl_add_db")
    let db = newGlenDB(dir)
    let id1 = db.add("notes", %*{"text": "first"})
    let id2 = db.add("notes", %*{"text": "second"})
    check id1.len == 26
    check id2.len == 26
    check id1 != id2
    # Lexicographic order ≈ insert order (ULID timestamp prefix).
    check id1 < id2
    check db.get("notes", id1)["text"].s == "first"
    check db.get("notes", id2)["text"].s == "second"
    db.close()

  test "Collection.add forwards through the proxy":
    let dir = freshDir("glen_dsl_add_coll")
    let db = newGlenDB(dir)
    let notes = db["notes"]
    let id = notes.add(%*{"text": "from collection"})
    check notes.get(id)["text"].s == "from collection"
    db.close()

  test "newId() before a txn enables cross-doc references":
    # `txn.add` is intentionally NOT provided (Nim's overload resolution
    # surfaces a template/helper ambiguity when `add`'s many stdlib
    # overloads enter the picture). Generate the ids before the txn block
    # — they're in scope inside the body and after for assertions.
    let dir = freshDir("glen_dsl_add_txn_explicit")
    let db = newGlenDB(dir)
    let firstId = newId()
    let secondId = newId()
    let res = txn(db, retries = 0):
      txn.put("links", firstId, %*{"linksTo": secondId})
      txn.put("links", secondId, %*{"linksTo": firstId})
    check res.status == glentxn.csOk
    check db.get("links", firstId)["linksTo"].s == secondId
    check db.get("links", secondId)["linksTo"].s == firstId
    db.close()

  test "addUsers (typed) round-trips through getUsers":
    let dir = freshDir("glen_dsl_add_typed")
    let db = newGlenDB(dir)
    let id = addUsers(db, Users(
      name: "Alice", age: 30'i64, email: "alice@example.com"))
    check id.len == 26
    let (ok, fetched) = getUsers(db, id)
    check ok
    check fetched.name == "Alice"
    db.close()

  test "ids generated rapidly stay unique":
    let dir = freshDir("glen_dsl_add_unique")
    let db = newGlenDB(dir)
    var seen = initHashSet[string]()
    for i in 0 ..< 200:
      let id = db.add("k", %*{"i": i})
      check id notin seen
      seen.incl(id)
    db.close()

  test "newId() entropy: 100k ids fit a strict-monotone, fully-unique set":
    # Belt-and-suspenders: stress the same-ms path heavily. Within a single
    # thread the suffix is incremented monotonically, so there must be no
    # duplicates AND every successive id must be > the last.
    var seen = initHashSet[string]()
    var prev = ""
    for _ in 0 ..< 100_000:
      let id = newId()
      check id.len == 26
      check id notin seen
      check id > prev   # strict monotonicity
      seen.incl(id)
      prev = id

  test "newId() suffix entropy: distinct suffixes across ms boundaries":
    # When the ms ticks, the suffix is re-drawn from the OS RNG. Across
    # many ms-tick events we should see effectively unique suffixes.
    # Sleep ~1ms between calls to force a re-randomise each time, then
    # check the 16-char suffixes (chars 10..25) have no duplicates.
    var suffixes = initHashSet[string]()
    for _ in 0 ..< 50:
      let id = newId()
      let suffix = id[10 .. 25]
      check suffix notin suffixes
      suffixes.incl(suffix)
      sleep(2)   # cross a ms boundary

# ---- schema with key: <field> --------------------------------------------

schema sessions:
  key: token       # use the `token` string field as the docId
  fields:
    token: zString().minLen(8)
    user:  zString()
    role:  zString().default("member")

schema orders:
  key: orderNum    # non-string key — `int64` here, stringified via $
  fields:
    orderNum: zInt()
    sku:      zString()
    qty:      zInt().default(1)

suite "dsl: schema key: <field>":
  test "string key — putSessions / addSessions derive docId from token":
    let dir = freshDir("glen_dsl_key_string")
    let db = newGlenDB(dir)
    let s = Sessions(
      token: "T-abcdef123", user: "alice", role: "admin")
    putSessions(db, s)
    # Doc is stored under the token, not a generated ULID.
    let raw = db.get(sessionsCollection, "T-abcdef123")
    check raw["user"].s == "alice"
    let (ok, fetched) = getSessions(db, "T-abcdef123")
    check ok and fetched.token == "T-abcdef123"

    # addSessions returns the derived id (= token), not a fresh ULID.
    let s2 = Sessions(token: "T-xyz9876543", user: "bob", role: "member")
    let returnedId = addSessions(db, s2)
    check returnedId == "T-xyz9876543"
    db.close()

  test "string key — repeated put is idempotent (single row)":
    let dir = freshDir("glen_dsl_key_idempotent")
    let db = newGlenDB(dir)
    putSessions(db, Sessions(token: "T-sameToken", user: "v1", role: "member"))
    putSessions(db, Sessions(token: "T-sameToken", user: "v2", role: "admin"))
    check db.getAll(sessionsCollection).len == 1
    let (ok, fetched) = getSessions(db, "T-sameToken")
    check ok
    check fetched.user == "v2"
    check fetched.role == "admin"
    db.close()

  test "non-string key — int64 stringified via $":
    let dir = freshDir("glen_dsl_key_int")
    let db = newGlenDB(dir)
    let returnedId = addOrders(db, Orders(
      orderNum: 42'i64, sku: "WIDGET-1", qty: 3))
    check returnedId == "42"
    let raw = db.get(ordersCollection, "42")
    check raw["sku"].s == "WIDGET-1"
    check raw["qty"].i == 3

    putOrders(db, Orders(orderNum: 7'i64, sku: "BOLT", qty: 1))
    let (ok, fetched) = getOrders(db, "7")
    check ok and fetched.sku == "BOLT"
    db.close()

  test "putOrdersMany — keyed bulk form derives ids":
    let dir = freshDir("glen_dsl_bulk_keyed")
    let db = newGlenDB(dir)
    let written = putOrdersMany(db, [
      Orders(orderNum: 1'i64, sku: "A", qty: 2),
      Orders(orderNum: 2'i64, sku: "B", qty: 5),
      Orders(orderNum: 3'i64, sku: "C", qty: 1)
    ])
    check written == @["1", "2", "3"]
    let rows = getOrdersMany(db, ["1", "3"])
    check rows.len == 2
    db.close()

# ---- softDelete: <field> --------------------------------------------------

schema posts2:
  softDelete: deletedAt
  fields:
    title:     zString()
    body:      zString()
    deletedAt: optional(zInt())

suite "dsl: schema softDelete":
  test "softDeletePosts2 stamps the tombstone; restorePosts2 clears it":
    let dir = freshDir("glen_dsl_soft_delete")
    let db = newGlenDB(dir)
    db.put(posts2Collection, "p1",
           %*{"title": "hello", "body": "world"})
    softDeletePosts2(db, "p1")
    let raw = db.get(posts2Collection, "p1")
    check raw["deletedAt"].kind == vkInt
    check raw["deletedAt"].i > 0

    # Typed read still works (deletedAt is optional, so it parses).
    let (ok, fetched) = getPosts2(db, "p1")
    check ok
    check fetched.deletedAt.isSome

    restorePosts2(db, "p1")
    let raw2 = db.get(posts2Collection, "p1")
    check raw2["deletedAt"].isNil
    let (_, fetched2) = getPosts2(db, "p1")
    check fetched2.deletedAt.isNone
    db.close()

  test "softDelete on missing row is a no-op":
    let dir = freshDir("glen_dsl_soft_delete_missing")
    let db = newGlenDB(dir)
    softDeletePosts2(db, "ghost")    # must not raise
    restorePosts2(db, "ghost")       # must not raise
    check db.get(posts2Collection, "ghost").isNil
    db.close()

# ---- index field forms (ident, tuple, string) ----------------------------

schema people:
  fields:
    name: zString()
    age:  zInt()
    city: zString()
  indexes:
    byName:    equality name              # bare ident
    byAge:     range    age               # bare ident
    byNameAge: equality (name, age)       # tuple → composite
    byCity:    equality "city"            # string literal still works

suite "dsl: schema index field syntax":
  test "ident, tuple, and string forms all register correctly":
    let dir = freshDir("glen_dsl_index_forms")
    let db = newGlenDB(dir)
    registerPeopleSchema(db)
    db.put(peopleCollection, "p1",
           %*{"name": "Alice", "age": 30, "city": "NYC"})
    db.put(peopleCollection, "p2",
           %*{"name": "Alice", "age": 25, "city": "LA"})
    db.put(peopleCollection, "p3",
           %*{"name": "Bob",   "age": 30, "city": "NYC"})

    # Single-field equality (ident form)
    check db.findBy(peopleCollection, "byName", VString("Alice")).len == 2

    # Single-field equality (string form)
    check db.findBy(peopleCollection, "byCity", VString("NYC")).len == 2

    # Composite equality on (name, age) — Alice@30 picks p1 only
    let composite = db.findBy(peopleCollection, "byNameAge",
      VArray(@[VString("Alice"), VInt(30)]))
    check composite.len == 1
    check composite[0][0] == "p1"
    db.close()

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

# ---- strict: true — error on undeclared fields ---------------------------

schema strictItems:
  strict: true
  fields:
    name: zString()
    qty:  zInt()

suite "dsl: schema strict mode":
  test "strict: parses ok when only declared fields are present":
    let r = parseStrictItems(%*{"name": "widget", "qty": 3})
    check r.ok
    check r.value.name == "widget"
    check r.value.qty == 3

  test "strict: rejects an unknown field with a clear issue":
    let r = parseStrictItems(%*{"name": "widget", "qty": 3, "rogue": 1})
    check not r.ok
    var sawRogueIssue = false
    for issue in r.issues:
      if issue.message.contains("rogue"):
        sawRogueIssue = true
    check sawRogueIssue

  test "strict: still loaded raw doc keeps the extras (storage is unchanged)":
    let dir = freshDir("glen_dsl_strict_storage")
    let db = newGlenDB(dir)
    db.put(strictItemsCollection, "x1",
           %*{"name": "w", "qty": 1, "rogue": 99})
    # Raw read retains the rogue field — strict mode only affects parsing.
    check db.get(strictItemsCollection, "x1")["rogue"].i == 99
    # But typed read fails because of the strict check.
    let r = parseStrictItems(db.get(strictItemsCollection, "x1"))
    check not r.ok
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

# ---- query: select / count / first / exists -------------------------------

suite "dsl: query projections + reductions":
  test "select: projects each row to listed fields":
    let dir = freshDir("glen_dsl_select")
    let db = newGlenDB(dir)
    db.put("u", "1", %*{"name": "alice", "age": 30, "secret": "x"})
    db.put("u", "2", %*{"name": "bob",   "age": 25, "secret": "y"})
    let projected = query(db, "u"):
      select:
        name
        age
    check projected.len == 2
    for (id, doc) in projected:
      check not doc["name"].isNil
      check not doc["age"].isNil
      check doc["secret"].isNil   # stripped
    db.close()

  test "select: dotted path uses leaf as key":
    let dir = freshDir("glen_dsl_select_dot")
    let db = newGlenDB(dir)
    db.put("u", "1", %*{"name": "alice", "addr": {"city": "NYC", "zip": 10001}})
    let r = query(db, "u"):
      select:
        name
        addr.city
    check r.len == 1
    check r[0][1]["name"].s == "alice"
    check r[0][1]["city"].s == "NYC"
    db.close()

  test "count: returns int":
    let dir = freshDir("glen_dsl_count")
    let db = newGlenDB(dir)
    db.put("u", "1", %*{"role": "admin"})
    db.put("u", "2", %*{"role": "guest"})
    db.put("u", "3", %*{"role": "admin"})
    let n = query(db, "u"):
      where: role == "admin"
      count: ()
    check n == 2
    db.close()

  test "first: returns ok=true with row, ok=false on empty":
    let dir = freshDir("glen_dsl_first")
    let db = newGlenDB(dir)
    db.put("u", "1", %*{"name": "alice", "age": 30})
    db.put("u", "2", %*{"name": "bob",   "age": 25})
    let hit = query(db, "u"):
      where: age >= 30
      orderBy: age desc
      first: ()
    check hit.ok and hit.id == "1" and hit.value["name"].s == "alice"
    let miss = query(db, "u"):
      where: age >= 100
      first: ()
    check not miss.ok
    db.close()

  test "exists: returns bool":
    let dir = freshDir("glen_dsl_exists")
    let db = newGlenDB(dir)
    db.put("u", "1", %*{"role": "admin"})
    let yes = query(db, "u"):
      where: role == "admin"
      exists: ()
    check yes
    let no = query(db, "u"):
      where: role == "godmode"
      exists: ()
    check not no
    db.close()

  test "page: bundles rows + cursor + hasMore":
    let dir = freshDir("glen_dsl_page")
    let db = newGlenDB(dir)
    for i in 0 ..< 25:
      let id = (if i < 10: "0" & $i else: $i)   # zero-pad for sortability
      db.put("u", id, %*{"i": i})
    # First page of 10 — should be hasMore=true.
    let p1 = query(db, "u"):
      orderBy: i asc
      limit: 10
      page: ()
    check p1.rows.len == 10
    check p1.hasMore
    check p1.cursor.len > 0
    check p1.rows[0][1]["i"].i == 0
    check p1.rows[9][1]["i"].i == 9
    # Second page — start after the previous cursor.
    let p2 = query(db, "u"):
      orderBy: i asc
      limit: 10
      after: p1.cursor
      page: ()
    check p2.rows.len == 10
    check p2.hasMore
    check p2.rows[0][1]["i"].i == 10
    # Third page — only 5 left, hasMore must be false.
    let p3 = query(db, "u"):
      orderBy: i asc
      limit: 10
      after: p2.cursor
      page: ()
    check p3.rows.len == 5
    check not p3.hasMore
    db.close()

  test "page: with no limit — hasMore is always false":
    let dir = freshDir("glen_dsl_page_unlim")
    let db = newGlenDB(dir)
    for i in 0 ..< 5:
      db.put("u", $i, %*{"i": i})
    let p = query(db, "u"):
      page: ()
    check p.rows.len == 5
    check not p.hasMore

# ---- query: geo / vector prefilter ----------------------------------------

suite "dsl: query near / similar prefilters":
  test "near: filters by haversine radius then post-filters predicates":
    let dir = freshDir("glen_dsl_near")
    let db = newGlenDB(dir)
    db.put("p", "nyc",    %*{"name": "NYC",    "lon": -73.98, "lat": 40.75, "open": true})
    db.put("p", "philly", %*{"name": "Philly", "lon": -75.16, "lat": 39.95, "open": true})
    db.put("p", "boston", %*{"name": "Boston", "lon": -71.06, "lat": 42.36, "open": false})
    db.put("p", "la",     %*{"name": "LA",     "lon": -118.24, "lat": 34.05, "open": true})
    db.createGeoIndex("p", "byLoc", "lon", "lat")

    # Within ~200km of NYC, only NYC + Philly qualify
    let near200 = query(db, "p"):
      near: ("byLoc", -73.98, 40.75, 200_000.0)
    check near200.len == 2
    var ids: seq[string] = @[]
    for (id, _) in near200: ids.add(id)
    check "nyc" in ids and "philly" in ids

    # Same prefilter, post-filter to open=true → drops Boston (out of range
    # anyway) but importantly keeps where: working alongside near:
    let openNear = query(db, "p"):
      near: ("byLoc", -73.98, 40.75, 500_000.0)
      where: open == true
    var openIds: seq[string] = @[]
    for (id, _) in openNear: openIds.add(id)
    check "boston" notin openIds   # filtered out by `open == true`
    check "nyc" in openIds and "philly" in openIds
    db.close()

  test "near: composes with select and count":
    let dir = freshDir("glen_dsl_near_select")
    let db = newGlenDB(dir)
    db.put("p", "a", %*{"name": "A", "lon": -73.0, "lat": 40.0})
    db.put("p", "b", %*{"name": "B", "lon": -73.5, "lat": 40.5})
    db.createGeoIndex("p", "byLoc", "lon", "lat")
    let n = query(db, "p"):
      near: ("byLoc", -73.0, 40.0, 500_000.0)
      count: ()
    check n == 2
    let projected = query(db, "p"):
      near: ("byLoc", -73.0, 40.0, 500_000.0)
      select:
        name
    check projected.len == 2
    check not projected[0][1]["name"].isNil
    check projected[0][1]["lon"].isNil   # stripped
    db.close()

# ---- engines: series + tiles ---------------------------------------------

suite "dsl: engine proxies":
  test "db.series writes to <db.dir>/series/<name>.gts and round-trips":
    let dir = freshDir("glen_dsl_series")
    let db = newGlenDB(dir)
    let s = db.series("temp.s1")
    s.append(1000, 22.5)
    s.append(2000, 22.7)
    s.append(3000, 23.0)
    s.flush()
    check fileExists(seriesPath(db, "temp.s1"))
    check seriesExists(db, "temp.s1")
    let rng = s.range(0, 3500)
    check rng.len == 3
    check rng[0] == (1000'i64, 22.5)
    s.close()
    db.close()

  test "withSeries auto-closes":
    let dir = freshDir("glen_dsl_with_series")
    let db = newGlenDB(dir)
    withSeries(db, "metrics") do:
      s.append(100, 1.0)
      s.append(200, 2.0)
      s.flush()
    # After the template exits, file is closed and we can re-open it.
    let s2 = db.series("metrics")
    let r = s2.range(0, 500)
    check r.len == 2
    s2.close()
    db.close()

  test "db.tiles creates a stack under <db.dir>/tiles/<name>/ and openTiles attaches":
    let dir = freshDir("glen_dsl_tiles")
    let db = newGlenDB(dir)
    let bb = bbox(0.0, 0.0, 10.0, 10.0)
    let stack = db.tiles("radar", bb, rows = 4, cols = 4, channels = 1)
    var mesh = newGeoMesh(bb, 4, 4, 1)
    for r in 0 ..< 4:
      for c in 0 ..< 4:
        mesh[r, c, 0] = float64(r * 4 + c)
    stack.appendFrame(1000, mesh)
    stack.flush()
    stack.close()

    check tilesExists(db, "radar")
    let again = db.openTiles("radar")
    check again.rows == 4 and again.cols == 4
    let history = again.readPointHistory(5.0, 5.0, 0, 2000)
    check history.len == 1
    again.close()
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

  test "liveCount: tracks current count and fires on change":
    let dir = freshDir("glen_dsl_live_count")
    let db = newGlenDB(dir)
    db.put("u", "1", %*{"role": "admin"})
    db.put("u", "2", %*{"role": "guest"})
    let count = liveCount(db, "u"):
      where: role == "admin"
    check count.value == 1
    var fired: seq[int] = @[]
    count.onChange(proc (n: int) = fired.add(n))
    check fired == @[1]   # immediate hydrate
    db.put("u", "3", %*{"role": "admin"})    # +1 → 2
    db.put("u", "1", %*{"role": "guest"})    # -1 → 1
    db.put("u", "2", %*{"role": "guest"})    # no change
    check count.value == 1
    check fired == @[1, 2, 1]
    count.close()
    db.close()

  test "liveExists: fires only on true ↔ false transitions":
    let dir = freshDir("glen_dsl_live_exists")
    let db = newGlenDB(dir)
    let exists = liveExists(db, "u"):
      where: role == "admin"
    var fired: seq[bool] = @[]
    exists.onChange(proc (b: bool) = fired.add(b))
    check fired == @[false]   # initial state
    check exists.value == false
    db.put("u", "1", %*{"role": "admin"})       # false → true
    db.put("u", "2", %*{"role": "admin"})       # true → true (no fire)
    db.put("u", "3", %*{"role": "admin"})       # still true
    check exists.value == true
    db.delete("u", "1")
    db.delete("u", "2")
    db.delete("u", "3")                         # true → false
    check fired == @[false, true, false]
    exists.close()
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

# ---- vectors: vec() helper, VectorIndex proxy, threshold ------------------

suite "dsl: vec32() helper":
  test "vec3232([a, b, c]) builds a seq[float32]":
    let v = vec32([1.0, 2.0, 3.0])
    check v is seq[float32]
    check v == @[1.0'f32, 2.0'f32, 3.0'f32]

  test "vec32 accepts mixed-precision floats":
    let v = vec32([1.0, 2.5, 3.0])
    check v.len == 3
    check v[0] == 1.0'f32 and v[1] == 2.5'f32 and v[2] == 3.0'f32

  test "vec32 from a runtime seq[float64]":
    let raw: seq[float64] = @[1.0, 2.0, 3.0]
    let v = vec32(raw)
    check v == @[1.0'f32, 2.0'f32, 3.0'f32]

suite "dsl: VectorIndex proxy":
  test "create / upsert / search top-k":
    let dir = freshDir("glen_dsl_vectors")
    let db = newGlenDB(dir)
    let idx = db.vectors("docs", "byEmbed")
    idx.create("embedding", dim = 3)
    idx.upsert("a", %*{"text": "hello",   "embedding": [1.0, 0.0, 0.0]})
    idx.upsert("b", %*{"text": "world",   "embedding": [0.0, 1.0, 0.0]})
    idx.upsert("c", %*{"text": "hello!",  "embedding": [0.95, 0.05, 0.0]})

    # Searching with q ≈ a should return a and c at the top.
    let hits = idx.search(vec32([1.0, 0.0, 0.0]), k = 2)
    check hits.len == 2
    var ids: seq[string] = @[]
    for (id, _) in hits: ids.add(id)
    check "a" in ids and "c" in ids
    db.close()

  test "searchWithin: threshold-based retrieval":
    let dir = freshDir("glen_dsl_vectors_within")
    let db = newGlenDB(dir)
    let idx = db.vectors("docs", "byEmbed")
    idx.create("embedding", dim = 3)
    # Cosine distance: d = 1 - cos(a, b)
    idx.upsert("very_similar", %*{"embedding": [1.0, 0.0, 0.01]})    # d ≈ 0
    idx.upsert("kinda_similar", %*{"embedding": [0.7, 0.7, 0.0]})    # d ≈ 0.29
    idx.upsert("far",           %*{"embedding": [0.0, 1.0, 0.0]})    # d ≈ 1

    let close = idx.searchWithin(vec32([1.0, 0.0, 0.0]), maxDistance = 0.05)
    var closeIds: seq[string] = @[]
    for (id, _) in close: closeIds.add(id)
    check "very_similar" in closeIds
    check "kinda_similar" notin closeIds
    check "far" notin closeIds
    db.close()

  test "searchDocs returns matched documents inline":
    let dir = freshDir("glen_dsl_vectors_docs")
    let db = newGlenDB(dir)
    let idx = db.vectors("docs", "byEmbed")
    idx.create("embedding", dim = 2)
    idx.upsert("a", %*{"title": "first",  "embedding": [1.0, 0.0]})
    idx.upsert("b", %*{"title": "second", "embedding": [0.0, 1.0]})
    let triples = idx.searchDocs(vec32([1.0, 0.0]), k = 1)
    check triples.len == 1
    check triples[0][0] == "a"
    check triples[0][2]["title"].s == "first"
    db.close()

# ---- vectors: schema-level zVector(dim) -----------------------------------

schema notes:
  fields:
    title:     zString()
    embedding: zVector(3)
  indexes:
    byEmbed:   vector embedding, 3, vmCosine

suite "dsl: schema vector field":
  test "zVector field round-trips through typed put/get":
    let dir = freshDir("glen_dsl_schema_vec")
    let db = newGlenDB(dir)
    registerNotesSchema(db)
    putNotes(db, "n1", Notes(
      title: "alpha",
      embedding: @[1.0'f32, 2.0'f32, 3.0'f32]))
    let (ok, fetched) = getNotes(db, "n1")
    check ok
    check fetched.title == "alpha"
    check fetched.embedding == @[1.0'f32, 2.0'f32, 3.0'f32]
    # Index was registered too; nearest-neighbour lookup works.
    let hits = db.vectors(notesCollection, "byEmbed").search(
      vec32([1.0, 2.0, 3.0]), k = 1)
    check hits.len == 1
    check hits[0][0] == "n1"
    db.close()

  test "zVector rejects wrong dim and non-numeric values":
    let r1 = parseNotes(%*{"title": "t", "embedding": [1.0, 2.0]})
    check not r1.ok
    let r2 = parseNotes(%*{"title": "t", "embedding": [1.0, "x", 3.0]})
    check not r2.ok

# ---- query: similar: with threshold ---------------------------------------

suite "dsl: query similar: threshold":
  test "threshold-only similar:":
    let dir = freshDir("glen_dsl_similar_thr")
    let db = newGlenDB(dir)
    db.createVectorIndex("docs", "byEmbed", "embedding", 3, vmCosine)
    db.put("docs", "near",  %*{"embedding": [1.0, 0.0, 0.01], "kind": "a"})
    db.put("docs", "kinda", %*{"embedding": [0.7, 0.7, 0.0],  "kind": "a"})
    db.put("docs", "far",   %*{"embedding": [0.0, 1.0, 0.0],  "kind": "a"})

    let q = vec32([1.0, 0.0, 0.0])
    let hits = query(db, "docs"):
      similar: ("byEmbed", q, threshold = 0.05)
    var ids: seq[string] = @[]
    for (id, _) in hits: ids.add(id)
    check "near" in ids
    check "kinda" notin ids
    check "far" notin ids
    db.close()

  test "top-k AND threshold (positional 4-tuple)":
    let dir = freshDir("glen_dsl_similar_4tup")
    let db = newGlenDB(dir)
    db.createVectorIndex("docs", "byEmbed", "embedding", 3, vmCosine)
    for i in 0 ..< 10:
      let v = %*{
        "embedding": [1.0 - float(i)*0.05, float(i)*0.05, 0.0],
        "i": i}
      db.put("docs", $i, v)
    let q = vec32([1.0, 0.0, 0.0])
    # Cap at 2 results AND require distance ≤ 0.1 (cosine ≥ 0.9).
    let hits = query(db, "docs"):
      similar: ("byEmbed", q, 2, 0.1)
    check hits.len <= 2
    db.close()

# ---- geo proxy + polygonLit ------------------------------------------------

suite "dsl: GeoIndex proxy":
  test "create / near / nearest / inBBox":
    let dir = freshDir("glen_dsl_geo_proxy")
    let db = newGlenDB(dir)
    let g = db.geo("places", "byLoc")
    g.create("lon", "lat")
    db.put("places", "nyc",    %*{"name": "NYC",    "lon": -73.98, "lat": 40.75})
    db.put("places", "philly", %*{"name": "Philly", "lon": -75.16, "lat": 39.95})
    db.put("places", "la",     %*{"name": "LA",     "lon": -118.24, "lat": 34.05})

    # 200 km of NYC catches Philly, not LA.
    let nearby = g.near(-73.98, 40.75, 200_000.0)
    var ids: seq[string] = @[]
    for (id, _, _) in nearby: ids.add(id)
    check "nyc" in ids and "philly" in ids and "la" notin ids

    # Top-2 nearest from LA.
    let knn = g.nearest(-118.24, 34.05, k = 2)
    check knn.len == 2
    check knn[0][0] == "la"

    # BBox covering just the NE corner.
    let inBox = g.inBBox(-80.0, 35.0, -70.0, 45.0)
    var boxIds: seq[string] = @[]
    for (id, _) in inBox: boxIds.add(id)
    check "nyc" in boxIds and "philly" in boxIds and "la" notin boxIds
    db.close()

suite "dsl: polygonLit":
  test "polygonLit + readPolygon round-trip":
    let region = polygonLit [
      (-74.0, 40.7), (-73.9, 40.7), (-73.9, 40.8), (-74.0, 40.8)]
    check region.kind == vkArray
    check region.arr.len == 4
    let p = readPolygon(region)
    check p.vertices.len == 4
    check p.vertices[0] == (-74.0, 40.7)

  test "polygonLit feeds findPointsInPolygon via the geo proxy":
    let dir = freshDir("glen_dsl_polygonlit")
    let db = newGlenDB(dir)
    let g = db.geo("places", "byLoc")
    g.create("lon", "lat")
    db.put("places", "in1",  %*{"lon": -73.95, "lat": 40.75})
    db.put("places", "out1", %*{"lon": -75.16, "lat": 39.95})

    let manhattan = readPolygon(polygonLit [
      (-74.02, 40.70), (-73.93, 40.70),
      (-73.93, 40.80), (-74.02, 40.80)])
    let hits = g.inPolygon(manhattan)
    var ids: seq[string] = @[]
    for (id, _) in hits: ids.add(id)
    check "in1" in ids
    check "out1" notin ids
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
