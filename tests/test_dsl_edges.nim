## Extensive edge-case coverage for the Glen DSL surface. The main
## `test_dsl.nim` covers the happy paths; this file focuses on the
## awkward corners — empty inputs, missing fields, type mismatches,
## boundary conditions, and behaviours documented in `docs/dsl.md`
## that aren't otherwise locked behind tests.

import std/[unittest, os, math, options, sets, strutils, tables]
import glen/glen
import glen/types, glen/db as glendb, glen/txn as glentxn
import glen/dsl

proc freshDir(name: string): string =
  result = getTempDir() / name
  removeDir(result)
  createDir(result)

# ===========================================================================
# %* literal — corner cases
# ===========================================================================

suite "edges: %* literal":
  test "empty object and empty array":
    let o = %*{}
    check o.kind == vkObject and o.obj.len == 0
    let a = %*[]
    check a.kind == vkArray and a.arr.len == 0

  test "deeply nested with mixed types":
    let v = %*{
      "a": [{"b": [1, 2, {"c": "deep"}]}, true, nil],
      "n": -42,
      "f": -3.14
    }
    check v["a"].arr[0]["b"].arr[2]["c"].s == "deep"
    check v["a"].arr[1].b == true
    check v["a"].arr[2].kind == vkNull
    check v["n"].i == -42
    check v["f"].f == -3.14

  test "special floats round-trip through %*":
    let v = %*{"inf": Inf, "nan": NaN, "neg": -Inf, "zero": 0.0, "negz": -0.0}
    check v["inf"].f == Inf
    check v["nan"].f.classify == fcNaN
    check v["neg"].f == -Inf
    check v["zero"].f == 0.0
    check v["negz"].f == -0.0

  test "ident keys equal string keys":
    let a = %*{name: "alice"}
    let b = %*{"name": "alice"}
    check a == b

  test "array of objects":
    let v = %*[{"id": 1}, {"id": 2}, {"id": 3}]
    check v.kind == vkArray
    check v.arr.len == 3
    check v.arr[1]["id"].i == 2

  test "interpolated string variable":
    let name = "Alice"
    let v = %*{"hello": name}
    check v["hello"].s == "Alice"

  test "interpolated seq becomes VArray":
    let xs = @[1, 2, 3]
    let v = %*{"xs": xs}
    check v["xs"].kind == vkArray
    check v["xs"].arr.len == 3
    check v["xs"].arr[0].i == 1

# ===========================================================================
# Collection proxy — edges
# ===========================================================================

suite "edges: Collection proxy":
  test "iteration over empty collection yields nothing":
    let dir = freshDir("glen_edge_coll_empty")
    let db = newGlenDB(dir)
    let c = db["empty"]
    var n = 0
    for _ in c: inc n
    check n == 0
    check c.getAll().len == 0
    db.close()

  test "double-delete is a no-op":
    let dir = freshDir("glen_edge_coll_double_del")
    let db = newGlenDB(dir)
    let c = db["users"]
    c.put("u1", %*{"x": 1})
    c.delete("u1")
    c.delete("u1")    # must not raise
    check not c.contains("u1")
    db.close()

  test "putMany / deleteMany via proxy":
    let dir = freshDir("glen_edge_coll_bulk")
    let db = newGlenDB(dir)
    let c = db["k"]
    c.putMany([("a", %*{"v": 1}), ("b", %*{"v": 2}), ("c", %*{"v": 3})])
    check c.getAll().len == 3
    c.deleteMany(["a", "c"])
    check c.getAll().len == 1
    check c.contains("b")
    db.close()

  test "very long ids round-trip":
    let dir = freshDir("glen_edge_coll_longid")
    let db = newGlenDB(dir)
    let longId = "x".repeat(1000)
    db["k"].put(longId, %*{"v": 42})
    check db["k"].get(longId)["v"].i == 42
    db.close()

# ===========================================================================
# query — corner cases
# ===========================================================================

suite "edges: query block":
  test "empty result set returns empty seq":
    let dir = freshDir("glen_edge_q_empty")
    let db = newGlenDB(dir)
    db.put("u", "1", %*{"role": "admin"})
    let rows = query(db, "u"):
      where: role == "godmode"
    check rows.len == 0
    db.close()

  test "query against non-existent collection returns empty":
    let dir = freshDir("glen_edge_q_noColl")
    let db = newGlenDB(dir)
    let rows = query(db, "ghost"):
      where: x == 1
    check rows.len == 0
    db.close()

  test "deeply nested field paths":
    let dir = freshDir("glen_edge_q_nested")
    let db = newGlenDB(dir)
    db.put("u", "1", %*{"profile": {"contact": {"email": "a@x.io"}}})
    db.put("u", "2", %*{"profile": {"contact": {"email": "b@x.io"}}})
    let rows = query(db, "u"):
      where: profile.contact.email == "a@x.io"
    check rows.len == 1
    check rows[0][0] == "1"
    db.close()

  test "all six comparison operators":
    let dir = freshDir("glen_edge_q_ops")
    let db = newGlenDB(dir)
    for i in 0 ..< 5: db.put("k", $i, %*{"i": i})
    let eq = query(db, "k"):
      where: i == 2
    let ne = query(db, "k"):
      where: i != 2
    let lt = query(db, "k"):
      where: i < 2
    let le = query(db, "k"):
      where: i <= 2
    let gt = query(db, "k"):
      where: i > 2
    let ge = query(db, "k"):
      where: i >= 2
    check eq.len == 1
    check ne.len == 4
    check lt.len == 2
    check le.len == 3
    check gt.len == 2
    check ge.len == 3
    db.close()

  test "where in [] (empty list) returns nothing":
    let dir = freshDir("glen_edge_q_in_empty")
    let db = newGlenDB(dir)
    db.put("u", "1", %*{"role": "admin"})
    let rows = query(db, "u"):
      where: role in []
    check rows.len == 0
    db.close()

  test "contains is case-sensitive":
    let dir = freshDir("glen_edge_q_contains")
    let db = newGlenDB(dir)
    db.put("u", "1", %*{"name": "Alice"})
    let r1 = query(db, "u"):
      where: name.contains("alice")
    check r1.len == 0
    let r2 = query(db, "u"):
      where: name.contains("Alice")
    check r2.len == 1
    db.close()

  test "multiple orderBy with mixed asc/desc":
    let dir = freshDir("glen_edge_q_multi_order")
    let db = newGlenDB(dir)
    db.put("u", "1", %*{"team": "a", "score": 10})
    db.put("u", "2", %*{"team": "a", "score": 30})
    db.put("u", "3", %*{"team": "b", "score": 20})
    db.put("u", "4", %*{"team": "b", "score": 5})
    let rows = query(db, "u"):
      orderBy:
        team asc
        score desc
    # team a (desc score): 2, 1; team b (desc score): 3, 4
    check rows[0][0] == "2"
    check rows[1][0] == "1"
    check rows[2][0] == "3"
    check rows[3][0] == "4"
    db.close()

  test "limit 0 returns everything":
    let dir = freshDir("glen_edge_q_limit_zero")
    let db = newGlenDB(dir)
    for i in 0 ..< 7: db.put("k", $i, %*{"i": i})
    let rows = query(db, "k"):
      limit: 0
    check rows.len == 7
    db.close()

  test "page() with no results: hasMore=false, empty rows":
    let dir = freshDir("glen_edge_q_page_empty")
    let db = newGlenDB(dir)
    let p = query(db, "k"):
      where: x == 1
      limit: 10
      page: ()
    check p.rows.len == 0
    check not p.hasMore

  test "select followed by orderBy works":
    let dir = freshDir("glen_edge_q_select_order")
    let db = newGlenDB(dir)
    db.put("u", "1", %*{"name": "alice", "age": 30, "secret": "x"})
    db.put("u", "2", %*{"name": "bob",   "age": 25, "secret": "y"})
    let rows = query(db, "u"):
      orderBy: age desc
      select:
        name
        age
    check rows.len == 2
    check rows[0][1]["age"].i == 30
    check rows[0][1]["secret"].isNil    # stripped
    db.close()

# ===========================================================================
# txn — retry, rollback, edge cases
# ===========================================================================

suite "edges: txn":
  test "txn retry actually re-runs body on conflict":
    # Simulate a conflict: read a doc, then have another writer race
    # ahead and update it before we commit. Our txn should detect the
    # version mismatch and retry; the second attempt sees the new value.
    let dir = freshDir("glen_edge_txn_retry")
    let db = newGlenDB(dir)
    db.put("k", "v", %*{"n": 0})
    var attempts = 0
    let res = txn(db, retries = 3):
      inc attempts
      let cur = txn.get("k", "v")
      if attempts == 1:
        # First attempt: simulate concurrent write *after* our read but
        # before our commit. Bumps the doc's version, so commit will fail.
        db.put("k", "v", %*{"n": cur["n"].i + 100})
      txn.put("k", "v", %*{"n": cur["n"].i + 1})
    check res.status == glentxn.csOk
    check attempts == 2
    # Final state: 0 + 100 (concurrent) + 1 (our second attempt re-read) = 101
    check db.get("k", "v")["n"].i == 101
    db.close()

  test "exception during txn leaves no partial writes":
    let dir = freshDir("glen_edge_txn_no_partial")
    let db = newGlenDB(dir)
    db.put("k", "a", %*{"n": 1})
    let res = txn(db, retries = 0):
      txn.put("k", "a", %*{"n": 999})
      txn.put("k", "b", %*{"n": 999})
      raise newException(ValueError, "boom")
    check res.status == glentxn.csInvalid
    # Original a unchanged; b never created.
    check db.get("k", "a")["n"].i == 1
    check db.get("k", "b").isNil
    db.close()

  test "retries exhausted returns csConflict":
    let dir = freshDir("glen_edge_txn_exhaust")
    let db = newGlenDB(dir)
    db.put("k", "v", %*{"n": 0})
    var attempts = 0
    let res = txn(db, retries = 2):
      inc attempts
      let cur = txn.get("k", "v")
      # Always race ahead before commit — we'll never win.
      db.put("k", "v", %*{"n": cur["n"].i + 1})
      txn.put("k", "v", %*{"n": cur["n"].i + 100})
    check attempts == 3   # 1 + retries
    check res.status == glentxn.csConflict
    db.close()

  test "txn.delete + txn.put on same id in one transaction":
    let dir = freshDir("glen_edge_txn_del_put")
    let db = newGlenDB(dir)
    db.put("k", "x", %*{"n": 1})
    let res = txn(db, retries = 0):
      txn.delete("k", "x")
      txn.put("k", "x", %*{"n": 2})
    check res.status == glentxn.csOk
    check db.get("k", "x")["n"].i == 2
    db.close()

# ===========================================================================
# schema — corner cases beyond the happy path
# ===========================================================================

schema accountsX:
  version: 3
  fields:
    name:    zString()
    balance: zInt().default(0)
    role:    zString().default("member")
  migrations:
    0 -> 1:
      doc["role"] = VString("member")
    1 -> 2:
      doc["balance"] = VInt(0)
    2 -> 3:
      # Rename "credit" → "balance" if still present.
      let legacy = doc["credit"]
      if not legacy.isNil:
        doc["balance"] = legacy

suite "edges: schema migrations":
  test "doc starting from arbitrary baseline (v1) skips earlier migrations":
    let dir = freshDir("glen_edge_mig_baseline")
    let db = newGlenDB(dir)
    # v1: has role but no balance, "credit" still around.
    db.put(accountsXCollection, "old1",
      %*{"name": "Alice", "role": "admin", "_v": 1, "credit": 50})
    # v2: balance present, no credit.
    db.put(accountsXCollection, "newish",
      %*{"name": "Bob", "role": "member", "_v": 2, "balance": 10})
    migrateAccountsX(db)
    let r1 = db.get(accountsXCollection, "old1")
    # Baseline v1 → only the 1->2 (set balance=0) and 2->3 (rename credit) ran.
    # 1->2 sets balance=0, then 2->3 sees credit and overwrites balance=50.
    check r1["balance"].i == 50
    check r1["_v"].i == 3
    let r2 = db.get(accountsXCollection, "newish")
    # Baseline v2 → only 2->3 ran. No "credit" field, so balance stays 10.
    check r2["balance"].i == 10
    check r2["_v"].i == 3
    db.close()

  test "migrate is idempotent — second run is a no-op":
    let dir = freshDir("glen_edge_mig_idem")
    let db = newGlenDB(dir)
    db.put(accountsXCollection, "x",
      %*{"name": "C", "role": "admin", "balance": 7, "_v": 0})
    migrateAccountsX(db)
    let after1 = db.get(accountsXCollection, "x").clone()
    migrateAccountsX(db)
    let after2 = db.get(accountsXCollection, "x")
    check after1 == after2
    db.close()

# Schema without `key:` — putUsers requires explicit id.
schema basicEntries:
  fields:
    title: zString()

# Schema with `key:` — putBasicKeyed is one-arg.
schema basicKeyed:
  key: slug
  fields:
    slug:  zString()
    title: zString()

suite "edges: schema generated procs":
  test "registerSchema is idempotent":
    let dir = freshDir("glen_edge_sch_register_idem")
    let db = newGlenDB(dir)
    registerBasicEntriesSchema(db)
    registerBasicEntriesSchema(db)   # second call must not raise
    db.close()

  test "validate succeeds + fails on the same record":
    let good = validateBasicEntries(%*{"title": "ok"})
    check good.ok
    let bad = validateBasicEntries(%*{"title": 42})    # wrong type
    check not bad.ok

  test "putBasicKeyed + getBasicKeyed via derived id":
    let dir = freshDir("glen_edge_sch_keyed")
    let db = newGlenDB(dir)
    putBasicKeyed(db, BasicKeyed(slug: "post-1", title: "Hello"))
    let (ok, fetched) = getBasicKeyed(db, "post-1")
    check ok
    check fetched.title == "Hello"
    db.close()

# ===========================================================================
# liveQuery — corner cases
# ===========================================================================

suite "edges: liveQuery":
  test "predicate against missing field — never matches":
    let dir = freshDir("glen_edge_lq_missing")
    let db = newGlenDB(dir)
    db.put("u", "1", %*{"name": "alice"})        # no `role` field
    let live = liveQuery(db, "u"):
      where: role == "admin"
    check live.len == 0
    db.put("u", "2", %*{"name": "bob"})            # also no role
    check live.len == 0
    db.put("u", "3", %*{"role": "admin"})          # now matches
    check live.len == 1
    live.close()
    db.close()

  test "predicate type mismatch: equality fails, ordering falls through":
    # Documented in `glen/query.nim`: mismatched-type comparisons compare
    # via the ValueKind enum ordinal so the sort stays total. So
    # `string >= int` is *not* "no match" — it follows the kind order.
    # This test pins that behaviour so future planner changes can't
    # silently flip it.
    let dir = freshDir("glen_edge_lq_typemix")
    let db = newGlenDB(dir)
    db.put("u", "1", %*{"score": "high"})         # string, not int
    db.put("u", "2", %*{"score": 100})            # int

    # Equality across types: never matches (cross-type values never compare equal).
    let eq = liveQuery(db, "u"):
      where: score == 50
    check eq.len == 0
    eq.close()

    # Ordering across types: kind ordinal decides. vkInt=2, vkString=4 →
    # string > int, so a string score IS considered ≥ 50 by the planner.
    let ge = liveQuery(db, "u"):
      where: score >= 50
    # Both rows match: u2 because 100 >= 50, u1 because string > int by ordinal.
    check ge.len == 2
    ge.close()
    db.close()

  test "two liveQueries on same collection are independent":
    let dir = freshDir("glen_edge_lq_two")
    let db = newGlenDB(dir)
    let admins = liveQuery(db, "u"):
      where: role == "admin"
    let guests = liveQuery(db, "u"):
      where: role == "guest"
    db.put("u", "a", %*{"role": "admin"})
    db.put("u", "b", %*{"role": "guest"})
    db.put("u", "c", %*{"role": "admin"})
    check admins.len == 2
    check guests.len == 1
    admins.close()
    db.put("u", "d", %*{"role": "guest"})
    check guests.len == 2     # other live still works
    guests.close()
    db.close()

  test "match-then-unmatch-then-match-again":
    let dir = freshDir("glen_edge_lq_flap")
    let db = newGlenDB(dir)
    let live = liveQuery(db, "u"):
      where: active == true
    var added, removed: int
    discard live.onChange(proc (ev: LiveQueryEvent) =
      case ev.kind
      of lqAdded:   inc added
      of lqRemoved: inc removed
      else: discard)
    db.put("u", "x", %*{"active": true})    # added
    db.put("u", "x", %*{"active": false})   # removed
    db.put("u", "x", %*{"active": true})    # added again
    check added == 2
    check removed == 1
    live.close()
    db.close()

  test "snapshot is consistent — not a live view":
    let dir = freshDir("glen_edge_lq_snap")
    let db = newGlenDB(dir)
    db.put("u", "1", %*{"r": "a"})
    let live = liveQuery(db, "u"):
      where: r == "a"
    let snap = live.snapshot()
    db.put("u", "2", %*{"r": "a"})
    # Snapshot taken BEFORE the second put — len reflects that.
    check snap.len == 1
    check live.len == 2
    live.close()
    db.close()

# ===========================================================================
# liveCount / liveExists — edges
# ===========================================================================

suite "edges: live aggregations":
  test "liveCount: deleting a row drops the count":
    let dir = freshDir("glen_edge_lc_del")
    let db = newGlenDB(dir)
    db.put("u", "1", %*{"role": "admin"})
    db.put("u", "2", %*{"role": "admin"})
    let lc = liveCount(db, "u"):
      where: role == "admin"
    check lc.value == 2
    db.delete("u", "1")
    check lc.value == 1
    db.delete("u", "2")
    check lc.value == 0
    lc.close()
    db.close()

  test "liveExists: callback runs once on register with current state":
    let dir = freshDir("glen_edge_le_register")
    let db = newGlenDB(dir)
    db.put("u", "1", %*{"role": "admin"})
    let le = liveExists(db, "u"):
      where: role == "admin"
    var seen: seq[bool] = @[]
    le.onChange(proc (b: bool) = seen.add(b))
    check seen == @[true]                # initial hydrate
    le.close()
    db.close()

# ===========================================================================
# add() / newId — edges
# ===========================================================================

suite "edges: add() and newId":
  test "newId() always produces 26-char Crockford strings":
    const Crockford = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"
    for _ in 0 ..< 50:
      let id = newId()
      check id.len == 26
      for c in id:
        check c in Crockford

  test "db.add and db.put can coexist; iteration sees both":
    let dir = freshDir("glen_edge_add_mixed")
    let db = newGlenDB(dir)
    discard db.add("k", %*{"src": "auto"})
    db.put("k", "manual", %*{"src": "manual"})
    discard db.add("k", %*{"src": "auto"})
    check db.getAll("k").len == 3
    db.close()

  test "addUsers with key returns the key value, not a ULID":
    # Already partly tested; this ensures consistency over many calls.
    let dir = freshDir("glen_edge_add_keyed")
    let db = newGlenDB(dir)
    let returned = addBasicKeyed(db, BasicKeyed(slug: "p1", title: "x"))
    check returned == "p1"
    db.close()

# ===========================================================================
# Vector index — edges
# ===========================================================================

suite "edges: vectors":
  test "vec32 of an empty seq":
    let v = vec32(newSeq[float64]())
    check v.len == 0

  test "search against an empty index returns empty":
    let dir = freshDir("glen_edge_vec_empty_idx")
    let db = newGlenDB(dir)
    let idx = db.vectors("docs", "byEmbed")
    idx.create("embedding", dim = 3)
    let hits = idx.search(vec32([1.0, 0.0, 0.0]), k = 5)
    check hits.len == 0
    db.close()

  test "searchWithin maxResults clipping":
    let dir = freshDir("glen_edge_vec_clip")
    let db = newGlenDB(dir)
    let idx = db.vectors("docs", "byEmbed")
    idx.create("embedding", dim = 3)
    for i in 0 ..< 5:
      idx.upsert($i, %*{"embedding": [1.0, 0.0, 0.0]})  # all identical
    let hits = idx.searchWithin(vec32([1.0, 0.0, 0.0]),
                                maxDistance = 1.0,
                                maxResults = 3)
    check hits.len == 3
    db.close()

  test "vmL2 metric works (Euclidean)":
    let dir = freshDir("glen_edge_vec_l2")
    let db = newGlenDB(dir)
    db.createVectorIndex("docs", "byEmbed", "embedding", 2, vmL2)
    db.put("docs", "near", %*{"embedding": [1.0, 1.0]})
    db.put("docs", "far",  %*{"embedding": [10.0, 10.0]})
    let hits = db.findNearestVector("docs", "byEmbed",
                                    vec32([1.0, 1.0]), k = 1)
    check hits.len == 1
    check hits[0][0] == "near"
    db.close()

  test "wrong-dim insert is silently dropped (no exception)":
    let dir = freshDir("glen_edge_vec_dim_mismatch")
    let db = newGlenDB(dir)
    let idx = db.vectors("docs", "byEmbed")
    idx.create("embedding", dim = 3)
    idx.upsert("good", %*{"embedding": [1.0, 0.0, 0.0]})
    idx.upsert("bad",  %*{"embedding": [1.0, 0.0]})       # only 2 dims
    let hits = idx.search(vec32([1.0, 0.0, 0.0]), k = 5)
    var ids: seq[string] = @[]
    for (id, _) in hits: ids.add(id)
    check "good" in ids
    check "bad" notin ids
    db.close()

# ===========================================================================
# Geo proxy — edges
# ===========================================================================

suite "edges: geo":
  test "near against empty index returns nothing":
    let dir = freshDir("glen_edge_geo_empty")
    let db = newGlenDB(dir)
    let g = db.geo("places", "byLoc")
    g.create("lon", "lat")
    let hits = g.near(0.0, 0.0, 1_000_000.0)
    check hits.len == 0
    db.close()

  test "nearest with k > total returns all (capped)":
    let dir = freshDir("glen_edge_geo_k_exceed")
    let db = newGlenDB(dir)
    let g = db.geo("places", "byLoc")
    g.create("lon", "lat")
    db.put("places", "a", %*{"lon": 0.0, "lat": 0.0})
    db.put("places", "b", %*{"lon": 1.0, "lat": 1.0})
    let hits = g.nearest(0.0, 0.0, k = 100)
    check hits.len == 2
    db.close()

  test "polygonLit + readPolygon round-trip":
    let p1 = polygonLit [(0.0, 0.0), (1.0, 0.0), (1.0, 1.0), (0.0, 1.0)]
    let p2 = readPolygon(p1)
    check p2.vertices.len == 4
    check p2.vertices[2] == (1.0, 1.0)

  test "polygonLit with three vertices is fine for findPointsInPolygon":
    let dir = freshDir("glen_edge_poly_triangle")
    let db = newGlenDB(dir)
    let g = db.geo("places", "byLoc")
    g.create("lon", "lat")
    db.put("places", "in",  %*{"lon": 0.3, "lat": 0.3})
    db.put("places", "out", %*{"lon": 5.0, "lat": 5.0})
    let triangle = readPolygon(polygonLit [
      (0.0, 0.0), (1.0, 0.0), (0.5, 1.0)])
    let hits = g.inPolygon(triangle)
    var ids: seq[string] = @[]
    for (id, _) in hits: ids.add(id)
    check "in" in ids
    check "out" notin ids
    db.close()

# ===========================================================================
# Watch — edges
# ===========================================================================

suite "edges: watch":
  test "WatchScope.close() is idempotent":
    let dir = freshDir("glen_edge_watch_close_idem")
    let db = newGlenDB(dir)
    let scope = watch(db):
      doc "users", "u1":
        discard
    scope.close()
    scope.close()    # must not crash
    db.close()

  test "field watch fires only on the watched path":
    let dir = freshDir("glen_edge_watch_field")
    let db = newGlenDB(dir)
    var hits = 0
    let scope = watch(db):
      field "users", "u1", "name":
        inc hits
    db.put("users", "u1", %*{"name": "Alice"})       # +1
    db.put("users", "u1", %*{"name": "Alice"})       # same → no fire
    db.put("users", "u1", %*{"name": "Bob"})         # +1
    db.put("users", "u1", %*{"name": "Bob", "age": 1}) # name unchanged → no fire
    check hits == 2
    scope.close()
    db.close()

  test "collection watch sees deletes too":
    let dir = freshDir("glen_edge_watch_col_del")
    let db = newGlenDB(dir)
    var events = 0
    let scope = watch(db):
      collection "k":
        inc events
    db.put("k", "x", %*{"n": 1})
    db.delete("k", "x")
    check events == 2
    scope.close()
    db.close()
