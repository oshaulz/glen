## Volume / stress tests for the DSL surface.
## Not benchmarks — just enough scale to surface algorithmic bugs and
## quadratic blowups that the small unit tests miss. Each test should
## complete in well under a second on a modern laptop.

import std/[unittest, os, math, sets, strutils, times]
import glen/glen
import glen/types, glen/db as glendb, glen/txn as glentxn
import glen/dsl

proc freshDir(name: string): string =
  result = getTempDir() / name
  removeDir(result)
  createDir(result)

# ===========================================================================
# 10K docs through schema typed CRUD
# ===========================================================================

schema entries:
  fields:
    title: zString()
    score: zInt()

suite "stress: schema CRUD at scale":
  test "10k typed put + get + getMany round-trip":
    let dir = freshDir("glen_stress_schema_10k")
    let db = newGlenDB(dir)
    registerEntriesSchema(db)

    var batch: seq[(string, Entries)] = @[]
    for i in 0 ..< 10_000:
      batch.add(("e-" & align($i, 6, '0'),
                 Entries(title: "title-" & $i, score: int64(i mod 1000))))
    let written = putEntriesMany(db, batch)
    check written.len == 10_000

    # Spot-check ordering preserved.
    check db.get(entriesCollection, "e-000000")["score"].i == 0
    check db.get(entriesCollection, "e-009999")["score"].i == 9999 mod 1000

    # Bulk read 1000 of them.
    var ids: seq[string] = @[]
    for i in 0 ..< 1000:
      ids.add("e-" & align($(i * 10), 6, '0'))
    let rows = getEntriesMany(db, ids)
    check rows.len == 1000
    db.close()

# ===========================================================================
# Vector index at scale
# ===========================================================================

suite "stress: vector index":
  test "1k 64-dim vectors: insert + threshold search":
    let dir = freshDir("glen_stress_vec_1k")
    let db = newGlenDB(dir)
    let idx = db.vectors("docs", "byEmbed")
    idx.create("embedding", dim = 64)

    # Build 1000 vectors clustered around 8 centres so threshold queries
    # have meaningful results.
    proc rng(seed: uint32): uint32 =
      var s = seed
      s = s xor (s shl 13)
      s = s xor (s shr 17)
      s = s xor (s shl 5)
      s

    var s: uint32 = 12345
    for i in 0 ..< 1000:
      var v = newSeq[float64](64)
      let cluster = i mod 8
      for j in 0 ..< 64:
        s = rng(s)
        let noise = (float64(s mod 100) - 50.0) / 1000.0    # ±0.05
        v[j] = (if j == cluster: 1.0 else: 0.0) + noise
      idx.upsert($i, %*{"embedding": v})

    # Query at cluster 3: should find ~125 docs (1000/8) within a tight band.
    var q = newSeq[float64](64)
    q[3] = 1.0
    let near = idx.searchWithin(vec32(q), maxDistance = 0.05)
    check near.len > 50      # cluster size ≈ 125 minus those that drift past 0.05
    check near.len < 200
    # All hits should be from cluster 3.
    for (id, _) in near:
      let i = parseInt(id)
      check (i mod 8) == 3
    db.close()

# ===========================================================================
# Many sequential newId() calls remain monotone and unique
# ===========================================================================

suite "stress: newId monotonicity":
  test "5000 sequential newId calls strictly increase":
    var prev = ""
    var seen = initHashSet[string]()
    for _ in 0 ..< 5000:
      let id = newId()
      check id.len == 26
      check id notin seen
      check id > prev
      seen.incl(id)
      prev = id

# ===========================================================================
# liveQuery under heavy mutation
# ===========================================================================

suite "stress: liveQuery under heavy mutation":
  test "1000 random puts/deletes — events match the final set":
    let dir = freshDir("glen_stress_lq")
    let db = newGlenDB(dir)
    let live = liveQuery(db, "k"):
      where: active == true
    var added, removed, updated: int
    discard live.onChange(proc (ev: LiveQueryEvent) =
      case ev.kind
      of lqAdded:   inc added
      of lqRemoved: inc removed
      of lqUpdated: inc updated
      else: discard)

    # 200 ids, randomly toggled.
    for round in 0 ..< 5:
      for i in 0 ..< 200:
        let active = ((round * 31 + i * 17) mod 3) != 0
        db.put("k", "doc-" & $i, %*{"active": active})

    # The final state must be self-consistent.
    let finalActive = query(db, "k"):
      where: active == true
      count: ()
    check live.len == finalActive
    check added >= live.len   # every match originated as an add at some point
    live.close()
    db.close()

# ===========================================================================
# Cursor pagination through a large set
# ===========================================================================

suite "stress: full pagination walk":
  test "walk 5000 rows in pages of 100, no duplicates, no gaps":
    let dir = freshDir("glen_stress_paginate")
    let db = newGlenDB(dir)
    for i in 0 ..< 5000:
      db.put("k", "row-" & align($i, 5, '0'), %*{"i": i})

    var seen = initHashSet[string]()
    var cursor = ""
    var pageNum = 0
    while true:
      let p = query(db, "k"):
        orderBy: i asc
        limit: 100
        after: cursor
        page: ()
      inc pageNum
      check pageNum < 60      # should finish in 50 pages exactly
      for (id, _) in p.rows:
        check id notin seen
        seen.incl(id)
      if not p.hasMore: break
      cursor = p.cursor
    check seen.len == 5000
    db.close()

# ===========================================================================
# TileStack: many frames + range aggregate
# ===========================================================================

suite "stress: tilestack many frames":
  test "100 frames + aggregateInPolygonRange":
    let dir = freshDir("glen_stress_ts")
    let db = newGlenDB(dir)
    let bb = bbox(0.0, 0.0, 10.0, 10.0)
    let stack = db.tiles("rain", bb,
                         rows = 8, cols = 8, channels = 1)

    for t in 0 ..< 100:
      var m = newGeoMesh(bb, 8, 8, 1)
      for r in 0 ..< 8:
        for c in 0 ..< 8:
          m[r, c, 0] = float64(t)        # constant per frame, increasing in time
      stack.appendFrame(int64(t * 1000), m)
    stack.flush()

    let watershed = Polygon(vertices: @[
      (1.0, 1.0), (9.0, 1.0), (9.0, 9.0), (1.0, 9.0)])
    let series = stack.aggregateInPolygonRange(
      0, 200_000, watershed, akMean)
    check series.len == 100
    # Each frame's mean equals its index.
    for i, (ts, v) in series:
      check ts == int64(i * 1000)
      check abs(v - float64(i)) < 1e-9
    stack.close()
    db.close()

# ===========================================================================
# 1000 sequential transactions
# ===========================================================================

suite "stress: many small transactions":
  test "1000 single-doc txns commit to the right final state":
    let dir = freshDir("glen_stress_txn")
    let db = newGlenDB(dir)
    db.put("counter", "c", %*{"n": 0})
    for _ in 0 ..< 1000:
      let res = txn(db, retries = 5):
        let cur = txn.get("counter", "c")
        txn.put("counter", "c", %*{"n": cur["n"].i + 1})
      check res.status == glentxn.csOk
    check db.get("counter", "c")["n"].i == 1000
    db.close()