## Integration tests — combine multiple DSL features in realistic
## end-to-end scenarios. These catch interactions that per-feature
## tests miss (e.g. schema + soft delete + query, or vector + geo
## prefilter, or liveQuery + transactions).

import std/[unittest, os, math, options, sets, strutils, tables]
import glen/glen
import glen/types, glen/db as glendb, glen/txn as glentxn
import glen/dsl

proc freshDir(name: string): string =
  result = getTempDir() / name
  removeDir(result)
  createDir(result)

# ===========================================================================
# Schema + Collection + query + Migration + soft delete + key — full lifecycle
# ===========================================================================

schema todoItems:
  version: 2
  key: id
  fields:
    id:        zString().minLen(1)
    title:     zString().minLen(1)
    done:      zBool().default(false)
    priority:  zInt().default(3)
    deletedAt: optional(zInt())
    tags:      zArray(zString()).default(@[])
  indexes:
    byDone:     equality done
    byPriority: range    priority
  softDelete: deletedAt
  migrations:
    0 -> 1:
      if doc["priority"].isNil:
        doc["priority"] = VInt(3)
    1 -> 2:
      if doc["tags"].isNil:
        doc["tags"] = VArray(@[])

suite "integration: full schema lifecycle":
  test "register, populate, soft-delete, restore, query, migrate":
    let dir = freshDir("glen_int_full")
    let db = newGlenDB(dir)
    registerTodoItemsSchema(db)

    # Pre-existing v0 doc that needs migration: missing priority + tags.
    db.put(todoItemsCollection, "old1",
      %*{"id": "old1", "title": "ancient", "_v": 0, "done": false})
    migrateTodoItems(db)
    let migrated = db.get(todoItemsCollection, "old1")
    check migrated["priority"].i == 3
    check migrated["tags"].kind == vkArray
    check migrated["_v"].i == 2

    # Typed CRUD via the schema-generated procs.
    putTodoItems(db, TodoItems(
      id: "buy-milk", title: "Buy milk", priority: 1, tags: @["errand"]))
    putTodoItems(db, TodoItems(
      id: "ship-it", title: "Ship Glen 1.0", priority: 5, tags: @["work"]))

    # Soft delete the first one — it stays in storage but tombstones.
    softDeleteTodoItems(db, "buy-milk")
    check db.get(todoItemsCollection, "buy-milk")["deletedAt"].kind == vkInt

    # Active list: query the index, filter out soft-deleted.
    let active = query(db, todoItemsCollection):
      where:
        deletedAt == nil
      orderBy: priority desc
    var activeIds: seq[string] = @[]
    for (id, _) in active: activeIds.add(id)
    check activeIds == @["ship-it", "old1"]    # buy-milk hidden

    # Restore + verify it's back.
    restoreTodoItems(db, "buy-milk")
    check db.get(todoItemsCollection, "buy-milk")["deletedAt"].isNil
    let restored = query(db, todoItemsCollection):
      where: deletedAt == nil
    check restored.len == 3
    db.close()

# ===========================================================================
# Vector + Geo: "find similar items near a location"
# ===========================================================================

suite "integration: vector + geo prefilter combo":
  test "spatial filter + vector similarity ranking":
    let dir = freshDir("glen_int_vec_geo")
    let db = newGlenDB(dir)

    # Build geo + vector indexes on the same collection.
    db.createGeoIndex("listings", "byLoc", "lon", "lat")
    db.createVectorIndex("listings", "byEmbed", "embedding", 3, vmCosine)

    # Three listings: two near NYC (one matching the embedding well, one not),
    # one far away (LA) that DOES match the embedding.
    db.put("listings", "nyc_match",
      %*{"lon": -73.98, "lat": 40.75,
         "embedding": [1.0, 0.0, 0.0], "kind": "cafe"})
    db.put("listings", "nyc_diff",
      %*{"lon": -74.01, "lat": 40.72,
         "embedding": [0.0, 1.0, 0.0], "kind": "bar"})
    db.put("listings", "la_match",
      %*{"lon": -118.24, "lat": 34.05,
         "embedding": [1.0, 0.0, 0.0], "kind": "cafe"})

    # Query: things "similar to my taste" within 10km of NYC.
    let myTaste = vec32([1.0, 0.0, 0.0])
    let nearbyIds = block:
      let res = db.findWithinRadius("listings", "byLoc",
                                    -73.98, 40.75, 10_000.0)
      var ids = initHashSet[string]()
      for (id, _, _) in res: ids.incl(id)
      ids
    let similar = db.findNearestVector("listings", "byEmbed", myTaste, k = 10)
    var both: seq[(string, float32)] = @[]
    for (id, dist) in similar:
      if id in nearbyIds: both.add((id, dist))

    check both.len == 2                         # nyc_match + nyc_diff
    check both[0][0] == "nyc_match"             # closest by embedding
    db.close()

# ===========================================================================
# liveQuery + transactions: txn commits fire as a single event burst
# ===========================================================================

suite "integration: liveQuery during transactions":
  test "uncommitted txn does not fire events; commit fires them all":
    let dir = freshDir("glen_int_lq_txn")
    let db = newGlenDB(dir)
    let live = liveQuery(db, "k"):
      where: kind == "alpha"
    var added = 0
    discard live.onChange(proc (ev: LiveQueryEvent) =
      if ev.kind == lqAdded: inc added)

    # Stage three puts under one txn; live query should see nothing
    # until commit.
    let res = txn(db, retries = 0):
      txn.put("k", "1", %*{"kind": "alpha"})
      txn.put("k", "2", %*{"kind": "alpha"})
      txn.put("k", "3", %*{"kind": "beta"})
      check live.len == 0    # not yet visible
    check res.status == glentxn.csOk
    check live.len == 2
    check added == 2
    live.close()
    db.close()

# ===========================================================================
# Sync + schema + queries: replicate validated docs across two nodes
# ===========================================================================

schema notes:
  fields:
    title: zString().minLen(1)
    body:  zString()

suite "integration: sync + schema":
  test "validated docs replicate across two in-memory peers":
    let dirA = freshDir("glen_int_sync_a")
    let dirB = freshDir("glen_int_sync_b")
    let dbA = newGlenDB(dirA)
    let dbB = newGlenDB(dirB)
    registerNotesSchema(dbA)
    registerNotesSchema(dbB)

    let tA = InMemoryTransport()
    let tB = InMemoryTransport()
    tA.peerOf = tB; tB.peerOf = tA
    let syncA = sync(dbA):
      peer "B":
        transport: tA
        intervalMs: 0
    let syncB = sync(dbB):
      peer "A":
        transport: tB
        intervalMs: 0

    # Both nodes write to the same logical row but disjoint fields'd
    # be conflicting; here we just write disjoint rows on each node.
    putNotes(dbA, "n1", Notes(title: "from A", body: "first"))
    putNotes(dbB, "n2", Notes(title: "from B", body: "second"))

    # Push-pull both ways.
    syncA.tickAll(); syncB.tickAll()
    syncA.tickAll(); syncB.tickAll()      # one more round to settle

    # Both nodes must now have both rows AND those rows must validate.
    for db in [dbA, dbB]:
      let (okA, na) = getNotes(db, "n1")
      check okA and na.title == "from A"
      let (okB, nb) = getNotes(db, "n2")
      check okB and nb.title == "from B"
    dbA.close(); dbB.close()

# ===========================================================================
# Pagination through query: stable cursor across mutations
# ===========================================================================

suite "integration: cursor pagination":
  test "cursor pagination is stable when rows are inserted mid-iteration":
    let dir = freshDir("glen_int_pagination")
    let db = newGlenDB(dir)
    for i in 0 ..< 20:
      db.put("k", "id-" & align($i, 3, '0'),
             %*{"i": i, "g": "old"})

    # First page.
    let p1 = query(db, "k"):
      where: g == "old"
      orderBy: i asc
      limit: 5
      page: ()
    check p1.rows.len == 5
    check p1.hasMore

    # Inject more rows that wouldn't show up in our paged view.
    for i in 100 ..< 110:
      db.put("k", "noise-" & $i, %*{"i": i, "g": "new"})

    # Second page should resume from where p1 left off, ignoring the
    # new "g": "new" rows entirely.
    let p2 = query(db, "k"):
      where: g == "old"
      orderBy: i asc
      limit: 5
      after: p1.cursor
      page: ()
    check p2.rows.len == 5
    check p2.rows[0][1]["i"].i == 5
    db.close()

# ===========================================================================
# liveCount + transactions: aggregations stay consistent
# ===========================================================================

suite "integration: liveCount with bulk operations":
  test "liveCount reflects putMany + deleteMany correctly":
    let dir = freshDir("glen_int_lc_bulk")
    let db = newGlenDB(dir)
    let lc = liveCount(db, "k"):
      where: kind == "thing"
    db.putMany("k", [
      ("a", %*{"kind": "thing"}),
      ("b", %*{"kind": "thing"}),
      ("c", %*{"kind": "other"}),
      ("d", %*{"kind": "thing"})])
    check lc.value == 3
    db.deleteMany("k", ["a", "d"])
    check lc.value == 1
    lc.close()
    db.close()

# ===========================================================================
# Spatial: weather workflow combining tilestack + polygon aggregation
# ===========================================================================

suite "integration: weather workflow":
  test "store frames, aggregate over a polygon, sample at point":
    let dir = freshDir("glen_int_weather")
    let db = newGlenDB(dir)
    let bb = bbox(0.0, 0.0, 10.0, 10.0)

    # 4×4 mesh of "rainfall" — diagonal hot-spot.
    proc mkFrame(scale: float64): GeoMesh =
      result = newGeoMesh(bb, 4, 4, 1)
      for r in 0 ..< 4:
        for c in 0 ..< 4:
          let dist = abs(float64(r - c))
          result[r, c, 0] = (4.0 - dist) * scale

    let stack = db.tiles("rainfall", bb,
                         rows = 4, cols = 4, channels = 1)
    stack.appendFrame(1000, mkFrame(1.0))
    stack.appendFrame(2000, mkFrame(2.0))
    stack.appendFrame(3000, mkFrame(3.0))
    stack.flush()

    # Aggregate over a 4-cell square around the centre.
    let watershed = Polygon(vertices: @[
      (3.0, 3.0), (7.0, 3.0), (7.0, 7.0), (3.0, 7.0)])

    let series = stack.aggregateInPolygonRange(
      0, 5000, watershed, akMean)
    check series.len == 3
    # Means should be monotonically increasing (frame scales are 1, 2, 3).
    check series[0][1] < series[1][1]
    check series[1][1] < series[2][1]

    # Bilinear sample at the centre of the mesh on frame 2.
    let v = stack.sampleAt(2000, 5.0, 5.0, kind = imBilinear)
    check not v.isNaN
    check v > 0.0

    stack.close()
    db.close()

# ===========================================================================
# add() + Collection iteration + sort by id
# ===========================================================================

suite "integration: add() preserves insert order via sortable ids":
  test "iterating after sequential db.add gives chronological order":
    let dir = freshDir("glen_int_add_order")
    let db = newGlenDB(dir)
    var ids: seq[string] = @[]
    for i in 0 ..< 20:
      ids.add(db.add("events", %*{"seq": i}))
    # newId() guarantees same-thread monotonicity, so the ids we got
    # back from sequential db.add must already be in ascending order.
    for i in 1 ..< ids.len:
      check ids[i] > ids[i - 1]
    db.close()

# ===========================================================================
# Schema + bulk + page + count: a real listing UI
# ===========================================================================

suite "integration: schema + bulk + page + count (listing UI)":
  test "seed via putMany, count, paginate":
    let dir = freshDir("glen_int_listing")
    let db = newGlenDB(dir)
    registerNotesSchema(db)
    var seed: seq[(string, Notes)] = @[]
    for i in 0 ..< 25:
      seed.add(("note-" & align($i, 3, '0'),
                Notes(title: "Note " & $i, body: "body " & $i)))
    discard putNotesMany(db, seed)
    let total = query(db, notesCollection):
      count: ()
    check total == 25
    let page1 = query(db, notesCollection):
      orderBy: title asc
      limit: 10
      page: ()
    check page1.rows.len == 10
    check page1.hasMore
    let page3 = query(db, notesCollection):
      orderBy: title asc
      limit: 10
      after: page1.cursor
      page: ()
    check page3.rows.len == 10
    let lastPage = query(db, notesCollection):
      orderBy: title asc
      limit: 10
      after: page3.cursor
      page: ()
    check lastPage.rows.len == 5
    check not lastPage.hasMore
    db.close()
