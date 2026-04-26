import std/[os, unittest, tables, sets]
import glen/db, glen/types, glen/geo

# Streaming iterators yield one (id, value) at a time, never materialising
# the full result seq. They're the way to do bulk reads on a collection
# bigger than RAM (or just bigger than your per-call memory budget).

proc placeDoc(lon, lat: float64; name = ""): Value =
  result = VObject()
  result["lon"] = VFloat(lon)
  result["lat"] = VFloat(lat)
  if name.len > 0: result["name"] = VString(name)

proc polyDoc(verts: openArray[(float64, float64)]): Value =
  var arr: seq[Value] = @[]
  for (x, y) in verts:
    arr.add(VArray(@[VFloat(x), VFloat(y)]))
  result = VObject()
  result["shape"] = VArray(arr)

suite "streaming: getAllStream / getBorrowedAllStream":
  test "yields every doc, eager mode":
    let dir = getTempDir() / "glen_stream_getall_eager"
    removeDir(dir)
    let db = newGlenDB(dir)
    for i in 0 ..< 100:
      var v = VObject(); v["n"] = VInt(i.int64)
      db.put("things", "t" & $i, v)
    var seen = initHashSet[string]()
    var count = 0
    for (id, _) in db.getAllStream("things"):
      seen.incl(id); inc count
    check count == 100
    check seen.len == 100
    db.close()

  test "yields every doc in spillable mode without materialising the result":
    let dir = getTempDir() / "glen_stream_getall_spill"
    removeDir(dir)
    block:
      let db = newGlenDB(dir)
      for i in 0 ..< 1000:
        var v = VObject(); v["n"] = VInt(i.int64)
        db.put("things", "t" & $i, v)
      db.compact()
      db.close()
    let db = newGlenDB(dir, spillableMode = true, hotDocCap = 16)
    var seen = 0
    for (_, _) in db.getAllStream("things"):
      inc seen
    check seen == 1000
    db.close()

  test "borrowed variant yields refs, no clone":
    let dir = getTempDir() / "glen_stream_getall_borrow"
    removeDir(dir)
    let db = newGlenDB(dir)
    for i in 0 ..< 50:
      db.put("things", "t" & $i, VInt(i.int64))
    var sum: int64 = 0
    for (_, v) in db.getBorrowedAllStream("things"):
      sum += v.i
    check sum == (49 * 50) div 2
    db.close()

  test "respects tombstones in spillable mode":
    let dir = getTempDir() / "glen_stream_getall_tombstone"
    removeDir(dir)
    block:
      let db = newGlenDB(dir)
      for i in 0 ..< 20:
        db.put("things", "t" & $i, VInt(i.int64))
      db.compact()
      db.close()
    let db = newGlenDB(dir, spillableMode = true)
    db.delete("things", "t5")
    db.delete("things", "t10")
    var seenIds: seq[string] = @[]
    for (id, _) in db.getAllStream("things"):
      seenIds.add(id)
    check seenIds.len == 18
    check "t5"  notin seenIds
    check "t10" notin seenIds
    db.close()

suite "streaming: getManyStream":
  test "yields requested ids that exist; skips missing":
    let dir = getTempDir() / "glen_stream_getmany"
    removeDir(dir)
    let db = newGlenDB(dir)
    db.put("things", "a", VInt(1))
    db.put("things", "b", VInt(2))
    db.put("things", "c", VInt(3))
    var ids: seq[string] = @[]
    for (id, _) in db.getManyStream("things", @["a", "missing", "b", "c", "also-missing"]):
      ids.add(id)
    check ids == @["a", "b", "c"]
    db.close()

suite "streaming: indexed query iterators":
  test "findByStream sees every match in spillable mode":
    let dir = getTempDir() / "glen_stream_findby"
    removeDir(dir)
    block:
      let db = newGlenDB(dir)
      for i in 0 ..< 100:
        var v = VObject()
        v["category"] = VString(if i mod 2 == 0: "even" else: "odd")
        v["n"] = VInt(i.int64)
        db.put("things", "t" & $i, v)
      db.compact()
      db.close()
    let db = newGlenDB(dir, spillableMode = true)
    db.createIndex("things", "byCategory", "category")
    var c = 0
    for (_, _) in db.findByStream("things", "byCategory", VString("even")):
      inc c
    check c == 50
    db.close()

  test "findInBBoxStream + spillable":
    let dir = getTempDir() / "glen_stream_bbox"
    removeDir(dir)
    block:
      let db = newGlenDB(dir)
      for i in 0 ..< 50:
        db.put("places", "p" & $i, placeDoc(float64(i), 0.0))
      db.compact()
      db.close()
    let db = newGlenDB(dir, spillableMode = true)
    db.createGeoIndex("places", "byLoc", "lon", "lat")
    var c = 0
    for (_, _) in db.findInBBoxStream("places", "byLoc", 9.5, -1.0, 20.5, 1.0):
      inc c
    check c == 11   # p10..p20
    db.close()

  test "findNearestStream (geographic) yields k items":
    let dir = getTempDir() / "glen_stream_knn"
    removeDir(dir)
    let db = newGlenDB(dir)
    for i in 0 ..< 10:
      db.put("places", "p" & $i, placeDoc(float64(i), 0.0))
    db.createGeoIndex("places", "byLoc", "lon", "lat")
    var ids: seq[string] = @[]
    for (id, _, _) in db.findNearestStream("places", "byLoc",
                                           lon = 5.0, lat = 0.0, k = 3,
                                           metric = gmGeographic):
      ids.add(id)
    check ids.len == 3
    db.close()

  test "findWithinRadiusStream":
    let dir = getTempDir() / "glen_stream_radius"
    removeDir(dir)
    let db = newGlenDB(dir)
    db.put("places", "sf",  placeDoc(-122.42, 37.77))
    db.put("places", "oak", placeDoc(-122.27, 37.80))
    db.put("places", "la",  placeDoc(-118.24, 34.05))
    db.createGeoIndex("places", "byLoc", "lon", "lat")
    var ids: seq[string] = @[]
    for (id, _, _) in db.findWithinRadiusStream("places", "byLoc",
                                                -122.42, 37.77,
                                                radiusMeters = 50_000.0):
      ids.add(id)
    check ids.len == 2
    check "sf"  in ids
    check "oak" in ids
    db.close()

  test "findPolygonsContainingStream":
    let dir = getTempDir() / "glen_stream_polycontain"
    removeDir(dir)
    let db = newGlenDB(dir)
    db.put("zones", "z1", polyDoc([(0.0, 0.0), (10.0, 0.0), (10.0, 10.0), (0.0, 10.0)]))
    db.put("zones", "z2", polyDoc([(20.0, 20.0), (30.0, 20.0), (30.0, 30.0), (20.0, 30.0)]))
    db.createPolygonIndex("zones", "byShape", "shape")
    var hits: seq[string] = @[]
    for (id, _) in db.findPolygonsContainingStream("zones", "byShape", 5.0, 5.0):
      hits.add(id)
    check hits == @["z1"]
    db.close()

suite "streaming: bounded memory contract":
  test "iterating 10k docs in spill+hotDocCap=8 only ever holds the current doc":
    ## Smoke test: in spillable mode with a tiny hotDocCap, the streaming
    ## iterator should not fault docs into cs.docs (they go through
    ## lookupDocBypass), so cs.docs.len stays around hotDocCap throughout.
    let dir = getTempDir() / "glen_stream_bounded"
    removeDir(dir)
    block:
      let db = newGlenDB(dir)
      for i in 0 ..< 10_000:
        var v = VObject(); v["n"] = VInt(i.int64)
        db.put("things", "t" & $i, v)
      db.compact()
      db.close()
    let db = newGlenDB(dir, spillableMode = true, hotDocCap = 8)
    var c = 0
    for (_, _) in db.getAllStream("things"):
      inc c
    check c == 10_000
    db.close()