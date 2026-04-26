import std/[os, unittest, math, random, tables]
import glen/db, glen/types, glen/geo

proc placeDoc(lon, lat: float64; name = ""): Value =
  result = VObject()
  result["lon"] = VFloat(lon)
  result["lat"] = VFloat(lat)
  if name.len > 0: result["name"] = VString(name)

suite "geo: rtree primitives":
  test "bbox area / union / intersects":
    let a = bbox(0.0, 0.0, 10.0, 10.0)
    let b = bbox(5.0, 5.0, 15.0, 15.0)
    check intersects(a, b)
    let u = union(a, b)
    check u.minX == 0.0 and u.maxX == 15.0
    let c = bbox(20.0, 20.0, 30.0, 30.0)
    check not intersects(a, c)

  test "haversine round-trip ~1m precision":
    # 1 degree of latitude ~ 111.32 km at equator
    let d = haversineMeters(0.0, 0.0, 0.0, 1.0)
    check d > 110_000.0 and d < 112_000.0

suite "rtree: build, query, delete":
  test "bulk-load + bbox search":
    let t = newRTree()
    var pts: seq[(string, BBox)] = @[]
    for i in 0 ..< 1000:
      let x = float64(i mod 100)
      let y = float64(i div 100)
      pts.add(("p" & $i, point(x, y)))
    t.bulkLoad(pts)
    check t.len == 1000
    let hits = t.searchBBox(bbox(10.0, 0.0, 20.0, 5.0), 0)
    # x in [10,20], y in [0,5] = 11 * 6 = 66 points
    check hits.len == 66

  test "incremental insert + delete":
    let t = newRTree()
    for i in 0 ..< 200:
      t.insert("p" & $i, point(float64(i), float64(i)))
    check t.len == 200
    check t.remove("p50")
    check t.len == 199
    check not t.remove("p50")  # already gone
    let hits = t.searchBBox(bbox(45.0, 45.0, 55.0, 55.0))
    check "p50" notin hits
    check "p49" in hits
    check "p51" in hits

  test "knn returns sorted by distance":
    let t = newRTree()
    for i in 0 ..< 100:
      t.insert("p" & $i, point(float64(i), 0.0))
    let res = t.nearest(50.5, 0.0, 5)
    check res.len == 5
    var lastD = -1.0
    for (_, d) in res:
      check d >= lastD
      lastD = d
    # closest must be p50 or p51
    check res[0][0] == "p50" or res[0][0] == "p51"

  test "nearestWithin respects radius":
    let t = newRTree()
    for i in 0 ..< 100:
      t.insert("p" & $i, point(float64(i), 0.0))
    let res = t.nearestWithin(50.0, 0.0, 3.0, 0)
    # within Euclidean distance 3 of (50, 0): p47..p53 = 7 points
    check res.len == 7
    for (_, d) in res:
      check d <= 3.0001

suite "geo index integration":
  test "createGeoIndex bulk-loads existing docs":
    let dir = getTempDir() / "glen_test_geo_bulk"
    removeDir(dir)
    let db = newGlenDB(dir)
    db.put("places", "sf",  placeDoc(-122.42, 37.77, "SF"))
    db.put("places", "ny",  placeDoc(-74.00, 40.71, "NY"))
    db.put("places", "lon", placeDoc(-0.13, 51.50, "London"))
    db.createGeoIndex("places", "byLoc", "lon", "lat")
    let near = db.findInBBox("places", "byLoc", -130.0, 30.0, -70.0, 45.0)
    var ids: seq[string] = @[]
    for (id, _) in near: ids.add(id)
    check "sf" in ids
    check "ny" in ids
    check "lon" notin ids

  test "incremental put updates geo index":
    let dir = getTempDir() / "glen_test_geo_inc"
    removeDir(dir)
    let db = newGlenDB(dir)
    db.createGeoIndex("places", "byLoc", "lon", "lat")
    db.put("places", "a", placeDoc(0.0, 0.0))
    db.put("places", "b", placeDoc(1.0, 1.0))
    db.put("places", "c", placeDoc(10.0, 10.0))
    let res = db.findInBBox("places", "byLoc", -0.5, -0.5, 1.5, 1.5)
    check res.len == 2

  test "moving a doc reindexes":
    let dir = getTempDir() / "glen_test_geo_move"
    removeDir(dir)
    let db = newGlenDB(dir)
    db.createGeoIndex("places", "byLoc", "lon", "lat")
    db.put("places", "x", placeDoc(0.0, 0.0))
    db.put("places", "x", placeDoc(50.0, 50.0))    # moved
    let near0 = db.findInBBox("places", "byLoc", -1.0, -1.0, 1.0, 1.0)
    check near0.len == 0
    let near50 = db.findInBBox("places", "byLoc", 49.0, 49.0, 51.0, 51.0)
    check near50.len == 1

  test "delete removes from geo index":
    let dir = getTempDir() / "glen_test_geo_del"
    removeDir(dir)
    let db = newGlenDB(dir)
    db.createGeoIndex("places", "byLoc", "lon", "lat")
    db.put("places", "x", placeDoc(0.0, 0.0))
    db.delete("places", "x")
    let res = db.findInBBox("places", "byLoc", -1.0, -1.0, 1.0, 1.0)
    check res.len == 0

  test "findNearest returns k closest":
    let dir = getTempDir() / "glen_test_geo_knn"
    removeDir(dir)
    let db = newGlenDB(dir)
    for i in 0 ..< 50:
      db.put("places", "p" & $i, placeDoc(float64(i), 0.0))
    db.createGeoIndex("places", "byLoc", "lon", "lat")
    let res = db.findNearest("places", "byLoc", 25.4, 0.0, 3)
    check res.len == 3
    var ids: seq[string] = @[]
    for (id, _, _) in res: ids.add(id)
    check ids[0] == "p25"
    check "p26" in ids
    check "p24" in ids

  test "findWithinRadius haversine post-filter":
    let dir = getTempDir() / "glen_test_geo_radius"
    removeDir(dir)
    let db = newGlenDB(dir)
    # SF, OAK (~13km E), LA (~560km S)
    db.put("places", "sf",  placeDoc(-122.42, 37.77))
    db.put("places", "oak", placeDoc(-122.27, 37.80))
    db.put("places", "la",  placeDoc(-118.24, 34.05))
    db.createGeoIndex("places", "byLoc", "lon", "lat")
    # 50km from SF should hit SF + OAK but not LA
    let res = db.findWithinRadius("places", "byLoc", -122.42, 37.77, 50_000.0)
    var ids: seq[string] = @[]
    for (id, _, _) in res: ids.add(id)
    check "sf" in ids
    check "oak" in ids
    check "la" notin ids
    # 1000km should hit all three
    let big = db.findWithinRadius("places", "byLoc", -122.42, 37.77, 1_000_000.0)
    check big.len == 3

  test "geo index survives reopen via createGeoIndex":
    let dir = getTempDir() / "glen_test_geo_reopen"
    removeDir(dir)
    block:
      let db = newGlenDB(dir)
      for i in 0 ..< 100:
        db.put("places", "p" & $i, placeDoc(float64(i), 0.0))
      db.close()
    let db = newGlenDB(dir)
    db.createGeoIndex("places", "byLoc", "lon", "lat")
    let res = db.findInBBox("places", "byLoc", 9.5, -1.0, 20.5, 1.0)
    check res.len == 11  # p10..p20

suite "geo stress":
  test "1000 random points: bbox query consistent with linear scan":
    let dir = getTempDir() / "glen_test_geo_stress"
    removeDir(dir)
    let db = newGlenDB(dir)
    var rng = initRand(42)
    var pts: seq[(string, float64, float64)] = @[]
    for i in 0 ..< 1000:
      let x = rng.rand(1000.0)
      let y = rng.rand(1000.0)
      let id = "p" & $i
      pts.add((id, x, y))
      db.put("places", id, placeDoc(x, y))
    db.createGeoIndex("places", "byLoc", "lon", "lat")
    # 100 random bbox queries
    for q in 0 ..< 100:
      let cx = rng.rand(1000.0)
      let cy = rng.rand(1000.0)
      let r = rng.rand(50.0) + 1.0
      let res = db.findInBBox("places", "byLoc",
                              cx - r, cy - r, cx + r, cy + r)
      var expected: seq[string] = @[]
      for (id, x, y) in pts:
        if x >= cx - r and x <= cx + r and y >= cy - r and y <= cy + r:
          expected.add(id)
      check res.len == expected.len
