import std/[os, unittest]
import glen/db, glen/types, glen/geo

proc vert(x, y: float64): Value =
  VArray(@[VFloat(x), VFloat(y)])

proc polyValue(verts: openArray[(float64, float64)]): Value =
  var arr: seq[Value] = @[]
  for (x, y) in verts: arr.add(vert(x, y))
  VArray(arr)

proc polyDoc(verts: openArray[(float64, float64)]; name = ""): Value =
  result = VObject()
  result["shape"] = polyValue(verts)
  if name.len > 0: result["name"] = VString(name)

proc placeDoc(lon, lat: float64; name = ""): Value =
  result = VObject()
  result["lon"] = VFloat(lon)
  result["lat"] = VFloat(lat)
  if name.len > 0: result["name"] = VString(name)

suite "polygon primitives":
  test "polygonBBox":
    let p = Polygon(vertices: @[(0.0, 0.0), (10.0, 0.0), (5.0, 8.0)])
    let b = polygonBBox(p)
    check b.minX == 0.0 and b.maxX == 10.0
    check b.minY == 0.0 and b.maxY == 8.0

  test "pointInPolygon: triangle":
    let tri = Polygon(vertices: @[(0.0, 0.0), (10.0, 0.0), (5.0, 10.0)])
    check pointInPolygon(tri, 5.0, 1.0)        # near base, inside
    check pointInPolygon(tri, 5.0, 5.0)        # middle
    check not pointInPolygon(tri, 0.0, 5.0)    # left of triangle
    check not pointInPolygon(tri, 10.0, 5.0)   # right of triangle
    check not pointInPolygon(tri, 5.0, 11.0)   # above apex
    check not pointInPolygon(tri, 5.0, -1.0)   # below base

  test "pointInPolygon: square":
    let sq = Polygon(vertices: @[(0.0, 0.0), (10.0, 0.0), (10.0, 10.0), (0.0, 10.0)])
    check pointInPolygon(sq, 5.0, 5.0)
    check not pointInPolygon(sq, 15.0, 5.0)
    check not pointInPolygon(sq, -1.0, 5.0)

  test "pointInPolygon: concave (L-shape)":
    let l = Polygon(vertices: @[
      (0.0, 0.0), (10.0, 0.0), (10.0, 5.0),
      (5.0, 5.0), (5.0, 10.0), (0.0, 10.0)])
    check pointInPolygon(l, 2.0, 2.0)    # inside lower-left
    check pointInPolygon(l, 2.0, 8.0)    # inside upper-left
    check not pointInPolygon(l, 8.0, 8.0)  # in the carved-out notch

  test "readPolygonFromValue: shape errors":
    let (ok1, _) = readPolygonFromValue(VArray(@[vert(0.0, 0.0), vert(1.0, 0.0)]))
    check not ok1   # only 2 vertices
    let (ok2, _) = readPolygonFromValue(VString("not a polygon"))
    check not ok2
    let (ok3, p) = readPolygonFromValue(polyValue([(0.0, 0.0), (1.0, 0.0), (0.5, 1.0)]))
    check ok3
    check p.vertices.len == 3

suite "polygon index":
  test "createPolygonIndex bulk-loads existing docs":
    let dir = getTempDir() / "glen_test_polyidx_bulk"
    removeDir(dir)
    let db = newGlenDB(dir)
    db.put("zones", "z1", polyDoc([(0.0, 0.0), (10.0, 0.0), (10.0, 10.0), (0.0, 10.0)], "lower"))
    db.put("zones", "z2", polyDoc([(20.0, 20.0), (30.0, 20.0), (30.0, 30.0), (20.0, 30.0)], "upper"))
    db.createPolygonIndex("zones", "byShape", "shape")
    let inLower = db.findPolygonsContaining("zones", "byShape", 5.0, 5.0)
    check inLower.len == 1
    check inLower[0][0] == "z1"
    let inUpper = db.findPolygonsContaining("zones", "byShape", 25.0, 25.0)
    check inUpper.len == 1
    check inUpper[0][0] == "z2"
    let nowhere = db.findPolygonsContaining("zones", "byShape", 100.0, 100.0)
    check nowhere.len == 0

  test "incremental put/delete update polygon index":
    let dir = getTempDir() / "glen_test_polyidx_inc"
    removeDir(dir)
    let db = newGlenDB(dir)
    db.createPolygonIndex("zones", "byShape", "shape")
    db.put("zones", "z1", polyDoc([(0.0, 0.0), (5.0, 0.0), (5.0, 5.0), (0.0, 5.0)]))
    let r1 = db.findPolygonsContaining("zones", "byShape", 2.0, 2.0)
    check r1.len == 1
    db.delete("zones", "z1")
    let r2 = db.findPolygonsContaining("zones", "byShape", 2.0, 2.0)
    check r2.len == 0

  test "moving a polygon reindexes":
    let dir = getTempDir() / "glen_test_polyidx_move"
    removeDir(dir)
    let db = newGlenDB(dir)
    db.createPolygonIndex("zones", "byShape", "shape")
    db.put("zones", "z1", polyDoc([(0.0, 0.0), (5.0, 0.0), (5.0, 5.0), (0.0, 5.0)]))
    db.put("zones", "z1", polyDoc([(50.0, 50.0), (55.0, 50.0), (55.0, 55.0), (50.0, 55.0)]))
    check db.findPolygonsContaining("zones", "byShape", 2.0, 2.0).len == 0
    check db.findPolygonsContaining("zones", "byShape", 52.0, 52.0).len == 1

  test "findPolygonsIntersecting bbox":
    let dir = getTempDir() / "glen_test_polyidx_isect"
    removeDir(dir)
    let db = newGlenDB(dir)
    db.put("zones", "z1", polyDoc([(0.0, 0.0), (10.0, 0.0), (10.0, 10.0), (0.0, 10.0)]))
    db.put("zones", "z2", polyDoc([(20.0, 20.0), (30.0, 20.0), (30.0, 30.0), (20.0, 30.0)]))
    db.put("zones", "z3", polyDoc([(40.0, 40.0), (50.0, 40.0), (50.0, 50.0), (40.0, 50.0)]))
    db.createPolygonIndex("zones", "byShape", "shape")
    # query bbox covers z1 and z2 but not z3
    let hits = db.findPolygonsIntersecting("zones", "byShape", 0.0, 0.0, 35.0, 35.0)
    var ids: seq[string] = @[]
    for (id, _) in hits: ids.add(id)
    check "z1" in ids
    check "z2" in ids
    check "z3" notin ids

suite "points in polygon":
  test "findPointsInPolygon over a points geo index":
    let dir = getTempDir() / "glen_test_pts_in_poly"
    removeDir(dir)
    let db = newGlenDB(dir)
    # 100 points in a 10x10 grid
    for i in 0 ..< 10:
      for j in 0 ..< 10:
        db.put("places", "p" & $i & "_" & $j, placeDoc(float64(i), float64(j)))
    db.createGeoIndex("places", "byLoc", "lon", "lat")

    # diamond covering the center: vertices form a 4-corner diamond around (5,5)
    let diamond = Polygon(vertices: @[
      (5.0, 2.0), (8.0, 5.0), (5.0, 8.0), (2.0, 5.0)])
    let inside = db.findPointsInPolygon("places", "byLoc", diamond)
    # diamond x+y between certain bounds; manually count
    var expected = 0
    for i in 0 ..< 10:
      for j in 0 ..< 10:
        if pointInPolygon(diamond, float64(i), float64(j)):
          inc expected
    check inside.len == expected
    check inside.len > 0

suite "geographic projection":
  test "findNearest with gmGeographic returns metres, ordered correctly":
    let dir = getTempDir() / "glen_test_geo_metric"
    removeDir(dir)
    let db = newGlenDB(dir)
    # SF (-122.42, 37.77), Oakland (-122.27, 37.80) ~13km, LA (-118.24, 34.05) ~560km
    db.put("places", "sf",  placeDoc(-122.42, 37.77))
    db.put("places", "oak", placeDoc(-122.27, 37.80))
    db.put("places", "la",  placeDoc(-118.24, 34.05))
    db.createGeoIndex("places", "byLoc", "lon", "lat")
    let res = db.findNearest("places", "byLoc",
                             lon = -122.42, lat = 37.77, k = 3,
                             metric = gmGeographic)
    check res.len == 3
    check res[0][0] == "sf"
    check res[0][1] < 1.0    # exactly at SF, ~0 metres
    check res[1][0] == "oak"
    check res[1][1] > 10_000.0 and res[1][1] < 20_000.0
    check res[2][0] == "la"
    check res[2][1] > 500_000.0

  test "haversineMinMeters: zero inside bbox, monotone outside":
    let b = bbox(0.0, 0.0, 1.0, 1.0)
    check haversineMinMeters(b, 0.5, 0.5) == 0.0
    let near = haversineMinMeters(b, 1.5, 0.5)   # 0.5 deg east of edge
    let far  = haversineMinMeters(b, 5.0, 0.5)   # 4 deg east of edge
    check near > 0.0
    check far > near
