## Tests for the antimeridian, polar, and spherical-polygon fixes.

import std/[unittest, os, math]
import glen/db, glen/geo, glen/types

proc freshDir(name: string): string =
  result = getTempDir() / name
  removeDir(result)
  createDir(result)

proc placeDoc(lon, lat: float): Value =
  result = VObject()
  result["lon"] = VFloat(lon)
  result["lat"] = VFloat(lat)

# ---- splitAntimeridian + clampPolarBBox -------------------------------------

suite "geo: splitAntimeridian":
  test "non-wrapping bbox passes through unchanged":
    let parts = splitAntimeridian(bbox(-10.0, 0.0, 10.0, 20.0))
    check parts.len == 1
    check parts[0].minX == -10.0 and parts[0].maxX == 10.0

  test "explicit wrap (minX > maxX) splits at ±180":
    let parts = splitAntimeridian(bbox(170.0, -5.0, -170.0, 5.0))
    check parts.len == 2
    let east  = parts[0]
    let west  = parts[1]
    check east.minX == 170.0 and east.maxX == 180.0
    check west.minX == -180.0 and west.maxX == -170.0
    check east.minY == -5.0 and east.maxY == 5.0
    check west.minY == -5.0 and west.maxY == 5.0

  test "out-of-range maxX (radiusBBox overshoot) splits":
    let parts = splitAntimeridian(bbox(170.0, 0.0, 190.0, 10.0))
    check parts.len == 2
    check parts[0].maxX == 180.0
    check parts[1].minX == -180.0
    check parts[1].maxX == -170.0   # 190 - 360

  test "out-of-range minX (negative overshoot) splits":
    let parts = splitAntimeridian(bbox(-185.0, 0.0, -170.0, 10.0))
    check parts.len == 2
    check parts[0].minX == -180.0
    check parts[1].maxX == 180.0
    check parts[1].minX == 175.0    # -185 + 360

  test "≥360° span collapses to full world":
    let parts = splitAntimeridian(bbox(-200.0, 0.0, 200.0, 10.0))
    check parts.len == 1
    check parts[0].minX == -180.0 and parts[0].maxX == 180.0

suite "geo: clampPolarBBox":
  test "clamps lat past ±90":
    let b = clampPolarBBox(bbox(-10.0, -95.0, 10.0, 100.0))
    check b.minY == -90.0 and b.maxY == 90.0
    check b.minX == -10.0 and b.maxX == 10.0

# ---- Antimeridian-aware findInBBox / findWithinRadius ----------------------

suite "geo: findInBBox with metric = gmGeographic":
  test "wrapping bbox query finds points on both sides of the date line":
    let dir = freshDir("glen_curv_bbox_wrap")
    let db = newGlenDB(dir)
    db.put("p", "fiji",       placeDoc(178.0,  -17.0))   # east of 0, near 180
    db.put("p", "samoa",      placeDoc(-172.0, -13.0))   # west of 0, near -180
    db.put("p", "newyork",    placeDoc(-74.0,   40.7))   # far away
    db.createGeoIndex("p", "byLoc", "lon", "lat")

    # A bbox that explicitly wraps: minX=170, maxX=-170. Without the fix
    # this would be an empty / inverted rectangle. With it, both Fiji and
    # Samoa show up; New York doesn't.
    let res = db.findInBBox("p", "byLoc",
                            170.0, -30.0, -170.0, 0.0,
                            metric = gmGeographic)
    var ids: seq[string] = @[]
    for (id, _) in res: ids.add(id)
    check "fiji" in ids
    check "samoa" in ids
    check "newyork" notin ids
    db.close()

  test "metric = gmPlanar (default) preserves planar-coord behaviour":
    # Sanity: non-geographic data in [0, 1000]² must not get split.
    let dir = freshDir("glen_curv_bbox_planar")
    let db = newGlenDB(dir)
    db.put("p", "a", placeDoc(200.0, 300.0))
    db.put("p", "b", placeDoc(800.0, 800.0))
    db.createGeoIndex("p", "byLoc", "lon", "lat")
    let res = db.findInBBox("p", "byLoc", 100.0, 100.0, 500.0, 500.0)
    var ids: seq[string] = @[]
    for (id, _) in res: ids.add(id)
    check ids == @["a"]
    db.close()

suite "geo: findWithinRadius across the antimeridian":
  test "5000 km radius around Suva (Fiji) reaches Samoa":
    let dir = freshDir("glen_curv_radius")
    let db = newGlenDB(dir)
    db.put("p", "suva",  placeDoc(178.4, -18.1))      # Fiji
    db.put("p", "apia",  placeDoc(-171.7, -13.8))     # Samoa
    db.put("p", "lima",  placeDoc(-77.0, -12.0))      # Peru — too far
    db.createGeoIndex("p", "byLoc", "lon", "lat")
    let res = db.findWithinRadius("p", "byLoc",
                                  178.4, -18.1, 5_000_000.0)
    var ids: seq[string] = @[]
    for (id, _, _) in res: ids.add(id)
    check "suva" in ids
    check "apia" in ids       # ~1100 km, crosses antimeridian
    check "lima" notin ids    # ~10 000 km away
    db.close()

# ---- Spherical point-in-polygon -------------------------------------------

suite "geo: pointInPolygonSpherical":
  test "small square: same answers as planar":
    let p = Polygon(vertices: @[
      (-10.0, -10.0), (10.0, -10.0), (10.0, 10.0), (-10.0, 10.0)])
    check pointInPolygonSpherical(p, 0.0, 0.0)            # inside
    check not pointInPolygonSpherical(p, 20.0, 0.0)       # outside

  test "polygon spanning the antimeridian":
    # CCW order around the small Pacific square spanning the date line:
    # (170, 10) → (170, -10) → (-170, -10) → (-170, 10).
    let p = Polygon(vertices: @[
      (170.0,  10.0), (170.0, -10.0), (-170.0, -10.0), (-170.0, 10.0)])
    check pointInPolygonSpherical(p, 180.0, 0.0)          # right on meridian
    check pointInPolygonSpherical(p, 175.0, 0.0)          # eastern half
    check pointInPolygonSpherical(p, -175.0, 0.0)         # western half
    check not pointInPolygonSpherical(p, 30.0, 30.0)      # mid-Asia
    check not pointInPolygonSpherical(p, -30.0, 30.0)     # Atlantic
    check not pointInPolygonSpherical(p, 0.0, 0.0)        # antipode of polygon

  test "polygon containing the north pole":
    # A ring at lat=80° spaced evenly around the pole. Walk vertices in
    # *increasing*-longitude order. Viewed from above the north pole
    # that's COUNTER-CLOCKWISE around the polar interior (the pole is
    # to the LEFT as you walk eastward), matching the algorithm's
    # CCW-around-interior expectation.
    var verts: seq[(float64, float64)] = @[]
    for i in 0 ..< 8:
      let lon = -180.0 + 360.0 * float(i) / 8.0
      verts.add((lon, 80.0))
    let p = Polygon(vertices: verts)
    check pointInPolygonSpherical(p, 0.0, 89.0)
    check pointInPolygonSpherical(p, 90.0, 85.0)
    check not pointInPolygonSpherical(p, 0.0, 60.0)        # well south of the ring

# ---- DSL query routing for geographic polygons -----------------------------

suite "geo: findPolygonsContaining with gmGeographic":
  test "antimeridian-spanning polygon, geographic mode finds points on both sides":
    let dir = freshDir("glen_curv_poly_anti")
    let db = newGlenDB(dir)
    # Vertices listed CCW around the date-line square (viewed from
    # above the antimeridian, looking down at the polygon's interior):
    # (170, 10) → (170, -10) → (-170, -10) → (-170, 10).
    let pacificBox = VArray(@[
      VArray(@[VFloat(170.0),  VFloat(10.0)]),
      VArray(@[VFloat(170.0),  VFloat(-10.0)]),
      VArray(@[VFloat(-170.0), VFloat(-10.0)]),
      VArray(@[VFloat(-170.0), VFloat(10.0)])])
    var pdoc = VObject()
    pdoc["shape"] = pacificBox
    db.put("zones", "pacific", pdoc)
    db.createPolygonIndex("zones", "byShape", "shape")

    # Point at (175, 0): inside the region on the sphere.
    let east = db.findPolygonsContaining("zones", "byShape",
                                         175.0, 0.0,
                                         metric = gmGeographic)
    check east.len == 1
    # Point at (-175, 0): also inside.
    let west = db.findPolygonsContaining("zones", "byShape",
                                         -175.0, 0.0,
                                         metric = gmGeographic)
    check west.len == 1
    # Point at (0, 0): outside.
    let outside = db.findPolygonsContaining("zones", "byShape",
                                            0.0, 0.0,
                                            metric = gmGeographic)
    check outside.len == 0
    db.close()
