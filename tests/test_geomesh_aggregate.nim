## Tests for GeoMesh + TileStack interpolation and polygon aggregation —
## the primitives weather/nowcasting workflows demand.

import std/[unittest, math, os, tables]
import glen/db, glen/geo, glen/geomesh, glen/tilestack, glen/types

# ---- GeoMesh.sampleAt -----------------------------------------------------

suite "geomesh: sampleAt with bilinear interpolation":
  test "imNearest matches valueAt":
    let bb = bbox(0.0, 0.0, 10.0, 10.0)
    var m = newGeoMesh(bb, rows = 2, cols = 2, channels = 1)
    m[0, 0, 0] = 1.0
    m[0, 1, 0] = 2.0
    m[1, 0, 0] = 3.0
    m[1, 1, 0] = 4.0
    # Centre of cell (0, 0) is (2.5, 7.5) (top-left in the grid because
    # row 0 is the top of the bbox = maxLat = 10).
    check m.sampleAt(2.5, 7.5, kind = imNearest) == 1.0
    check m.sampleAt(7.5, 7.5, kind = imNearest) == 2.0
    check m.sampleAt(2.5, 2.5, kind = imNearest) == 3.0
    check m.sampleAt(7.5, 2.5, kind = imNearest) == 4.0

  test "imBilinear at cell centres returns the cell value exactly":
    let bb = bbox(0.0, 0.0, 10.0, 10.0)
    var m = newGeoMesh(bb, rows = 2, cols = 2, channels = 1)
    m[0, 0, 0] = 1.0; m[0, 1, 0] = 2.0
    m[1, 0, 0] = 3.0; m[1, 1, 0] = 4.0
    check m.sampleAt(2.5, 7.5) == 1.0
    check m.sampleAt(7.5, 7.5) == 2.0
    check m.sampleAt(2.5, 2.5) == 3.0
    check m.sampleAt(7.5, 2.5) == 4.0

  test "imBilinear at the centre of all four cells returns the mean":
    let bb = bbox(0.0, 0.0, 10.0, 10.0)
    var m = newGeoMesh(bb, rows = 2, cols = 2, channels = 1)
    m[0, 0, 0] = 1.0; m[0, 1, 0] = 2.0
    m[1, 0, 0] = 3.0; m[1, 1, 0] = 4.0
    # Geometric centre of the mesh — equally weighted between all four cells.
    let v = m.sampleAt(5.0, 5.0)
    check abs(v - 2.5) < 1e-12

  test "imBilinear interpolates linearly along an edge":
    let bb = bbox(0.0, 0.0, 10.0, 10.0)
    var m = newGeoMesh(bb, rows = 1, cols = 2, channels = 1)
    m[0, 0, 0] = 0.0     # cell centre (2.5, 5)
    m[0, 1, 0] = 10.0    # cell centre (7.5, 5)
    # Halfway between the two cell centres, lat=5 (mesh middle).
    check abs(m.sampleAt(5.0, 5.0) - 5.0) < 1e-12
    # 75% of the way to the right cell.
    check abs(m.sampleAt(6.25, 5.0) - 7.5) < 1e-12

  test "imBilinear returns NaN out of bounds":
    let bb = bbox(0.0, 0.0, 10.0, 10.0)
    var m = newGeoMesh(bb, rows = 2, cols = 2, channels = 1)
    m[0, 0, 0] = 1.0
    let v = m.sampleAt(-5.0, -5.0)
    check v.classify == fcNaN

  test "imBilinear is correct for continuous fields (linear ramp)":
    # Build a 10×10 mesh whose value is exactly the lon coord at the
    # cell centre. Bilinear sampling should recover lon to high precision.
    let bb = bbox(0.0, 0.0, 100.0, 100.0)
    var m = newGeoMesh(bb, rows = 10, cols = 10, channels = 1)
    for r in 0 ..< 10:
      for c in 0 ..< 10:
        let (cx, _) = m.cellCenter(r, c)
        m[r, c, 0] = cx
    # At an arbitrary interior point, the recovered value should equal lon.
    let lon = 42.7
    let lat = 33.3
    let recovered = m.sampleAt(lon, lat)
    check abs(recovered - lon) < 1e-9

# ---- GeoMesh.aggregateInPolygon -------------------------------------------

suite "geomesh: aggregateInPolygon":
  test "rectangle aggregate over a 4×4 mesh":
    let bb = bbox(0.0, 0.0, 4.0, 4.0)
    var m = newGeoMesh(bb, rows = 4, cols = 4, channels = 1)
    # Fill with row index — top row = 0, bottom = 3.
    for r in 0 ..< 4:
      for c in 0 ..< 4:
        m[r, c, 0] = float64(r)
    # Polygon covering the bottom half (lat 0..2). Cell centres in rows
    # 2..3 fall inside (lat 1.5 and 0.5). 4 cells per row × 2 rows = 8 cells.
    let poly = Polygon(vertices: @[
      (0.0, 0.0), (4.0, 0.0), (4.0, 2.0), (0.0, 2.0)])
    let stats = m.aggregateInPolygonStats(poly)
    check stats.count == 8
    check stats.sum == 8.0 * 2.5      # mean of {2, 3} × 8 cells = 20
    check abs(stats.mean - 2.5) < 1e-12
    check stats.minValue == 2.0
    check stats.maxValue == 3.0

  test "agg shortcuts return the right scalar":
    let bb = bbox(0.0, 0.0, 4.0, 4.0)
    var m = newGeoMesh(bb, rows = 4, cols = 4, channels = 1)
    for r in 0 ..< 4:
      for c in 0 ..< 4:
        m[r, c, 0] = float64(r * 10 + c)
    let poly = Polygon(vertices: @[
      (0.0, 0.0), (4.0, 0.0), (4.0, 4.0), (0.0, 4.0)])
    check m.aggregateInPolygon(poly, akCount) == 16.0
    check m.aggregateInPolygon(poly, akMin) == 0.0
    check m.aggregateInPolygon(poly, akMax) == 33.0
    let mean = m.aggregateInPolygon(poly, akMean)
    check abs(mean - (0.0 + 1 + 2 + 3 + 10 + 11 + 12 + 13 +
                      20 + 21 + 22 + 23 + 30 + 31 + 32 + 33) / 16.0) < 1e-12

  test "polygon outside the mesh returns NaN / 0":
    let bb = bbox(0.0, 0.0, 4.0, 4.0)
    var m = newGeoMesh(bb, rows = 4, cols = 4, channels = 1)
    for r in 0 ..< 4:
      for c in 0 ..< 4:
        m[r, c, 0] = 1.0
    let outside = Polygon(vertices: @[
      (100.0, 100.0), (110.0, 100.0), (110.0, 110.0), (100.0, 110.0)])
    check m.aggregateInPolygon(outside, akCount) == 0.0
    check m.aggregateInPolygon(outside, akMean).classify == fcNaN

  test "spherical metric works for tiny polygons":
    # A tiny polygon over a small mesh: spherical and planar should
    # agree on which cells qualify.
    let bb = bbox(-1.0, -1.0, 1.0, 1.0)
    var m = newGeoMesh(bb, rows = 4, cols = 4, channels = 1)
    for r in 0 ..< 4:
      for c in 0 ..< 4: m[r, c, 0] = 1.0
    # CCW polygon enclosing the centre.
    let poly = Polygon(vertices: @[
      (-0.5, -0.5), (0.5, -0.5), (0.5, 0.5), (-0.5, 0.5)])
    let pCount = m.aggregateInPolygon(poly, akCount, metric = gmPlanar)
    let sCount = m.aggregateInPolygon(poly, akCount, metric = gmGeographic)
    check pCount == sCount
    check pCount > 0

# ---- TileStack.sampleAt + aggregateInPolygon ------------------------------

proc dummyMeshAt(bb: BBox; rows, cols: int; baseValue: float64): GeoMesh =
  result = newGeoMesh(bb, rows, cols, channels = 1)
  for r in 0 ..< rows:
    for c in 0 ..< cols:
      result[r, c, 0] = baseValue + float64(r * cols + c)

suite "tilestack: sampleAt":
  test "sampleAt at a frame returns the bilinear value":
    let dir = getTempDir() / "glen_ts_sampleat"
    removeDir(dir)
    let bb = bbox(0.0, 0.0, 10.0, 10.0)
    let s = newTileStack(dir, bb, rows = 2, cols = 2, channels = 1)
    let m = dummyMeshAt(bb, 2, 2, 1.0)    # values 1, 2, 3, 4 across 4 cells
    s.appendFrame(1000, m)
    s.flush()
    # Centre of mesh = bilinear average of all four = 2.5.
    let v = s.sampleAt(1000, 5.0, 5.0)
    check abs(v - 2.5) < 1e-9
    # Missing timestamp returns NaN.
    let miss = s.sampleAt(9999, 5.0, 5.0)
    check miss.classify == fcNaN
    s.close()

suite "tilestack: aggregateInPolygon":
  test "single-frame aggregate":
    let dir = getTempDir() / "glen_ts_agg"
    removeDir(dir)
    let bb = bbox(0.0, 0.0, 4.0, 4.0)
    let s = newTileStack(dir, bb, rows = 4, cols = 4, channels = 1)
    var m = newGeoMesh(bb, 4, 4, 1)
    for r in 0 ..< 4:
      for c in 0 ..< 4: m[r, c, 0] = float64(r * 4 + c)
    s.appendFrame(2000, m)
    s.flush()
    let poly = Polygon(vertices: @[
      (1.0, 1.0), (3.0, 1.0), (3.0, 3.0), (1.0, 3.0)])
    let mean = s.aggregateInPolygon(2000, poly, akMean)
    # Cells whose centre lies in [(1, 1) .. (3, 3)] — that's a 2×2 block
    # in the middle. Indices (1,1), (1,2), (2,1), (2,2) → values 5, 6, 9, 10.
    check abs(mean - (5.0 + 6 + 9 + 10) / 4.0) < 1e-9
    s.close()

  test "aggregateInPolygonRange returns one scalar per frame":
    let dir = getTempDir() / "glen_ts_agg_range"
    removeDir(dir)
    let bb = bbox(0.0, 0.0, 4.0, 4.0)
    let s = newTileStack(dir, bb, rows = 4, cols = 4, channels = 1)
    var m1 = newGeoMesh(bb, 4, 4, 1)
    var m2 = newGeoMesh(bb, 4, 4, 1)
    for r in 0 ..< 4:
      for c in 0 ..< 4:
        m1[r, c, 0] = 1.0
        m2[r, c, 0] = 2.0
    s.appendFrame(1000, m1)
    s.appendFrame(2000, m2)
    s.flush()
    let poly = Polygon(vertices: @[
      (0.0, 0.0), (4.0, 0.0), (4.0, 4.0), (0.0, 4.0)])
    let series = s.aggregateInPolygonRange(0, 5000, poly, akMean)
    check series.len == 2
    check series[0] == (1000'i64, 1.0)
    check series[1] == (2000'i64, 2.0)
    s.close()
