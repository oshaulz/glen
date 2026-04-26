import std/[os, unittest, math]
import glen/db, glen/types, glen/geo, glen/linalg, glen/geomesh

const eps = 1e-12

suite "geomesh: cell math":
  test "cellAt clamps and rejects out-of-bounds":
    let m = newGeoMesh(bbox(0.0, 0.0, 10.0, 10.0), rows = 5, cols = 5)
    let (ok1, _, _) = m.cellAt(-1.0, 5.0)   # west of bbox
    check not ok1
    let (ok2, _, _) = m.cellAt(5.0, 100.0)  # north of bbox
    check not ok2
    let (ok3, r, c) = m.cellAt(5.0, 5.0)
    check ok3
    # bbox 0..10 split into 5 cells of width 2; (5,5) lands in row 2 col 2
    # row 0 == top (lat 8..10); lat 5 → fy = (10-5)/10 = 0.5 → row 2.
    check r == 2 and c == 2

  test "cellAt: top-left corner = (0, 0)":
    let m = newGeoMesh(bbox(0.0, 0.0, 10.0, 10.0), rows = 4, cols = 4)
    let (ok, r, c) = m.cellAt(0.0, 10.0)
    check ok and r == 0 and c == 0

  test "cellAt: bottom-right corner = (rows-1, cols-1)":
    let m = newGeoMesh(bbox(0.0, 0.0, 10.0, 10.0), rows = 4, cols = 4)
    let (ok, r, c) = m.cellAt(10.0, 0.0)
    check ok and r == 3 and c == 3

  test "cellCenter inverts cellAt":
    let m = newGeoMesh(bbox(-122.5, 37.5, -122.0, 38.0), rows = 10, cols = 10)
    for r in 0 ..< m.rows:
      for c in 0 ..< m.cols:
        let (lon, lat) = m.cellCenter(r, c)
        let (ok, r2, c2) = m.cellAt(lon, lat)
        check ok and r2 == r and c2 == c

suite "geomesh: read/write per channel":
  test "scalar mesh: setCell + valueAt round-trip":
    var m = newGeoMesh(bbox(0.0, 0.0, 10.0, 10.0), rows = 3, cols = 3)
    m.setCell(1, 1, [42.0])
    let (lon, lat) = m.cellCenter(1, 1)
    check abs(m.valueAt(lon, lat) - 42.0) < eps

  test "multi-channel: vectorAt returns the full per-cell vector":
    var m = newGeoMesh(
      bbox(0.0, 0.0, 4.0, 4.0), rows = 2, cols = 2,
      channels = 3,
      labels = @["rain", "snow", "clear"])
    m.setCell(0, 0, [0.7, 0.1, 0.2])
    m.setCell(0, 1, [0.5, 0.0, 0.5])
    m.setCell(1, 0, [0.1, 0.0, 0.9])
    m.setCell(1, 1, [0.0, 0.0, 1.0])
    let (lon, lat) = m.cellCenter(0, 0)
    let v = m.vectorAt(lon, lat)
    check v == @[0.7, 0.1, 0.2]

  test "channelIndex resolves labels":
    var m = newGeoMesh(
      bbox(0.0, 0.0, 1.0, 1.0), rows = 1, cols = 1,
      channels = 3, labels = @["a", "b", "c"])
    check m.channelIndex("b") == 1
    check m.channelIndex("missing") == -1

  test "labels length must match channels or be empty":
    expect(ValueError):
      discard newGeoMesh(bbox(0.0, 0.0, 1.0, 1.0), 1, 1, channels = 2,
                         labels = @["only-one"])

suite "geomesh: value (de)serialization":
  test "round-trip preserves bbox, shape, labels, data":
    var src = newGeoMesh(
      bbox(-1.5, 2.0, 3.0, 7.5), rows = 3, cols = 4,
      channels = 2, labels = @["wet", "dry"])
    for r in 0 ..< src.rows:
      for c in 0 ..< src.cols:
        src.setCell(r, c, [float64(r) + 0.1 * float64(c), 1.0 - 0.1 * float64(c)])
    let v = src.toValue()
    let (ok, back) = readGeoMesh(v)
    check ok
    check back.bbox.minX == src.bbox.minX
    check back.bbox.maxY == src.bbox.maxY
    check back.rows == src.rows and back.cols == src.cols
    check back.channels == src.channels
    check back.labels == src.labels
    check back.data == src.data

  test "data is encoded as VBytes (compact)":
    let m = newGeoMesh(bbox(0.0, 0.0, 1.0, 1.0), rows = 100, cols = 100, channels = 5)
    let v = m.toValue()
    check v["data"].kind == vkBytes
    # 100*100*5 floats × 8 bytes = 400_000 bytes
    check v["data"].bytes.len == 400_000

  test "readGeoMesh rejects malformed input":
    # missing required field
    var bad = VObject()
    bad["rows"] = VInt(1); bad["cols"] = VInt(1); bad["channels"] = VInt(1)
    bad["data"] = VBytes(@[byte 0])
    let (ok, _) = readGeoMesh(bad)
    check not ok

suite "geomesh inside docs":
  test "round-trip a multi-class probability mesh through the database":
    let dir = getTempDir() / "glen_geomesh_doc"
    removeDir(dir)
    let db = newGlenDB(dir)

    var mesh = newGeoMesh(
      bbox(-122.5, 37.5, -122.0, 38.0),
      rows = 8, cols = 8, channels = 3,
      labels = @["rain", "snow", "clear"])
    # fill with plausible probabilities (made up)
    for r in 0 ..< mesh.rows:
      for c in 0 ..< mesh.cols:
        let rain  = 0.05 + 0.1 * float64(r)
        let snow  = if r > 5: 0.2 else: 0.0
        let clear = 1.0 - rain - snow
        mesh.setCell(r, c, [rain, snow, clear])

    var doc = VObject()
    doc["model"]    = VString("rain-v3")
    doc["asof"]     = VString("2026-04-26T12:00Z")
    doc["forecast"] = mesh.toValue()
    db.put("predictions", "today-utc-12", doc)

    # Round-trip: reopen, decode, sample at a specific point
    db.close()
    let db2 = newGlenDB(dir)
    let got = db2.get("predictions", "today-utc-12")
    check not got.isNil
    let (ok, m) = readGeoMesh(got["forecast"])
    check ok
    check m.bbox.minX == -122.5
    check m.channels == 3
    let v = m.vectorAt(-122.25, 37.75)        # roughly mid-mesh
    check v.len == 3
    # probabilities at a single cell should sum to 1 by construction
    check abs(v[0] + v[1] + v[2] - 1.0) < 1e-9
    db2.close()

  test "indexed lookup: find which mesh covers a query point":
    let dir = getTempDir() / "glen_geomesh_idx"
    removeDir(dir)
    let db = newGlenDB(dir)

    # Two meshes with disjoint bboxes
    let westMesh = newGeoMesh(bbox(-125.0, 35.0, -120.0, 40.0), 4, 4, 1)
    let eastMesh = newGeoMesh(bbox(-80.0,  35.0, -75.0,  40.0), 4, 4, 1)

    # Polygon doc: a closed rectangle matching the mesh bbox.
    proc bboxRect(b: BBox): Value =
      VArray(@[
        VArray(@[VFloat(b.minX), VFloat(b.minY)]),
        VArray(@[VFloat(b.maxX), VFloat(b.minY)]),
        VArray(@[VFloat(b.maxX), VFloat(b.maxY)]),
        VArray(@[VFloat(b.minX), VFloat(b.maxY)])])

    var westDoc = VObject()
    westDoc["region"] = bboxRect(westMesh.bbox)
    westDoc["mesh"]   = westMesh.toValue()
    var eastDoc = VObject()
    eastDoc["region"] = bboxRect(eastMesh.bbox)
    eastDoc["mesh"]   = eastMesh.toValue()
    db.put("predictions", "west", westDoc)
    db.put("predictions", "east", eastDoc)

    db.createPolygonIndex("predictions", "byRegion", "region")

    # Query: which mesh covers SF-ish?
    let hits = db.findPolygonsContaining("predictions", "byRegion",
                                         x = -122.42, y = 37.77)
    check hits.len == 1
    check hits[0][0] == "west"
    db.close()
