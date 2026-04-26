import std/[os, unittest, tables]
import glen/db, glen/types, glen/geo

proc placeDoc(lon, lat: float64; tags: openArray[(string, Value)] = []): Value =
  result = VObject()
  result["lon"] = VFloat(lon)
  result["lat"] = VFloat(lat)
  for (k, v) in tags: result[k] = v

proc polyValue(verts: openArray[(float64, float64)]): Value =
  var arr: seq[Value] = @[]
  for (x, y) in verts:
    arr.add(VArray(@[VFloat(x), VFloat(y)]))
  VArray(arr)

proc polyDoc(verts: openArray[(float64, float64)]): Value =
  result = VObject()
  result["shape"] = polyValue(verts)

suite "index persistence: manifest auto-rebuild":
  test "equality index survives reopen without create call":
    let dir = getTempDir() / "glen_persist_eq"
    removeDir(dir)
    block:
      let db = newGlenDB(dir)
      var v = VObject(); v["name"] = VString("Alice")
      db.put("users", "u1", v)
      var v2 = VObject(); v2["name"] = VString("Bob")
      db.put("users", "u2", v2)
      db.createIndex("users", "byName", "name")
      db.close()

    let db = newGlenDB(dir)
    # No createIndex call here — should be auto-rebuilt from manifest
    let alices = db.findBy("users", "byName", VString("Alice"))
    check alices.len == 1
    check alices[0][0] == "u1"
    db.close()

  test "geo index survives reopen without create call":
    let dir = getTempDir() / "glen_persist_geo"
    removeDir(dir)
    block:
      let db = newGlenDB(dir)
      for i in 0 ..< 50:
        db.put("places", "p" & $i, placeDoc(float64(i), 0.0))
      db.createGeoIndex("places", "byLoc", "lon", "lat")
      db.close()

    let db = newGlenDB(dir)
    let res = db.findInBBox("places", "byLoc", 9.5, -1.0, 20.5, 1.0)
    check res.len == 11
    db.close()

  test "polygon index survives reopen without create call":
    let dir = getTempDir() / "glen_persist_poly"
    removeDir(dir)
    block:
      let db = newGlenDB(dir)
      db.put("zones", "z1", polyDoc([(0.0, 0.0), (10.0, 0.0), (10.0, 10.0), (0.0, 10.0)]))
      db.put("zones", "z2", polyDoc([(20.0, 20.0), (30.0, 20.0), (30.0, 30.0), (20.0, 30.0)]))
      db.createPolygonIndex("zones", "byShape", "shape")
      db.close()

    let db = newGlenDB(dir)
    let inLower = db.findPolygonsContaining("zones", "byShape", 5.0, 5.0)
    check inLower.len == 1
    check inLower[0][0] == "z1"
    db.close()

  test "drop persists across reopen":
    let dir = getTempDir() / "glen_persist_drop"
    removeDir(dir)
    block:
      let db = newGlenDB(dir)
      db.put("places", "p1", placeDoc(1.0, 1.0))
      db.createGeoIndex("places", "byLoc", "lon", "lat")
      db.close()
    block:
      let db = newGlenDB(dir)
      db.dropGeoIndex("places", "byLoc")
      db.close()
    let db = newGlenDB(dir)
    # index should NOT come back
    let res = db.findInBBox("places", "byLoc", 0.0, 0.0, 10.0, 10.0)
    check res.len == 0
    db.close()

  test "post-create mutations are reflected after reopen":
    let dir = getTempDir() / "glen_persist_mutations"
    removeDir(dir)
    block:
      let db = newGlenDB(dir)
      db.createGeoIndex("places", "byLoc", "lon", "lat")
      db.put("places", "a", placeDoc(1.0, 1.0))
      db.put("places", "b", placeDoc(2.0, 2.0))
      db.close()

    let db = newGlenDB(dir)
    let res = db.findInBBox("places", "byLoc", 0.0, 0.0, 5.0, 5.0)
    check res.len == 2
    db.close()

suite "index persistence: binary dump (.gri / .gpi)":
  test "compact dumps geo index; reopen loads from dump":
    let dir = getTempDir() / "glen_persist_dump_geo"
    removeDir(dir)
    block:
      let db = newGlenDB(dir)
      for i in 0 ..< 100:
        db.put("places", "p" & $i, placeDoc(float64(i), 0.0))
      db.createGeoIndex("places", "byLoc", "lon", "lat")
      db.compact()    # writes .gri alongside snapshot
      db.close()
    # the .gri file should exist
    check fileExists(dir / "places.byLoc.gri")
    # reopen and confirm queries work; the file was loaded directly (no replay needed since WAL was reset)
    let db = newGlenDB(dir)
    let res = db.findInBBox("places", "byLoc", 9.5, -1.0, 20.5, 1.0)
    check res.len == 11
    db.close()

  test "compact dumps polygon index; reopen loads from dump":
    let dir = getTempDir() / "glen_persist_dump_poly"
    removeDir(dir)
    block:
      let db = newGlenDB(dir)
      db.put("zones", "z1", polyDoc([(0.0, 0.0), (10.0, 0.0), (10.0, 10.0), (0.0, 10.0)]))
      db.put("zones", "z2", polyDoc([(20.0, 20.0), (30.0, 20.0), (30.0, 30.0), (20.0, 30.0)]))
      db.createPolygonIndex("zones", "byShape", "shape")
      db.compact()
      db.close()
    check fileExists(dir / "zones.byShape.gpi")
    let db = newGlenDB(dir)
    check db.findPolygonsContaining("zones", "byShape", 5.0, 5.0).len == 1
    check db.findPolygonsContaining("zones", "byShape", 25.0, 25.0).len == 1
    db.close()

  test "dump + WAL replay merge: compact, then more writes, reopen sees both":
    let dir = getTempDir() / "glen_persist_dump_merge"
    removeDir(dir)
    block:
      let db = newGlenDB(dir)
      for i in 0 ..< 50:
        db.put("places", "p" & $i, placeDoc(float64(i), 0.0))
      db.createGeoIndex("places", "byLoc", "lon", "lat")
      db.compact()    # snapshot + .gri at this point
      # post-compact mutations go only to the WAL
      for i in 50 ..< 60:
        db.put("places", "p" & $i, placeDoc(float64(i), 0.0))
      db.delete("places", "p10")
      db.close()

    let db = newGlenDB(dir)
    # all 60 - 1 = 59 docs should be queryable
    let allRes = db.findInBBox("places", "byLoc", -1.0, -1.0, 100.0, 1.0)
    check allRes.len == 59
    # specifically the post-compact insert AND the post-compact delete must apply
    let around10 = db.findInBBox("places", "byLoc", 9.5, -1.0, 10.5, 1.0)
    check around10.len == 0    # p10 was deleted post-compact
    let around55 = db.findInBBox("places", "byLoc", 54.5, -1.0, 55.5, 1.0)
    check around55.len == 1    # p55 was added post-compact
    db.close()

  test "torn dump file falls back to bulk-rebuild":
    let dir = getTempDir() / "glen_persist_dump_torn"
    removeDir(dir)
    block:
      let db = newGlenDB(dir)
      for i in 0 ..< 30:
        db.put("places", "p" & $i, placeDoc(float64(i), 0.0))
      db.createGeoIndex("places", "byLoc", "lon", "lat")
      db.compact()
      db.close()
    # corrupt the .gri's CRC trailer
    let path = dir / "places.byLoc.gri"
    let f = open(path, fmReadWriteExisting)
    let sz = f.getFileSize()
    f.setFilePos(sz - 1)
    var junk: byte = 0xAA
    discard f.writeBuffer(addr junk, 1)
    f.close()
    # reopen — bad CRC must NOT crash, must rebuild from docs
    let db = newGlenDB(dir)
    let res = db.findInBBox("places", "byLoc", -1.0, -1.0, 100.0, 1.0)
    check res.len == 30
    db.close()

suite "index persistence: dump round-trip (raw)":
  test "dumpGeoIndex / tryLoadGeoIndex":
    let path = getTempDir() / "glen_geo_rawdump.gri"
    if fileExists(path): removeFile(path)
    let src = newGeoIndex("byLoc", "lon", "lat")
    var entries: seq[(string, BBox)] = @[]
    for i in 0 ..< 200:
      entries.add(("p" & $i, point(float64(i), float64(i mod 10))))
    src.tree.bulkLoad(entries)
    dumpGeoIndex(src, path)

    let dst = newGeoIndex("byLoc", "lon", "lat")
    check tryLoadGeoIndex(dst, path)
    check dst.tree.len == 200
    let hits = dst.tree.searchBBox(bbox(50.0, -1.0, 60.0, 11.0))
    # x in [50, 60] = 11 points
    check hits.len == 11

  test "dumpPolygonIndex / tryLoadPolygonIndex":
    let path = getTempDir() / "glen_poly_rawdump.gpi"
    if fileExists(path): removeFile(path)
    let src = newPolygonIndex("byShape", "shape")
    var docsTable = initTable[string, Value]()
    docsTable["a"] = polyDoc([(0.0, 0.0), (5.0, 0.0), (5.0, 5.0), (0.0, 5.0)])
    docsTable["b"] = polyDoc([(10.0, 10.0), (20.0, 10.0), (20.0, 20.0), (10.0, 20.0)])
    src.bulkBuild(docsTable)
    dumpPolygonIndex(src, path)

    let dst = newPolygonIndex("byShape", "shape")
    check tryLoadPolygonIndex(dst, path)
    check dst.tree.len == 2
    check dst.polygons.len == 2
    check pointInPolygon(dst.polygons["a"], 2.0, 2.0)
    check pointInPolygon(dst.polygons["b"], 15.0, 15.0)
    check not pointInPolygon(dst.polygons["a"], 15.0, 15.0)
