# Geospatial benchmarks. Mirrors the style of test_bench.nim — plain stdout
# numbers, no asserts. Run with -d:release for realistic figures:
#
#   nimble bench_geo
#
# Or by hand:
#   nim c -r -d:release --mm:orc --passC:-O3 --threads:on --path:src \
#         tests/test_bench_geo.nim

import std/[os, times, strformat, random, tables]
import glen/db as glendb, glen/types, glen/geo

# ---------------- helpers ----------------

proc msSince(t0: float): int64 = int64((epochTime() - t0) * 1000.0)
proc rate(n: int; dtMs: int64): float =
  if dtMs == 0: 0.0 else: (float(n) * 1000.0) / float(dtMs)

proc placeDoc(lon, lat: float64): Value =
  result = VObject()
  result["lon"] = VFloat(lon)
  result["lat"] = VFloat(lat)

proc randomPoints(n: int; seed = 1; box = (-180.0, -85.0, 180.0, 85.0)):
                  seq[(string, float64, float64)] =
  var rng = initRand(seed)
  result = newSeq[(string, float64, float64)](n)
  let (minLon, minLat, maxLon, maxLat) = box
  for i in 0 ..< n:
    let lon = minLon + rng.rand(maxLon - minLon)
    let lat = minLat + rng.rand(maxLat - minLat)
    result[i] = ("p" & $i, lon, lat)

# ---------------- raw R-tree ----------------

proc benchRTreeBulkLoad(n: int) =
  let pts = randomPoints(n, seed = 11)
  var entries: seq[(string, BBox)] = @[]
  for (id, x, y) in pts: entries.add((id, point(x, y)))
  let t0 = epochTime()
  let t = newRTree()
  t.bulkLoad(entries)
  let dt = msSince(t0)
  echo &"BENCH rtree bulkLoad (STR):     {n:>9} entries in {dt:>5} ms => {rate(n, dt):>10.0f} entries/s"

proc benchRTreeIncrementalInsert(n: int) =
  let pts = randomPoints(n, seed = 12)
  let t = newRTree()
  let t0 = epochTime()
  for (id, x, y) in pts: t.insert(id, point(x, y))
  let dt = msSince(t0)
  echo &"BENCH rtree insert (Guttman):   {n:>9} ops     in {dt:>5} ms => {rate(n, dt):>10.0f} ops/s"

proc benchRTreeBBoxSearch(n, queries: int) =
  let pts = randomPoints(n, seed = 13)
  let t = newRTree()
  var entries: seq[(string, BBox)] = @[]
  for (id, x, y) in pts: entries.add((id, point(x, y)))
  t.bulkLoad(entries)
  var rng = initRand(99)
  var totalHits = 0
  let t0 = epochTime()
  for _ in 0 ..< queries:
    let cx = -180.0 + rng.rand(360.0)
    let cy =  -85.0 + rng.rand(170.0)
    let r = 5.0       # ~5° square — small selectivity vs full globe
    totalHits += t.searchBBox(bbox(cx - r, cy - r, cx + r, cy + r), 0).len
  let dt = msSince(t0)
  echo &"BENCH rtree searchBBox (5°):    {queries:>9} queries in {dt:>5} ms => {rate(queries, dt):>10.0f} q/s  (avg hits ~{totalHits div queries})"

proc benchRTreeKNN(n, queries, k: int) =
  let pts = randomPoints(n, seed = 14)
  let t = newRTree()
  var entries: seq[(string, BBox)] = @[]
  for (id, x, y) in pts: entries.add((id, point(x, y)))
  t.bulkLoad(entries)
  var rng = initRand(101)
  var sumDist = 0.0
  let t0 = epochTime()
  for _ in 0 ..< queries:
    let qx = -180.0 + rng.rand(360.0)
    let qy =  -85.0 + rng.rand(170.0)
    for (_, d) in t.nearest(qx, qy, k): sumDist += d
  let dt = msSince(t0)
  echo &"BENCH rtree nearest k={k:<3}:        {queries:>9} queries in {dt:>5} ms => {rate(queries, dt):>10.0f} q/s"

proc benchRTreeNearestGeo(n, queries, k: int) =
  let pts = randomPoints(n, seed = 15)
  let t = newRTree()
  var entries: seq[(string, BBox)] = @[]
  for (id, x, y) in pts: entries.add((id, point(x, y)))
  t.bulkLoad(entries)
  var rng = initRand(103)
  var sumMetres = 0.0
  let t0 = epochTime()
  for _ in 0 ..< queries:
    let qx = -180.0 + rng.rand(360.0)
    let qy =  -85.0 + rng.rand(170.0)
    for (_, m) in t.nearestGeo(qx, qy, k): sumMetres += m
  let dt = msSince(t0)
  echo &"BENCH rtree nearestGeo k={k:<3}:     {queries:>9} queries in {dt:>5} ms => {rate(queries, dt):>10.0f} q/s"

# ---------------- geo index integrated with GlenDB ----------------

proc benchGeoIndexCreate(db: glendb.GlenDB; n: int) =
  let pts = randomPoints(n, seed = 21)
  let t0Ingest = epochTime()
  for (id, x, y) in pts: db.put("places", id, placeDoc(x, y))
  let dtIngest = msSince(t0Ingest)
  echo &"BENCH put (with no geo index):  {n:>9} docs    in {dtIngest:>5} ms => {rate(n, dtIngest):>10.0f} docs/s"

  let t1 = epochTime()
  db.createGeoIndex("places", "byLoc", "lon", "lat")
  let dt1 = msSince(t1)
  echo &"BENCH createGeoIndex (STR):     {n:>9} docs    in {dt1:>5} ms => {rate(n, dt1):>10.0f} docs/s"

proc benchGeoIndexedPut(db: glendb.GlenDB; n: int) =
  ## Cost of put when an active geo index is being maintained incrementally.
  let pts = randomPoints(n, seed = 22, box = (-100.0, 30.0, -90.0, 40.0))
  let t0 = epochTime()
  for (id, x, y) in pts: db.put("places", "fresh-" & id, placeDoc(x, y))
  let dt = msSince(t0)
  echo &"BENCH put (with geo index):     {n:>9} docs    in {dt:>5} ms => {rate(n, dt):>10.0f} docs/s"

proc benchFindWithinRadius(db: glendb.GlenDB; queries: int) =
  var rng = initRand(31)
  var hits = 0
  let t0 = epochTime()
  for _ in 0 ..< queries:
    let lon = -180.0 + rng.rand(360.0)
    let lat =  -85.0 + rng.rand(170.0)
    hits += db.findWithinRadius("places", "byLoc",
                                 lon, lat, radiusMeters = 100_000.0).len
  let dt = msSince(t0)
  echo &"BENCH findWithinRadius (100km): {queries:>9} queries in {dt:>5} ms => {rate(queries, dt):>10.0f} q/s  (avg hits ~{hits div queries})"

proc benchFindNearestPlanar(db: glendb.GlenDB; queries, k: int) =
  var rng = initRand(32)
  let t0 = epochTime()
  for _ in 0 ..< queries:
    discard db.findNearest("places", "byLoc",
                           -180.0 + rng.rand(360.0),
                           -85.0 + rng.rand(170.0),
                           k, metric = gmPlanar)
  let dt = msSince(t0)
  echo &"BENCH findNearest planar k={k:<3}:  {queries:>9} queries in {dt:>5} ms => {rate(queries, dt):>10.0f} q/s"

proc benchFindNearestGeographic(db: glendb.GlenDB; queries, k: int) =
  var rng = initRand(33)
  let t0 = epochTime()
  for _ in 0 ..< queries:
    discard db.findNearest("places", "byLoc",
                           -180.0 + rng.rand(360.0),
                           -85.0 + rng.rand(170.0),
                           k, metric = gmGeographic)
  let dt = msSince(t0)
  echo &"BENCH findNearest geo k={k:<3}:     {queries:>9} queries in {dt:>5} ms => {rate(queries, dt):>10.0f} q/s"

# ---------------- polygon index ----------------

proc randomPolygonDoc(rng: var Rand): Value =
  ## Random axis-aligned 4-vertex polygon, side length ~3°, anywhere on the globe.
  let cx = -170.0 + rng.rand(340.0)
  let cy =  -80.0 + rng.rand(160.0)
  let half = 1.0 + rng.rand(2.0)
  result = VObject()
  result["shape"] = VArray(@[
    VArray(@[VFloat(cx - half), VFloat(cy - half)]),
    VArray(@[VFloat(cx + half), VFloat(cy - half)]),
    VArray(@[VFloat(cx + half), VFloat(cy + half)]),
    VArray(@[VFloat(cx - half), VFloat(cy + half)])])

proc benchPolygonIndex(db: glendb.GlenDB; n, queries: int) =
  var rng = initRand(41)
  let t0Ingest = epochTime()
  for i in 0 ..< n:
    db.put("zones", "z" & $i, randomPolygonDoc(rng))
  let dtIngest = msSince(t0Ingest)
  echo &"BENCH put polygons:             {n:>9} docs    in {dtIngest:>5} ms => {rate(n, dtIngest):>10.0f} docs/s"

  let t1 = epochTime()
  db.createPolygonIndex("zones", "byShape", "shape")
  let dt1 = msSince(t1)
  echo &"BENCH createPolygonIndex (STR): {n:>9} docs    in {dt1:>5} ms => {rate(n, dt1):>10.0f} docs/s"

  var rngQ = initRand(42)
  var hits = 0
  let t2 = epochTime()
  for _ in 0 ..< queries:
    let qx = -170.0 + rngQ.rand(340.0)
    let qy =  -80.0 + rngQ.rand(160.0)
    hits += db.findPolygonsContaining("zones", "byShape", qx, qy).len
  let dt2 = msSince(t2)
  echo &"BENCH findPolygonsContaining:   {queries:>9} queries in {dt2:>5} ms => {rate(queries, dt2):>10.0f} q/s  (avg hits ~{hits div max(1, queries)})"

# ---------------- persistence ----------------

proc benchPersistence(dir: string) =
  ## Measures: compact (snapshot+.gri dump), then a full close+reopen, then
  ## the same close+reopen against a torn .gri (to time the bulk-rebuild path).
  let t0 = epochTime()
  let db = newGlenDB(dir)
  let dtOpen = msSince(t0)
  echo &"BENCH reopen w/ persisted geo:  ............. in {dtOpen:>5} ms"
  db.close()

  let db2 = newGlenDB(dir)
  let t1 = epochTime()
  db2.compact()
  let dt1 = msSince(t1)
  echo &"BENCH compact (snapshot+.gri):  ............. in {dt1:>5} ms"
  db2.close()

  # Corrupt the .gri trailer to force the bulk-rebuild fallback.
  let griPath = dir / "places.byLoc.gri"
  if fileExists(griPath):
    let f = open(griPath, fmReadWriteExisting)
    let sz = f.getFileSize()
    f.setFilePos(sz - 1)
    var junk: byte = 0xAA
    discard f.writeBuffer(addr junk, 1)
    f.close()
  let t2 = epochTime()
  let db3 = newGlenDB(dir)
  let dt2 = msSince(t2)
  echo &"BENCH reopen w/ bad .gri (rebuild path): .... in {dt2:>5} ms"
  db3.close()

# ---------------- main ----------------

when isMainModule:
  echo "------- raw R-tree (in-memory only, no GlenDB) -------"
  benchRTreeBulkLoad(100_000)
  benchRTreeBulkLoad(1_000_000)
  benchRTreeIncrementalInsert(100_000)
  benchRTreeBBoxSearch(100_000, 10_000)
  benchRTreeBBoxSearch(1_000_000, 1_000)
  benchRTreeKNN(100_000, 10_000, 10)
  benchRTreeKNN(1_000_000, 1_000, 10)
  benchRTreeNearestGeo(100_000, 10_000, 10)

  echo "\n------- GlenDB-integrated geo index -------"
  let dir = getTempDir() / "glen_bench_geo"
  if dirExists(dir): removeDir(dir)
  let database = newGlenDB(dir,
    cacheCapacity = 128 * 1024 * 1024, cacheShards = 32,
    walFlushEveryBytes = 16 * 1024 * 1024, lockStripesCount = 64)

  let N = 100_000
  benchGeoIndexCreate(database, N)
  benchGeoIndexedPut(database, 50_000)

  benchFindWithinRadius(database, 5_000)
  benchFindNearestPlanar(database, 5_000, k = 10)
  benchFindNearestGeographic(database, 5_000, k = 10)

  echo "\n------- polygon index -------"
  benchPolygonIndex(database, 50_000, 5_000)

  echo "\n------- persistence -------"
  database.close()
  benchPersistence(dir)
