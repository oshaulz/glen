## Benchmark: read-only mode vs read-write mode.
##
## Measures four things on the same dataset:
##   1. Open time (how fast does newGlenDB return?)
##   2. Random `get` throughput
##   3. Range-scan throughput (whole-collection iteration)
##   4. Parallel-open: how many read-only handles can attach concurrently
##      (vs read-write, which holds the WAL exclusively)
##
## Build and run with:
##   nim c -r -d:release --mm:orc --passC:-O3 --threads:on \
##     --path:src tests/test_bench_readonly.nim

import std/[os, times, strformat, strutils, monotimes, random, math]
import glen/glen
import glen/types, glen/db as glendb
import glen/dsl

const
  DocCount      = 50_000
  GetSamples    = 50_000
  OpenSamples   = 20
  SeriesSamples = 100_000     # samples per series file
  SeriesQueries = 5_000       # range queries to time
  GeoCount      = 20_000
  GeoQueries    = 10_000
  VecCount      = 5_000
  VecDim        = 64
  VecQueries    = 1_000

proc freshDir(name: string): string =
  result = getTempDir() / name
  removeDir(result)
  createDir(result)

proc seed(dir: string) =
  let db = newGlenDB(dir)
  var batch: seq[(string, Value)] = newSeq[(string, Value)](DocCount)
  for i in 0 ..< DocCount:
    batch[i] = ("doc-" & align($i, 7, '0'),
                %*{"i": i, "name": "name-" & $i, "tag": $(i mod 17)})
  db.putMany("k", batch)
  db.createIndex("k", "byTag", "tag")
  db.snapshotAll()
  db.compact()
  db.close()

template timeit(body: untyped): float =
  ## Returns elapsed seconds for `body` as float.
  let t0 = getMonoTime()
  body
  inMicroseconds(getMonoTime() - t0).float / 1_000_000.0

# ---------------------------------------------------------------------------

proc benchOpen(dir: string) =
  echo "—— open time (snapshot already compacted) ——"
  var totalRO = 0.0
  var totalRW = 0.0
  for _ in 0 ..< OpenSamples:
    let rwSec = timeit:
      block:
        let db = newGlenDB(dir)
        db.close()
    totalRW = totalRW + rwSec
    let roSec = timeit:
      block:
        let db = newGlenDB(dir, readOnly = true)
        db.close()
    totalRO = totalRO + roSec
  let avgRW = totalRW / OpenSamples.float * 1000.0
  let avgRO = totalRO / OpenSamples.float * 1000.0
  echo &"  RW open avg: {avgRW:6.2f} ms"
  echo &"  RO open avg: {avgRO:6.2f} ms"
  echo &"  Speedup:     {avgRW / avgRO:6.2f}×"

proc benchRandomGet(db: GlenDB; label: string): float =
  randomize(42)
  var total = 0
  let sec = timeit:
    for _ in 0 ..< GetSamples:
      let i = rand(DocCount - 1)
      let v = db.get("k", "doc-" & align($i, 7, '0'))
      if not v.isNil: inc total
  let ops = float(GetSamples) / sec
  echo &"  {label}: {sec*1000:7.1f} ms total, {ops:>10.0f} get/s ({total} hits)"
  ops

proc benchScan(db: GlenDB; label: string): int =
  var n = 0
  let sec = timeit:
    for _, _ in db.getBorrowedAllStream("k"): inc n
  let ops = float(n) / sec
  echo &"  {label}: {sec*1000:7.1f} ms scan, {ops:>10.0f} doc/s"
  n

proc seedSeries(dir: string) =
  let db = newGlenDB(dir)
  let s = db.series("temp.s1")
  for i in 0 ..< SeriesSamples:
    s.append(int64(i * 1000), 20.0 + sin(float64(i) / 100.0))
  s.flush()
  s.close()
  db.close()

proc benchSeriesRange(db: GlenDB; label: string;
                      cacheSize = DefaultDecodedChunkCacheSize): float =
  let s = db.series("temp.s1", decodedChunkCacheSize = cacheSize)
  randomize(7)
  var totalSamples = 0
  let sec = timeit:
    for _ in 0 ..< SeriesQueries:
      # Random window of 100 samples within the series.
      let start = rand(SeriesSamples - 100)
      let rng = s.range(int64(start * 1000),
                        int64((start + 99) * 1000))
      totalSamples = totalSamples + rng.len
  s.close()
  let ops = float(SeriesQueries) / sec
  echo &"  {label}: {sec*1000:7.1f} ms total, {ops:>10.0f} range/s ({totalSamples} samples)"
  ops

proc seedGeo(dir: string) =
  let db = newGlenDB(dir)
  randomize(11)
  for i in 0 ..< GeoCount:
    db.put("places", "p" & $i,
           %*{"lon": rand(-180.0 .. 180.0),
              "lat": rand(-85.0 .. 85.0),
              "kind": $(i mod 7)})
  db.createGeoIndex("places", "byLoc", "lon", "lat")
  db.snapshotAll()
  db.compact()
  db.close()

proc benchGeoNearest(db: GlenDB; label: string): float =
  randomize(13)
  var hits = 0
  let sec = timeit:
    for _ in 0 ..< GeoQueries:
      let lon = rand(-180.0 .. 180.0)
      let lat = rand(-85.0 .. 85.0)
      let res = db.findNearest("places", "byLoc", lon, lat, 5,
                               metric = gmGeographic)
      hits = hits + res.len
  let ops = float(GeoQueries) / sec
  echo &"  {label}: {sec*1000:7.1f} ms total, {ops:>10.0f} kNN/s ({hits} hits)"
  ops

proc seedVectors(dir: string) =
  let db = newGlenDB(dir)
  let idx = db.vectors("docs", "byEmbed")
  idx.create("embedding", dim = VecDim)
  randomize(17)
  for i in 0 ..< VecCount:
    var v = newSeq[float64](VecDim)
    for j in 0 ..< VecDim: v[j] = rand(-1.0 .. 1.0)
    idx.upsert($i, %*{"embedding": v})
  db.snapshotAll()
  db.compact()
  db.close()

proc benchVectorSearch(db: GlenDB; label: string): float =
  let idx = db.vectors("docs", "byEmbed")
  randomize(19)
  var hits = 0
  let sec = timeit:
    for _ in 0 ..< VecQueries:
      var q = newSeq[float64](VecDim)
      for j in 0 ..< VecDim: q[j] = rand(-1.0 .. 1.0)
      let res = idx.search(vec32(q), k = 10)
      hits = hits + res.len
  let ops = float(VecQueries) / sec
  echo &"  {label}: {sec*1000:7.1f} ms total, {ops:>10.0f} kNN/s ({hits} hits)"
  ops

proc benchParallel(dir: string) =
  echo "—— concurrent opens of the same dir ——"
  # Open many read-only handles at once.
  var handles: seq[GlenDB] = @[]
  let many = timeit:
    for _ in 0 ..< 100:
      handles.add(newGlenDB(dir, readOnly = true))
  echo &"  100 RO opens: {many*1000:6.1f} ms ({many*10:.2f} ms each on avg)"
  # Sanity: every handle reads.
  var ok = 0
  for h in handles:
    let v = h.get("k", "doc-0000000")
    if not v.isNil: inc ok
  echo &"  All {ok} handles read OK"
  for h in handles: h.close()

# ---------------------------------------------------------------------------

when isMainModule:
  let dir = freshDir("glen_bench_readonly")
  echo &"=== Glen read-only mode benchmark ==="
  echo &"Dataset: {DocCount} docs in one collection, one equality index"
  echo &"Snapshot is compacted before timing (WAL empty)"
  echo ""

  echo &"Seeding..."
  let seedSec = timeit: seed(dir)
  echo &"  seeded in {seedSec*1000:.0f} ms"
  echo ""

  benchOpen(dir)
  echo ""

  echo "—— random get (50k samples) ——"
  var roGetOps, rwGetOps: float
  block:
    let rw = newGlenDB(dir)
    rwGetOps = benchRandomGet(rw, "RW")
    rw.close()
  block:
    let ro = newGlenDB(dir, readOnly = true)
    roGetOps = benchRandomGet(ro, "RO")
    ro.close()
  echo &"  RO/RW get throughput ratio: {roGetOps / rwGetOps:.2f}×"

  echo ""
  echo "—— full scan ——"
  block:
    let rw = newGlenDB(dir)
    discard benchScan(rw, "RW")
    rw.close()
  block:
    let ro = newGlenDB(dir, readOnly = true)
    discard benchScan(ro, "RO")
    ro.close()

  echo ""
  benchParallel(dir)

  # ---- Time series ------------------------------------------------------
  let dirSer = freshDir("glen_bench_readonly_series")
  echo ""
  echo &"Seeding {SeriesSamples} time-series samples..."
  let serSeedSec = timeit: seedSeries(dirSer)
  echo &"  seeded in {serSeedSec*1000:.0f} ms"
  echo &"—— time-series random range ({SeriesQueries} 100-sample windows) ——"
  var rwSer, roSer, roTightSer: float
  block:
    let rw = newGlenDB(dirSer)
    rwSer = benchSeriesRange(rw, "RW (default cache)")
    rw.close()
  block:
    let ro = newGlenDB(dirSer, readOnly = true)
    roSer = benchSeriesRange(ro, "RO (default cache)")
    ro.close()
  # Same workload with cache size = 1 — every query that crosses a chunk
  # boundary forces a re-decode. Demonstrates the 'tight RAM' tradeoff.
  block:
    let ro = newGlenDB(dirSer, readOnly = true)
    roTightSer = benchSeriesRange(ro, "RO (cache = 1, tight RAM)",
                                  cacheSize = 1)
    ro.close()
  echo &"  RO/RW range throughput ratio: {roSer / rwSer:.2f}×"
  echo &"  Cache-size impact (default vs 1): {roSer / roTightSer:.2f}× faster with default cache"

  # ---- Geo R-tree ------------------------------------------------------
  let dirGeo = freshDir("glen_bench_readonly_geo")
  echo ""
  echo &"Seeding {GeoCount} geo points + R-tree..."
  let geoSeedSec = timeit: seedGeo(dirGeo)
  echo &"  seeded in {geoSeedSec*1000:.0f} ms"
  echo &"—— geo k-NN (haversine, k=5, {GeoQueries} queries) ——"
  var rwGeo, roGeo: float
  block:
    let rw = newGlenDB(dirGeo)
    rwGeo = benchGeoNearest(rw, "RW")
    rw.close()
  block:
    let ro = newGlenDB(dirGeo, readOnly = true)
    roGeo = benchGeoNearest(ro, "RO")
    ro.close()
  echo &"  RO/RW geo throughput ratio: {roGeo / rwGeo:.2f}×"

  # ---- Vector HNSW ------------------------------------------------------
  let dirVec = freshDir("glen_bench_readonly_vec")
  echo ""
  echo &"Seeding {VecCount} {VecDim}-dim vectors + HNSW..."
  let vecSeedSec = timeit: seedVectors(dirVec)
  echo &"  seeded in {vecSeedSec*1000:.0f} ms"
  echo &"—— vector k-NN (cosine, k=10, {VecQueries} queries) ——"
  var rwVec, roVec: float
  block:
    let rw = newGlenDB(dirVec)
    rwVec = benchVectorSearch(rw, "RW")
    rw.close()
  block:
    let ro = newGlenDB(dirVec, readOnly = true)
    roVec = benchVectorSearch(ro, "RO")
    ro.close()
  echo &"  RO/RW vector throughput ratio: {roVec / rwVec:.2f}×"

  echo ""
  echo "Done."
