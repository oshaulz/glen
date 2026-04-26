# Time-series benchmarks: standalone Gorilla TSDB (`glen/timeseries`) and
# the tile time-stack engine (`glen/tilestack`). Run with -d:release for
# realistic figures:
#
#   nimble bench_timeseries
#
# Or by hand:
#   nim c -r -d:release --mm:orc --passC:-O3 --threads:on --path:src \
#         tests/test_bench_timeseries.nim

import std/[os, times, strformat, math, random, strutils]
import glen/timeseries
import glen/geo, glen/geomesh, glen/tilestack

proc msSince(t0: float): int64 = int64((epochTime() - t0) * 1000.0)
proc rate(n: int; dtMs: int64): float =
  if dtMs == 0: 0.0 else: (float(n) * 1000.0) / float(dtMs)

# ============================================================================
# Gorilla TSDB benchmarks
# ============================================================================

proc benchSeriesAppend(label, path: string; n: int; valueGen: proc(i: int): float64) =
  if fileExists(path): removeFile(path)
  let s = openSeries(path, blockSize = 4096, fsyncOnFlush = false)
  let t0 = epochTime()
  for i in 0 ..< n:
    s.append(int64(i) * 1000, valueGen(i))
  s.flush()
  let dt = msSince(t0)
  s.close()
  let bytes = getFileSize(path)
  let bitsPerSample = (float(bytes) * 8.0) / float(n)
  echo &"BENCH series append {label:<22} {n:>9} samples in {dt:>5} ms => {rate(n, dt):>10.0f} samples/s  (on disk: {bytes:>10} B, {bitsPerSample:>5.2f} bits/sample)"

proc benchSeriesRange(path: string; n, queries: int) =
  let s = openSeries(path)
  var rng = initRand(7)
  var totalReturned = 0
  let t0 = epochTime()
  for _ in 0 ..< queries:
    let from0 = rng.rand(n - 1) * 1000
    let span = 100 + rng.rand(900)
    let to0 = from0 + span * 1000
    totalReturned += s.range(int64(from0), int64(to0)).len
  let dt = msSince(t0)
  s.close()
  echo &"BENCH series range (random):    {queries:>9} queries in {dt:>5} ms => {rate(queries, dt):>10.0f} q/s  (avg returned ~{totalReturned div queries})"

proc benchSeriesLatestN(path: string; queries, k: int) =
  let s = openSeries(path)
  let t0 = epochTime()
  for _ in 0 ..< queries:
    discard s.latest(k)
  let dt = msSince(t0)
  s.close()
  echo &"BENCH series latest n={k:<5}      {queries:>9} queries in {dt:>5} ms => {rate(queries, dt):>10.0f} q/s"

proc benchSeriesReopenScan(path: string) =
  let t0 = epochTime()
  let s = openSeries(path)
  let dt = msSince(t0)
  let total = s.len
  s.close()
  echo &"BENCH series open (scan blocks): {total:>9} samples in {dt:>5} ms"

# ============================================================================
# Tile time-stack benchmarks
# ============================================================================

proc fillRadarFrame(rows, cols: int; step: int): GeoMesh =
  ## Storm-like sparse field: clear sky everywhere except a moving disc.
  result = newGeoMesh(bbox(0.0, 0.0, float64(cols), float64(rows)),
                      rows = rows, cols = cols, channels = 1)
  let cx = (cols div 4) + (step mod (cols div 2))
  let cy = rows div 2
  for r in 0 ..< rows:
    for c in 0 ..< cols:
      let dr = r - cy; let dc = c - cx
      if dr * dr + dc * dc <= 64:
        result[r, c, 0] = 30.0 + float64(step) * 0.05

proc benchTileStackAppend(dir: string; rows, cols, frames, tileSize, chunkSize: int) =
  if dirExists(dir): removeDir(dir)
  let s = newTileStack(dir,
    bbox = bbox(0.0, 0.0, float64(cols), float64(rows)),
    rows = rows, cols = cols, channels = 1,
    tileSize = tileSize, chunkSize = chunkSize, labels = @["dbz"])
  let t0 = epochTime()
  for i in 0 ..< frames:
    s.appendFrame(int64(i) * 60_000, fillRadarFrame(rows, cols, i))
  s.flush()
  let dt = msSince(t0)
  s.close()

  # Disk size summary: total of all tile_*.tts plus the manifest.
  var diskBytes = 0
  for kind, path in walkDir(dir):
    if kind == pcFile:
      if path.endsWith(".tts") or path.endsWith(".tsm"):
        diskBytes += int(getFileSize(path))
  let cellsTotal = rows * cols * frames
  let bitsPerCell = (float(diskBytes) * 8.0) / float(cellsTotal)
  let rawBytes = cellsTotal * 8
  let ratio =
    if diskBytes == 0: 0.0
    else: float(rawBytes) / float(diskBytes)
  echo &"BENCH tilestack append ({rows}×{cols}, {frames} frames): {frames:>5} frames in {dt:>5} ms => {rate(frames, dt):>10.0f} frames/s  (disk {diskBytes:>10} B vs raw {rawBytes:>10} B = {ratio:>5.1f}× compression, {bitsPerCell:>5.2f} bits/cell)"

proc benchTileStackPointHistory(dir: string; queries: int) =
  let s = openTileStack(dir)
  var rng = initRand(13)
  var totalSamples = 0
  let t0 = epochTime()
  for _ in 0 ..< queries:
    let lon = rng.rand(s.cols.float64 - 1.0)
    let lat = rng.rand(s.rows.float64 - 1.0)
    totalSamples += s.readPointHistory(lon, lat,
                                       low(int64), high(int64), 0).len
  let dt = msSince(t0)
  s.close()
  echo &"BENCH tilestack readPointHistory: {queries:>7} queries in {dt:>5} ms => {rate(queries, dt):>10.0f} q/s  (avg ~{totalSamples div queries} samples per call)"

proc benchTileStackReadFrame(dir: string; numFrames, queries: int) =
  ## numFrames must match the number of frames the producer wrote (frames are
  ## stored at i*60_000 ms for i in [0, numFrames)).
  let s = openTileStack(dir)
  var rng = initRand(17)
  let t0 = epochTime()
  for _ in 0 ..< queries:
    let i = rng.rand(numFrames - 1)
    discard s.readFrame(int64(i) * 60_000)
  let dt = msSince(t0)
  s.close()
  echo &"BENCH tilestack readFrame:        {queries:>7} queries in {dt:>5} ms => {rate(queries, dt):>10.0f} q/s"

# ============================================================================
# Main
# ============================================================================

when isMainModule:
  echo "------- glen/timeseries (Gorilla scalar TSDB) -------"
  let path = getTempDir() / "glen_bench_ts.gts"

  let n = 1_000_000
  # Constant value: ideal compressibility (~1 bit/sample)
  benchSeriesAppend("constant",        path,
                    n, proc(i: int): float64 = 42.0)
  benchSeriesAppend("regular cadence", path,
                    n, proc(i: int): float64 = float64(i) * 0.5)
  benchSeriesAppend("smooth (sin)",    path,
                    n, proc(i: int): float64 = sin(float64(i) * 0.001))
  benchSeriesAppend("noisy",           path,
                    n,
                    proc(i: int): float64 =
                      sin(float64(i) * 0.001) +
                      (float64(i mod 1000) - 500.0) / 1_000_000.0)

  # Reuse the noisy series for read benchmarks (it's the most realistic).
  benchSeriesReopenScan(path)
  benchSeriesRange(path, n, 5_000)
  benchSeriesLatestN(path, 100_000, 100)
  benchSeriesLatestN(path,  10_000, 1000)
  removeFile(path)

  echo "\n------- glen/tilestack (Gorilla raster-over-time) -------"

  let dir = getTempDir() / "glen_bench_tilestack"

  # Small radar-shaped grid, modest frame count: typical NEXRAD-ish workload.
  benchTileStackAppend(dir, rows = 200, cols = 200, frames = 200,
                       tileSize = 64, chunkSize = 64)
  benchTileStackPointHistory(dir, queries = 1000)
  benchTileStackReadFrame(dir, numFrames = 200, queries = 200)
  removeDir(dir)

  # Larger grid, fewer frames — model-output style.
  benchTileStackAppend(dir, rows = 512, cols = 512, frames = 64,
                       tileSize = 128, chunkSize = 32)
  benchTileStackPointHistory(dir, queries = 1000)
  benchTileStackReadFrame(dir, numFrames = 64, queries = 50)
  removeDir(dir)
