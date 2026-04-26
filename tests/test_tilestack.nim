import std/[os, unittest, math, random]
import glen/geo, glen/geomesh, glen/tilestack

const eps = 1e-12

proc fillFrame(rows, cols, channels: int;
               step: int; rng: var Rand): GeoMesh =
  result = newGeoMesh(bbox(0.0, 0.0, float64(cols), float64(rows)),
                      rows = rows, cols = cols, channels = channels)
  for r in 0 ..< rows:
    for c in 0 ..< cols:
      var values: seq[float64] = @[]
      for k in 0 ..< channels:
        values.add(float64(step) * 0.01 + 0.001 * float64(r * cols + c) + 0.0001 * float64(k))
      result.setCell(r, c, values)

suite "tilestack: encode/decode round-trip":
  test "single tile, single frame":
    let dir = getTempDir() / "glen_ts_single_frame"
    removeDir(dir)
    let s = newTileStack(dir,
      bbox = bbox(0.0, 0.0, 8.0, 8.0), rows = 8, cols = 8, channels = 1,
      tileSize = 16, chunkSize = 4)   # one tile covers everything
    var rng = initRand(1)
    let f0 = fillFrame(8, 8, 1, step = 0, rng = rng)
    s.appendFrame(1000, f0)
    s.flush()
    let (ok, back) = s.readFrame(1000)
    check ok
    check back.rows == 8 and back.cols == 8 and back.channels == 1
    for i in 0 ..< back.data.len:
      check abs(back.data[i] - f0.data[i]) < eps
    s.close()

  test "multi-tile, multi-frame, multi-channel":
    let dir = getTempDir() / "glen_ts_multi"
    removeDir(dir)
    let s = newTileStack(dir,
      bbox = bbox(0.0, 0.0, 32.0, 32.0), rows = 32, cols = 32, channels = 3,
      tileSize = 16, chunkSize = 8,
      labels = @["a", "b", "c"])
    var rng = initRand(7)
    var stored: seq[(int64, GeoMesh)] = @[]
    for i in 0 ..< 20:
      let f = fillFrame(32, 32, 3, step = i, rng = rng)
      let ts = int64(1000 + i * 1000)
      stored.add((ts, f))
      s.appendFrame(ts, f)
    s.flush()
    for (ts, want) in stored:
      let (ok, got) = s.readFrame(ts)
      check ok
      check got.data.len == want.data.len
      for i in 0 ..< want.data.len:
        check abs(got.data[i] - want.data[i]) < eps
    s.close()

  test "non-square tiling (edge tiles smaller)":
    let dir = getTempDir() / "glen_ts_edge_tiles"
    removeDir(dir)
    let s = newTileStack(dir,
      bbox = bbox(0.0, 0.0, 70.0, 50.0), rows = 50, cols = 70, channels = 1,
      tileSize = 32, chunkSize = 4)    # grid: ceil(50/32)=2 × ceil(70/32)=3
    let (gridR, gridC) = s.tileGridDims()
    check gridR == 2 and gridC == 3
    var rng = initRand(2)
    let f = fillFrame(50, 70, 1, step = 1, rng = rng)
    s.appendFrame(1000, f)
    s.flush()
    let (ok, got) = s.readFrame(1000)
    check ok
    check got.data == f.data
    s.close()

suite "tilestack: point history":
  test "single cell decoded out of all-tile chunks":
    let dir = getTempDir() / "glen_ts_point"
    removeDir(dir)
    let s = newTileStack(dir,
      bbox = bbox(0.0, 0.0, 16.0, 16.0), rows = 16, cols = 16, channels = 1,
      tileSize = 8, chunkSize = 8)
    # Write 30 frames; each cell's value is a known function of (ts, r, c).
    var expectedValues: seq[(int64, float64)] = @[]
    for i in 0 ..< 30:
      let ts = int64(1000 + i * 1000)
      let v = sin(float64(i) * 0.1) + 0.001 * float64(i)
      var f = newGeoMesh(s.bbox, rows = 16, cols = 16, channels = 1)
      # set cell (3, 5) to a specific value tied to ts; everything else stays 0
      f[3, 5, 0] = v
      s.appendFrame(ts, f)
      expectedValues.add((ts, v))
    s.flush()
    # cellAt should pick row=3, col=5 for a query in the middle of that cell
    let (lon, lat) = newGeoMesh(s.bbox, 16, 16, 1).cellCenter(3, 5)
    let history = s.readPointHistory(lon, lat,
                                     fromMs = low(int64), toMs = high(int64))
    check history.len == expectedValues.len
    for i, (ts, v) in expectedValues:
      check history[i][0] == ts
      check abs(history[i][1] - v) < eps
    s.close()

  test "respects time bounds":
    let dir = getTempDir() / "glen_ts_point_bounds"
    removeDir(dir)
    let s = newTileStack(dir,
      bbox = bbox(0.0, 0.0, 8.0, 8.0), rows = 8, cols = 8, channels = 1,
      tileSize = 8, chunkSize = 4)
    for i in 0 ..< 10:
      var f = newGeoMesh(s.bbox, rows = 8, cols = 8, channels = 1)
      f[0, 0, 0] = float64(i)
      s.appendFrame(int64(i) * 1000, f)
    s.flush()
    # Should pull only ts in [3000, 7000] — that's i=3..7 = 5 frames
    let h = s.readPointHistory(0.5, 7.5,
                               fromMs = 3000, toMs = 7000)
    check h.len == 5
    check h[0][0] == 3000 and h[^1][0] == 7000
    s.close()

  test "channel selection":
    let dir = getTempDir() / "glen_ts_point_channel"
    removeDir(dir)
    let s = newTileStack(dir,
      bbox = bbox(0.0, 0.0, 4.0, 4.0), rows = 4, cols = 4, channels = 3,
      tileSize = 4, chunkSize = 4,
      labels = @["a", "b", "c"])
    for i in 0 ..< 8:
      var f = newGeoMesh(s.bbox, rows = 4, cols = 4, channels = 3)
      f[1, 1, 0] = float64(i) * 1.0
      f[1, 1, 1] = float64(i) * 10.0
      f[1, 1, 2] = float64(i) * 100.0
      s.appendFrame(int64(i), f)
    s.flush()
    let (lon, lat) = newGeoMesh(s.bbox, 4, 4, 3).cellCenter(1, 1)
    let hA = s.readPointHistory(lon, lat, low(int64), high(int64), channel = s.channelIndex("a"))
    let hB = s.readPointHistory(lon, lat, low(int64), high(int64), channel = s.channelIndex("b"))
    let hC = s.readPointHistory(lon, lat, low(int64), high(int64), channel = s.channelIndex("c"))
    check hA[3][1] == 3.0
    check hB[3][1] == 30.0
    check hC[3][1] == 300.0
    s.close()

suite "tilestack: persistence":
  test "manifest + chunks survive reopen":
    let dir = getTempDir() / "glen_ts_persist"
    removeDir(dir)
    block:
      let s = newTileStack(dir,
        bbox = bbox(-1.0, -1.0, 1.0, 1.0), rows = 4, cols = 4, channels = 1,
        tileSize = 2, chunkSize = 4)
      for i in 0 ..< 10:
        var f = newGeoMesh(s.bbox, rows = 4, cols = 4, channels = 1)
        for r in 0 ..< 4:
          for c in 0 ..< 4:
            f[r, c, 0] = float64(i * 100 + r * 4 + c)
        s.appendFrame(int64(i) * 1000, f)
      s.close()
    let s = openTileStack(dir)
    check s.rows == 4 and s.cols == 4
    let (ok, f5) = s.readFrame(5000)
    check ok
    for r in 0 ..< 4:
      for c in 0 ..< 4:
        check f5[r, c, 0] == float64(500 + r * 4 + c)
    s.close()

  test "torn chunk tail is tolerated":
    let dir = getTempDir() / "glen_ts_torn"
    removeDir(dir)
    block:
      let s = newTileStack(dir,
        bbox = bbox(0.0, 0.0, 2.0, 2.0), rows = 2, cols = 2, channels = 1,
        tileSize = 2, chunkSize = 4)
      for i in 0 ..< 8:
        var f = newGeoMesh(s.bbox, 2, 2, 1)
        f[0, 0, 0] = float64(i)
        s.appendFrame(int64(i), f)
      s.close()
    # Find tile_0_0.tts, corrupt the last 4 bytes (the CRC of the second chunk)
    let p = dir / "tile_0_0.tts"
    let f = open(p, fmReadWriteExisting)
    let sz = f.getFileSize()
    f.setFilePos(sz - 4)
    var junk: array[4, byte] = [0xFF'u8, 0xFF, 0xFF, 0xFF]
    discard f.writeBuffer(addr junk[0], 4)
    f.close()
    let s = openTileStack(dir)
    # First chunk (4 frames, ts 0..3) should be readable; second chunk lost.
    let (ok0, _) = s.readFrame(0)
    check ok0
    let (ok7, _) = s.readFrame(7)
    check not ok7
    s.close()

suite "tilestack: range query":
  test "readFrameRange returns frames in window in order":
    let dir = getTempDir() / "glen_ts_range"
    removeDir(dir)
    let s = newTileStack(dir,
      bbox = bbox(0.0, 0.0, 4.0, 4.0), rows = 4, cols = 4, channels = 1,
      tileSize = 4, chunkSize = 5)
    for i in 0 ..< 20:
      var f = newGeoMesh(s.bbox, 4, 4, 1)
      f[0, 0, 0] = float64(i)
      s.appendFrame(int64(i) * 100, f)
    s.flush()
    let frames = s.readFrameRange(500, 1500)
    # i=5..15 inclusive = 11 frames
    check frames.len == 11
    check frames[0][0] == 500
    check frames[^1][0] == 1500
    for i, (ts, m) in frames:
      check m[0, 0, 0] == float64(int(ts) div 100)
    s.close()

suite "tilestack: compression sanity":
  test "radar-like sparse field compresses dramatically":
    # Most cells are 0 (clear sky); a moving "storm" region has non-zero values
    # that change slowly. This is the workload Gorilla was designed for.
    let dir = getTempDir() / "glen_ts_compress"
    removeDir(dir)
    let s = newTileStack(dir,
      bbox = bbox(0.0, 0.0, 64.0, 64.0), rows = 64, cols = 64, channels = 1,
      tileSize = 64, chunkSize = 128)
    for i in 0 ..< 128:
      var f = newGeoMesh(s.bbox, 64, 64, 1)
      # Storm at cells (cx ± 4, cy ± 4) drifting east.
      let cx = 16 + i div 16
      let cy = 32
      for r in 0 ..< 64:
        for c in 0 ..< 64:
          let dr = r - cy; let dc = c - cx
          if dr * dr + dc * dc <= 16:
            f[r, c, 0] = 30.0 + float64(i) * 0.05    # slowly intensifying
      s.appendFrame(int64(i) * 1000, f)
    s.flush()
    s.close()
    let p = dir / "tile_0_0.tts"
    let bytes = getFileSize(p)
    # Raw: 128 × 64 × 64 × 8 = 4_194_304 bytes.
    # Sparse + slowly-varying ⇒ XOR=0 on most samples ⇒ ~1 bit/cell/frame.
    check bytes > 0
    check bytes < 200_000

  test "fully-varying field still compresses better than raw":
    # Every cell wiggles slightly each frame — close to worst-case for XOR
    # because the meaningful-bit window is ~30 bits/sample. Should still beat raw.
    let dir = getTempDir() / "glen_ts_compress_full"
    removeDir(dir)
    let s = newTileStack(dir,
      bbox = bbox(0.0, 0.0, 64.0, 64.0), rows = 64, cols = 64, channels = 1,
      tileSize = 64, chunkSize = 128)
    for i in 0 ..< 128:
      var f = newGeoMesh(s.bbox, 64, 64, 1)
      let phase = float64(i) * 0.01
      for r in 0 ..< 64:
        for c in 0 ..< 64:
          f[r, c, 0] = sin(0.05 * float64(r) + phase) * cos(0.05 * float64(c) + phase)
      s.appendFrame(int64(i) * 1000, f)
    s.flush()
    s.close()
    let p = dir / "tile_0_0.tts"
    let bytes = getFileSize(p)
    # 4 MB raw; expect modest savings (2-4× target) for fully-varying smooth data.
    check bytes < 4_000_000
