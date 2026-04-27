import std/[os, unittest, math, strutils]
import glen/geo, glen/geomesh, glen/tilestack

const eps = 1e-12

proc constFrame(rows, cols, channels: int; v: float64): GeoMesh =
  result = newGeoMesh(bbox(0.0, 0.0, float64(cols), float64(rows)),
                      rows = rows, cols = cols, channels = channels)
  for i in 0 ..< result.data.len:
    result.data[i] = v

proc tileFileSize(dir: string): int64 =
  for kind, path in walkDir(dir):
    if kind == pcFile and path.endsWith(".tts"):
      result += getFileSize(path)

suite "tilestack: constant-chunk RLE":
  test "all-zero radar field encodes to a tiny chunk":
    let dir = getTempDir() / "glen_tilestack_rle_zero"
    removeDir(dir)
    let s = newTileStack(dir,
      bbox = bbox(0.0, 0.0, 64.0, 64.0),
      rows = 64, cols = 64, channels = 1,
      tileSize = 64, chunkSize = 32)
    let zero = constFrame(64, 64, 1, 0.0)
    for i in 0 ..< 32:
      s.appendFrame(1_000_000 + int64(i) * 60_000, zero)
    s.flush()
    s.close()
    # 32 frames × 4096 cells × 8 bytes = 1 MB raw. With constant-chunk RLE,
    # the on-disk file should be a few hundred bytes (header + ts + 1×f64 + crc).
    let sz = tileFileSize(dir)
    check sz < 300   # generous upper bound

  test "constant-chunk round-trip preserves values":
    let dir = getTempDir() / "glen_tilestack_rle_round"
    removeDir(dir)
    let s = newTileStack(dir,
      bbox = bbox(0.0, 0.0, 32.0, 32.0),
      rows = 32, cols = 32, channels = 2,
      tileSize = 32, chunkSize = 16)
    let frame = constFrame(32, 32, 2, 3.14)
    for i in 0 ..< 16:
      s.appendFrame(int64(i) * 1000, frame)
    s.flush()
    # Read back every frame.
    for i in 0 ..< 16:
      let (ok, back) = s.readFrame(int64(i) * 1000)
      check ok
      for v in back.data:
        check abs(v - 3.14) < eps
    s.close()
    # Reopen and verify.
    let s2 = openTileStack(dir)
    let (ok, back) = s2.readFrame(0)
    check ok
    for v in back.data:
      check abs(v - 3.14) < eps
    # readPointHistory should also yield the constant.
    let pts = s2.readPointHistory(16.0, 16.0, 0, 16_000_000)
    check pts.len == 16
    for (_, v) in pts:
      check abs(v - 3.14) < eps
    s2.close()

  test "mixed (non-constant) chunk falls back to Gorilla":
    let dir = getTempDir() / "glen_tilestack_rle_mixed"
    removeDir(dir)
    let s = newTileStack(dir,
      bbox = bbox(0.0, 0.0, 16.0, 16.0),
      rows = 16, cols = 16, channels = 1,
      tileSize = 16, chunkSize = 8)
    # Each frame has a single non-zero cell; rest zero. Not whole-chunk-constant.
    for i in 0 ..< 8:
      var m = newGeoMesh(bbox(0.0, 0.0, 16.0, 16.0),
                         rows = 16, cols = 16, channels = 1)
      m.data[i] = 1.0
      s.appendFrame(int64(i) * 1000, m)
    s.flush()
    # Read back: every frame's hot cell should still be 1.0.
    for i in 0 ..< 8:
      let (ok, back) = s.readFrame(int64(i) * 1000)
      check ok
      check abs(back.data[i] - 1.0) < eps
    s.close()

  test "single-frame constant chunk":
    let dir = getTempDir() / "glen_tilestack_rle_single"
    removeDir(dir)
    let s = newTileStack(dir,
      bbox = bbox(0.0, 0.0, 8.0, 8.0),
      rows = 8, cols = 8, channels = 1,
      tileSize = 8, chunkSize = 1)
    let m = constFrame(8, 8, 1, -2.5)
    s.appendFrame(42, m)
    s.flush()
    let (ok, back) = s.readFrame(42)
    check ok
    for v in back.data:
      check abs(v + 2.5) < eps
    s.close()

  test "constant chunk durably round-trips after close + reopen":
    let dir = getTempDir() / "glen_tilestack_rle_durable"
    removeDir(dir)
    block:
      let s = newTileStack(dir,
        bbox = bbox(0.0, 0.0, 16.0, 16.0),
        rows = 16, cols = 16, channels = 1,
        tileSize = 16, chunkSize = 4)
      let zero = constFrame(16, 16, 1, 0.0)
      for i in 0 ..< 4: s.appendFrame(int64(i), zero)
      s.flush()
      s.close()
    # Reopen
    let s2 = openTileStack(dir)
    for i in 0 ..< 4:
      let (ok, back) = s2.readFrame(int64(i))
      check ok
      for v in back.data: check abs(v) < eps
    s2.close()
