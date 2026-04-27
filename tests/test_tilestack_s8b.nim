import std/[os, unittest, math, strutils]
import glen/geo, glen/geomesh, glen/tilestack

const eps = 1e-12

proc waveFrame(rows, cols: int; t: int): GeoMesh =
  result = newGeoMesh(bbox(0.0, 0.0, float64(cols), float64(rows)),
                      rows = rows, cols = cols, channels = 1)
  for r in 0 ..< rows:
    for c in 0 ..< cols:
      result.data[r * cols + c] = sin(float64(t) * 0.05 + float64(r * cols + c) * 0.001)

proc tileFileSize(dir: string): int64 =
  for kind, path in walkDir(dir):
    if kind == pcFile and path.endsWith(".tts"):
      result += getFileSize(path)

suite "tilestack: Simple-8b ts codec":
  test "regular-cadence chunk round-trips through Simple-8b path":
    let dir = getTempDir() / "glen_tilestack_s8b_round"
    removeDir(dir)
    let s = newTileStack(dir,
      bbox = bbox(0.0, 0.0, 16.0, 16.0),
      rows = 16, cols = 16, channels = 1,
      tileSize = 16, chunkSize = 128)
    # 128 frames at exactly 60_000ms cadence — DoDs are all 0 except the
    # first delta. This is precisely the case where S8b RLE wins.
    for i in 0 ..< 128:
      let f = waveFrame(16, 16, i)
      s.appendFrame(int64(i) * 60_000, f)
    s.flush()
    s.close()
    # Reopen and verify every frame decodes correctly.
    let s2 = openTileStack(dir)
    for i in 0 ..< 128:
      let (ok, back) = s2.readFrame(int64(i) * 60_000)
      check ok
      let want = waveFrame(16, 16, i)
      for j in 0 ..< back.data.len:
        check abs(back.data[j] - want.data[j]) < eps
    s2.close()

  test "irregular timestamps still round-trip (codec choice transparent)":
    let dir = getTempDir() / "glen_tilestack_s8b_irregular"
    removeDir(dir)
    let s = newTileStack(dir,
      bbox = bbox(0.0, 0.0, 8.0, 8.0),
      rows = 8, cols = 8, channels = 1,
      tileSize = 8, chunkSize = 64)
    # Wildly irregular cadence — DoDs are large, S8b probably loses to DoD.
    var ts: int64 = 0
    var stamps: seq[int64] = @[]
    for i in 0 ..< 64:
      ts += int64(i * i + 1) * 1000
      stamps.add(ts)
      let f = waveFrame(8, 8, i)
      s.appendFrame(ts, f)
    s.flush()
    s.close()
    let s2 = openTileStack(dir)
    for i in 0 ..< 64:
      let (ok, back) = s2.readFrame(stamps[i])
      check ok
    s2.close()

  test "constant chunk + Simple-8b ts round-trips":
    let dir = getTempDir() / "glen_tilestack_s8b_const"
    removeDir(dir)
    let s = newTileStack(dir,
      bbox = bbox(0.0, 0.0, 16.0, 16.0),
      rows = 16, cols = 16, channels = 1,
      tileSize = 16, chunkSize = 128)
    # Whole-chunk-constant + regular cadence: triggers BOTH the constant-chunk
    # path AND the Simple-8b ts codec.
    let zero = newGeoMesh(bbox(0.0, 0.0, 16.0, 16.0), rows = 16, cols = 16, channels = 1)
    for i in 0 ..< 128:
      s.appendFrame(int64(i) * 60_000, zero)
    s.flush()
    s.close()
    # Should be very small: header + S8b ts + 1 f64 + crc.
    let sz = tileFileSize(dir)
    check sz < 200
    let s2 = openTileStack(dir)
    for i in 0 ..< 128:
      let (ok, back) = s2.readFrame(int64(i) * 60_000)
      check ok
      for v in back.data: check abs(v) < eps
    s2.close()
