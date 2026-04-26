import std/[os, unittest, math, random, times]
import glen/timeseries

proc cleanup(path: string) =
  if fileExists(path): removeFile(path)
  if fileExists(path & ".compact"): removeFile(path & ".compact")

suite "timeseries: encoding round-trip":
  test "single sample":
    let p = getTempDir() / "glen_ts_single.gts"
    cleanup(p)
    let s = openSeries(p, blockSize = 16)
    s.append(100, 3.14)
    s.flush()
    let r = s.range(0, 1000)
    check r.len == 1
    check r[0][0] == 100
    check r[0][1] == 3.14
    s.close()

  test "monotonic timestamps, varying values":
    let p = getTempDir() / "glen_ts_mono.gts"
    cleanup(p)
    let s = openSeries(p, blockSize = 64)
    var rng = initRand(7)
    var expected: seq[(int64, float64)] = @[]
    var ts: int64 = 1_700_000_000_000
    for i in 0 ..< 500:
      ts += int64(900 + rng.rand(200))   # ~1s spacing with jitter
      let v = sin(float64(i) * 0.05) + rng.rand(1.0) * 0.001
      expected.add((ts, v))
      s.append(ts, v)
    s.flush()
    let got = s.range(low(int64), high(int64))
    check got.len == expected.len
    for i in 0 ..< expected.len:
      check got[i][0] == expected[i][0]
      check abs(got[i][1] - expected[i][1]) < 1e-12
    s.close()

  test "many full blocks decode identically":
    let p = getTempDir() / "glen_ts_blocks.gts"
    cleanup(p)
    let s = openSeries(p, blockSize = 32)
    var expected: seq[(int64, float64)] = @[]
    for i in 0 ..< 200:
      let ts = int64(i) * 1000
      let v = float64(i) * 0.5
      expected.add((ts, v))
      s.append(ts, v)
    s.flush()
    let got = s.range(low(int64), high(int64))
    check got.len == 200
    for i in 0 ..< 200:
      check got[i] == expected[i]
    s.close()

  test "constant value (xor=0 path)":
    let p = getTempDir() / "glen_ts_const.gts"
    cleanup(p)
    let s = openSeries(p, blockSize = 64)
    for i in 0 ..< 100:
      s.append(int64(i), 42.0)
    s.flush()
    let got = s.range(low(int64), high(int64))
    check got.len == 100
    for (_, v) in got: check v == 42.0
    s.close()

  test "regular cadence (DoD=0 path)":
    let p = getTempDir() / "glen_ts_regular.gts"
    cleanup(p)
    let s = openSeries(p, blockSize = 64)
    for i in 0 ..< 100:
      s.append(int64(i) * 1000, float64(i))
    s.flush()
    let got = s.range(low(int64), high(int64))
    check got.len == 100
    for i in 0 ..< 100:
      check got[i] == (int64(i) * 1000, float64(i))
    s.close()

suite "timeseries: queries":
  test "range filters by ts":
    let p = getTempDir() / "glen_ts_range.gts"
    cleanup(p)
    let s = openSeries(p, blockSize = 32)
    for i in 0 ..< 100:
      s.append(int64(i) * 100, float64(i))
    s.flush()
    let got = s.range(2000, 5000)
    check got.len == 31  # ts in [2000, 5000] inclusive: i=20..50
    check got[0][0] == 2000
    check got[^1][0] == 5000
    s.close()

  test "latest returns last n in chronological order":
    let p = getTempDir() / "glen_ts_latest.gts"
    cleanup(p)
    let s = openSeries(p, blockSize = 16)
    for i in 0 ..< 100:
      s.append(int64(i), float64(i) * 2.0)
    s.flush()
    let last5 = s.latest(5)
    check last5.len == 5
    check last5[0][0] == 95
    check last5[^1][0] == 99
    check last5[^1][1] == 198.0
    s.close()

  test "len includes active and closed blocks":
    let p = getTempDir() / "glen_ts_len.gts"
    cleanup(p)
    let s = openSeries(p, blockSize = 10)
    for i in 0 ..< 25:
      s.append(int64(i), 1.0)
    check s.len == 25
    s.close()

suite "timeseries: persistence":
  test "reopen sees previously-flushed blocks":
    let p = getTempDir() / "glen_ts_reopen.gts"
    cleanup(p)
    block:
      let s = openSeries(p, blockSize = 32)
      for i in 0 ..< 50:
        s.append(int64(i) * 10, float64(i))
      s.close()
    let s = openSeries(p)
    check s.len == 50
    let got = s.range(low(int64), high(int64))
    check got.len == 50
    for i in 0 ..< 50:
      check got[i] == (int64(i) * 10, float64(i))
    s.close()

  test "torn tail is tolerated":
    let p = getTempDir() / "glen_ts_torn.gts"
    cleanup(p)
    block:
      let s = openSeries(p, blockSize = 16)
      for i in 0 ..< 32:
        s.append(int64(i), float64(i))
      s.close()
    # corrupt the last few bytes
    let f = open(p, fmReadWriteExisting)
    let sz = f.getFileSize()
    f.setFilePos(sz - 4)
    var junk: array[4, byte] = [0xFF'u8, 0xFF, 0xFF, 0xFF]
    discard f.writeBuffer(addr junk[0], 4)
    f.close()
    let s = openSeries(p)
    # last block should be dropped (CRC fail), survivor block remains
    check s.len == 16
    s.close()

suite "timeseries: retention":
  test "dropBlocksBefore removes old blocks":
    let p = getTempDir() / "glen_ts_drop.gts"
    cleanup(p)
    let s = openSeries(p, blockSize = 10)
    for i in 0 ..< 50:
      s.append(int64(i) * 100, float64(i))
    s.flush()
    s.dropBlocksBefore(2500)
    let got = s.range(low(int64), high(int64))
    # blocks held i=0..9, 10..19, 20..29 (endTs 2900), 30..39, 40..49
    # cutoff 2500: drop block 0..9 (endTs 900) and 10..19 (endTs 1900).
    # 20..29 has endTs=2900 which is >= 2500, kept.
    check got.len == 30
    check got[0][0] == 2000
    s.close()
    # reopen confirms persistence of the rewrite
    let s2 = openSeries(p)
    check s2.len == 30
    s2.close()

suite "timeseries: compression sanity":
  test "regular data compresses to a few bits per sample":
    let p = getTempDir() / "glen_ts_compress.gts"
    cleanup(p)
    let s = openSeries(p, blockSize = 4096)
    for i in 0 ..< 4096:
      s.append(int64(i) * 1000, 50.0 + sin(float64(i) * 0.01))
    s.flush()
    s.close()
    let bytes = getFileSize(p)
    # ~16 bytes baseline per (ts, value) without encoding = 65536 raw.
    # Gorilla on this kind of data should land well under 10 bytes/sample.
    check bytes < 40_000
    # and obviously > 0
    check bytes > 0

  test "out-of-order samples flush active block (no data loss)":
    let p = getTempDir() / "glen_ts_ooo.gts"
    cleanup(p)
    let s = openSeries(p, blockSize = 100)
    s.append(1000, 1.0)
    s.append(2000, 2.0)
    s.append(3000, 3.0)
    s.append(2500, 2.5)   # out of order — forces a new block
    s.append(4000, 4.0)
    s.flush()
    let got = s.range(low(int64), high(int64))
    check got.len == 5
    s.close()
