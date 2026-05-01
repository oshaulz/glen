import std/[os, unittest, math, random, times, strutils, sequtils, sets]
import glen/sharded
import glen/timeseries
import glen/geo

proc cleanDir(p: string) =
  if dirExists(p): removeDir(p)
  createDir(p)

let MsPerDay = 86_400_000'i64

suite "sharded: geohash":
  test "encode is stable, length matches precision":
    check encodeGeohash(-73.98, 40.75, 5).len == 5
    check encodeGeohash(0.0, 0.0, 8).len == 8
    # Same point encodes the same way every time.
    let a = encodeGeohash(-73.98, 40.75, 6)
    let b = encodeGeohash(-73.98, 40.75, 6)
    check a == b

  test "shorter prefix is a prefix of a longer one":
    let p4 = encodeGeohash(-73.98, 40.75, 4)
    let p6 = encodeGeohash(-73.98, 40.75, 6)
    check p6.startsWith(p4)

  test "geohashBBox contains the encoded point":
    let lon = -73.98; let lat = 40.75
    let h = encodeGeohash(lon, lat, 5)
    let b = geohashBBox(h)
    check b.minX <= lon and lon <= b.maxX
    check b.minY <= lat and lat <= b.maxY

  test "cells in bbox cover every point inside":
    # Manhattan-ish bbox.
    let b = bbox(-74.05, 40.65, -73.85, 40.85)
    let cells = geohashCellsInBBox(b, 5)
    check cells.len > 0
    # Every random point inside b should fall into one of the cells.
    var rng = initRand(42)
    for _ in 0 ..< 200:
      let lon = rng.rand(b.minX .. b.maxX)
      let lat = rng.rand(b.minY .. b.maxY)
      let h = encodeGeohash(lon, lat, 5)
      check h in cells

# ----------------------------------------------------------------------

suite "sharded: time-only routing":
  test "tbDay puts samples from different days into different shards":
    let dir = getTempDir() / "glen_sharded_day"
    cleanDir(dir)
    let s = openShardedSeries(dir, tbDayPolicy())
    let day1 = 1_700_000_000_000'i64                 # some Tuesday-ish
    let day2 = day1 + MsPerDay
    s.append(day1, 10.0)
    s.append(day1 + 1000, 11.0)
    s.append(day2, 20.0)
    s.flush()
    let keys = s.shardKeysOnDisk()
    check keys.len == 2
    s.close()

  test "tbHour shards by hour":
    let dir = getTempDir() / "glen_sharded_hour"
    cleanDir(dir)
    let s = openShardedSeries(dir, tbHourPolicy())
    let t = 1_700_000_000_000'i64
    s.append(t, 1.0)
    s.append(t + 3_600_000, 2.0)         # +1h
    s.append(t + 7_200_000, 3.0)         # +2h
    s.flush()
    check s.shardKeysOnDisk().len == 3
    s.close()

  test "range fans out across shards and orders by ts":
    let dir = getTempDir() / "glen_sharded_range"
    cleanDir(dir)
    let s = openShardedSeries(dir, tbDayPolicy())
    let day1 = 1_700_000_000_000'i64
    s.append(day1 + 5000, 5.0)
    s.append(day1 + MsPerDay + 1000, 10.0)
    s.append(day1 + 1000, 1.0)
    s.flush()
    let xs = s.range(day1, day1 + 2 * MsPerDay)
    check xs.len == 3
    # Must be chronological even though they came from different shards.
    for i in 1 ..< xs.len:
      check xs[i][0] >= xs[i-1][0]
    s.close()

  test "range only opens shards whose window intersects the query":
    let dir = getTempDir() / "glen_sharded_skip"
    cleanDir(dir)
    let s = openShardedSeries(dir, tbDayPolicy(),
                              maxOpenShards = 8)
    let day1 = 1_700_000_000_000'i64
    # Five separate days.
    for i in 0 ..< 5:
      s.append(day1 + int64(i) * MsPerDay + 1000, float64(i))
    s.flush()
    # Query just day 2 → expect 1 sample, but file count = 5.
    let xs = s.range(day1 + 2 * MsPerDay,
                     day1 + 3 * MsPerDay - 1)
    check xs.len == 1
    check xs[0][1] == 2.0
    check s.shardKeysOnDisk().len == 5
    s.close()

  test "tbCustomMs with 1-minute buckets":
    let dir = getTempDir() / "glen_sharded_custom"
    cleanDir(dir)
    let p = ShardingPolicy(timeKind: tbCustomMs,
                           customTimeBucketMs: 60_000)
    let s = openShardedSeries(dir, p)
    s.append(0, 0.0)
    s.append(30_000, 0.5)            # same minute bucket
    s.append(60_000, 1.0)            # next minute bucket
    s.append(120_000, 2.0)           # next next
    s.flush()
    check s.shardKeysOnDisk().len == 3
    let xs = s.range(0, 200_000)
    check xs.len == 4
    s.close()

# ----------------------------------------------------------------------

suite "sharded: geo+time routing":
  test "appendAt routes by geohash + day":
    let dir = getTempDir() / "glen_sharded_geo_time"
    cleanDir(dir)
    let p = geoTimePolicy(tbDay, geohashPrecision = 4)
    let s = openShardedSeries(dir, p)
    let t = 1_700_000_000_000'i64
    s.appendAt(t, -73.98, 40.75, 22.0)        # NYC
    s.appendAt(t, 2.35, 48.85, 18.0)          # Paris
    s.appendAt(t + MsPerDay, -73.98, 40.75, 23.0)  # NYC next day
    s.flush()
    let keys = s.shardKeysOnDisk()
    check keys.len == 3       # (NYC, day1), (Paris, day1), (NYC, day2)
    s.close()

  test "rangeIn restricts fan-out to overlapping cells":
    let dir = getTempDir() / "glen_sharded_rangein"
    cleanDir(dir)
    let p = geoTimePolicy(tbDay, geohashPrecision = 4)
    let s = openShardedSeries(dir, p)
    let t = 1_700_000_000_000'i64
    # Three disjoint locations.
    s.appendAt(t, -73.98, 40.75, 22.0)        # NYC
    s.appendAt(t, 2.35, 48.85, 18.0)          # Paris
    s.appendAt(t, 139.7, 35.7, 14.0)          # Tokyo
    s.flush()
    # Bbox around NYC only.
    let nycBBox = bbox(-74.1, 40.6, -73.8, 40.9)
    let xs = s.rangeIn(nycBBox, t - 1, t + 1)
    check xs.len == 1
    check xs[0][1] == 22.0
    s.close()

  test "appending samples with no time policy still works (geo-only)":
    let dir = getTempDir() / "glen_sharded_geoonly"
    cleanDir(dir)
    let p = ShardingPolicy(timeKind: tbNone,
                           geoKind: gbGeohash,
                           geohashPrecision: 3)
    let s = openShardedSeries(dir, p)
    s.appendAt(1, -73.98, 40.75, 1.0)
    s.appendAt(2, -73.98, 40.75, 2.0)         # same cell
    s.appendAt(3, 2.35, 48.85, 3.0)           # different cell
    s.flush()
    check s.shardKeysOnDisk().len == 2
    let xs = s.range(0, 100)
    check xs.len == 3
    s.close()

  test "wrong append flavour for the policy raises":
    let dir = getTempDir() / "glen_sharded_wrong"
    cleanDir(dir)
    let pTime = tbDayPolicy()
    let s1 = openShardedSeries(dir & "_t", pTime)
    expect ValueError:
      s1.appendAt(0, 0.0, 0.0, 1.0)            # geo on time-only
    s1.close()
    let pGeo = ShardingPolicy(timeKind: tbNone,
                              geoKind: gbGeohash,
                              geohashPrecision: 3)
    let s2 = openShardedSeries(dir & "_g", pGeo)
    expect ValueError:
      s2.append(0, 1.0)                         # plain on geo policy
    s2.close()

# ----------------------------------------------------------------------

suite "sharded: retention":
  test "dropBefore removes shards whose window ends before cutoff":
    let dir = getTempDir() / "glen_sharded_retention"
    cleanDir(dir)
    let s = openShardedSeries(dir, tbDayPolicy())
    let day0 = 1_700_000_000_000'i64
    for i in 0 ..< 5:
      s.append(day0 + int64(i) * MsPerDay + 1000, float64(i))
    s.flush()
    check s.shardKeysOnDisk().len == 5
    # Cut off everything before day 3.
    let dropped = s.dropBefore(day0 + 3 * MsPerDay)
    check dropped == 3
    check s.shardKeysOnDisk().len == 2
    # Surviving samples are days 3 and 4.
    let xs = s.range(low(int64), high(int64))
    check xs.len == 2
    check xs[0][1] == 3.0
    check xs[1][1] == 4.0
    s.close()

# ----------------------------------------------------------------------

suite "sharded: open shard LRU":
  test "many distinct shards stay correct under a small handle cap":
    let dir = getTempDir() / "glen_sharded_lru"
    cleanDir(dir)
    let s = openShardedSeries(dir, tbDayPolicy(),
                              maxOpenShards = 2)
    let day0 = 1_700_000_000_000'i64
    # 10 shards, each with one sample.
    for i in 0 ..< 10:
      s.append(day0 + int64(i) * MsPerDay + 1000, float64(i))
    s.flush()
    check s.shardKeysOnDisk().len == 10
    let all = s.range(low(int64), high(int64))
    check all.len == 10
    var rng = initRand(7)
    # Random shard pokes — no crash, correct values.
    for _ in 0 ..< 50:
      let i = rng.rand(9)
      let xs = s.range(day0 + int64(i) * MsPerDay,
                       day0 + int64(i) * MsPerDay + MsPerDay - 1)
      check xs.len == 1
      check xs[0][1] == float64(i)
    s.close()

# ----------------------------------------------------------------------

suite "sharded: persistence":
  test "reopen sees all previously written shards":
    let dir = getTempDir() / "glen_sharded_persist"
    cleanDir(dir)
    block:
      let s = openShardedSeries(dir, tbDayPolicy())
      let day0 = 1_700_000_000_000'i64
      for i in 0 ..< 4:
        s.append(day0 + int64(i) * MsPerDay + 1000, float64(i))
      s.flush()
      s.close()
    block:
      let s = openShardedSeries(dir, tbDayPolicy())
      let xs = s.range(low(int64), high(int64))
      check xs.len == 4
      s.close()

# ----------------------------------------------------------------------

import glen/glen
import glen/dsl

suite "sharded: db proxy":
  test "db.shardedSeries lives under <db.dir>/series/<name>/":
    let dir = getTempDir() / "glen_sharded_db"
    if dirExists(dir): removeDir(dir)
    createDir(dir)
    let db = newGlenDB(dir)
    let s = db.shardedSeries("temps",
                             geoTimePolicy(tbDay, geohashPrecision = 4))
    let t = 1_700_000_000_000'i64
    s.appendAt(t, -73.98, 40.75, 22.0)        # NYC
    s.appendAt(t, 2.35, 48.85, 18.0)          # Paris
    s.flush()
    check dirExists(dir / "series" / "temps")
    check s.shardKeysOnDisk().len == 2
    let nyc = s.rangeIn(bbox(-74.1, 40.6, -73.8, 40.9), t - 1, t + 1)
    check nyc.len == 1
    s.close()
    db.close()

  test "read-only db opens shards read-only":
    let dir = getTempDir() / "glen_sharded_db_ro"
    if dirExists(dir): removeDir(dir)
    createDir(dir)
    block:
      let db = newGlenDB(dir)
      let s = db.shardedSeries("metric", tbDayPolicy())
      let t = 1_700_000_000_000'i64
      s.append(t, 1.0)
      s.append(t + MsPerDay, 2.0)
      s.flush()
      s.close()
      db.close()
    block:
      let db = newGlenDB(dir, readOnly = true)
      let s = db.shardedSeries("metric", tbDayPolicy())
      check s.range(low(int64), high(int64)).len == 2
      expect IOError:
        s.append(1, 99.0)                     # writes raise on RO db
      s.close()
      db.close()
