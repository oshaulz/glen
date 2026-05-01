## Glen sharded timeseries — split a logical series across many on-disk
## shards, partitioned by time bucket and/or geographic bucket (geohash
## prefix). Each shard is a regular `glen/timeseries` file; the shard
## manager just routes `append` / `range` / retention to the right files.
##
## Two pain points motivate this:
##
##   1. **Retention at scale.** `dropBlocksBefore` on a 10 GB series rewrites
##      the entire file. With a daily time shard, dropping yesterday is just
##      `removeFile` — O(1) instead of O(file).
##
##   2. **Open / RAM cost on huge series.** A 9-billion-sample series carries
##      a ~130 MB block index in RAM. Daily shards keep each in-memory index
##      small and let cold shards stay closed (LRU evicts handles).
##
## Optional geographic sharding extends this to data tagged with (lon, lat):
## samples land in a geohash-prefix-keyed file, and range queries that carry
## a bounding box only fan out to overlapping cells.
##
## ## Quick shape
##
## ```nim
## import glen/sharded
## let p = ShardingPolicy(timeKind: tbDay, geoKind: gbGeohash, geohashPrecision: 4)
## let s = openShardedSeries(rootDir, p)
## s.append(nowMs, 22.3)                     # time-only shards
## s.appendAt(nowMs, -73.98, 40.75, 22.3)    # if geoKind != gbNone
## let xs = s.range(t0, t1)
## let ys = s.rangeIn(bbox(-74.1, 40.7, -73.8, 40.9), t0, t1)
## s.dropBefore(cutoffMs)                    # removes whole shard files
## s.close()
## ```
##
## ## On-disk layout
##
##   `<rootDir>/<key>.gts`
##
## Where `<key>` is one of:
##   * time-only:  `2026-04-15`            (tbDay)
##   * geo-only:   `9q5b`                  (gbGeohash, precision 4)
##   * both:       `2026-04-15__9q5b`      (double-underscore separator)
##
## `<rootDir>` is created on first append. Existing shards are discovered by
## scanning the directory (no separate manifest); shard files written by an
## older version are picked up automatically.

import std/[os, locks, tables, times, strutils, algorithm, sets]
import glen/timeseries
import glen/geo

# ---------------------------------------------------------------------------
# Geohash (base32, lon/lat-interleaved)
# ---------------------------------------------------------------------------

const Base32 = "0123456789bcdefghjkmnpqrstuvwxyz"

proc base32Idx(ch: char): int =
  for i in 0 ..< Base32.len:
    if Base32[i] == ch: return i
  -1

proc encodeGeohash*(lon, lat: float64; precision: int): string =
  ## Standard geohash. Interleaves longitude / latitude bits, base32-encodes
  ## 5 bits at a time. Precision is in characters (1..12 makes sense; each
  ## additional char shrinks the cell ~5.5×).
  doAssert precision >= 1 and precision <= 12
  var minLat = -90.0
  var maxLat = 90.0
  var minLon = -180.0
  var maxLon = 180.0
  result = newStringOfCap(precision)
  var bits = 0
  var ch = 0
  var even = true
  while result.len < precision:
    if even:
      let mid = (minLon + maxLon) * 0.5
      if lon >= mid:
        ch = (ch shl 1) or 1
        minLon = mid
      else:
        ch = ch shl 1
        maxLon = mid
    else:
      let mid = (minLat + maxLat) * 0.5
      if lat >= mid:
        ch = (ch shl 1) or 1
        minLat = mid
      else:
        ch = ch shl 1
        maxLat = mid
    even = not even
    inc bits
    if bits == 5:
      result.add(Base32[ch])
      bits = 0
      ch = 0

proc geohashBBox*(hash: string): BBox =
  ## Bounding box of the geohash cell. For an empty / unknown hash returns
  ## the world bbox.
  var minLat = -90.0
  var maxLat = 90.0
  var minLon = -180.0
  var maxLon = 180.0
  var even = true
  for c in hash:
    let idx = base32Idx(c)
    if idx < 0: break
    for j in countdown(4, 0):
      let bit = ((idx shr j) and 1) != 0
      if even:
        let mid = (minLon + maxLon) * 0.5
        if bit: minLon = mid else: maxLon = mid
      else:
        let mid = (minLat + maxLat) * 0.5
        if bit: minLat = mid else: maxLat = mid
      even = not even
  bbox(minLon, minLat, maxLon, maxLat)

proc geohashCellsInBBox*(b: BBox; precision: int): seq[string] =
  ## Enumerate every precision-`precision` geohash cell whose bbox intersects
  ## `b`. Walks the cell grid from the SW corner; for very wide bboxes at
  ## high precision the result can be large — caller should choose a
  ## precision proportional to query selectivity.
  result = @[]
  if precision < 1: return
  # Approximate cell extent at this precision. Geohash alternates lon/lat
  # bits; an even-precision cell is square-ish. Step half a cell to ensure
  # neighbour pickup near borders.
  let lonBits = precision * 5 div 2 + (precision * 5 mod 2)  # ceil
  let latBits = precision * 5 div 2
  let lonStep = 360.0 / float64(1 shl lonBits)
  let latStep = 180.0 / float64(1 shl latBits)
  let step = min(lonStep, latStep) * 0.5
  var seen = initHashSet[string]()
  var lat = b.minY
  while lat <= b.maxY + step:
    var lon = b.minX
    while lon <= b.maxX + step:
      let h = encodeGeohash(
        clamp(lon, -180.0, 180.0),
        clamp(lat, -90.0, 90.0),
        precision)
      if h notin seen:
        seen.incl(h)
        # Include only cells that actually intersect the bbox.
        if intersects(geohashBBox(h), b):
          result.add(h)
      lon += step
    lat += step
  # Catch the four corners exactly in case the step grid missed them
  # (tight bboxes well inside one cell).
  for (cx, cy) in [(b.minX, b.minY), (b.minX, b.maxY),
                   (b.maxX, b.minY), (b.maxX, b.maxY)]:
    let h = encodeGeohash(clamp(cx, -180.0, 180.0),
                          clamp(cy, -90.0, 90.0), precision)
    if h notin seen:
      seen.incl(h)
      result.add(h)

# ---------------------------------------------------------------------------
# Sharding policy
# ---------------------------------------------------------------------------

type
  TimeBucketKind* = enum
    tbNone        ## no time sharding (one shard regardless of ts)
    tbHour
    tbDay
    tbWeek        ## weeks since the unix epoch, as `w<weekIdx>`
    tbMonth
    tbYear
    tbCustomMs    ## fixed-width buckets of `customTimeBucketMs` milliseconds

  GeoBucketKind* = enum
    gbNone
    gbGeohash     ## fixed-precision geohash prefix
    gbCustom      ## caller supplies a `(lon, lat) → key` proc

  ShardingPolicy* = ref object
    timeKind*: TimeBucketKind
    customTimeBucketMs*: int64           ## used iff timeKind == tbCustomMs
    geoKind*: GeoBucketKind
    geohashPrecision*: int               ## used iff geoKind == gbGeohash
    customGeoBucket*: proc (lon, lat: float64): string {.gcsafe.}
    customGeoCellsInBBox*:
      proc (b: BBox): seq[string] {.gcsafe.}
      ## Enumerator for `gbCustom`: returns every shard key whose data could
      ## fall inside bbox `b`. Required for `rangeIn` to be correct under
      ## custom geo policies.

proc tbDayPolicy*(): ShardingPolicy =
  ShardingPolicy(timeKind: tbDay, geoKind: gbNone)
proc tbHourPolicy*(): ShardingPolicy =
  ShardingPolicy(timeKind: tbHour, geoKind: gbNone)
proc tbMonthPolicy*(): ShardingPolicy =
  ShardingPolicy(timeKind: tbMonth, geoKind: gbNone)
proc geoTimePolicy*(timeKind: TimeBucketKind; geohashPrecision: int): ShardingPolicy =
  ShardingPolicy(timeKind: timeKind,
                 geoKind: gbGeohash,
                 geohashPrecision: geohashPrecision)

# ---------------------------------------------------------------------------
# Time bucket encoding / decoding
# ---------------------------------------------------------------------------

const MsPerSecond = 1000'i64
const MsPerHour = 3600_000'i64
const MsPerDay = 86_400_000'i64

proc timeBucketKey*(p: ShardingPolicy; tsMs: int64): string =
  ## Produces the time-portion of a shard key for the given ts.
  ## tbNone returns the empty string.
  case p.timeKind
  of tbNone:
    ""
  of tbHour:
    let t = utc(fromUnix(tsMs div MsPerSecond))
    t.format("yyyy-MM-dd-HH")
  of tbDay:
    let t = utc(fromUnix(tsMs div MsPerSecond))
    t.format("yyyy-MM-dd")
  of tbWeek:
    # Weeks since unix epoch, padded for lexicographic ordering.
    let weekIdx = tsMs div (7'i64 * MsPerDay)
    "w" & align($weekIdx, 12, '0')
  of tbMonth:
    let t = utc(fromUnix(tsMs div MsPerSecond))
    t.format("yyyy-MM")
  of tbYear:
    let t = utc(fromUnix(tsMs div MsPerSecond))
    t.format("yyyy")
  of tbCustomMs:
    doAssert p.customTimeBucketMs > 0,
      "customTimeBucketMs must be > 0 for tbCustomMs"
    # Bucket index, formatted with leading zeros so lexicographic ordering
    # matches numeric ordering.
    let idx = tsMs div p.customTimeBucketMs
    "c" & align($idx, 16, '0')

proc timeBucketBounds*(p: ShardingPolicy; key: string): (int64, int64) =
  ## Returns `[startMs, endMs)` for a time bucket key. Used by query routing
  ## to skip shards that don't overlap a query window.
  ## Falls back to a lifetime-spanning range for unknown keys.
  case p.timeKind
  of tbNone:
    (low(int64), high(int64))
  of tbHour:
    try:
      let t = parse(key, "yyyy-MM-dd-HH", utc())
      let s = t.toTime.toUnix * MsPerSecond
      (s, s + MsPerHour)
    except: (low(int64), high(int64))
  of tbDay:
    try:
      let t = parse(key, "yyyy-MM-dd", utc())
      let s = t.toTime.toUnix * MsPerSecond
      (s, s + MsPerDay)
    except: (low(int64), high(int64))
  of tbWeek:
    if key.len < 2 or key[0] != 'w':
      (low(int64), high(int64))
    else:
      try:
        let idx = parseBiggestInt(key.substr(1))
        let s = int64(idx) * 7'i64 * MsPerDay
        (s, s + 7'i64 * MsPerDay)
      except: (low(int64), high(int64))
  of tbMonth:
    try:
      let t = parse(key, "yyyy-MM", utc())
      let s = t.toTime.toUnix * MsPerSecond
      # Conservative upper bound: 31 days.
      (s, s + 31'i64 * MsPerDay)
    except: (low(int64), high(int64))
  of tbYear:
    try:
      let yr = parseInt(key)
      let startOfYear = dateTime(yr, mJan, 1, zone = utc())
      let s = startOfYear.toTime.toUnix * MsPerSecond
      (s, s + 366'i64 * MsPerDay)
    except: (low(int64), high(int64))
  of tbCustomMs:
    if key.len < 2 or key[0] != 'c':
      (low(int64), high(int64))
    else:
      try:
        let idx = parseBiggestInt(key.substr(1))
        let s = int64(idx) * p.customTimeBucketMs
        (s, s + p.customTimeBucketMs)
      except: (low(int64), high(int64))

# ---------------------------------------------------------------------------
# Shard key composition: <timeKey>__<geoKey>
# ---------------------------------------------------------------------------

const KeySep = "__"

proc geoBucketKey(p: ShardingPolicy; lon, lat: float64): string =
  case p.geoKind
  of gbNone: ""
  of gbGeohash:
    doAssert p.geohashPrecision >= 1, "geohashPrecision must be >= 1"
    encodeGeohash(lon, lat, p.geohashPrecision)
  of gbCustom:
    doAssert not p.customGeoBucket.isNil,
      "gbCustom requires customGeoBucket"
    p.customGeoBucket(lon, lat)

proc composeShardKey(timeKey, geoKey: string): string =
  if timeKey.len == 0 and geoKey.len == 0: "_"
  elif timeKey.len == 0: geoKey
  elif geoKey.len == 0: timeKey
  else: timeKey & KeySep & geoKey

proc decomposeShardKey(p: ShardingPolicy; key: string): (string, string) =
  ## Reverses `composeShardKey`. Returns ("", "") on parse failure (which
  ## the caller treats as "scan-everything" fallback).
  let needTime = p.timeKind != tbNone
  let needGeo  = p.geoKind != gbNone
  if needTime and needGeo:
    let i = key.find(KeySep)
    if i < 0: return ("", "")
    (key.substr(0, i - 1), key.substr(i + KeySep.len))
  elif needTime:
    (key, "")
  elif needGeo:
    ("", key)
  else:
    ("", "")

# ---------------------------------------------------------------------------
# ShardedSeries
# ---------------------------------------------------------------------------

const DefaultMaxOpenShards* = 64

type
  ShardedSeries* = ref object
    rootDir*: string
    policy*: ShardingPolicy
    blockSize*: int
    fsyncOnFlush*: bool
    decodedChunkCacheSize*: int
    readOnly*: bool
    maxOpenShards*: int
    # OrderedTable doubles as our LRU: insertion order is access-order.
    handles: OrderedTable[string, Series]
    lock: Lock

const SeriesExt = ".gts"

proc keyToPath(s: ShardedSeries; key: string): string =
  s.rootDir / (key & SeriesExt)

proc pathToKey(p: string): string =
  let n = splitFile(p).name
  n

proc evictIfNeeded(s: ShardedSeries) =
  ## Caller must hold s.lock.
  while s.handles.len > s.maxOpenShards:
    var firstKey: string
    var found = false
    for k, _ in s.handles:
      firstKey = k
      found = true
      break
    if not found: break
    let h = s.handles[firstKey]
    s.handles.del(firstKey)
    h.close()

proc openShardLocked(s: ShardedSeries; key: string;
                     createIfMissing: bool): Series =
  ## Caller must hold s.lock. Returns nil if `createIfMissing` is false and
  ## the file does not exist.
  if key in s.handles:
    let h = s.handles[key]
    s.handles.del(key)
    s.handles[key] = h   # bump to back of LRU
    return h
  let path = s.keyToPath(key)
  let exists = fileExists(path)
  if not exists and not createIfMissing:
    return nil
  if not exists:
    if not dirExists(s.rootDir): createDir(s.rootDir)
  let series = openSeries(path,
                          blockSize = s.blockSize,
                          fsyncOnFlush = s.fsyncOnFlush,
                          decodedChunkCacheSize = s.decodedChunkCacheSize,
                          readOnly = s.readOnly)
  s.handles[key] = series
  s.evictIfNeeded()
  series

proc openShardedSeries*(rootDir: string;
                        policy: ShardingPolicy;
                        blockSize = DefaultBlockSize;
                        fsyncOnFlush = false;
                        decodedChunkCacheSize = DefaultDecodedChunkCacheSize;
                        readOnly = false;
                        maxOpenShards = DefaultMaxOpenShards): ShardedSeries =
  ## Open or create a sharded series rooted at `rootDir`. The directory
  ## itself is created on first append (read-only opens against a missing
  ## dir succeed but list zero shards).
  doAssert maxOpenShards >= 1
  doAssert policy.timeKind != tbNone or policy.geoKind != gbNone,
    "ShardingPolicy must shard by time, geo, or both"
  if policy.timeKind == tbCustomMs:
    doAssert policy.customTimeBucketMs > 0
  if policy.geoKind == gbGeohash:
    doAssert policy.geohashPrecision >= 1 and policy.geohashPrecision <= 12
  if policy.geoKind == gbCustom:
    doAssert not policy.customGeoBucket.isNil
  if not readOnly and not dirExists(rootDir):
    createDir(rootDir)
  result = ShardedSeries(
    rootDir: rootDir, policy: policy,
    blockSize: blockSize, fsyncOnFlush: fsyncOnFlush,
    decodedChunkCacheSize: decodedChunkCacheSize,
    readOnly: readOnly, maxOpenShards: maxOpenShards,
    handles: initOrderedTable[string, Series]())
  initLock(result.lock)

proc shardKeysOnDisk*(s: ShardedSeries): seq[string] =
  ## All shard keys present on disk (cheap directory walk; no Series opens).
  result = @[]
  if not dirExists(s.rootDir): return
  for kind, path in walkDir(s.rootDir):
    if kind != pcFile: continue
    if not path.endsWith(SeriesExt): continue
    result.add(pathToKey(path))

proc append*(s: ShardedSeries; tsMs: int64; value: float64) =
  ## Time-only / no-geo append. Raises if the policy needs a (lon, lat)
  ## (i.e. `geoKind != gbNone`).
  if s.readOnly:
    raise newException(IOError, "ShardedSeries opened read-only")
  if s.policy.geoKind != gbNone:
    raise newException(ValueError,
      "this policy requires geo coords; use appendAt instead")
  let tk = timeBucketKey(s.policy, tsMs)
  let key = composeShardKey(tk, "")
  acquire(s.lock)
  defer: release(s.lock)
  let series = s.openShardLocked(key, createIfMissing = true)
  series.append(tsMs, value)

proc appendAt*(s: ShardedSeries; tsMs: int64; lon, lat: float64;
               value: float64) =
  ## Geo-aware append. Routes by both time bucket (if any) and geo bucket.
  if s.readOnly:
    raise newException(IOError, "ShardedSeries opened read-only")
  if s.policy.geoKind == gbNone:
    raise newException(ValueError,
      "this policy is time-only; use append instead")
  let tk = timeBucketKey(s.policy, tsMs)
  let gk = geoBucketKey(s.policy, lon, lat)
  let key = composeShardKey(tk, gk)
  acquire(s.lock)
  defer: release(s.lock)
  let series = s.openShardLocked(key, createIfMissing = true)
  series.append(tsMs, value)

proc flush*(s: ShardedSeries) =
  if s.readOnly: return
  acquire(s.lock)
  defer: release(s.lock)
  for k, h in s.handles: h.flush()

proc close*(s: ShardedSeries) =
  acquire(s.lock)
  defer: release(s.lock)
  for k, h in s.handles: h.close()
  s.handles.clear()

# --- query routing ---------------------------------------------------------

proc timeOverlaps(p: ShardingPolicy; key: string;
                  fromMs, toMs: int64): bool =
  if p.timeKind == tbNone: return true
  let (s, e) = timeBucketBounds(p, key)
  not (e <= fromMs or s > toMs)

proc geoOverlaps(p: ShardingPolicy; key: string; b: BBox): bool =
  case p.geoKind
  of gbNone: true
  of gbGeohash:
    if key.len == 0: false
    else: intersects(geohashBBox(key), b)
  of gbCustom:
    # Without an enumerator, we can't reverse-map geo keys to bboxes;
    # callers using gbCustom + rangeIn should provide customGeoCellsInBBox.
    # This codepath is only hit when iterating shardKeysOnDisk to filter,
    # so the safe answer is "include it".
    true

proc range*(s: ShardedSeries;
            fromMs, toMs: int64): seq[(int64, float64)] =
  ## Range across all shards, regardless of geo.
  result = @[]
  if fromMs > toMs: return
  let allKeys = s.shardKeysOnDisk()
  acquire(s.lock)
  defer: release(s.lock)
  for key in allKeys:
    let (tk, _) = decomposeShardKey(s.policy, key)
    if not timeOverlaps(s.policy, tk, fromMs, toMs): continue
    let series = s.openShardLocked(key, createIfMissing = false)
    if series.isNil: continue
    let xs = series.range(fromMs, toMs)
    for x in xs: result.add(x)
  result.sort(proc (a, b: (int64, float64)): int = cmp(a[0], b[0]))

proc rangeIn*(s: ShardedSeries; b: BBox;
              fromMs, toMs: int64): seq[(int64, float64)] =
  ## Range query restricted to a bounding box. Only fans out to shards
  ## whose geo bucket intersects `b`. With `geoKind == gbNone`, equivalent
  ## to plain `range`.
  result = @[]
  if fromMs > toMs: return
  if s.policy.geoKind == gbNone:
    return s.range(fromMs, toMs)
  # Build candidate geo keys from the policy (cheap math, doesn't touch disk).
  var candidateGeoKeys: seq[string] = @[]
  case s.policy.geoKind
  of gbGeohash:
    candidateGeoKeys = geohashCellsInBBox(b, s.policy.geohashPrecision)
  of gbCustom:
    if s.policy.customGeoCellsInBBox.isNil:
      # Fallback: consult disk + per-shard geoOverlaps (which always says
      # true under gbCustom, so this becomes a full scan).
      for key in s.shardKeysOnDisk():
        let (_, gk) = decomposeShardKey(s.policy, key)
        if gk notin candidateGeoKeys: candidateGeoKeys.add(gk)
    else:
      candidateGeoKeys = s.policy.customGeoCellsInBBox(b)
  of gbNone: discard
  let candidateSet = block:
    var s2 = initHashSet[string]()
    for k in candidateGeoKeys: s2.incl(k)
    s2
  let allKeys = s.shardKeysOnDisk()
  acquire(s.lock)
  defer: release(s.lock)
  for key in allKeys:
    let (tk, gk) = decomposeShardKey(s.policy, key)
    if not timeOverlaps(s.policy, tk, fromMs, toMs): continue
    if gk notin candidateSet: continue
    let series = s.openShardLocked(key, createIfMissing = false)
    if series.isNil: continue
    let xs = series.range(fromMs, toMs)
    for x in xs: result.add(x)
  result.sort(proc (a, b: (int64, float64)): int = cmp(a[0], b[0]))

# --- retention -------------------------------------------------------------

proc dropBefore*(s: ShardedSeries; cutoffMs: int64): int =
  ## Removes every shard whose entire time window is strictly before
  ## `cutoffMs`. Returns the number of shards removed. With time-less
  ## policies (tbNone), no shards are eligible and 0 is returned —
  ## fall back to per-shard `dropBlocksBefore` if you need that case.
  result = 0
  if s.readOnly:
    raise newException(IOError, "ShardedSeries opened read-only")
  if s.policy.timeKind == tbNone: return
  let allKeys = s.shardKeysOnDisk()
  acquire(s.lock)
  defer: release(s.lock)
  for key in allKeys:
    let (tk, _) = decomposeShardKey(s.policy, key)
    let (_, e) = timeBucketBounds(s.policy, tk)
    if e <= cutoffMs:
      # Close handle if open, then remove file.
      if key in s.handles:
        s.handles[key].close()
        s.handles.del(key)
      removeFile(s.keyToPath(key))
      inc result

proc len*(s: ShardedSeries): int =
  ## Total samples across all shards (opens every shard once — O(n) shards).
  result = 0
  let allKeys = s.shardKeysOnDisk()
  acquire(s.lock)
  defer: release(s.lock)
  for key in allKeys:
    let series = s.openShardLocked(key, createIfMissing = false)
    if series.isNil: continue
    result += series.len
