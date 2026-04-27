import std/os
import std/times
import std/strformat
import glen/db as glendb, glen/types, glen/txn, glen/wal

proc benchPut(db: glendb.GlenDB; N: int; keys: seq[string]) =
  let t0 = epochTime()
  for i in 0 ..< N:
    var v = VObject()
    v["n"] = VInt(i.int64)
    db.put("bench", keys[i], v)
  let dtMs = int64((epochTime() - t0) * 1000.0)
  let rate = if dtMs == 0: 0.0 else: (float(N) * 1000.0) / float(dtMs)
  echo &"BENCH puts: {N} ops in {dtMs} ms => {rate:.1f} ops/s"

proc benchGet(db: glendb.GlenDB; N: int; keys: seq[string]) =
  let t0 = epochTime()
  var sum = 0
  for i in 0 ..< N:
    let k = i mod N
    let v = db.get("bench", keys[k])
    if not v.isNil: inc sum
  let dtMs = int64((epochTime() - t0) * 1000.0)
  let rate = if dtMs == 0: 0.0 else: (float(N) * 1000.0) / float(dtMs)
  echo &"BENCH gets: {N} ops in {dtMs} ms => {rate:.1f} ops/s (hits {sum})"

proc benchGetBorrowed(db: glendb.GlenDB; N: int; keys: seq[string]) =
  let t0 = epochTime()
  var sum = 0
  for i in 0 ..< N:
    let k = i mod N
    let v = db.getBorrowed("bench", keys[k])
    if not v.isNil: inc sum
  let dtMs = int64((epochTime() - t0) * 1000.0)
  let rate = if dtMs == 0: 0.0 else: (float(N) * 1000.0) / float(dtMs)
  echo &"BENCH gets(borrowed): {N} ops in {dtMs} ms => {rate:.1f} ops/s (hits {sum})"

proc benchGetMany(db: glendb.GlenDB; N: int; batchSize: int; keys: seq[string]) =
  ## Prepare ids
  let t0 = epochTime()
  var pos = 0
  var hits = 0
  while pos < keys.len:
    let until = min(pos + batchSize, keys.len)
    let sliceIds = keys[pos ..< until]
    for (_, v) in db.getMany("bench", sliceIds):
      if not v.isNil: inc hits
    pos = until
  let dtMs = int64((epochTime() - t0) * 1000.0)
  let ops = (keys.len div batchSize) + (if keys.len mod batchSize == 0: 0 else: 1)
  let rate = if dtMs == 0: 0.0 else: (float(ops) * 1000.0) / float(dtMs)
  echo &"BENCH getMany: {N} docs in {dtMs} ms => {rate:.1f} batches/s (hits {hits})"

proc benchTxn(db: glendb.GlenDB; N: int) =
  let t0 = epochTime()
  for i in 0 ..< N:
    let t = db.beginTxn()
    var v = VObject()
    v["x"] = VInt(i.int64)
    t.stagePut(Id(collection: "benchTxn", docId: "t" & $i, version: 0'u64), v)
    discard db.commit(t)
  let dtMs = int64((epochTime() - t0) * 1000.0)
  let rate = if dtMs == 0: 0.0 else: (float(N) * 1000.0) / float(dtMs)
  echo &"BENCH txn commits: {N} ops in {dtMs} ms => {rate:.1f} ops/s"

when isMainModule:
  let dir = getTempDir() / "glen_bench_db"
  if dirExists(dir): removeDir(dir)
  let database = newGlenDB(dir, cacheCapacity = 128*1024*1024, cacheShards = 32, walFlushEveryBytes = 8*1024*1024, lockStripesCount = 64)
  let N = 100_000
  var keys: seq[string] = newSeq[string](N)
  for i in 0 ..< N: keys[i] = "k" & $i

  benchPut(database, N, keys)
  # Close and reopen so read benches run against a cold cache
  database.close()
  let database2 = newGlenDB(dir, cacheCapacity = 128*1024*1024, cacheShards = 32, walFlushEveryBytes = 8*1024*1024, lockStripesCount = 64)

  benchGet(database2, N, keys)
  benchGetBorrowed(database2, N, keys)
  benchGetMany(database2, N, 100, keys)
  benchTxn(database2, 5_000)
  database2.close()

