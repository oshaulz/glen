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

proc benchPutMany(db: glendb.GlenDB; total: int; batchSize: int) =
  ## putMany throughput at a fixed batch size. Reports both
  ## ops/s (per individual document) and batches/s.
  let coll = "benchPM_" & $batchSize
  var batch: seq[(string, Value)] = newSeq[(string, Value)](batchSize)
  let t0 = epochTime()
  var written = 0
  var i = 0
  while written < total:
    let n = min(batchSize, total - written)
    if batch.len != n: batch.setLen(n)
    for j in 0 ..< n:
      var v = VObject()
      v["n"] = VInt((written + j).int64)
      batch[j] = ("k" & $(written + j), v)
    db.putMany(coll, batch)
    written += n
    inc i
  let dtMs = int64((epochTime() - t0) * 1000.0)
  let docRate = if dtMs == 0: 0.0 else: (float(total) * 1000.0) / float(dtMs)
  let batchRate = if dtMs == 0: 0.0 else: (float(i) * 1000.0) / float(dtMs)
  echo &"BENCH putMany batch={batchSize}: {total} docs in {dtMs} ms => {docRate:.1f} docs/s ({batchRate:.1f} batches/s)"

proc benchDeleteMany(db: glendb.GlenDB; total: int; batchSize: int) =
  ## Pre-populate a collection, then deleteMany at a fixed batch size.
  let coll = "benchDM_" & $batchSize
  var ids: seq[string] = newSeq[string](total)
  var seedBatch: seq[(string, Value)] = newSeq[(string, Value)](min(1000, total))
  var seeded = 0
  while seeded < total:
    let n = min(seedBatch.len, total - seeded)
    if seedBatch.len != n: seedBatch.setLen(n)
    for j in 0 ..< n:
      var v = VObject()
      v["n"] = VInt((seeded + j).int64)
      let id = "k" & $(seeded + j)
      seedBatch[j] = (id, v)
      ids[seeded + j] = id
    db.putMany(coll, seedBatch)
    seeded += n
  let t0 = epochTime()
  var deleted = 0
  var i = 0
  while deleted < total:
    let n = min(batchSize, total - deleted)
    db.deleteMany(coll, ids[deleted ..< deleted + n])
    deleted += n
    inc i
  let dtMs = int64((epochTime() - t0) * 1000.0)
  let docRate = if dtMs == 0: 0.0 else: (float(total) * 1000.0) / float(dtMs)
  let batchRate = if dtMs == 0: 0.0 else: (float(i) * 1000.0) / float(dtMs)
  echo &"BENCH deleteMany batch={batchSize}: {total} docs in {dtMs} ms => {docRate:.1f} docs/s ({batchRate:.1f} batches/s)"

proc benchTxnBatch(db: glendb.GlenDB; total: int; writesPerTxn: int) =
  ## Multi-write transactions: stage `writesPerTxn` puts per commit. Measures
  ## how OCC commit amortizes WAL + lock acquisition across larger txns.
  let coll = "benchTB_" & $writesPerTxn
  let t0 = epochTime()
  var written = 0
  var commits = 0
  while written < total:
    let n = min(writesPerTxn, total - written)
    let t = db.beginTxn()
    for j in 0 ..< n:
      var v = VObject()
      v["n"] = VInt((written + j).int64)
      t.stagePut(Id(collection: coll, docId: "k" & $(written + j), version: 0'u64), v)
    discard db.commit(t)
    written += n
    inc commits
  let dtMs = int64((epochTime() - t0) * 1000.0)
  let docRate = if dtMs == 0: 0.0 else: (float(total) * 1000.0) / float(dtMs)
  let commitRate = if dtMs == 0: 0.0 else: (float(commits) * 1000.0) / float(dtMs)
  echo &"BENCH txn writes/txn={writesPerTxn}: {total} docs in {dtMs} ms => {docRate:.1f} docs/s ({commitRate:.1f} commits/s)"

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
  # Batched-write throughput: amortizes WAL flush + lock acquisition across
  # the whole batch. Compare these rates against the single-doc `puts` /
  # `txn commits` lines above.
  benchPutMany(database2, 100_000, 10)
  benchPutMany(database2, 100_000, 100)
  benchPutMany(database2, 100_000, 1000)
  benchDeleteMany(database2, 100_000, 100)
  benchDeleteMany(database2, 100_000, 1000)
  benchTxnBatch(database2, 100_000, 10)
  benchTxnBatch(database2, 100_000, 100)
  benchTxnBatch(database2, 100_000, 1000)
  database2.close()

