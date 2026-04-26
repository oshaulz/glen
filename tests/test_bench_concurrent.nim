## Concurrent contention benchmark.
##
## Spawns N writer + M reader threads against a single GlenDB and reports
## throughput under three contention regimes:
##   - disjoint:   each writer owns its own collection (low stripe contention)
##   - shared:     all writers hammer one collection (max stripe contention)
##   - mixed RW:   writers and readers share one collection
##
## The point is to measure the lock layer under load, not raw single-threaded
## ops/s. Run with:
##   nim c -r -d:release --mm:orc --passC:-O3 --threads:on --path:src \
##     tests/test_bench_concurrent.nim

import std/[os, times, strformat, locks, atomics]
import glen/db as glendb, glen/types

const DEFAULT_OPS = 50_000

type
  Worker = object
    db: glendb.GlenDB
    collection: string
    keyPrefix: string
    opsTarget: int
    done: ptr Atomic[int]
    isReader: bool

var globalDoneCounter: Atomic[int]
var startGate: Atomic[bool]

proc waitForStart() =
  while not startGate.load(moAcquire):
    cpuRelax()

proc writerLoop(w: Worker) {.thread.} =
  waitForStart()
  {.cast(gcsafe).}:
    for i in 0 ..< w.opsTarget:
      var v = VObject()
      v["i"] = VInt(int64(i))
      w.db.put(w.collection, w.keyPrefix & $i, v)
  discard w.done[].fetchAdd(1, moRelease)

proc readerLoop(w: Worker) {.thread.} =
  waitForStart()
  {.cast(gcsafe).}:
    var hits = 0
    for i in 0 ..< w.opsTarget:
      let v = w.db.getBorrowed(w.collection, w.keyPrefix & $(i mod w.opsTarget))
      if not v.isNil: inc hits
  discard w.done[].fetchAdd(1, moRelease)

proc runScenario(name: string; db: glendb.GlenDB;
                 nWriters, nReaders, opsPerThread: int;
                 sharedCollection: bool) =
  globalDoneCounter.store(0)
  startGate.store(false)

  var threads = newSeq[Thread[Worker]](nWriters + nReaders)
  var workers = newSeq[Worker](nWriters + nReaders)

  # Pre-create every collection touched by any thread. ensureCollection() is
  # the documented serialisation point for outer-table mutations; without it
  # concurrent first-writes to different collections race on the GlenDB
  # ref's outer tables.
  if sharedCollection:
    db.ensureCollection("shared")
  else:
    for w in 0 ..< nWriters:
      db.ensureCollection("writer" & $w)

  for w in 0 ..< nWriters:
    let coll =
      if sharedCollection: "shared"
      else: "writer" & $w
    workers[w] = Worker(
      db: db,
      collection: coll,
      keyPrefix: "w" & $w & "k",
      opsTarget: opsPerThread,
      done: addr globalDoneCounter,
      isReader: false
    )

  for r in 0 ..< nReaders:
    let coll =
      if sharedCollection: "shared"
      else: "writer" & $(r mod max(1, nWriters))
    workers[nWriters + r] = Worker(
      db: db,
      collection: coll,
      keyPrefix: "w" & $(r mod max(1, nWriters)) & "k",
      opsTarget: opsPerThread,
      done: addr globalDoneCounter,
      isReader: true
    )

  for i in 0 ..< nWriters:
    createThread(threads[i], writerLoop, workers[i])
  for i in 0 ..< nReaders:
    createThread(threads[nWriters + i], readerLoop, workers[nWriters + i])

  let totalThreads = nWriters + nReaders
  let t0 = epochTime()
  startGate.store(true, moRelease)

  for t in threads.mitems:
    joinThread(t)
  let dtMs = int64((epochTime() - t0) * 1000.0)

  let totalOps = totalThreads * opsPerThread
  let rate =
    if dtMs == 0: 0.0
    else: (float(totalOps) * 1000.0) / float(dtMs)
  let writeOps = nWriters * opsPerThread
  let readOps  = nReaders * opsPerThread
  echo &"BENCH [{name}] writers={nWriters} readers={nReaders} ops/thread={opsPerThread} " &
       &"total={totalOps} ({writeOps}w+{readOps}r) in {dtMs} ms => {rate:.0f} ops/s"

when isMainModule:
  let dir = getTempDir() / "glen_concurrent_bench"
  if dirExists(dir): removeDir(dir)
  let db = newGlenDB(dir,
                     cacheCapacity = 128 * 1024 * 1024,
                     cacheShards = 32,
                     walFlushEveryBytes = 8 * 1024 * 1024,
                     lockStripesCount = 64)

  let opsPerThread = DEFAULT_OPS

  echo "--- disjoint collections (low stripe contention) ---"
  runScenario("disjoint-write-only", db, nWriters = 4, nReaders = 0,
              opsPerThread = opsPerThread, sharedCollection = false)
  runScenario("disjoint-mixed-rw",   db, nWriters = 4, nReaders = 4,
              opsPerThread = opsPerThread, sharedCollection = false)

  echo "--- shared collection (high stripe contention) ---"
  runScenario("shared-write-only",   db, nWriters = 4, nReaders = 0,
              opsPerThread = opsPerThread, sharedCollection = true)
  runScenario("shared-mixed-rw",     db, nWriters = 4, nReaders = 4,
              opsPerThread = opsPerThread, sharedCollection = true)

  echo "--- read-heavy (1 writer / 8 readers, shared) ---"
  runScenario("read-heavy-shared",   db, nWriters = 1, nReaders = 8,
              opsPerThread = opsPerThread, sharedCollection = true)

  db.close()
