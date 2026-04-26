import std/[os, unittest, tables, times, strformat, random]
import glen/db, glen/types, glen/storage

# Snapshot v3 = paged, mmap-backed index. Lookups binary-search the offset
# table directly off the OS page cache; no in-memory hash table for the index.

suite "snapshot v3 round-trip":
  test "writeSnapshotV3 + loadSnapshot eager":
    let dir = getTempDir() / "glen_snap_v3_eager"
    removeDir(dir)
    var docs = initTable[string, Value]()
    for i in 0 ..< 100:
      var v = VObject()
      v["n"] = VInt(i.int64)
      v["name"] = VString("doc-" & $i)
      docs["k" & $i] = v
    writeSnapshotV3(dir, "test", docs)
    let loaded = loadSnapshot(dir, "test")
    check loaded.len == 100
    check loaded["k42"]["n"].i == 42
    check loaded["k99"]["name"].s == "doc-99"

  test "openSnapshotMmap reads v3 metadata only":
    let dir = getTempDir() / "glen_snap_v3_mmap"
    removeDir(dir)
    var docs = initTable[string, Value]()
    for i in 0 ..< 50:
      var v = VObject(); v["k"] = VInt(i.int64)
      docs["d" & $i] = v
    writeSnapshotV3(dir, "stuff", docs)

    let mm = openSnapshotMmap(dir, "stuff")
    check not mm.isNil
    check mm.isV3
    check not mm.isV2          # exclusive
    check int(mm.docCount) == 50
    check mm.index.len == 0    # no in-memory index for v3
    # Lookups go through binary search on the mmap'd offsets.
    check loadDocFromMmap(mm, "d17")["k"].i == 17
    check loadDocFromMmap(mm, "d0")["k"].i == 0
    check loadDocFromMmap(mm, "d49")["k"].i == 49
    check loadDocFromMmap(mm, "d-missing").isNil
    check containsId(mm, "d17")
    check not containsId(mm, "d-missing")
    check snapshotDocCount(mm) == 50
    closeSnapshotMmap(mm)

  test "iterIds yields docs in sorted order (v3)":
    let dir = getTempDir() / "glen_snap_v3_iter"
    removeDir(dir)
    var docs = initTable[string, Value]()
    for i in [42, 7, 99, 0, 13]:
      docs["k" & $i] = VInt(i.int64)
    writeSnapshotV3(dir, "stuff", docs)

    let mm = openSnapshotMmap(dir, "stuff")
    var seen: seq[string] = @[]
    for id in iterIds(mm): seen.add(id)
    check seen.len == 5
    # v3 iteration is sorted; verify ordering
    check seen == @["k0", "k13", "k42", "k7", "k99"]
    closeSnapshotMmap(mm)

  test "back-compat: v2 file still mmaps as v2":
    let dir = getTempDir() / "glen_snap_v3_compat"
    removeDir(dir)
    var docs = initTable[string, Value]()
    docs["a"] = VInt(1)
    docs["b"] = VInt(2)
    writeSnapshotV2(dir, "stuff", docs)    # explicitly v2
    let mm = openSnapshotMmap(dir, "stuff")
    check not mm.isNil
    check mm.isV2
    check not mm.isV3
    check mm.index.len == 2
    check loadDocFromMmap(mm, "a").i == 1
    check containsId(mm, "b")
    closeSnapshotMmap(mm)

  test "v1 file still loads via eager loadSnapshot":
    let dir = getTempDir() / "glen_snap_v3_v1compat"
    removeDir(dir)
    var docs = initTable[string, Value]()
    docs["only"] = VString("from-v1")
    writeSnapshot(dir, "stuff", docs)      # v1 (no magic)
    let loaded = loadSnapshot(dir, "stuff")
    check loaded["only"].s == "from-v1"
    let mm = openSnapshotMmap(dir, "stuff")
    check not mm.isNil
    check not mm.isV2
    check not mm.isV3                       # neither — caller falls back
    closeSnapshotMmap(mm)

suite "v3 inside GlenDB":
  test "compact writes v3 by default; reopen mmaps it":
    let dir = getTempDir() / "glen_snap_v3_db"
    removeDir(dir)
    block:
      let db = newGlenDB(dir)
      for i in 0 ..< 200:
        var v = VObject(); v["n"] = VInt(i.int64)
        db.put("things", "t" & $i, v)
      db.compact()
      db.close()
    # Open spillable; v3 mmap should activate.
    let db = newGlenDB(dir, spillableMode = true)
    let v = db.get("things", "t100")
    check v["n"].i == 100
    db.close()

  test "lookup correctness across hot, dirty, snapshot-only docs":
    let dir = getTempDir() / "glen_snap_v3_mix"
    removeDir(dir)
    block:
      let db = newGlenDB(dir)
      for i in 0 ..< 100:
        db.put("things", "t" & $i, VInt(i.int64))
      db.compact()
      db.close()

    let db = newGlenDB(dir, spillableMode = true)
    db.put("things", "t50", VInt(9999))      # dirty override
    db.delete("things", "t10")                # tombstone
    db.put("things", "t-fresh", VString("hi")) # purely in-memory new

    # All four cases should be handled
    check db.get("things", "t0").i == 0           # snapshot-only, faulted
    check db.get("things", "t50").i == 9999       # in-memory dirty
    check db.get("things", "t10").isNil           # tombstoned
    check db.get("things", "t-fresh").s == "hi"   # new
    db.close()

suite "v3 stress + perf shape":
  test "100k docs: open is fast; lookup latency stays low across queries":
    ## Important property: open should be O(header read), not O(n).
    let dir = getTempDir() / "glen_snap_v3_stress"
    removeDir(dir)
    var docs = initTable[string, Value]()
    for i in 0 ..< 100_000:
      var v = VObject(); v["n"] = VInt(i.int64)
      docs["k-" & $i] = v
    writeSnapshotV3(dir, "huge", docs)
    let fileSize = getFileSize(dir / "huge.snap")
    echo &"v3 100k-doc snapshot: {fileSize} bytes"

    # Open: should be effectively instant (just header parse)
    let t0 = epochTime()
    let mm = openSnapshotMmap(dir, "huge")
    let openMs = (epochTime() - t0) * 1000.0
    echo &"v3 open (100k docs):   {openMs:.2f} ms"
    check openMs < 50.0     # generous; should be <5ms in practice
    check int(mm.docCount) == 100_000
    check mm.index.len == 0  # ← the win: no in-memory index

    # 10k random lookups
    var rng = initRand(7)
    var foundCount = 0
    let t1 = epochTime()
    for _ in 0 ..< 10_000:
      let id = "k-" & $rng.rand(99_999)
      let v = loadDocFromMmap(mm, id)
      if not v.isNil: inc foundCount
    let lookupMs = (epochTime() - t1) * 1000.0
    let lookupRate = 10_000.0 / (lookupMs / 1000.0)
    echo &"v3 random lookup rate: {lookupRate:.0f} q/s ({foundCount} found)"
    check foundCount == 10_000
    closeSnapshotMmap(mm)

  test "iteration walks all entries in sorted order":
    let dir = getTempDir() / "glen_snap_v3_iter_stress"
    removeDir(dir)
    var docs = initTable[string, Value]()
    for i in 0 ..< 1000:
      docs["k-" & $i] = VInt(i.int64)
    writeSnapshotV3(dir, "huge", docs)
    let mm = openSnapshotMmap(dir, "huge")
    var prevId = ""
    var count = 0
    for id in iterIds(mm):
      if count > 0:
        check id > prevId   # sorted
      prevId = id
      inc count
    check count == 1000
    closeSnapshotMmap(mm)
