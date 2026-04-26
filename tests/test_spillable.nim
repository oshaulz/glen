import std/[os, unittest, tables, sets]
import glen/db, glen/types, glen/storage

# A few smoke tests for snapshot v2 + spillable mode. The eager-mode test
# suites all auto-upgrade to v2 once the user calls compact() and so exercise
# the v2 reader path end-to-end already; this file targets the spill-specific
# behaviours.

suite "snapshot v2 round-trip":
  test "writeSnapshotV2 + loadSnapshot eager":
    let dir = getTempDir() / "glen_spill_v2_eager"
    removeDir(dir)
    var docs = initTable[string, Value]()
    for i in 0 ..< 100:
      var v = VObject()
      v["n"] = VInt(i.int64)
      v["name"] = VString("doc-" & $i)
      docs["k" & $i] = v
    writeSnapshotV2(dir, "test", docs)

    let loaded = loadSnapshot(dir, "test")
    check loaded.len == 100
    check loaded["k42"]["n"].i == 42
    check loaded["k99"]["name"].s == "doc-99"

  test "openSnapshotMmap reads index without reading bodies":
    let dir = getTempDir() / "glen_spill_mmap_index"
    removeDir(dir)
    var docs = initTable[string, Value]()
    for i in 0 ..< 50:
      var v = VObject(); v["k"] = VInt(i.int64)
      docs["d" & $i] = v
    writeSnapshotV2(dir, "stuff", docs)

    let mm = openSnapshotMmap(dir, "stuff")
    check not mm.isNil
    check mm.isV2
    check mm.index.len == 50
    # the body is read on demand
    let v = loadDocFromMmap(mm, "d17")
    check v["k"].i == 17
    closeSnapshotMmap(mm)

  test "v1 snapshot is detected and rejected by openSnapshotMmap":
    let dir = getTempDir() / "glen_spill_v1_only"
    removeDir(dir)
    var docs = initTable[string, Value]()
    docs["a"] = VString("hello")
    writeSnapshot(dir, "stuff", docs)    # v1 writer

    let mm = openSnapshotMmap(dir, "stuff")
    check not mm.isNil
    check not mm.isV2
    # eager loader still works on v1 (back-compat)
    let docsBack = loadSnapshot(dir, "stuff")
    check docsBack["a"].s == "hello"
    closeSnapshotMmap(mm)

suite "spillable mode: lazy load":
  test "open in spillable mode populates cs.docs lazily":
    let dir = getTempDir() / "glen_spill_lazy"
    removeDir(dir)
    block:
      let db = newGlenDB(dir)
      for i in 0 ..< 100:
        var v = VObject(); v["n"] = VInt(i.int64)
        db.put("things", "t" & $i, v)
      db.compact()    # writes snapshot v2
      db.close()

    let db = newGlenDB(dir, spillableMode = true)
    # Reading any doc faults it in from the mmap
    let v = db.get("things", "t42")
    check not v.isNil
    check v["n"].i == 42
    let v99 = db.get("things", "t99")
    check v99["n"].i == 99
    let vMissing = db.get("things", "no-such-doc")
    check vMissing.isNil
    db.close()

  test "spillable: put goes to memory, persists across reopen":
    let dir = getTempDir() / "glen_spill_put"
    removeDir(dir)
    block:
      let db = newGlenDB(dir)
      var v = VObject(); v["n"] = VInt(1)
      db.put("things", "x", v)
      db.compact()
      db.close()
    # Open spillable, modify, close (mutation goes to WAL).
    block:
      let db = newGlenDB(dir, spillableMode = true)
      var v2 = VObject(); v2["n"] = VInt(99)
      db.put("things", "x", v2)
      db.close()
    # Reopen normally — value should be 99.
    let db = newGlenDB(dir)
    check db.get("things", "x")["n"].i == 99
    db.close()

  test "spillable: delete tombstones, persists across reopen":
    let dir = getTempDir() / "glen_spill_delete"
    removeDir(dir)
    block:
      let db = newGlenDB(dir)
      db.put("things", "a", VString("hello"))
      db.put("things", "b", VString("world"))
      db.compact()
      db.close()
    block:
      let db = newGlenDB(dir, spillableMode = true)
      db.delete("things", "a")
      check db.get("things", "a").isNil
      check db.get("things", "b").s == "world"
      db.close()
    let db = newGlenDB(dir)
    check db.get("things", "a").isNil
    check db.get("things", "b").s == "world"
    db.close()

  test "spillable: hotDocCap evicts cold non-dirty docs":
    let dir = getTempDir() / "glen_spill_evict"
    removeDir(dir)
    block:
      let db = newGlenDB(dir)
      for i in 0 ..< 200:
        var v = VObject(); v["n"] = VInt(i.int64)
        db.put("things", "t" & $i, v)
      db.compact()
      db.close()
    # hotDocCap = 10 — opening doesn't load anything; reads cap at 10 in RAM.
    let db = newGlenDB(dir, spillableMode = true, hotDocCap = 10)
    # Touch many docs in sequence; only the most recent ~10 should be in cs.docs.
    for i in 0 ..< 50:
      let v = db.get("things", "t" & $i)
      check not v.isNil
    # Re-touch a far-back doc — it should fault back from disk correctly.
    let vRetry = db.get("things", "t3")
    check vRetry["n"].i == 3
    db.close()

  test "compact while spillable rewrites and remaps":
    let dir = getTempDir() / "glen_spill_compact"
    removeDir(dir)
    block:
      let db = newGlenDB(dir)
      for i in 0 ..< 50:
        var v = VObject(); v["n"] = VInt(i.int64)
        db.put("things", "t" & $i, v)
      db.compact()
      db.close()
    let db = newGlenDB(dir, spillableMode = true)
    # In-memory mutation
    db.put("things", "fresh", VString("brand new"))
    db.delete("things", "t0")
    # Compact: rewrites snapshot v2 with the new state, swaps mmap.
    db.compact()
    # Both the modified and the snapshot-only docs should still be queryable.
    check db.get("things", "fresh").s == "brand new"
    check db.get("things", "t0").isNil      # tombstoned and now compacted away
    check db.get("things", "t25")["n"].i == 25
    db.close()
    # Final reopen confirms persistence.
    let db2 = newGlenDB(dir, spillableMode = true)
    check db2.get("things", "fresh").s == "brand new"
    check db2.get("things", "t0").isNil
    db2.close()
