import std/[os, unittest, tables, sets, strutils]
import glen/db, glen/types, glen/txn, glen/storage

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

suite "spillable: bulk reads see snapshot-only docs":
  test "getMany returns docs even if not yet faulted":
    let dir = getTempDir() / "glen_spill_getmany"
    removeDir(dir)
    block:
      let db = newGlenDB(dir)
      for i in 0 ..< 100:
        var v = VObject(); v["n"] = VInt(i.int64)
        db.put("things", "t" & $i, v)
      db.compact()
      db.close()
    let db = newGlenDB(dir, spillableMode = true)
    var ids: seq[string] = @[]
    for i in 0 ..< 100: ids.add("t" & $i)
    let pairs = db.getMany("things", ids)
    check pairs.len == 100   # not 0
    var foundCount = 0
    for (_, v) in pairs:
      check not v.isNil; inc foundCount
    check foundCount == 100
    db.close()

  test "getAll returns snapshot + in-memory docs":
    let dir = getTempDir() / "glen_spill_getall"
    removeDir(dir)
    block:
      let db = newGlenDB(dir)
      for i in 0 ..< 50:
        db.put("things", "t" & $i, VInt(i.int64))
      db.compact()
      db.close()
    let db = newGlenDB(dir, spillableMode = true)
    db.put("things", "z-fresh", VString("new"))   # in-memory
    db.delete("things", "t10")                    # tombstone snapshot doc
    let all = db.getAll("things")
    # 50 snapshot - 1 deleted + 1 fresh = 50
    check all.len == 50
    var foundFresh = false
    var foundT10   = false
    for (id, _) in all:
      if id == "z-fresh": foundFresh = true
      if id == "t10":     foundT10 = true
    check foundFresh
    check not foundT10
    db.close()

  test "createIndex sees snapshot-only docs":
    let dir = getTempDir() / "glen_spill_index"
    removeDir(dir)
    block:
      let db = newGlenDB(dir)
      for i in 0 ..< 30:
        var v = VObject()
        v["category"] = VString(if i mod 2 == 0: "even" else: "odd")
        v["n"] = VInt(i.int64)
        db.put("things", "t" & $i, v)
      db.compact()
      db.close()
    let db = newGlenDB(dir, spillableMode = true)
    db.createIndex("things", "byCategory", "category")
    let evens = db.findBy("things", "byCategory", VString("even"))
    check evens.len == 15
    let odds = db.findBy("things", "byCategory", VString("odd"))
    check odds.len == 15
    db.close()

  test "createGeoIndex sees snapshot-only docs":
    let dir = getTempDir() / "glen_spill_geoidx"
    removeDir(dir)
    block:
      let db = newGlenDB(dir)
      for i in 0 ..< 30:
        var v = VObject()
        v["lon"] = VFloat(float64(i))
        v["lat"] = VFloat(0.0)
        db.put("places", "p" & $i, v)
      db.compact()
      db.close()
    let db = newGlenDB(dir, spillableMode = true)
    db.createGeoIndex("places", "byLoc", "lon", "lat")
    let res = db.findInBBox("places", "byLoc", 4.5, -1.0, 14.5, 1.0)
    check res.len == 10   # p5..p14
    db.close()

suite "spillable: dirty budget guardrail":
  test "putMany above maxDirtyDocs raises ValueError":
    let dir = getTempDir() / "glen_spill_budget_putmany"
    removeDir(dir)
    block:
      let db = newGlenDB(dir)
      db.put("things", "seed", VInt(0))
      db.compact()
      db.close()
    let db = newGlenDB(dir, spillableMode = true, maxDirtyDocs = 5)
    var batch: seq[(string, Value)] = @[]
    for i in 0 ..< 10:
      batch.add(("k" & $i, VInt(i.int64)))
    expect(ValueError):
      db.putMany("things", batch)
    db.close()

  test "deleteMany above maxDirtyDocs raises ValueError":
    let dir = getTempDir() / "glen_spill_budget_deletemany"
    removeDir(dir)
    block:
      let db = newGlenDB(dir)
      for i in 0 ..< 20:
        db.put("things", "k" & $i, VInt(i.int64))
      db.compact()
      db.close()
    let db = newGlenDB(dir, spillableMode = true, maxDirtyDocs = 5)
    var ids: seq[string] = @[]
    for i in 0 ..< 10: ids.add("k" & $i)
    expect(ValueError):
      db.deleteMany("things", ids)
    db.close()

  test "commit above maxDirtyDocs returns csInvalid (not csOk)":
    let dir = getTempDir() / "glen_spill_budget_commit"
    removeDir(dir)
    block:
      let db = newGlenDB(dir)
      db.put("things", "seed", VInt(0))
      db.compact()
      db.close()
    let db = newGlenDB(dir, spillableMode = true, maxDirtyDocs = 3)
    let t = db.beginTxn()
    for i in 0 ..< 10:
      t.stagePut(Id(collection: "things", docId: "k" & $i), VInt(i.int64))
    let r = db.commit(t)
    check r.status == csInvalid
    check r.message.contains("maxDirtyDocs")
    db.close()

  test "compact() lets the budget recover":
    let dir = getTempDir() / "glen_spill_budget_recover"
    removeDir(dir)
    block:
      let db = newGlenDB(dir)
      db.put("things", "seed", VInt(0))
      db.compact()
      db.close()
    let db = newGlenDB(dir, spillableMode = true, maxDirtyDocs = 5)
    var batch: seq[(string, Value)] = @[]
    for i in 0 ..< 5:
      batch.add(("a" & $i, VInt(i.int64)))
    db.putMany("things", batch)         # 5 dirty, at the cap
    db.compact()                         # flush; dirty resets
    var batch2: seq[(string, Value)] = @[]
    for i in 0 ..< 5:
      batch2.add(("b" & $i, VInt(i.int64)))
    db.putMany("things", batch2)        # OK now
    db.close()
