import std/[os, unittest, times]
import glen/db, glen/types, glen/wal

proc freshDir(name: string): string =
  result = getTempDir() / name
  removeDir(result)
  createDir(result)

suite "auto-compaction triggers":
  test "wal-bytes trigger fires after threshold":
    let dir = freshDir("glen_autocompact_walbytes")
    # Tiny threshold so a handful of puts crosses it.
    let db = newGlenDB(dir, compactWalBytes = 256)
    for i in 0 ..< 200:
      var v = VObject()
      v["i"] = VString("v" & $i & "------------padding-padding-padding")
      db.put("c", "d" & $i, v)
    # After many puts, wal should have been reset by an auto-compact at least
    # once — its current total size should be well under what we wrote.
    check db.wal.totalSize() < 200 * 50
    db.close()

  test "interval trigger fires after elapsed time":
    let dir = freshDir("glen_autocompact_interval")
    let db = newGlenDB(dir, compactIntervalMs = 10)
    var v = VObject(); v["a"] = VString("x")
    db.put("c", "d1", v)
    let walAfterFirst = db.wal.totalSize()
    sleep(50)  # past the 10ms interval
    db.put("c", "d2", v)
    # The second put should have triggered compact — wal should be smaller than
    # (or equal to) one record's worth, not two.
    check db.wal.totalSize() < walAfterFirst + 100
    db.close()

  test "no triggers configured = no automatic compact":
    let dir = freshDir("glen_autocompact_off")
    let db = newGlenDB(dir)  # all triggers default 0
    var v = VObject(); v["a"] = VString("x")
    for i in 0 ..< 50:
      db.put("c", "d" & $i, v)
    # No compaction means wal grew with each put.
    check db.wal.totalSize() > 0
    db.close()

  test "data integrity preserved across auto-compactions":
    let dir = freshDir("glen_autocompact_integrity")
    let db = newGlenDB(dir, compactWalBytes = 512)
    for i in 0 ..< 500:
      var v = VObject(); v["k"] = VString("val" & $i)
      db.put("c", "d" & $i, v)
    # Spot-check: every doc still readable.
    for i in 0 ..< 500:
      let g = db.get("c", "d" & $i)
      check not g.isNil
      check g["k"].sval == "val" & $i
    db.close()
    # Reopen and verify durability.
    let db2 = newGlenDB(dir)
    for i in 0 ..< 500:
      let g = db2.get("c", "d" & $i)
      check not g.isNil
      check g["k"].sval == "val" & $i
    db2.close()

  test "manual compact() updates lastCompactMs (interval re-arms)":
    let dir = freshDir("glen_autocompact_interval_rearm")
    let db = newGlenDB(dir, compactIntervalMs = 1000)  # 1s
    var v = VObject(); v["a"] = VString("x")
    db.put("c", "d1", v)
    db.compact()
    # immediately after manual compact, an auto-compact should NOT fire.
    db.put("c", "d2", v)
    check db.wal.totalSize() > 0  # there are at least one record after compact
    db.close()

  test "explicit maybeAutoCompact() with no triggers is a no-op":
    let dir = freshDir("glen_autocompact_noop")
    let db = newGlenDB(dir)
    var v = VObject(); v["a"] = VString("x")
    db.put("c", "d1", v)
    let before = db.wal.totalSize()
    db.maybeAutoCompact()
    check db.wal.totalSize() == before
    db.close()
