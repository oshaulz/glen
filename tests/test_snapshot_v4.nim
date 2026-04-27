import std/[unittest, os, tables, strutils]
import glen/types, glen/codec, glen/storage, glen/db

proc freshDir(name: string): string =
  result = getTempDir() / name
  removeDir(result); createDir(result)

proc mkUser(name: string; age: int; status: string): Value =
  result = VObject()
  result["name"] = VString(name)
  result["age"] = VInt(int64(age))
  result["status"] = VString(status)

proc snapshotSize(dir, collection: string): int64 =
  getFileSize(dir / (collection & ".snap"))

suite "snapshot v4 round-trip":
  test "writeSnapshotV4 + loadSnapshot eager":
    let dir = freshDir("glen_v4_eager")
    var docs = initTable[string, Value]()
    for i in 0 ..< 50:
      docs["d" & $i] = mkUser("u" & $i, i, "active")
    writeSnapshotV4(dir, "users", docs)
    let back = loadSnapshot(dir, "users")
    check back.len == 50
    for i in 0 ..< 50:
      let id = "d" & $i
      check back[id]["name"].s == "u" & $i
      check back[id]["status"].s == "active"

  test "openSnapshotMmap reads v4 metadata only":
    let dir = freshDir("glen_v4_mmap")
    var docs = initTable[string, Value]()
    for i in 0 ..< 100:
      docs["d" & $i] = mkUser("u" & $i, i, if (i and 1) == 0: "active" else: "pending")
    writeSnapshotV4(dir, "users", docs)
    let mm = openSnapshotMmap(dir, "users")
    check mm.isV4
    check mm.docCount == 100
    check not mm.keyDict.isNil
    check "name" in mm.keyDict.keyToId
    check "status" in mm.keyDict.keyToId
    check "age" in mm.keyDict.keyToId
    let v = loadDocFromMmap(mm, "d42")
    check not v.isNil
    check v["name"].s == "u42"
    closeSnapshotMmap(mm)

  test "v4 with no dictionary (threshold = 0) still works":
    let dir = freshDir("glen_v4_no_dict")
    var docs = initTable[string, Value]()
    for i in 0 ..< 10:
      docs["d" & $i] = mkUser("u" & $i, i, "active")
    writeSnapshotV4(dir, "users", docs, keyDictThreshold = 0,
                    valueDictThreshold = 0)
    let back = loadSnapshot(dir, "users")
    check back.len == 10
    let mm = openSnapshotMmap(dir, "users")
    check mm.isV4
    check mm.keyDict.isNil
    closeSnapshotMmap(mm)

  test "dict yields a meaningful size win":
    let dir = freshDir("glen_v4_size_win")
    var docs = initTable[string, Value]()
    for i in 0 ..< 1000:
      docs["d" & $i] = mkUser("u" & $i, i, "active")
    writeSnapshotV4(dir, "users", docs)
    let v4Size = snapshotSize(dir, "users")
    # Compare to v3 written separately.
    let dir3 = freshDir("glen_v3_size_baseline")
    writeSnapshotV3(dir3, "users", docs)
    let v3Size = snapshotSize(dir3, "users")
    # v4 should be smaller — three repeated keys ("name", "age", "status")
    # × 1000 docs × ~5 bytes each ≈ 15kB savings. Even after dict overhead,
    # net savings should be at least 5kB.
    check v4Size < v3Size
    check (v3Size - v4Size) > 5_000

  test "rare keys stay inline (below threshold)":
    let dir = freshDir("glen_v4_threshold")
    var docs = initTable[string, Value]()
    # 50 docs with common keys + 1 doc with a one-off key.
    for i in 0 ..< 50:
      docs["d" & $i] = mkUser("u" & $i, i, "active")
    var oddball = mkUser("z", 0, "active")
    oddball["temp_debug_field_appears_once"] = VString("here")
    docs["z"] = oddball
    writeSnapshotV4(dir, "users", docs, keyDictThreshold = 4)
    let mm = openSnapshotMmap(dir, "users")
    check mm.isV4
    check "name" in mm.keyDict.keyToId
    check "temp_debug_field_appears_once" notin mm.keyDict.keyToId
    let v = loadDocFromMmap(mm, "z")
    check not v.isNil
    check v["temp_debug_field_appears_once"].s == "here"
    closeSnapshotMmap(mm)

  test "back-compat: v3 file still loads":
    let dir = freshDir("glen_v4_backcompat")
    var docs = initTable[string, Value]()
    for i in 0 ..< 20:
      docs["d" & $i] = mkUser("u" & $i, i, "active")
    writeSnapshotV3(dir, "users", docs)   # legacy v3 writer
    let back = loadSnapshot(dir, "users")
    check back.len == 20
    let mm = openSnapshotMmap(dir, "users")
    check mm.isV3
    check not mm.isV4
    closeSnapshotMmap(mm)

  test "iterIds yields docs in sorted order (v4)":
    let dir = freshDir("glen_v4_iter")
    var docs = initTable[string, Value]()
    docs["c"] = mkUser("c", 1, "x")
    docs["a"] = mkUser("a", 2, "x")
    docs["b"] = mkUser("b", 3, "x")
    writeSnapshotV4(dir, "users", docs)
    let mm = openSnapshotMmap(dir, "users")
    var ids: seq[string] = @[]
    for id in iterIds(mm): ids.add(id)
    check ids == @["a", "b", "c"]
    closeSnapshotMmap(mm)

suite "snapshot v4 with value dictionary":
  test "value dict yields additional size win on enum-heavy collection":
    let dirA = freshDir("glen_v4_keys_only")
    let dirB = freshDir("glen_v4_keys_plus_values")
    var docs = initTable[string, Value]()
    let statuses = @["active", "inactive", "pending", "banned"]
    let regions  = @["us-east", "us-west", "eu-west", "ap-south"]
    for i in 0 ..< 1000:
      var v = mkUser("u" & $i, i, statuses[i mod 4])
      v["region"] = VString(regions[(i * 3) mod 4])
      docs["d" & $i] = v
    writeSnapshotV4(dirA, "users", docs, valueDictThreshold = 0)  # keys only
    writeSnapshotV4(dirB, "users", docs)                          # keys + values
    let sa = snapshotSize(dirA, "users")
    let sb = snapshotSize(dirB, "users")
    check sb < sa
    # ~4 enum status values × 1000 docs × ~5 bytes each = 20 KB potential.
    # Should net at least ~5 KB after dict overhead.
    check (sa - sb) > 5_000

  test "v4 with value dict round-trips correctly":
    let dir = freshDir("glen_v4_value_round")
    var docs = initTable[string, Value]()
    for i in 0 ..< 50:
      var v = mkUser("u" & $i, i, "active")
      v["region"] = VString("us-east")
      docs["d" & $i] = v
    writeSnapshotV4(dir, "users", docs)
    let mm = openSnapshotMmap(dir, "users")
    check mm.isV4
    check not mm.keyDict.isNil
    check "status" in mm.keyDict.valueDictByField
    check "active" in mm.keyDict.valueDictByField["status"].valToId
    let v = loadDocFromMmap(mm, "d10")
    check v["status"].s == "active"
    check v["region"].s == "us-east"
    closeSnapshotMmap(mm)

  test "rare values stay inline in v4":
    let dir = freshDir("glen_v4_rare_value")
    var docs = initTable[string, Value]()
    for i in 0 ..< 20:
      docs["d" & $i] = mkUser("u" & $i, i, "active")
    var oddball = mkUser("z", 0, "one-time-marker-string-not-shared")
    docs["z"] = oddball
    writeSnapshotV4(dir, "users", docs, valueDictThreshold = 4)
    let mm = openSnapshotMmap(dir, "users")
    if "status" in mm.keyDict.valueDictByField:
      let vd = mm.keyDict.valueDictByField["status"]
      check "active" in vd.valToId
      check "one-time-marker-string-not-shared" notin vd.valToId
    let v = loadDocFromMmap(mm, "z")
    check v["status"].s == "one-time-marker-string-not-shared"
    closeSnapshotMmap(mm)

suite "v4 inside GlenDB":
  test "compact() writes v4 by default; reopen mmaps it":
    let dir = freshDir("glen_v4_compact_default")
    block:
      let db = newGlenDB(dir)
      for i in 0 ..< 100:
        db.put("users", "u" & $i, mkUser("u" & $i, i, "active"))
      db.compact()
      db.close()
    # Confirm the file is v4 by sniffing magic.
    let f = readFile(dir / "users.snap")
    check f.startsWith(SnapshotV4Magic)
    # Reopen and verify lookups work.
    let db2 = newGlenDB(dir)
    let v = db2.get("users", "u42")
    check not v.isNil
    check v["name"].s == "u42"
    db2.close()

  test "spillable mode + v4 snapshot: lookups + dirty cycle":
    let dir = freshDir("glen_v4_spill")
    block:
      let db = newGlenDB(dir)
      for i in 0 ..< 500:
        db.put("users", "u" & $i, mkUser("u" & $i, i, "active"))
      db.compact()
      db.close()
    let db = newGlenDB(dir, spillableMode = true)
    # Snapshot reads
    for i in [10, 100, 250, 499]:
      let v = db.get("users", "u" & $i)
      check not v.isNil
      check v["status"].s == "active"
    # Dirty puts
    db.put("users", "u500", mkUser("u500", 500, "pending"))
    let v = db.get("users", "u500")
    check v["status"].s == "pending"
    # Compact again, reopen, still readable.
    db.compact()
    db.close()
    let db2 = newGlenDB(dir, spillableMode = true)
    let v500 = db2.get("users", "u500")
    check v500["status"].s == "pending"
    let u100 = db2.get("users", "u100")
    check u100["status"].s == "active"
    db2.close()
