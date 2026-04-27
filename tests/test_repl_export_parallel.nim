import std/[os, unittest, sets]
import glen/db, glen/types

# Verify exportChanges still produces correct results after the refactor that
# moved filtering/sorting outside the replLock. The key correctness invariants:
#   - every emitted change has a unique changeId
#   - cursor advances monotonically
#   - replaying from a cursor never re-emits already-seen changes
#   - filter combinations still work

proc freshDir(name: string): string =
  result = getTempDir() / name
  removeDir(result)
  createDir(result)

suite "parallel replication export (refactor correctness)":
  test "incremental export visits every change exactly once":
    let dir = freshDir("glen_repl_unique")
    let db = newGlenDB(dir)
    for i in 0 ..< 500:
      var v = VObject(); v["i"] = VInt(int64(i))
      db.put("c", "d" & $i, v)
    var seen = initHashSet[string]()
    var cursor: uint64 = 0
    while true:
      let (next, changes) = db.exportChanges(cursor)
      if changes.len == 0: break
      for ch in changes:
        check ch.changeId notin seen
        seen.incl(ch.changeId)
      check next > cursor
      cursor = next
    check seen.len == 500
    db.close()

  test "binary search start: cursor mid-range yields only the suffix":
    let dir = freshDir("glen_repl_suffix")
    let db = newGlenDB(dir)
    for i in 0 ..< 100:
      var v = VObject(); v["i"] = VInt(int64(i))
      db.put("c", "d" & $i, v)
    let (cursor, all) = db.exportChanges(0)
    check all.len == 100
    # Pick a midpoint cursor: should get the second half.
    let mid = uint64(50)
    let (_, half) = db.exportChanges(mid)
    check half.len == 50
    # Beyond the end: empty.
    let (_, none) = db.exportChanges(cursor)
    check none.len == 0
    db.close()

  test "filtered export still uses the per-collection paths":
    let dir = freshDir("glen_repl_filtered")
    let db = newGlenDB(dir)
    for i in 0 ..< 50:
      var v = VObject(); v["i"] = VInt(int64(i))
      db.put("a", "x" & $i, v)
      db.put("b", "x" & $i, v)
    block:
      let (_, only_a) = db.exportChanges(0, includeCollections = @["a"])
      check only_a.len == 50
      for ch in only_a: check ch.collection == "a"
    block:
      let (_, no_b) = db.exportChanges(0, excludeCollections = @["b"])
      check no_b.len == 50
      for ch in no_b: check ch.collection == "a"
    block:
      let (_, both) = db.exportChanges(0)
      check both.len == 100
    db.close()

  test "interleaved put/export sees no holes":
    let dir = freshDir("glen_repl_interleaved")
    let db = newGlenDB(dir)
    var cursor: uint64 = 0
    var seen = initHashSet[string]()
    for i in 0 ..< 200:
      var v = VObject(); v["i"] = VInt(int64(i))
      db.put("c", "d" & $i, v)
      let (next, ch) = db.exportChanges(cursor)
      for c in ch:
        check c.changeId notin seen
        seen.incl(c.changeId)
      cursor = next
    check seen.len == 200
    db.close()
