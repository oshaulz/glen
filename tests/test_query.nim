import std/[unittest, os, strutils]
import glen/db, glen/types, glen/query

proc freshDir(name: string): string =
  result = getTempDir() / name
  removeDir(result)
  createDir(result)

proc mkUser(name: string; age: int; status: string): Value =
  result = VObject()
  result["name"]   = VString(name)
  result["age"]    = VInt(int64(age))
  result["status"] = VString(status)

suite "query layer: filtering":
  test "whereEq with no index falls back to full scan":
    let dir = freshDir("glen_query_eq_scan")
    let db = newGlenDB(dir)
    db.put("u", "1", mkUser("alice",   30, "active"))
    db.put("u", "2", mkUser("bob",     25, "inactive"))
    db.put("u", "3", mkUser("carol",   40, "active"))
    var q = db.query("u")
    let r = q.whereEq("status", VString("active")).run()
    check r.len == 2
    var ids = ""
    for (id, _) in r: ids.add(id)
    check "1" in ids and "3" in ids
    db.close()

  test "whereEq uses index when one covers the field":
    let dir = freshDir("glen_query_eq_idx")
    let db = newGlenDB(dir)
    db.put("u", "1", mkUser("alice", 30, "active"))
    db.put("u", "2", mkUser("bob",   25, "inactive"))
    db.put("u", "3", mkUser("carol", 40, "active"))
    db.createIndex("u", "byStatus", "status")
    var q = db.query("u")
    let r = q.whereEq("status", VString("active")).run()
    check r.len == 2
    db.close()

  test "compound predicates: index + post-filter":
    let dir = freshDir("glen_query_compound")
    let db = newGlenDB(dir)
    for i in 0 ..< 50:
      db.put("u", "u" & $i,
             mkUser("name" & $i, 20 + (i mod 30),
                    if i mod 3 == 0: "active" else: "inactive"))
    db.createIndex("u", "byStatus", "status")
    var q = db.query("u")
    let r = q.whereEq("status", VString("active"))
              .whereGte("age", VInt(30))
              .run()
    for (_, doc) in r:
      check doc["status"].s == "active"
      check doc["age"].i >= 30
    db.close()

  test "whereIn flattens to multiple equality lookups":
    let dir = freshDir("glen_query_in")
    let db = newGlenDB(dir)
    db.put("u", "1", mkUser("a", 20, "active"))
    db.put("u", "2", mkUser("b", 21, "pending"))
    db.put("u", "3", mkUser("c", 22, "banned"))
    db.put("u", "4", mkUser("d", 23, "active"))
    db.createIndex("u", "byStatus", "status")
    var q = db.query("u")
    let r = q.whereIn("status",
                       [VString("active"), VString("pending")]).run()
    check r.len == 3

  test "whereContains on string fields":
    let dir = freshDir("glen_query_contains")
    let db = newGlenDB(dir)
    db.put("u", "1", mkUser("alice",   30, "active"))
    db.put("u", "2", mkUser("alistair",25, "active"))
    db.put("u", "3", mkUser("bob",     40, "active"))
    var q = db.query("u")
    let r = q.whereContains("name", "ali").run()
    check r.len == 2
    db.close()

suite "query layer: ordering, limit, cursor":
  test "orderBy ascending on numeric field":
    let dir = freshDir("glen_query_orderby_asc")
    let db = newGlenDB(dir)
    db.put("u", "1", mkUser("a", 30, "active"))
    db.put("u", "2", mkUser("b", 10, "active"))
    db.put("u", "3", mkUser("c", 20, "active"))
    var q = db.query("u")
    let r = q.orderByField("age").run()
    check r.len == 3
    check r[0][1]["age"].i == 10
    check r[1][1]["age"].i == 20
    check r[2][1]["age"].i == 30
    db.close()

  test "orderBy descending":
    let dir = freshDir("glen_query_orderby_desc")
    let db = newGlenDB(dir)
    db.put("u", "1", mkUser("a", 30, "active"))
    db.put("u", "2", mkUser("b", 10, "active"))
    db.put("u", "3", mkUser("c", 20, "active"))
    var q = db.query("u")
    let r = q.orderByField("age", ascending = false).run()
    check r[0][1]["age"].i == 30
    check r[2][1]["age"].i == 10
    db.close()

  test "limit + cursor for stable pagination":
    let dir = freshDir("glen_query_paginate")
    let db = newGlenDB(dir)
    for i in 0 ..< 25:
      db.put("u", "u" & $i, mkUser("u" & $i, i, "active"))
    var q1 = db.query("u")
    let page1 = q1.orderByField("age").limitN(10).run()
    check page1.len == 10
    let cur = nextCursor(page1)
    var q2 = db.query("u")
    let page2 = q2.orderByField("age").limitN(10).afterCursor(cur).run()
    check page2.len == 10
    var q3 = db.query("u")
    let page3 = q3.orderByField("age").limitN(10).afterCursor(nextCursor(page2)).run()
    check page3.len == 5
    # Verify no duplicates across pages.
    var seen: seq[string] = @[]
    for page in [page1, page2, page3]:
      for (id, _) in page: seen.add(id)
    var unique: seq[string] = @[]
    for id in seen:
      if id notin unique: unique.add(id)
    check seen.len == unique.len
    db.close()

  test "stream variant yields the same docs":
    let dir = freshDir("glen_query_stream")
    let db = newGlenDB(dir)
    for i in 0 ..< 10:
      db.put("u", "u" & $i, mkUser("u" & $i, i, "active"))
    var q = db.query("u")
    var got: seq[string] = @[]
    for (id, _) in q.orderByField("age").runStream():
      got.add(id)
    check got.len == 10
    db.close()
