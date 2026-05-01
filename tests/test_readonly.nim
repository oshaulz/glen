## Tests for read-only mode (`newGlenDB(dir, readOnly = true)`).

import std/[unittest, os, strutils, algorithm, math]
import glen/glen
import glen/types, glen/db as glendb, glen/txn as glentxn
import glen/dsl

proc freshDir(name: string): string =
  result = getTempDir() / name
  removeDir(result)
  createDir(result)

proc seedSomeData(dir: string) =
  let db = newGlenDB(dir)
  db.put("users", "u1", %*{"name": "Alice", "age": 30, "role": "admin"})
  db.put("users", "u2", %*{"name": "Bob",   "age": 25, "role": "guest"})
  db.put("users", "u3", %*{"name": "Carol", "age": 41, "role": "admin"})
  db.createIndex("users", "byRole", "role")
  db.snapshotAll()
  db.close()

# ---- Reads work ----------------------------------------------------------

suite "readonly: reads see all the data":
  test "get / getAll / query work in readonly mode":
    let dir = freshDir("glen_ro_reads")
    seedSomeData(dir)
    let ro = newGlenDB(dir, readOnly = true)
    check ro.isReadOnly

    check ro.get("users", "u1")["name"].s == "Alice"
    check ro.getAll("users").len == 3

    let admins = query(ro, "users"):
      where: role == "admin"
      orderBy: age asc
    check admins.len == 2
    check admins[0][0] == "u2" or admins[0][0] == "u1"   # alice or carol
    ro.close()

  test "indexes loaded from manifest work":
    let dir = freshDir("glen_ro_indexes")
    seedSomeData(dir)
    let ro = newGlenDB(dir, readOnly = true)
    let hits = ro.findBy("users", "byRole", VString("admin"))
    check hits.len == 2
    ro.close()

# ---- Writes raise --------------------------------------------------------

suite "readonly: every write entry point raises":
  test "put / delete / putMany / deleteMany / commit / applyChanges":
    let dir = freshDir("glen_ro_writes")
    seedSomeData(dir)
    let ro = newGlenDB(dir, readOnly = true)

    expect IOError:
      ro.put("users", "x", %*{"name": "x"})
    expect IOError:
      ro.delete("users", "u1")
    expect IOError:
      ro.putMany("users", [("a", %*{}), ("b", %*{})])
    expect IOError:
      ro.deleteMany("users", ["u1", "u2"])
    expect IOError:
      let t = ro.beginTxn()
      t.stagePut(Id(collection: "users", docId: "x", version: 0'u64),
                 %*{"name": "x"})
      discard ro.commit(t)
    expect IOError:
      ro.applyChanges(@[])    # even empty batch should reject early

    # Read-only-confirmed: original data untouched.
    check ro.getAll("users").len == 3
    ro.close()

  test "index admin (create / drop / compact / snapshotAll) raises":
    let dir = freshDir("glen_ro_admin")
    seedSomeData(dir)
    let ro = newGlenDB(dir, readOnly = true)
    expect IOError: ro.createIndex("users", "byName", "name")
    expect IOError: ro.dropIndex("users", "byRole")
    expect IOError: ro.createGeoIndex("users", "g", "lon", "lat")
    expect IOError: ro.dropGeoIndex("users", "g")
    expect IOError: ro.createPolygonIndex("users", "p", "shape")
    expect IOError: ro.dropPolygonIndex("users", "p")
    expect IOError: ro.createVectorIndex("users", "v", "embed", 3)
    expect IOError: ro.dropVectorIndex("users", "v")
    expect IOError: ro.compact()
    expect IOError: ro.snapshotAll()
    expect IOError: ro.setWalSync(wsmAlways)
    expect IOError: ro.setPeerCursor("peer-x", 1'u64)
    expect IOError: ro.gcReplLog()
    ro.close()

# ---- Multiple read-only opens of the same dir ---------------------------

suite "readonly: concurrent opens":
  test "two read-only handles on the same dir both read fine":
    let dir = freshDir("glen_ro_two_opens")
    seedSomeData(dir)
    let a = newGlenDB(dir, readOnly = true)
    let b = newGlenDB(dir, readOnly = true)
    check a.get("users", "u1")["name"].s == "Alice"
    check b.get("users", "u1")["name"].s == "Alice"
    check a.getAll("users").len == 3
    check b.getAll("users").len == 3
    a.close(); b.close()

  test "read-only handle alongside read-write handle":
    # The read-write handle owns the WAL; the read-only handle does
    # NOT take a WAL lock and can coexist as long as you don't expect
    # the RO view to see writes the RW peer makes after RO open
    # (without explicit re-replay, which we don't expose).
    let dir = freshDir("glen_ro_mixed")
    seedSomeData(dir)
    let ro = newGlenDB(dir, readOnly = true)
    let rw = newGlenDB(dir)
    check ro.get("users", "u1")["name"].s == "Alice"
    rw.put("users", "u4", %*{"name": "Dave"})
    check rw.get("users", "u4")["name"].s == "Dave"
    # RO handle still works for the data it already has.
    check ro.get("users", "u1")["name"].s == "Alice"
    rw.close(); ro.close()

# ---- Open does not modify the directory ---------------------------------

suite "readonly: open is non-mutating":
  test "no new files appear after readOnly open":
    let dir = freshDir("glen_ro_no_writes")
    seedSomeData(dir)
    var before: seq[string] = @[]
    for kind, p in walkDir(dir):
      if kind == pcFile: before.add(p.extractFilename)
    before.sort

    let ro = newGlenDB(dir, readOnly = true)
    discard ro.get("users", "u1")
    ro.close()

    var after: seq[string] = @[]
    for kind, p in walkDir(dir):
      if kind == pcFile: after.add(p.extractFilename)
    after.sort
    check before == after

  test "opening an empty dir read-only does not create files":
    let dir = freshDir("glen_ro_empty")
    let ro = newGlenDB(dir, readOnly = true)
    var n = 0
    for kind, _ in walkDir(dir):
      if kind == pcFile: inc n
    check n == 0
    ro.close()

# ---- DSL surface still works for reads ---------------------------------

suite "readonly: DSL works for reads":
  test "Collection proxy iteration":
    let dir = freshDir("glen_ro_dsl_iter")
    seedSomeData(dir)
    let ro = newGlenDB(dir, readOnly = true)
    var n = 0
    for _, _ in ro["users"]: inc n
    check n == 3
    ro.close()

  test "liveQuery seeds OK in readonly (no events fire — no writes possible)":
    let dir = freshDir("glen_ro_dsl_live")
    seedSomeData(dir)
    let ro = newGlenDB(dir, readOnly = true)
    let live = liveQuery(ro, "users"):
      where: role == "admin"
    check live.len == 2
    live.close()
    ro.close()

  test "db.series in read-only mode: reads OK, writes raise":
    # Seed a series via a writable DB.
    let dir = freshDir("glen_ro_dsl_series")
    block:
      let rw = newGlenDB(dir)
      let s = rw.series("temp.s1")
      for i in 0 ..< 100:
        s.append(int64(i * 1000), float64(i))
      s.flush()
      s.close()
      rw.close()

    # Reopen read-only; reads work.
    let ro = newGlenDB(dir, readOnly = true)
    let s = ro.series("temp.s1")
    check s.readOnly
    let rng = s.range(0, 100_000)
    check rng.len == 100
    check rng[0] == (0'i64, 0.0)
    check rng[^1] == (99_000'i64, 99.0)
    # Writes raise.
    expect IOError:
      s.append(999_999, 42.0)
    expect IOError:
      s.dropBlocksBefore(50_000)
    s.close()       # safe — no flush attempted in RO mode
    ro.close()

  test "db.series exposes decodedChunkCacheSize":
    # Pass through the knob — semantic check is that two handles with
    # different cache sizes both work and return the same data. (We
    # don't measure RAM here; that's the bench's job.)
    let dir = freshDir("glen_ro_dsl_series_cache")
    block:
      let rw = newGlenDB(dir)
      let s = rw.series("metric", decodedChunkCacheSize = 4)
      for i in 0 ..< 50:
        s.append(int64(i * 1000), float64(i))
      s.flush(); s.close(); rw.close()
    let ro = newGlenDB(dir, readOnly = true)
    # `decodedChunkCacheSize = 1` is the smallest valid value — keeps only
    # the most-recently-decoded chunk hot, evicting on every new chunk.
    let bare = ro.series("metric", decodedChunkCacheSize = 1)
    let cached = ro.series("metric", decodedChunkCacheSize = 64)
    let r1 = bare.range(0, 200_000)
    let r2 = cached.range(0, 200_000)
    check r1.len == 50
    check r1 == r2
    bare.close(); cached.close(); ro.close()

  test "two read-only opens of the same series file coexist":
    let dir = freshDir("glen_ro_dsl_series_dual")
    block:
      let rw = newGlenDB(dir)
      let s = rw.series("metric")
      s.append(1, 1.0)
      s.append(2, 2.0)
      s.flush()
      s.close()
      rw.close()
    let ro1 = newGlenDB(dir, readOnly = true)
    let ro2 = newGlenDB(dir, readOnly = true)
    let s1 = ro1.series("metric")
    let s2 = ro2.series("metric")
    check s1.range(0, 10).len == 2
    check s2.range(0, 10).len == 2
    s1.close(); s2.close(); ro1.close(); ro2.close()

  test "db.openTiles in read-only mode: reads OK, appendFrame raises":
    let dir = freshDir("glen_ro_dsl_tiles")
    let bb = bbox(0.0, 0.0, 10.0, 10.0)
    block:
      let rw = newGlenDB(dir)
      let stack = rw.tiles("rain", bb, rows = 4, cols = 4, channels = 1)
      var m = newGeoMesh(bb, 4, 4, 1)
      for r in 0 ..< 4:
        for c in 0 ..< 4: m[r, c, 0] = float64(r * 4 + c)
      stack.appendFrame(1000, m)
      stack.flush()
      stack.close()
      rw.close()

    let ro = newGlenDB(dir, readOnly = true)
    let stack = ro.openTiles("rain")
    check stack.readOnly
    let v = stack.sampleAt(1000, 5.0, 5.0, kind = imBilinear)
    check v.classify != fcNaN
    expect IOError:
      stack.appendFrame(2000, newGeoMesh(bb, 4, 4, 1))
    stack.close()
    ro.close()

  test "db.tiles raises on read-only db (cannot create manifest)":
    let dir = freshDir("glen_ro_dsl_tiles_create")
    block:
      let rw = newGlenDB(dir)
      rw.close()
    let ro = newGlenDB(dir, readOnly = true)
    expect IOError:
      discard ro.tiles("new_stack", bbox(0.0, 0.0, 1.0, 1.0),
                      rows = 4, cols = 4, channels = 1)
    ro.close()

  test "vector index loaded from snapshot still searches":
    # Build a vector index, snapshot, reopen RO, search.
    let dir = freshDir("glen_ro_dsl_vec")
    let rw = newGlenDB(dir)
    let idx = rw.vectors("docs", "byEmbed")
    idx.create("embedding", dim = 3)
    idx.upsert("a", %*{"embedding": [1.0, 0.0, 0.0]})
    idx.upsert("b", %*{"embedding": [0.0, 1.0, 0.0]})
    rw.snapshotAll()
    rw.compact()       # write the .vri dump
    rw.close()

    let ro = newGlenDB(dir, readOnly = true)
    let hits = ro.vectors("docs", "byEmbed").search(
      vec32([1.0, 0.0, 0.0]), k = 1)
    check hits.len == 1
    check hits[0][0] == "a"
    ro.close()
