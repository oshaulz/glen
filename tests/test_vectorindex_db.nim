import std/[unittest, math, random, os, tables, strutils]
import glen/db, glen/types, glen/vectorindex

proc mkDoc(arr: seq[float32]; extras: openArray[(string, Value)] = @[]): Value =
  result = VObject()
  var v: seq[Value] = @[]
  for x in arr: v.add(VFloat(float64(x)))
  result["emb"] = VArray(v)
  for (k, vv) in extras: result[k] = vv

proc randomVec(rng: var Rand; dim: int): seq[float32] =
  result = newSeq[float32](dim)
  for i in 0 ..< dim:
    result[i] = float32(rng.gauss(0.0, 1.0))

proc freshDir(name: string): string =
  result = getTempDir() / name
  removeDir(result)
  createDir(result)

suite "GlenDB + VectorIndex":
  test "create on existing data, then findNearestVector":
    let dir = freshDir("glen_vidx_create")
    let db = newGlenDB(dir)
    var rng = initRand(7)
    var stored: seq[(string, seq[float32])] = @[]
    for i in 0 ..< 200:
      let id = "d" & $i
      let v = randomVec(rng, 8)
      stored.add((id, v))
      db.put("docs", id, mkDoc(v))
    db.createVectorIndex("docs", "by_emb", "emb", dim = 8, metric = vmL2)
    let q = randomVec(rng, 8)
    let r = db.findNearestVector("docs", "by_emb", q, 5)
    check r.len == 5
    db.close()

  test "online insert: findNearest sees newly-put docs":
    let dir = freshDir("glen_vidx_online")
    let db = newGlenDB(dir)
    db.createVectorIndex("docs", "by_emb", "emb", dim = 4, metric = vmL2)
    db.put("docs", "a", mkDoc(@[1.0'f32, 0, 0, 0]))
    db.put("docs", "b", mkDoc(@[0.0'f32, 1, 0, 0]))
    db.put("docs", "c", mkDoc(@[0.0'f32, 0, 1, 0]))
    let r = db.findNearestVector("docs", "by_emb", @[1.0'f32, 0.1, 0.0, 0.0], 1)
    check r.len == 1
    check r[0][0] == "a"
    db.close()

  test "delete: removed doc no longer returned":
    let dir = freshDir("glen_vidx_delete")
    let db = newGlenDB(dir)
    db.createVectorIndex("docs", "by_emb", "emb", dim = 2, metric = vmL2)
    db.put("docs", "a", mkDoc(@[1.0'f32, 0]))
    db.put("docs", "b", mkDoc(@[0.0'f32, 1]))
    db.delete("docs", "a")
    let r = db.findNearestVector("docs", "by_emb", @[1.0'f32, 0], 5)
    for (id, _) in r: check id != "a"
    db.close()

  test "persistence: index survives close + reopen via .vri dump":
    let dir = freshDir("glen_vidx_persist")
    var stored: seq[(string, seq[float32])] = @[]
    var rng = initRand(11)
    block:
      let db = newGlenDB(dir)
      db.createVectorIndex("docs", "by_emb", "emb", dim = 6, metric = vmL2)
      for i in 0 ..< 50:
        let id = "d" & $i
        let v = randomVec(rng, 6)
        stored.add((id, v))
        db.put("docs", id, mkDoc(v))
      db.compact()  # writes the .vri file
      db.close()
    # Reopen — should auto-load the .vri rather than rebuild from scratch.
    let db2 = newGlenDB(dir)
    let q = randomVec(rng, 6)
    let r = db2.findNearestVector("docs", "by_emb", q, 5)
    check r.len == 5
    db2.close()

  test "manifest: missing .vri triggers rebuild from snapshot":
    let dir = freshDir("glen_vidx_rebuild")
    var rng = initRand(13)
    block:
      let db = newGlenDB(dir)
      db.createVectorIndex("docs", "by_emb", "emb", dim = 4, metric = vmL2)
      for i in 0 ..< 30:
        db.put("docs", "d" & $i, mkDoc(randomVec(rng, 4)))
      db.compact()
      db.close()
    # Delete the .vri but leave the manifest entry.
    for kind, path in walkDir(dir):
      if kind == pcFile and path.endsWith(".vri"):
        removeFile(path)
    let db2 = newGlenDB(dir)
    let q = randomVec(rng, 4)
    let r = db2.findNearestVector("docs", "by_emb", q, 3)
    check r.len == 3
    db2.close()

  test "drop: removes index and manifest entry":
    let dir = freshDir("glen_vidx_drop")
    let db = newGlenDB(dir)
    db.createVectorIndex("docs", "by_emb", "emb", dim = 2, metric = vmL2)
    db.put("docs", "a", mkDoc(@[1.0'f32, 0]))
    db.dropVectorIndex("docs", "by_emb")
    let r = db.findNearestVector("docs", "by_emb", @[1.0'f32, 0], 5)
    check r.len == 0
    db.close()
    # Reopen: dropped index does not come back.
    let db2 = newGlenDB(dir)
    let r2 = db2.findNearestVector("docs", "by_emb", @[1.0'f32, 0], 5)
    check r2.len == 0
    db2.close()

  test "stream variant yields docs":
    let dir = freshDir("glen_vidx_stream")
    let db = newGlenDB(dir)
    db.createVectorIndex("docs", "by_emb", "emb", dim = 2, metric = vmL2)
    db.put("docs", "a", mkDoc(@[1.0'f32, 0]))
    db.put("docs", "b", mkDoc(@[0.0'f32, 1]))
    var got: seq[string] = @[]
    for (id, _, doc) in db.findNearestVectorStream("docs", "by_emb",
                                                    @[1.0'f32, 0], 2):
      check not doc.isNil
      got.add(id)
    check got.len == 2
    db.close()
