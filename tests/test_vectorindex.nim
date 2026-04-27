import std/[unittest, math, random, os, tables, algorithm]
import glen/types, glen/vectorindex

proc randomVec(rng: var Rand; dim: int): seq[float32] =
  result = newSeq[float32](dim)
  for i in 0 ..< dim:
    result[i] = float32(rng.gauss(0.0, 1.0))

proc bruteForceKnn(vecs: seq[seq[float32]]; query: seq[float32];
                   k: int; metric: VectorMetric): seq[int] =
  type DI = tuple[d: float32, i: int]
  var arr: seq[DI] = @[]
  for i, v in vecs:
    var d: float32
    case metric
    of vmCosine:
      var dot, nA, nQ: float32
      for j in 0 ..< v.len:
        dot += v[j] * query[j]
        nA += v[j] * v[j]
        nQ += query[j] * query[j]
      d = 1.0'f32 - dot / (sqrt(nA) * sqrt(nQ))
    of vmL2:
      var s: float32
      for j in 0 ..< v.len:
        let dd = v[j] - query[j]
        s += dd * dd
      d = sqrt(s)
    of vmDot:
      var dot: float32
      for j in 0 ..< v.len: dot += v[j] * query[j]
      d = -dot
    arr.add((d: d, i: i))
  arr.sort(proc (a, b: DI): int = cmp(a.d, b.d))
  result = newSeqOfCap[int](k)
  for i in 0 ..< min(k, arr.len): result.add(arr[i].i)

suite "hnsw: core search":
  test "knn returns the closest by L2 with reasonable recall":
    var rng = initRand(7)
    var h = newHnsw(dim = 16, M = 16, efConstruction = 100, efSearch = 50,
                    metric = vmL2)
    var vecs: seq[seq[float32]] = @[]
    for i in 0 ..< 1000:
      let v = randomVec(rng, 16)
      vecs.add(v)
      discard h.insert(v)
    var hits = 0
    let trials = 50
    for t in 0 ..< trials:
      let q = randomVec(rng, 16)
      let exact = bruteForceKnn(vecs, q, 10, vmL2)
      let approx = h.knnSearch(q, 10)
      var approxIds: seq[int] = @[]
      for (_, nid) in approx: approxIds.add(int(nid))
      var found = 0
      for x in exact:
        if x in approxIds: inc found
      hits += found
    let recall = float(hits) / float(trials * 10)
    check recall >= 0.9

  test "knn cosine with normalised vectors":
    var rng = initRand(11)
    var h = newHnsw(dim = 32, metric = vmCosine, M = 16, efSearch = 64,
                    efConstruction = 200)
    var vecs: seq[seq[float32]] = @[]
    for i in 0 ..< 500:
      let v = randomVec(rng, 32)
      vecs.add(v)
      discard h.insert(v)
    let q = randomVec(rng, 32)
    let exact = bruteForceKnn(vecs, q, 5, vmCosine)
    let approx = h.knnSearch(q, 5)
    check approx.len == 5
    var hits = 0
    for (_, nid) in approx:
      if int(nid) in exact: inc hits
    check hits >= 3   # cosine recall@5 ≥ 60%

  test "knn dot product":
    var rng = initRand(13)
    var h = newHnsw(dim = 8, metric = vmDot, M = 8, efSearch = 30,
                    efConstruction = 60)
    for i in 0 ..< 100:
      discard h.insert(randomVec(rng, 8))
    let q = randomVec(rng, 8)
    let r = h.knnSearch(q, 3)
    check r.len == 3
    # dot results: smaller (more negative) = better
    check r[0].dist <= r[1].dist
    check r[1].dist <= r[2].dist

  test "empty hnsw returns empty":
    let h = newHnsw(dim = 4)
    let r = h.knnSearch(@[1.0'f32, 2, 3, 4], 5)
    check r.len == 0

  test "single insert can be retrieved":
    var h = newHnsw(dim = 4, metric = vmL2)
    discard h.insert(@[1.0'f32, 0, 0, 0])
    let r = h.knnSearch(@[1.0'f32, 0, 0, 0], 1)
    check r.len == 1
    check r[0].dist < 1e-5

suite "hnsw: VectorIndex (Glen-facing)":
  test "indexDoc + findNearest by docId":
    let idx = newVectorIndex("emb", "vec", dim = 4, metric = vmL2)
    proc mkDoc(arr: seq[float32]): Value =
      result = VObject()
      var v: seq[Value] = @[]
      for x in arr: v.add(VFloat(float64(x)))
      result["vec"] = VArray(v)
    idx.indexDoc("a", mkDoc(@[1.0'f32, 0, 0, 0]))
    idx.indexDoc("b", mkDoc(@[0.0'f32, 1, 0, 0]))
    idx.indexDoc("c", mkDoc(@[0.0'f32, 0, 1, 0]))
    let r = idx.findNearest(@[1.0'f32, 0.1, 0.0, 0.0], 1)
    check r.len == 1
    check r[0][0] == "a"

  test "soft-delete: unindexed doc is filtered out":
    let idx = newVectorIndex("emb", "vec", dim = 2, metric = vmL2)
    proc mkDoc(arr: seq[float32]): Value =
      result = VObject()
      var v: seq[Value] = @[]
      for x in arr: v.add(VFloat(float64(x)))
      result["vec"] = VArray(v)
    idx.indexDoc("a", mkDoc(@[1.0'f32, 0]))
    idx.indexDoc("b", mkDoc(@[0.0'f32, 1]))
    idx.unindexDoc("a", mkDoc(@[1.0'f32, 0]))
    let r = idx.findNearest(@[1.0'f32, 0], 5)
    for (id, _) in r: check id != "a"

  test "persistence round-trip":
    let dir = getTempDir() / "glen_vri_persist"
    removeDir(dir); createDir(dir)
    let path = dir / "test.vri"
    let idx = newVectorIndex("emb", "vec", dim = 8, metric = vmL2)
    proc mkDoc(arr: seq[float32]): Value =
      result = VObject()
      var v: seq[Value] = @[]
      for x in arr: v.add(VFloat(float64(x)))
      result["vec"] = VArray(v)
    var rng = initRand(17)
    var inputs: seq[(string, seq[float32])] = @[]
    for i in 0 ..< 100:
      let id = "d" & $i
      let v = randomVec(rng, 8)
      inputs.add((id, v))
      idx.indexDoc(id, mkDoc(v))
    dumpVectorIndex(idx, path)
    # Load into a fresh index.
    let loaded = newVectorIndex("emb", "vec", dim = 8, metric = vmL2)
    check tryLoadVectorIndex(loaded, path)
    # Same query produces same results.
    let q = randomVec(rng, 8)
    let r1 = idx.findNearest(q, 5)
    let r2 = loaded.findNearest(q, 5)
    check r1.len == r2.len
    for i in 0 ..< r1.len:
      check r1[i][0] == r2[i][0]
      check abs(r1[i][1] - r2[i][1]) < 1e-3

  test "load with mismatched dim returns false":
    let dir = getTempDir() / "glen_vri_mismatch"
    removeDir(dir); createDir(dir)
    let path = dir / "test.vri"
    let idx = newVectorIndex("emb", "vec", dim = 4)
    dumpVectorIndex(idx, path)
    let bad = newVectorIndex("emb", "vec", dim = 8)
    check not tryLoadVectorIndex(bad, path)
