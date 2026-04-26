import std/[os, unittest, math]
import glen/db, glen/types, glen/linalg

const eps = 1e-9

proc approxEq(a, b: float64; tol = eps): bool =
  abs(a - b) < tol

suite "vector ops":
  test "basic arithmetic":
    let a = vec(1.0, 2.0, 3.0)
    let b = vec(4.0, 5.0, 6.0)
    check add(a, b) == @[5.0, 7.0, 9.0]
    check sub(b, a) == @[3.0, 3.0, 3.0]
    check scale(a, 2.0) == @[2.0, 4.0, 6.0]
    check hadamard(a, b) == @[4.0, 10.0, 18.0]

  test "operator sugar":
    let a = vec(1.0, 2.0)
    let b = vec(3.0, 4.0)
    check (a + b) == @[4.0, 6.0]
    check (b - a) == @[2.0, 2.0]
    check (a * 3.0) == @[3.0, 6.0]
    check (3.0 * a) == @[3.0, 6.0]

  test "dot, norm, normalize":
    let a = vec(3.0, 4.0)
    check dot(a, a) == 25.0
    check approxEq(norm(a), 5.0)
    let n = normalize(a)
    check approxEq(norm(n), 1.0)
    check approxEq(n[0], 0.6)
    check approxEq(n[1], 0.8)

  test "normalize on zero vector returns zero":
    let z = zerosV(3)
    check normalize(z) == @[0.0, 0.0, 0.0]

  test "euclidean and cosine":
    let a = vec(1.0, 0.0)
    let b = vec(0.0, 1.0)
    check approxEq(euclidean(a, b), sqrt(2.0))
    check approxEq(cosine(a, b), 0.0)
    let c = vec(1.0, 1.0)
    check approxEq(cosine(a, c), 1.0 / sqrt(2.0))

  test "in-place ops":
    var a = vec(1.0, 2.0, 3.0)
    addInPlace(a, vec(10.0, 20.0, 30.0))
    check a == @[11.0, 22.0, 33.0]
    scaleInPlace(a, 0.5)
    check a == @[5.5, 11.0, 16.5]

  test "shape mismatch raises":
    expect(ValueError):
      discard add(vec(1.0, 2.0), vec(1.0, 2.0, 3.0))

suite "matrix ops":
  test "constructors":
    let m = matFromRows(@[@[1.0, 2.0], @[3.0, 4.0]])
    check m.rows == 2 and m.cols == 2
    check m[0, 0] == 1.0 and m[0, 1] == 2.0
    check m[1, 0] == 3.0 and m[1, 1] == 4.0

    let id3 = eye(3)
    check id3[0, 0] == 1.0 and id3[1, 1] == 1.0 and id3[2, 2] == 1.0
    check id3[0, 1] == 0.0

  test "transpose":
    let m = matFromRows(@[@[1.0, 2.0, 3.0], @[4.0, 5.0, 6.0]])
    let t = transpose(m)
    check (t.rows, t.cols) == (3, 2)
    check t[0, 0] == 1.0 and t[0, 1] == 4.0
    check t[2, 1] == 6.0
    # transposing twice round-trips
    let tt = transpose(t)
    check tt.rows == m.rows and tt.cols == m.cols
    for i in 0 ..< m.rows:
      for j in 0 ..< m.cols:
        check tt[i, j] == m[i, j]

  test "matmul: identity is left/right identity":
    let m = matFromRows(@[@[2.0, 0.0], @[1.0, 3.0]])
    let id2 = eye(2)
    check (id2 * m).data == m.data
    check (m * id2).data == m.data

  test "matmul: explicit small case":
    let a = matFromRows(@[@[1.0, 2.0], @[3.0, 4.0]])
    let b = matFromRows(@[@[5.0, 6.0], @[7.0, 8.0]])
    let r = a * b
    check r[0, 0] == 19.0   # 1*5 + 2*7
    check r[0, 1] == 22.0   # 1*6 + 2*8
    check r[1, 0] == 43.0   # 3*5 + 4*7
    check r[1, 1] == 50.0

  test "matmul: rectangular":
    let a = matFromRows(@[@[1.0, 2.0, 3.0]])           # 1×3
    let b = matFromRows(@[@[4.0], @[5.0], @[6.0]])     # 3×1
    let r = a * b                                       # 1×1
    check (r.rows, r.cols) == (1, 1)
    check r[0, 0] == 32.0   # 1*4 + 2*5 + 3*6

  test "matvec":
    let m = matFromRows(@[@[1.0, 2.0], @[3.0, 4.0], @[5.0, 6.0]])
    let v = vec(7.0, 8.0)
    let r = m * v
    check r == @[23.0, 53.0, 83.0]   # row dots

  test "scalar add/sub/scale":
    let a = matFromRows(@[@[1.0, 2.0], @[3.0, 4.0]])
    let b = matFromRows(@[@[10.0, 20.0], @[30.0, 40.0]])
    let s = a + b
    check s.data == @[11.0, 22.0, 33.0, 44.0]
    let d = b - a
    check d.data == @[9.0, 18.0, 27.0, 36.0]
    let q = a * 2.5
    check q.data == @[2.5, 5.0, 7.5, 10.0]

  test "trace":
    let m = matFromRows(@[@[1.0, 2.0, 3.0], @[4.0, 5.0, 6.0], @[7.0, 8.0, 9.0]])
    check trace(m) == 15.0   # 1 + 5 + 9

  test "shape mismatch raises":
    expect(ValueError):
      discard matFromRows(@[@[1.0, 2.0], @[3.0]])      # ragged
    expect(ValueError):
      # (1×2) * (3×1): inner dims 2 ≠ 3
      discard matFromRows(@[@[1.0, 2.0]]) * matFromRows(@[@[1.0], @[2.0], @[3.0]])

suite "value (de)serialization":
  test "vector round-trip":
    let v = vec(0.5, 1.5, -2.5)
    let val = toValue(v)
    check val.kind == vkArray
    let (ok, back) = readVector(val)
    check ok
    check back == @[0.5, 1.5, -2.5]

  test "vector accepts vkInt entries":
    var arr = VArray(@[VInt(1), VFloat(2.5), VInt(3)])
    let (ok, v) = readVector(arr)
    check ok
    check v == @[1.0, 2.5, 3.0]

  test "vector rejects non-numeric entries":
    var arr = VArray(@[VFloat(1.0), VString("oops")])
    let (ok, _) = readVector(arr)
    check not ok

  test "matrix round-trip":
    let m = matFromRows(@[@[1.0, 2.0, 3.0], @[4.0, 5.0, 6.0]])
    let val = toValue(m)
    let (ok, back) = readMatrix(val)
    check ok
    check (back.rows, back.cols) == (2, 3)
    check back.data == m.data

  test "matrix rejects ragged rows":
    var bad = VArray(@[
      VArray(@[VFloat(1.0), VFloat(2.0)]),
      VArray(@[VFloat(3.0)]),
    ])
    let (ok, _) = readMatrix(bad)
    check not ok

  test "empty matrix is valid":
    let (ok, m) = readMatrix(VArray(@[]))
    check ok
    check m.rows == 0 and m.cols == 0

suite "vectors live inside docs":
  test "store and retrieve a vector embedded in a document":
    let dir = getTempDir() / "glen_test_linalg_doc"
    removeDir(dir)
    let db = newGlenDB(dir)
    var doc = VObject()
    doc["name"]      = VString("face-id-42")
    doc["embedding"] = toValue(vec(0.1, 0.2, 0.3, 0.4))
    db.put("faces", "f1", doc)

    let got = db.get("faces", "f1")
    check not got.isNil
    let (ok, e) = readVector(got["embedding"])
    check ok
    check e == @[0.1, 0.2, 0.3, 0.4]
    db.close()

  test "matrix in a document survives reopen":
    let dir = getTempDir() / "glen_test_linalg_reopen"
    removeDir(dir)
    block:
      let db = newGlenDB(dir)
      var doc = VObject()
      doc["weights"] = toValue(matFromRows(@[
        @[0.1, 0.2],
        @[0.3, 0.4],
        @[0.5, 0.6]]))
      db.put("models", "m1", doc)
      db.close()
    let db = newGlenDB(dir)
    let got = db.get("models", "m1")
    let (ok, m) = readMatrix(got["weights"])
    check ok
    check (m.rows, m.cols) == (3, 2)
    check m[1, 0] == 0.3
    check m[2, 1] == 0.6
    db.close()

  test "cosine similarity over stored embeddings":
    let dir = getTempDir() / "glen_test_linalg_cos"
    removeDir(dir)
    let db = newGlenDB(dir)
    var a = VObject(); a["e"] = toValue(vec(1.0, 0.0, 0.0))
    var b = VObject(); b["e"] = toValue(vec(0.0, 1.0, 0.0))
    var c = VObject(); c["e"] = toValue(vec(1.0, 1.0, 0.0))
    db.put("vecs", "a", a)
    db.put("vecs", "b", b)
    db.put("vecs", "c", c)
    let q = vec(1.0, 0.0, 0.0)
    var best = ""
    var bestSim = -2.0
    for (id, doc) in db.getAll("vecs"):
      let (ok, e) = readVector(doc["e"])
      check ok
      let s = cosine(q, e)
      if s > bestSim:
        bestSim = s; best = id
    check best == "a"
    db.close()
