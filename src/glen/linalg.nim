# Glen linear algebra — basic Vector and Matrix types with arithmetic and
# Value (de)serialization. No indexing; this is purely a data-type layer so
# that vectors and matrices can ride along inside documents.
#
# Design choices:
#   * Vector = seq[float64]. Plain, idiomatic, zero ceremony.
#   * Matrix is row-major flat (one allocation, cache-friendly for matmul).
#   * Operations return fresh values; in-place variants are spelled `*InPlace`
#     for hot loops.
#   * Document storage shape:
#       Vector → VArray([VFloat(x), ...])
#       Matrix → VArray([VArray([VFloat(...)]) per row])
#     vkInt is also accepted on read (treated as float) for ergonomics.

import std/[math, strutils]
import glen/types

type
  Vector* = seq[float64]

  Matrix* = object
    rows*: int
    cols*: int
    data*: seq[float64]   # row-major, len == rows * cols

# --------- Vector constructors ---------

proc vec*(xs: varargs[float64]): Vector {.inline.} =
  result = @xs

proc zerosV*(n: int): Vector =
  result = newSeq[float64](n)

proc onesV*(n: int): Vector =
  result = newSeq[float64](n)
  for i in 0 ..< n: result[i] = 1.0

# --------- Matrix constructors ---------

proc newMatrix*(rows, cols: int): Matrix =
  ## Zero-initialised (rows × cols) matrix.
  doAssert rows >= 0 and cols >= 0
  Matrix(rows: rows, cols: cols, data: newSeq[float64](rows * cols))

proc matFromRows*(rows: openArray[seq[float64]]): Matrix =
  ## Build a matrix from a sequence of equal-length rows.
  if rows.len == 0: return Matrix(rows: 0, cols: 0, data: @[])
  let cols = rows[0].len
  for r in rows:
    if r.len != cols:
      raise newException(ValueError, "matFromRows: ragged rows")
  result = newMatrix(rows.len, cols)
  for i, r in rows:
    for j in 0 ..< cols:
      result.data[i * cols + j] = r[j]

proc matFromFlat*(rows, cols: int; data: openArray[float64]): Matrix =
  if data.len != rows * cols:
    raise newException(ValueError, "matFromFlat: data.len != rows*cols")
  result = newMatrix(rows, cols)
  for i in 0 ..< data.len: result.data[i] = data[i]

proc zerosM*(rows, cols: int): Matrix = newMatrix(rows, cols)

proc onesM*(rows, cols: int): Matrix =
  result = newMatrix(rows, cols)
  for i in 0 ..< result.data.len: result.data[i] = 1.0

proc eye*(n: int): Matrix =
  ## n × n identity.
  result = newMatrix(n, n)
  for i in 0 ..< n: result.data[i * n + i] = 1.0

# --------- Matrix indexing ---------

proc `[]`*(m: Matrix; r, c: int): float64 {.inline.} =
  m.data[r * m.cols + c]

proc `[]=`*(m: var Matrix; r, c: int; v: float64) {.inline.} =
  m.data[r * m.cols + c] = v

proc shape*(m: Matrix): (int, int) {.inline.} = (m.rows, m.cols)

# --------- Vector operations ---------

template assertSameLen(a, b: Vector; op: string) =
  if a.len != b.len:
    raise newException(ValueError, op & ": length mismatch (" & $a.len & " vs " & $b.len & ")")

proc add*(a, b: Vector): Vector =
  assertSameLen(a, b, "vector add")
  result = newSeq[float64](a.len)
  for i in 0 ..< a.len: result[i] = a[i] + b[i]

proc sub*(a, b: Vector): Vector =
  assertSameLen(a, b, "vector sub")
  result = newSeq[float64](a.len)
  for i in 0 ..< a.len: result[i] = a[i] - b[i]

proc scale*(a: Vector; s: float64): Vector =
  result = newSeq[float64](a.len)
  for i in 0 ..< a.len: result[i] = a[i] * s

proc hadamard*(a, b: Vector): Vector =
  ## Elementwise product.
  assertSameLen(a, b, "vector hadamard")
  result = newSeq[float64](a.len)
  for i in 0 ..< a.len: result[i] = a[i] * b[i]

proc dot*(a, b: Vector): float64 =
  assertSameLen(a, b, "vector dot")
  result = 0.0
  for i in 0 ..< a.len: result += a[i] * b[i]

proc norm*(a: Vector): float64 =
  ## L2 norm.
  var s = 0.0
  for x in a: s += x * x
  sqrt(s)

proc normalize*(a: Vector): Vector =
  let n = norm(a)
  if n == 0.0: return a
  result = newSeq[float64](a.len)
  for i in 0 ..< a.len: result[i] = a[i] / n

proc euclidean*(a, b: Vector): float64 =
  ## L2 distance.
  assertSameLen(a, b, "euclidean")
  var s = 0.0
  for i in 0 ..< a.len:
    let d = a[i] - b[i]
    s += d * d
  sqrt(s)

proc cosine*(a, b: Vector): float64 =
  ## Cosine similarity in [-1, 1]. 0 if either vector is zero.
  assertSameLen(a, b, "cosine")
  let na = norm(a); let nb = norm(b)
  if na == 0.0 or nb == 0.0: return 0.0
  dot(a, b) / (na * nb)

# In-place variants for hot loops

proc addInPlace*(a: var Vector; b: Vector) =
  assertSameLen(a, b, "vector addInPlace")
  for i in 0 ..< a.len: a[i] += b[i]

proc scaleInPlace*(a: var Vector; s: float64) =
  for i in 0 ..< a.len: a[i] *= s

# --------- Matrix operations ---------

template assertSameShape(a, b: Matrix; op: string) =
  if a.rows != b.rows or a.cols != b.cols:
    raise newException(ValueError,
      op & ": shape mismatch (" & $a.rows & "×" & $a.cols &
      " vs " & $b.rows & "×" & $b.cols & ")")

proc add*(a, b: Matrix): Matrix =
  assertSameShape(a, b, "matrix add")
  result = newMatrix(a.rows, a.cols)
  for i in 0 ..< a.data.len: result.data[i] = a.data[i] + b.data[i]

proc sub*(a, b: Matrix): Matrix =
  assertSameShape(a, b, "matrix sub")
  result = newMatrix(a.rows, a.cols)
  for i in 0 ..< a.data.len: result.data[i] = a.data[i] - b.data[i]

proc scale*(a: Matrix; s: float64): Matrix =
  result = newMatrix(a.rows, a.cols)
  for i in 0 ..< a.data.len: result.data[i] = a.data[i] * s

proc transpose*(a: Matrix): Matrix =
  result = newMatrix(a.cols, a.rows)
  for i in 0 ..< a.rows:
    for j in 0 ..< a.cols:
      result.data[j * a.rows + i] = a.data[i * a.cols + j]

proc matmul*(a, b: Matrix): Matrix =
  ## (a.rows × a.cols) · (b.rows × b.cols) → (a.rows × b.cols).
  ## Naive O(n³); cache-friendly enough for modest sizes (~1000³ is the soft
  ## ceiling before you'd want BLAS).
  if a.cols != b.rows:
    raise newException(ValueError,
      "matmul: inner dim mismatch (" & $a.cols & " vs " & $b.rows & ")")
  result = newMatrix(a.rows, b.cols)
  # Loop order ikj keeps b's row in cache as the innermost axis.
  for i in 0 ..< a.rows:
    let aRow = i * a.cols
    let rRow = i * b.cols
    for k in 0 ..< a.cols:
      let aik = a.data[aRow + k]
      if aik == 0.0: continue
      let bRow = k * b.cols
      for j in 0 ..< b.cols:
        result.data[rRow + j] += aik * b.data[bRow + j]

proc matvec*(a: Matrix; v: Vector): Vector =
  ## Matrix · vector.
  if a.cols != v.len:
    raise newException(ValueError,
      "matvec: dim mismatch (" & $a.cols & " vs " & $v.len & ")")
  result = newSeq[float64](a.rows)
  for i in 0 ..< a.rows:
    var acc = 0.0
    let off = i * a.cols
    for j in 0 ..< a.cols:
      acc += a.data[off + j] * v[j]
    result[i] = acc

proc trace*(a: Matrix): float64 =
  ## Sum of diagonal. Defined for any square matrix.
  if a.rows != a.cols:
    raise newException(ValueError, "trace: non-square")
  result = 0.0
  for i in 0 ..< a.rows: result += a.data[i * a.cols + i]

# --------- Operator sugar ---------

proc `+`*(a, b: Vector): Vector {.inline.} = add(a, b)
proc `-`*(a, b: Vector): Vector {.inline.} = sub(a, b)
proc `*`*(a: Vector; s: float64): Vector {.inline.} = scale(a, s)
proc `*`*(s: float64; a: Vector): Vector {.inline.} = scale(a, s)

proc `+`*(a, b: Matrix): Matrix {.inline.} = add(a, b)
proc `-`*(a, b: Matrix): Matrix {.inline.} = sub(a, b)
proc `*`*(a: Matrix; s: float64): Matrix {.inline.} = scale(a, s)
proc `*`*(s: float64; a: Matrix): Matrix {.inline.} = scale(a, s)
proc `*`*(a, b: Matrix): Matrix {.inline.} = matmul(a, b)
proc `*`*(a: Matrix; v: Vector): Vector {.inline.} = matvec(a, v)

# --------- Pretty printing ---------

proc `$`*(m: Matrix): string =
  var rows: seq[string] = @[]
  for i in 0 ..< m.rows:
    var cols: seq[string] = @[]
    for j in 0 ..< m.cols:
      cols.add($m.data[i * m.cols + j])
    rows.add("[" & cols.join(", ") & "]")
  "Matrix(" & $m.rows & "×" & $m.cols & ", [" & rows.join(", ") & "])"

# --------- Value (de)serialization ---------

proc readFloatV(v: Value): (bool, float64) {.inline.} =
  if v.isNil: return (false, 0.0)
  case v.kind
  of vkFloat: (true, v.f)
  of vkInt:   (true, float64(v.i))
  else:       (false, 0.0)

proc toValue*(v: Vector): Value =
  ## Encode as VArray([VFloat(x), ...]).
  var items: seq[Value] = @[]
  items.setLen(v.len)
  for i, x in v: items[i] = VFloat(x)
  VArray(items)

proc toValue*(m: Matrix): Value =
  ## Encode as VArray of VArray rows of VFloat.
  var rows: seq[Value] = @[]
  rows.setLen(m.rows)
  for i in 0 ..< m.rows:
    var row: seq[Value] = @[]
    row.setLen(m.cols)
    for j in 0 ..< m.cols:
      row[j] = VFloat(m.data[i * m.cols + j])
    rows[i] = VArray(row)
  VArray(rows)

proc readVector*(v: Value): (bool, Vector) =
  ## Returns (false, _) if the shape is wrong (not an array, or contains a
  ## non-numeric entry).
  if v.isNil or v.kind != vkArray: return (false, @[])
  var out2: Vector = newSeq[float64](v.arr.len)
  for i, item in v.arr:
    let (ok, f) = readFloatV(item)
    if not ok: return (false, @[])
    out2[i] = f
  (true, out2)

proc readMatrix*(v: Value): (bool, Matrix) =
  ## Accepts a VArray of VArray rows; rows must be equal length. Empty matrix
  ## (0×0) is a valid input via VArray([]).
  if v.isNil or v.kind != vkArray: return (false, Matrix())
  if v.arr.len == 0: return (true, Matrix(rows: 0, cols: 0, data: @[]))
  if v.arr[0].isNil or v.arr[0].kind != vkArray: return (false, Matrix())
  let cols = v.arr[0].arr.len
  var m = newMatrix(v.arr.len, cols)
  for i, rowVal in v.arr:
    if rowVal.isNil or rowVal.kind != vkArray or rowVal.arr.len != cols:
      return (false, Matrix())
    for j, cell in rowVal.arr:
      let (ok, f) = readFloatV(cell)
      if not ok: return (false, Matrix())
      m.data[i * cols + j] = f
  (true, m)
