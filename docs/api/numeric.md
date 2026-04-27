# Numeric API — `glen/linalg`

`Vector` (1-D) and `Matrix` (row-major flat 2-D) value types with arithmetic
and `Value` round-trip helpers. Designed to ride inside ordinary documents —
no new `ValueKind`, no codec changes.

## Types

```nim
type
  Vector* = seq[float64]              # plain seq

  Matrix* = object
    rows*, cols*: int
    data*: seq[float64]               # row-major flat: data[row * cols + col]
```

Row-major flat layout makes `matmul` cache-friendly (ikj loop with skip-on-zero).

## Vector

### Constructors

```nim
let a = vec(1.0, 2.0, 3.0)
let z = zerosV(5)
let o = onesV(5)
```

### Operations

```nim
# Pure (return fresh values)
add(a, b)            # also: a + b
sub(a, b)            # also: a - b
scale(a, s)          # also: a * s, s * a
hadamard(a, b)       # elementwise product
dot(a, b): float64
norm(a): float64     # L2
normalize(a): Vector # zero vector returns unchanged
euclidean(a, b)
cosine(a, b)         # similarity in [-1, 1]; 0 if either is zero

# In-place (for hot loops)
addInPlace(a, b)     # a += b
scaleInPlace(a, s)   # a *= s
```

## Matrix

### Constructors

```nim
let m = matFromRows(@[@[1.0, 2.0], @[3.0, 4.0]])
let m2 = matFromFlat(rows = 2, cols = 2, data = @[1.0, 2.0, 3.0, 4.0])
let z = zerosM(rows, cols)
let o = onesM(rows, cols)
let id3 = eye(3)                        # 3×3 identity
let blank = newMatrix(rows, cols)        # zero-init
```

### Indexing

```nim
m[r, c]: float64
m[r, c] = v
shape(m): (int, int)                    # (rows, cols)
```

### Operations

```nim
# Pure
add(a, b)            # also: a + b
sub(a, b)            # also: a - b
scale(a, s)          # also: a * s, s * a
transpose(a)
matmul(a, b)         # also: a * b
matvec(a, v)         # also: a * v
trace(a): float64
```

`matmul` uses an ikj loop order with skip-on-zero — naive O(n³) but
cache-friendly. Soft ceiling around 1000³ before you'd want BLAS bindings.

### Operator sugar

```nim
let s = a + b
let d = a - b
let m = a * 2.0
let M2 = transpose(M) * M
let y = M * x        # matrix · vector
```

## Value (de)serialization

Vectors and matrices ride inside ordinary documents as nested `VFloat` arrays
— no new `ValueKind`, no codec changes. They round-trip through everything in
the document model (snapshots, WAL, replication, subscriptions) for free.

### Vector

```nim
toValue(v: Vector): Value
readVector(v: Value): (bool, Vector)   # accepts vkInt entries as float
```

```nim
var doc = VObject()
doc["embedding"] = toValue(vec(0.1, 0.2, 0.3, 0.4))
db.put("faces", "f1", doc)

let stored = db.get("faces", "f1")
let (ok, e) = readVector(stored["embedding"])
```

### Matrix

```nim
toValue(m: Matrix): Value
readMatrix(v: Value): (bool, Matrix)   # rejects ragged rows
```

```nim
let W = matFromRows(@[@[0.1, 0.2], @[0.3, 0.4]])
var doc = VObject(); doc["weights"] = toValue(W)
db.put("models", "m1", doc)

let (ok, m) = readMatrix(db.get("models", "m1")["weights"])
```

## Use cases

### Embedding storage + nearest-neighbour scan

```nim
import glen/db, glen/types, glen/linalg

# Store embeddings
for (id, vec) in myEmbeddings:
  var doc = VObject()
  doc["embedding"] = toValue(vec)
  db.put("vectors", id, doc)

# Linear-scan + cosine top-K (fine for thousands; for millions you'd want
# a real vector index — not yet shipped)
let q = vec(0.1, 0.2, 0.3, 0.4)
var best: seq[(string, float64)] = @[]
for (id, doc) in db.getBorrowedAllStream("vectors"):
  let (ok, e) = readVector(doc["embedding"])
  if ok: best.add((id, cosine(q, e)))
best.sort(proc (a, b: (string, float64)): int = cmp(b[1], a[1]))
for i in 0 ..< min(10, best.len): echo best[i]
```

### Storing model weights

```nim
# Save
var modelDoc = VObject()
modelDoc["W1"] = toValue(W1)             # Matrix
modelDoc["b1"] = toValue(bias1)          # Vector
modelDoc["W2"] = toValue(W2)
modelDoc["b2"] = toValue(bias2)
db.put("models", "checkpoint-42", modelDoc)

# Load
let m = db.get("models", "checkpoint-42")
let (_, W1) = readMatrix(m["W1"])
let (_, b1) = readVector(m["b1"])
```

### Inline math

```nim
proc forward(W: Matrix; b, x: Vector): Vector =
  let z = W * x + b                       # matvec + add
  result = newSeq[float64](z.len)
  for i, zi in z: result[i] = max(zi, 0.0) # ReLU
```

## What's not there

- **No SIMD** — pure Nim. Naive matmul peaks ~1 GFLOPs on a Mac M5; cuts off
  around 1000³ before you'd want hand-tuned BLAS.
- **No new `ValueKind`** — vectors are `VArray` of `VFloat`. ~9 bytes/element
  on disk via the existing codec; fine for moderate sizes. For millions of
  large vectors you'd want a packed `VBytes` representation; see
  [api/spatial.md#geomesh](spatial.md#geomesh) which does exactly that.
- **No element-wise broadcasting, axis ops, slicing** beyond `add`/`sub`/`mul`/`dot`.
- **No determinant / inverse / decompositions** beyond `trace`. Reach for an
  external lib for solving linear systems.
- **No collection-wide nearest-neighbour search at the `glen/linalg` level**
  — the per-doc Vector primitives are scalar. For approximate KNN over an
  embedding field across an entire collection, see `createVectorIndex` in
  [api/core.md](core.md#vector-index-hnsw) (HNSW with cosine / L2 / dot).

## See also

- [Architecture](../architecture.md)
- [api/spatial.md#geomesh](spatial.md#geomesh) — when you want a packed-bytes
  raster instead of a nested-array Matrix
- [Getting started](../getting-started.md)
