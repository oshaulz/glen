## %* — Value literal macro for Glen.
##
## Mirrors the shape of `std/json`'s `%*`, but produces a Glen `Value`. Lets
## you write
##
##   let u = %*{
##     "name": "Alice",
##     "age": 30,
##     "tags": ["admin", "ops"],
##     "addr": { "city": "NYC", "lon": -73.98, "lat": 40.75 }
##   }
##
## instead of the `VObject() / v["k"] = VString(..)` ladder. Identifiers and
## arbitrary expressions are wrapped via `toValue` overloads at runtime, so
## you can interpolate variables — `%*{ "name": userName }` works because
## `toValue(string): Value` is in scope.

import std/[macros, tables]
import glen/types
import glen/geo

# ---- Runtime overloads used by the macro for non-literal expressions ----

proc toValue*(v: Value): Value {.inline.} =
  if v.isNil: VNull() else: v

proc toValue*(s: string): Value {.inline.} = VString(s)
proc toValue*(b: bool): Value {.inline.} = VBool(b)
proc toValue*[T: SomeInteger](i: T): Value {.inline.} = VInt(int64(i))
proc toValue*[T: SomeFloat](f: T): Value {.inline.} = VFloat(float64(f))
proc toValue*(bs: seq[byte]): Value {.inline.} = VBytes(bs)

proc toValue*[T](items: openArray[T]): Value =
  var arr: seq[Value] = newSeqOfCap[Value](items.len)
  for it in items: arr.add(toValue(it))
  VArray(arr)

proc toValue*[T](items: seq[T]): Value =
  var arr: seq[Value] = newSeqOfCap[Value](items.len)
  for it in items: arr.add(toValue(it))
  VArray(arr)

proc toValue*(t: Table[string, Value]): Value =
  result = VObject()
  for k, v in t.pairs: result[k] = v

# ---- Macro implementation ----

proc buildValueExpr(node: NimNode): NimNode =
  ## Walks a literal-shaped AST and emits Glen Value construction calls.
  ## Falls back to `toValue(expr)` for anything we don't recognise as a
  ## structural literal.
  case node.kind
  of nnkTableConstr:
    # {"k": v, ...} → VObject() with assignments
    let objSym = genSym(nskVar, "vobj")
    var stmts = newStmtList()
    stmts.add(newVarStmt(objSym, newCall(bindSym"VObject")))
    for child in node:
      if child.kind != nnkExprColonExpr:
        error("%*: object entries must be `key: value`", child)
      let keyNode = child[0]
      var keyExpr: NimNode
      case keyNode.kind
      of nnkStrLit, nnkRStrLit, nnkTripleStrLit: keyExpr = keyNode
      of nnkIdent, nnkSym: keyExpr = newLit($keyNode)
      else:
        error("%*: object keys must be string literals or identifiers, got " & $keyNode.kind, keyNode)
      let valExpr = buildValueExpr(child[1])
      stmts.add(newAssignment(
        newTree(nnkBracketExpr, objSym, keyExpr),
        valExpr
      ))
    stmts.add(objSym)
    result = newBlockStmt(stmts)
  of nnkBracket:
    # [a, b, c] → VArray(@[a', b', c'])
    var elems = newTree(nnkPrefix, ident"@", newTree(nnkBracket))
    for child in node:
      elems[1].add(buildValueExpr(child))
    result = newCall(bindSym"VArray", elems)
  of nnkStrLit, nnkRStrLit, nnkTripleStrLit:
    result = newCall(bindSym"VString", node)
  of nnkIntLit..nnkUInt64Lit:
    result = newCall(bindSym"VInt", newCall(ident"int64", node))
  of nnkFloatLit..nnkFloat64Lit:
    result = newCall(bindSym"VFloat", node)
  of nnkNilLit:
    result = newCall(bindSym"VNull")
  of nnkIdent, nnkSym:
    let s = $node
    case s
    of "true":  result = newCall(bindSym"VBool", ident"true")
    of "false": result = newCall(bindSym"VBool", ident"false")
    of "nil":   result = newCall(bindSym"VNull")
    else:
      result = newCall(bindSym"toValue", node)
  else:
    # Arbitrary expression — defer to runtime toValue overloads.
    result = newCall(bindSym"toValue", node)

macro `%*`*(body: untyped): Value =
  ## Build a Glen `Value` from a JSON-like literal.
  ##
  ## Recognised forms:
  ##   * `{}` → empty VObject
  ##   * `{"k": v, ...}` → VObject (keys may be string literals or identifiers)
  ##   * `[a, b, c]` → VArray
  ##   * string / int / float / bool / nil literals → VString / VInt / VFloat / VBool / VNull
  ##   * any other expression → `toValue(expr)` at runtime
  result = buildValueExpr(body)

# ---- Vector literal — `%v[...]` ------------------------------------------
#
# Glen's HNSW index works on `seq[float32]` (and `openArray[float32]`).
# Building one from a literal of mixed-precision numbers is verbose:
#
#     let v = @[float32(1.0), float32(2.0), float32(3.0)]
#
# `%v[1.0, 2.0, 3.0]` does the same thing in one expression, accepting
# int / float / float32 literals interchangeably and converting at
# compile time when possible, runtime when the element is a non-literal.

proc vec32*[T](xs: openArray[T]): seq[float32] =
  ## Build a `seq[float32]` from a sequence of mixed-precision numbers.
  ## Useful for query vectors and small fixed embeddings without the
  ## per-element `float32(...)` casts:
  ##
  ##   db.findNearestVector(coll, idx, vec32([1.0, 2.0, 3.0]), k = 10)
  ##   db.findNearestVector(coll, idx, vec32 [1, 2.5, 3], k = 10)   # command form
  ##
  ## Accepts ints, floats, float32, float64 — anything `float32(x)` can
  ## convert. For runtime-built vectors prefer this over hand-written
  ## `@[float32(a), float32(b), ...]`. The name is `vec32` rather than
  ## `vec` because `glen/linalg` already exports a `vec` for its Vector
  ## (float64) type.
  result = newSeqOfCap[float32](xs.len)
  for x in xs: result.add(float32(x))

# ---- Polygon literal — `polygonLit [(lon, lat), ...]` --------------------
#
# Polygons are stored as `VArray([VArray([VFloat(x), VFloat(y)]), ...])`.
# Building one by hand is verbose; this macro accepts a bracket of
# (lon, lat) tuples and returns the right Value shape, ready for
# `db.put(coll, id, %*{ "shape": polygonLit [(lon1, lat1), ... ] })`
# or for direct passing to `findPointsInPolygon`.

macro polygonLit*(arr: untyped): Value =
  ## Build a polygon `Value` from a bracket of `(lon, lat)` tuples:
  ##
  ##   let region = polygonLit [
  ##     (-74.0, 40.7), (-73.9, 40.7), (-73.9, 40.8), (-74.0, 40.8)]
  ##
  ## At least three vertices are required (validated at the call site
  ## by the consumer; the macro itself accepts any non-empty list).
  if arr.kind != nnkBracket:
    error("polygonLit expects a bracket of (lon, lat) tuples", arr)
  var outer = newTree(nnkBracket)
  for vert in arr:
    case vert.kind
    of nnkPar, nnkTupleConstr:
      if vert.len != 2:
        error("polygonLit: each vertex must be a (lon, lat) pair", vert)
      var inner = newTree(nnkBracket)
      inner.add(newCall(bindSym"VFloat", newCall(ident"float64", vert[0])))
      inner.add(newCall(bindSym"VFloat", newCall(ident"float64", vert[1])))
      outer.add(newCall(bindSym"VArray",
        newTree(nnkPrefix, ident"@", inner)))
    else:
      error("polygonLit: each vertex must be a (lon, lat) pair, got " & $vert.kind, vert)
  result = newCall(bindSym"VArray", newTree(nnkPrefix, ident"@", outer))

proc readPolygon*(v: Value): Polygon =
  ## Decode a polygon `Value` (as produced by `polygonLit` or stored in
  ## a doc field) back into a `geo.Polygon` ready for `findPointsInPolygon`.
  ## Returns an empty polygon on shape mismatch — the geo procs reject
  ## `vertices.len < 3` themselves, so this stays infallible.
  if v.isNil or v.kind != vkArray: return Polygon(vertices: @[])
  var verts: seq[(float64, float64)] = @[]
  for vert in v.arr:
    if vert.isNil or vert.kind != vkArray or vert.arr.len < 2: continue
    let xv = vert.arr[0]
    let yv = vert.arr[1]
    var x, y: float64
    case xv.kind
    of vkFloat: x = xv.f
    of vkInt:   x = float64(xv.i)
    else: continue
    case yv.kind
    of vkFloat: y = yv.f
    of vkInt:   y = float64(yv.i)
    else: continue
    verts.add((x, y))
  Polygon(vertices: verts)
