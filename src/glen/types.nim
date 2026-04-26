# Glen core value types (Convex-like)

import std/[tables, hashes, strutils, options]

# Supported Value kinds

type
  ValueKind* = enum
    vkNull, vkBool, vkInt, vkFloat, vkString, vkBytes, vkArray, vkObject, vkId

  Id* = object
    collection*: string
    docId*: string            # Could be ULID/UUID like string
    version*: uint64          # Incremented per mutation for OCC/subscriptions

  Value* = ref object
    case kind*: ValueKind
    of vkNull: discard
    of vkBool: b*: bool
    of vkInt: i*: int64
    of vkFloat: f*: float64
    of vkString: s*: string
    of vkBytes: bytes*: seq[byte]
    of vkArray: arr*: seq[Value]
    of vkObject: obj*: Table[string, Value]
    of vkId: id*: Id

# Hybrid Logical Clock for multi-master ordering
type
  Hlc* = object
    wallMillis*: int64
    counter*: uint32
    nodeId*: string

proc hlcCompare*(a, b: Hlc): int {.noSideEffect.} =
  if a.wallMillis < b.wallMillis: return -1
  if a.wallMillis > b.wallMillis: return 1
  if a.counter < b.counter: return -1
  if a.counter > b.counter: return 1
  if a.nodeId < b.nodeId: return -1
  if a.nodeId > b.nodeId: return 1
  0
# Constructors
proc VNull*(): Value = Value(kind: vkNull)
proc VBool*(b: bool): Value = Value(kind: vkBool, b: b)
proc VInt*(i: int64): Value = Value(kind: vkInt, i: i)
proc VFloat*(f: float64): Value = Value(kind: vkFloat, f: f)
proc VString*(s: string): Value = Value(kind: vkString, s: s)
proc VBytes*(bytes: openArray[byte]): Value = Value(kind: vkBytes, bytes: @bytes)
proc VArray*(items: seq[Value]): Value = Value(kind: vkArray, arr: items)
proc VObject*(): Value = Value(kind: vkObject, obj: initTable[string, Value]())
proc VId*(collection, docId: string; version: uint64 = 0): Value = Value(kind: vkId, id: Id(collection: collection, docId: docId, version: version))

# Convenience for object building
proc `[]=`*(v: Value; key: string; val: Value) =
  if v.kind != vkObject: raise newException(ValueError, "Not an object value")
  v.obj[key] = val

proc `[]`*(v: Value; key: string): Value =
  # Safe bracket access: returns nil if not an object or key missing
  if v.isNil or v.kind != vkObject:
    return nil
  if key in v.obj:
    return v.obj[key]
  return nil

proc getStrict*(v: Value; key: string): Value =
  ## Strict accessor: raises if not an object or key missing
  if v.isNil or v.kind != vkObject:
    raise newException(ValueError, "Not an object value")
  if key notin v.obj:
    raise newException(ValueError, "Key not found: " & key)
  v.obj[key]

# Hashing for Id and Value (shallow for performance) used in cache keys
proc hash*(x: Id): Hash =
  result = result !& hash(x.collection)
  result = result !& hash(x.docId)
  result = result !& hash(x.version)
  result = !$result

proc hash*(v: Value): Hash =
  if v.isNil: return 0
  result = hash(ord(v.kind))
  case v.kind
  of vkNull: discard
  of vkBool: result = result !& hash(v.b)
  of vkInt: result = result !& hash(v.i)
  of vkFloat: result = result !& hash(cast[int64](v.f))
  of vkString: result = result !& hash(v.s)
  of vkBytes: result = result !& hash(v.bytes)
  of vkArray:
    for it in v.arr: result = result !& hash(it)
  of vkObject:
    # Order-independent fold so structurally-equal objects hash the same.
    var acc: Hash = 0
    for k, vv in v.obj:
      acc = acc xor (hash(k) !& hash(vv))
    result = result !& acc
  of vkId: result = result !& hash(v.id)
  result = !$result

# Equality (shallow for speed - deepEq helper if needed)
proc `==`*(a, b: Id): bool {.noSideEffect.} = a.collection == b.collection and a.docId == b.docId and a.version == b.version

proc `==`*(a, b: Value): bool {.noSideEffect.} =
  if a.isNil or b.isNil: return a.isNil and b.isNil
  if a.kind != b.kind: return false
  case a.kind
  of vkNull: true
  of vkBool: a.b == b.b
  of vkInt: a.i == b.i
  of vkFloat: a.f == b.f
  of vkString: a.s == b.s
  of vkBytes: a.bytes == b.bytes
  of vkArray: a.arr == b.arr # deep compare seq
  of vkObject: a.obj == b.obj
  of vkId: a.id == b.id

# Deep clone
proc clone*(v: Value): Value =
  case v.kind
  of vkNull: VNull()
  of vkBool: VBool(v.b)
  of vkInt: VInt(v.i)
  of vkFloat: VFloat(v.f)
  of vkString: VString(v.s)
  of vkBytes: VBytes(v.bytes)
  of vkArray:
    var items: seq[Value] = @[]
    items.setLen(v.arr.len)
    for i, it in v.arr: items[i] = clone(it)
    VArray(items)
  of vkObject:
    var o = VObject()
    for k, vv in v.obj: o.obj[k] = clone(vv)
    o
  of vkId: VId(v.id.collection, v.id.docId, v.id.version)

# Pretty representation
proc `$`*(id: Id): string = id.collection & ":" & id.docId & "@" & $id.version

proc `$`*(v: Value): string =
  if v.isNil: return "null"
  case v.kind
  of vkNull: "null"
  of vkBool: $(v.b)
  of vkInt: $(v.i)
  of vkFloat: $(v.f)
  of vkString: '"' & v.s & '"'
  of vkBytes: "<bytes " & $v.bytes.len & ">"
  of vkArray:
    var parts: seq[string] = @[]
    for it in v.arr: parts.add($it)
    "[" & parts.join(", ") & "]"
  of vkObject:
    var parts: seq[string] = @[]
    for k, vv in v.obj: parts.add(k & ": " & $vv)
    "{" & parts.join(", ") & "}"
  of vkId: "$" & $v.id

# -------- API ergonomics: safe accessors --------

proc isObject*(v: Value): bool = not v.isNil and v.kind == vkObject

proc hasKey*(v: Value; key: string): bool =
  if not v.isObject: return false
  key in v.obj

proc getOrNil*(v: Value; key: string): Value =
  if not v.isObject: return nil
  if key in v.obj: v.obj[key] else: nil

proc getOrDefault*(v: Value; key: string; defaultValue: Value): Value =
  let got = v.getOrNil(key)
  if got.isNil: defaultValue else: got

proc getOpt*(v: Value; key: string): Option[Value] =
  let got = v.getOrNil(key)
  if got.isNil: none(Value) else: some(got)

proc toBoolOpt*(v: Value): Option[bool] =
  if v.isNil or v.kind != vkBool: none(bool) else: some(v.b)

proc toIntOpt*(v: Value): Option[int64] =
  if v.isNil or v.kind != vkInt: none(int64) else: some(v.i)

proc toFloatOpt*(v: Value): Option[float64] =
  if v.isNil or v.kind != vkFloat: none(float64) else: some(v.f)

proc toStringOpt*(v: Value): Option[string] =
  if v.isNil or v.kind != vkString: none(string) else: some(v.s)

proc toBytesOpt*(v: Value): Option[seq[byte]] =
  if v.isNil or v.kind != vkBytes: none(seq[byte]) else: some(v.bytes)

proc toArrayOpt*(v: Value): Option[seq[Value]] =
  if v.isNil or v.kind != vkArray: none(seq[Value]) else: some(v.arr)

proc toObjectOpt*(v: Value): Option[Table[string, Value]] =
  if v.isNil or v.kind != vkObject: none(Table[string, Value]) else: some(v.obj)

proc toIdOpt*(v: Value): Option[Id] =
  if v.isNil or v.kind != vkId: none(Id) else: some(v.id)
