# Glen binary codec
# Simple tagged format: 1 byte tag, then length/value.
# Tags: 0 null,1 bool false,2 bool true,3 int,4 float64,5 string,6 bytes,
#       7 array,8 object,9 id,10 object-with-key-dict

import std/[streams, tables, algorithm]
import glen/types
import glen/config
import glen/errors

const TAG_NULL = 0'u8
const TAG_BOOL_FALSE = 1'u8
const TAG_BOOL_TRUE = 2'u8
const TAG_INT = 3'u8
const TAG_FLOAT = 4'u8
const TAG_STRING = 5'u8
const TAG_BYTES = 6'u8
const TAG_ARRAY = 7'u8
const TAG_OBJECT = 8'u8
const TAG_ID = 9'u8
const TAG_OBJECT_DICT* = 10'u8
const TAG_STRING_DICT* = 11'u8

# ---- Key dictionary (snapshot v4 +) ----
#
# A KeyDict pairs an `idToKey` array (decoder lookup) with a `keyToId` table
# (encoder lookup). When encoding a body for inclusion in a snapshot that
# carries a dict, the encoder emits TAG_OBJECT_DICT instead of TAG_OBJECT;
# each field's key is a varuint marker:
#   marker low bit == 1 → dict-id  = marker shr 1
#   marker low bit == 0 → inline   ; length = marker shr 1, then bytes
# The decoder dispatches on the tag and resolves dict refs through the dict
# bound for the current decode (thread-local).
#
# WAL bodies and replication wire payloads NEVER carry dict refs because
# they have no shared dict scope — they always use plain TAG_OBJECT. Only
# snapshot bodies can carry TAG_OBJECT_DICT.

type
  ValueDict* = ref object
    idToVal*: seq[string]
    valToId*: Table[string, uint32]

  KeyDict* = ref object
    idToKey*: seq[string]
    keyToId*: Table[string, uint32]
    # Optional per-field-name value dictionary. When present, the encoder
    # emits TAG_STRING_DICT(id) for string values whose (field, value) pair
    # has a dict id; the decoder reverses it. Empty / nil means "no value
    # dictionarisation for this snapshot".
    valueDictByField*: Table[string, ValueDict]

proc newValueDict*(values: openArray[string]): ValueDict =
  result = ValueDict(idToVal: @values, valToId: initTable[string, uint32]())
  for i, v in values: result.valToId[v] = uint32(i)

proc newKeyDict*(keys: openArray[string] = []): KeyDict =
  ## Build a KeyDict from a list of keys (stable order: idToKey[i] = keys[i]).
  result = KeyDict(
    idToKey: @keys,
    keyToId: initTable[string, uint32](),
    valueDictByField: initTable[string, ValueDict]()
  )
  for i, k in keys: result.keyToId[k] = uint32(i)

## Maximum string or bytes length allowed during decode to protect memory usage.
var cfg {.threadvar.}: GlenConfig
proc ensureCfg() =
  if cfg.maxStringOrBytes == 0:
    cfg = loadConfig()

# Thread-local active dict for the current encode / decode call. Set by
# encodeWithDict / decodeWithDict and torn down (set to nil) on exit. Plain
# encode / decode see nil here and produce / accept TAG_OBJECT only.
var activeEncodeDict {.threadvar.}: KeyDict
var activeDecodeDict {.threadvar.}: KeyDict

var encStream {.threadvar.}: StringStream

proc writeVarUint(s: Stream; x: uint64) =
  var v = x
  while true:
    var b = uint8(v and 0x7F)
    v = v shr 7
    if v != 0:
      b = b or 0x80'u8
      s.write(b)
    else:
      s.write(b)
      break

proc readVarUint(s: Stream): uint64 =
  var shift: uint32 = 0
  var iterations = 0
  while true:
    let b = s.readUint8()
    result = result or (uint64(b and 0x7F) shl shift)
    if (b and 0x80) == 0: break
    shift += 7
    inc iterations
    if iterations > 10: # >70 bits would overflow typical sizes we use
      raise newException(ValueError, "varuint too long")

proc encodeValue(ms: Stream; v: Value) =
  case v.kind
  of vkNull: ms.write(TAG_NULL)
  of vkBool: ms.write(if v.b: TAG_BOOL_TRUE else: TAG_BOOL_FALSE)
  of vkInt:
    ms.write(TAG_INT)
    # zigzag for small negatives
    let zz = (uint64(v.i) shl 1) xor uint64(v.i shr 63)
    writeVarUint(ms, zz)
  of vkFloat:
    ms.write(TAG_FLOAT); ms.write(v.f)
  of vkString:
    ms.write(TAG_STRING); writeVarUint(ms, uint64(v.s.len)); ms.write(v.s)
  of vkBytes:
    ms.write(TAG_BYTES); writeVarUint(ms, uint64(v.bytes.len)); if v.bytes.len > 0: ms.writeData(addr v.bytes[0], v.bytes.len)
  of vkArray:
    ms.write(TAG_ARRAY); writeVarUint(ms, uint64(v.arr.len)); for it in v.arr: encodeValue(ms, it)
  of vkObject:
    if activeEncodeDict.isNil:
      ms.write(TAG_OBJECT); writeVarUint(ms, uint64(v.obj.len))
      for k, vv in v.obj:
        writeVarUint(ms, uint64(k.len)); ms.write(k); encodeValue(ms, vv)
    else:
      # Dict-aware encoding. Per-field varuint marker for the key:
      #   low bit = 1 → dict-id   = marker shr 1
      #   low bit = 0 → inline    ; length = marker shr 1, then bytes
      # Plus: for string values whose (field, value) pair has a value-dict
      # entry, emit TAG_STRING_DICT(id) instead of TAG_STRING(len, bytes).
      ms.write(TAG_OBJECT_DICT); writeVarUint(ms, uint64(v.obj.len))
      for k, vv in v.obj:
        let id = activeEncodeDict.keyToId.getOrDefault(k, high(uint32))
        if id != high(uint32):
          writeVarUint(ms, (uint64(id) shl 1) or 1'u64)
        else:
          writeVarUint(ms, uint64(k.len) shl 1); ms.write(k)
        var emittedDict = false
        if vv.kind == vkString and k in activeEncodeDict.valueDictByField:
          let vd = activeEncodeDict.valueDictByField[k]
          if not vd.isNil:
            let vid = vd.valToId.getOrDefault(vv.s, high(uint32))
            if vid != high(uint32):
              ms.write(TAG_STRING_DICT)
              writeVarUint(ms, uint64(vid))
              emittedDict = true
        if not emittedDict:
          encodeValue(ms, vv)
  of vkId:
    ms.write(TAG_ID)
    writeVarUint(ms, uint64(v.id.collection.len)); ms.write(v.id.collection)
    writeVarUint(ms, uint64(v.id.docId.len)); ms.write(v.id.docId)
    ms.write(v.id.version)

## Encode a `Value` to Glen's compact binary format. No dict — keys inline.
proc encode*(v: Value): string =
  ensureCfg()
  if encStream.isNil:
    encStream = newStringStream()
  else:
    encStream.setPosition(0)
    encStream.data.setLen(0)
  let prev = activeEncodeDict
  activeEncodeDict = nil
  try:
    encodeValue(encStream, v)
  finally:
    activeEncodeDict = prev
  result = encStream.data

## Encode `v` using `dict` to compress repeated object keys. Produces output
## that can ONLY be read by `decodeWithDict` with the same dict; never put
## the result on the wire or in a WAL — it's only for snapshot bodies.
proc encodeWithDict*(v: Value; dict: KeyDict): string =
  ensureCfg()
  if encStream.isNil:
    encStream = newStringStream()
  else:
    encStream.setPosition(0)
    encStream.data.setLen(0)
  let prev = activeEncodeDict
  activeEncodeDict = dict
  try:
    encodeValue(encStream, v)
  finally:
    activeEncodeDict = prev
  result = encStream.data

var decStream {.threadvar.}: StringStream

proc decodeValue(ms: Stream): Value =
  if ms.atEnd: raiseCodec("Unexpected EOF")
  let tag = ms.readUint8()
  case tag
  of TAG_NULL: return VNull()
  of TAG_BOOL_FALSE: return VBool(false)
  of TAG_BOOL_TRUE: return VBool(true)
  of TAG_INT:
    let zz = readVarUint(ms)
    let i = int64(zz shr 1) xor -int64(zz and 1)
    return VInt(i)
  of TAG_FLOAT: return VFloat(ms.readFloat64())
  of TAG_STRING:
    let L = int(readVarUint(ms));
    if L < 0 or L > cfg.maxStringOrBytes: raiseCodec("string too large")
    return VString(ms.readStr(L))
  of TAG_BYTES:
    let L = int(readVarUint(ms));
    if L < 0 or L > cfg.maxStringOrBytes: raiseCodec("bytes too large")
    var buf = newSeq[byte](L)
    if L > 0: discard ms.readData(addr buf[0], L)
    return VBytes(buf)
  of TAG_ARRAY:
    let n = int(readVarUint(ms))
    if n < 0 or n > cfg.maxArrayLen: raiseCodec("array too large")
    var items: seq[Value] = newSeq[Value](n)
    for i in 0..<n: items[i] = decodeValue(ms)
    return VArray(items)
  of TAG_OBJECT:
    let n = int(readVarUint(ms))
    if n < 0 or n > cfg.maxObjectFields: raiseCodec("object too large")
    var o = Value(kind: vkObject, obj: initTable[string, Value](if n <= 0: 4 else: n * 2))
    for i in 0..<n:
      let klen = int(readVarUint(ms))
      if klen < 0 or klen > cfg.maxStringOrBytes: raiseCodec("key too large")
      let k = ms.readStr(klen)
      o.obj[k] = decodeValue(ms)
    return o
  of TAG_OBJECT_DICT:
    if activeDecodeDict.isNil:
      raiseCodec("TAG_OBJECT_DICT requires decodeWithDict (no dict bound)")
    let n = int(readVarUint(ms))
    if n < 0 or n > cfg.maxObjectFields: raiseCodec("object too large")
    var o = Value(kind: vkObject, obj: initTable[string, Value](if n <= 0: 4 else: n * 2))
    for i in 0..<n:
      let marker = readVarUint(ms)
      var k: string
      if (marker and 1'u64) != 0:
        let id = int(marker shr 1)
        if id < 0 or id >= activeDecodeDict.idToKey.len:
          raiseCodec("dict-id " & $id & " out of range")
        k = activeDecodeDict.idToKey[id]
      else:
        let klen = int(marker shr 1)
        if klen < 0 or klen > cfg.maxStringOrBytes: raiseCodec("key too large")
        k = ms.readStr(klen)
      # Peek the value's tag. TAG_STRING_DICT is special: the field name
      # gates the lookup. Anything else delegates to decodeValue.
      let savedPos = ms.getPosition()
      let valTag = ms.readUint8()
      if valTag == TAG_STRING_DICT:
        if k notin activeDecodeDict.valueDictByField:
          raiseCodec("TAG_STRING_DICT for field '" & k & "' has no value dict")
        let vd = activeDecodeDict.valueDictByField[k]
        let valId = int(readVarUint(ms))
        if valId < 0 or valId >= vd.idToVal.len:
          raiseCodec("string-dict id out of range for '" & k & "'")
        o.obj[k] = VString(vd.idToVal[valId])
      else:
        ms.setPosition(savedPos)
        o.obj[k] = decodeValue(ms)
    return o
  of TAG_ID:
    let clen = int(readVarUint(ms));
    if clen < 0 or clen > cfg.maxStringOrBytes: raiseCodec("collection too large")
    let collection = ms.readStr(clen)
    let dlen = int(readVarUint(ms));
    if dlen < 0 or dlen > cfg.maxStringOrBytes: raiseCodec("docId too large")
    let docId = ms.readStr(dlen)
    let ver = ms.readUint64()
    return VId(collection, docId, ver)
  of TAG_STRING_DICT:
    raiseCodec("TAG_STRING_DICT outside TAG_OBJECT_DICT context")
  else:
    raiseCodec("Unknown tag: " & $tag)

## Decode Glen's compact binary format into a `Value`.
## Raises `CodecError` for malformed or too-large inputs. Raises on
## TAG_OBJECT_DICT — use `decodeWithDict` for snapshot v4 bodies.
proc decode*(data: string): Value =
  ensureCfg()
  if decStream.isNil:
    decStream = newStringStream(data)
  else:
    decStream.setPosition(0)
    decStream.data = data
  let prev = activeDecodeDict
  activeDecodeDict = nil
  try:
    result = decodeValue(decStream)
  finally:
    activeDecodeDict = prev

## Decode a snapshot-v4 body where TAG_OBJECT_DICT field markers reference
## entries in `dict.idToKey`. Falls back to plain TAG_OBJECT for any object
## that was encoded inline (mixed-key support).
proc decodeWithDict*(data: string; dict: KeyDict): Value =
  ensureCfg()
  if decStream.isNil:
    decStream = newStringStream(data)
  else:
    decStream.setPosition(0)
    decStream.data = data
  let prev = activeDecodeDict
  activeDecodeDict = dict
  try:
    result = decodeValue(decStream)
  finally:
    activeDecodeDict = prev

# ---- Helpers used by snapshot v4 builder ----

iterator collectKeys*(v: Value): string =
  ## Recursively yield every object-field key in `v`, including nested objects
  ## and objects inside arrays. Caller dedups + counts.
  var stack: seq[Value] = @[v]
  while stack.len > 0:
    let cur = stack.pop()
    if cur.isNil: continue
    case cur.kind
    of vkObject:
      for k, vv in cur.obj:
        yield k
        stack.add(vv)
    of vkArray:
      for it in cur.arr: stack.add(it)
    else: discard

proc buildKeyDict*(docs: Table[string, Value]; threshold: int): KeyDict =
  ## Walk every doc, count key occurrences, keep keys with count ≥ `threshold`.
  ## Returns a stable-order dict (sorted by descending frequency, ties broken
  ## by lex order — keeps the most-used keys at the lowest dict ids so they
  ## get the shortest varuints).
  var counts = initTable[string, int]()
  for _, v in docs:
    for k in collectKeys(v):
      counts[k] = counts.getOrDefault(k, 0) + 1
  var keep: seq[(string, int)] = @[]
  for k, c in counts:
    if c >= threshold: keep.add((k, c))
  # Sort: highest count first, lex-asc on ties.
  keep.sort(proc (a, b: (string, int)): int =
    if a[1] != b[1]: cmp(b[1], a[1]) else: cmp(a[0], b[0]))
  var keys: seq[string] = newSeqOfCap[string](keep.len)
  for (k, _) in keep: keys.add(k)
  newKeyDict(keys)

iterator collectFieldStringPairs(v: Value): (string, string) =
  ## Yield every (parentFieldName, stringValue) pair under `v`. Recurses into
  ## nested objects but does NOT descend into arrays — top-level arrays of
  ## strings have no field key, and per-element field-keying isn't useful.
  var stack: seq[Value] = @[v]
  while stack.len > 0:
    let cur = stack.pop()
    if cur.isNil: continue
    if cur.kind == vkObject:
      for k, vv in cur.obj:
        if vv.isNil: continue
        if vv.kind == vkString:
          yield (k, vv.s)
        elif vv.kind == vkObject:
          stack.add(vv)
        elif vv.kind == vkArray:
          # arrays of objects: recurse into each element
          for item in vv.arr:
            if not item.isNil and item.kind == vkObject:
              stack.add(item)

proc populateValueDicts*(dict: KeyDict; docs: Table[string, Value];
                        threshold: int) =
  ## Add per-field value dictionaries to `dict`. For each field name,
  ## count distinct string values; if a value occurs ≥ threshold times, add
  ## it to that field's value dict. Fields whose values never cross the
  ## threshold get no entry in `valueDictByField`.
  if threshold <= 0: return
  var perField = initTable[string, Table[string, int]]()
  for _, v in docs:
    for (fk, sv) in collectFieldStringPairs(v):
      if fk notin perField: perField[fk] = initTable[string, int]()
      perField[fk][sv] = perField[fk].getOrDefault(sv, 0) + 1
  for fk, valCounts in perField:
    var keep: seq[(string, int)] = @[]
    for v, c in valCounts:
      if c >= threshold: keep.add((v, c))
    if keep.len == 0: continue
    keep.sort(proc (a, b: (string, int)): int =
      if a[1] != b[1]: cmp(b[1], a[1]) else: cmp(a[0], b[0]))
    var values: seq[string] = newSeqOfCap[string](keep.len)
    for (s, _) in keep: values.add(s)
    dict.valueDictByField[fk] = newValueDict(values)
