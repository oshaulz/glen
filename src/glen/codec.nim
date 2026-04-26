# Glen binary codec
# Simple tagged format: 1 byte tag, then length/value.
# Tags: 0 null,1 bool false,2 bool true,3 int,4 float64,5 string,6 bytes,7 array,8 object,9 id

import std/[streams, tables]
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

## Maximum string or bytes length allowed during decode to protect memory usage.
var cfg {.threadvar.}: GlenConfig
proc ensureCfg() =
  if cfg.maxStringOrBytes == 0:
    cfg = loadConfig()

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
    ms.write(TAG_OBJECT); writeVarUint(ms, uint64(v.obj.len))
    for k, vv in v.obj:
      writeVarUint(ms, uint64(k.len)); ms.write(k); encodeValue(ms, vv)
  of vkId:
    ms.write(TAG_ID)
    writeVarUint(ms, uint64(v.id.collection.len)); ms.write(v.id.collection)
    writeVarUint(ms, uint64(v.id.docId.len)); ms.write(v.id.docId)
    ms.write(v.id.version)

## Encode a `Value` to Glen's compact binary format.
proc encode*(v: Value): string =
  ensureCfg()
  if encStream.isNil:
    encStream = newStringStream()
  else:
    encStream.setPosition(0)
    encStream.data.setLen(0)
  encodeValue(encStream, v)
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
  of TAG_ID:
    let clen = int(readVarUint(ms));
    if clen < 0 or clen > cfg.maxStringOrBytes: raiseCodec("collection too large")
    let collection = ms.readStr(clen)
    let dlen = int(readVarUint(ms));
    if dlen < 0 or dlen > cfg.maxStringOrBytes: raiseCodec("docId too large")
    let docId = ms.readStr(dlen)
    let ver = ms.readUint64()
    return VId(collection, docId, ver)
  else:
    raiseCodec("Unknown tag: " & $tag)

## Decode Glen's compact binary format into a `Value`.
## Raises `CodecError` for malformed or too-large inputs.
proc decode*(data: string): Value =
  ensureCfg()
  if decStream.isNil:
    decStream = newStringStream(data)
  else:
    decStream.setPosition(0)
    decStream.data = data
  result = decodeValue(decStream)
