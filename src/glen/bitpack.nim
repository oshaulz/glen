# Glen bit-packing primitives.
#
# Shared by timeseries.nim (single-stream Gorilla TSDB) and tilestack.nim
# (per-cell Gorilla streams within radar/raster time stacks).
#
# Provides:
#   * BitWriter / BitReader — MSB-first big-endian bit packing
#   * zigzag / unzigzag — signed → unsigned mapping for compact varints
#   * encodeDoD / decodeDoD — delta-of-delta with 1/2/3/4-bit prefix codes
#   * encodeXor / decodeXor — Gorilla XOR encoding for IEEE-754 floats
#   * fnv1a32 — payload checksum

import std/math
export math   # for callers that want NaN, Inf etc

# --------- BitWriter ---------

type BitWriter* = object
  buf*: seq[byte]
  bitOffset*: int

proc newBitWriter*(): BitWriter =
  BitWriter(buf: newSeqOfCap[byte](256), bitOffset: 0)

proc writeBit*(w: var BitWriter; bit: bool) =
  let byteIdx = w.bitOffset shr 3
  let inByte  = w.bitOffset and 7
  if byteIdx >= w.buf.len: w.buf.add(0'u8)
  if bit:
    w.buf[byteIdx] = w.buf[byteIdx] or (1'u8 shl (7 - inByte))
  inc w.bitOffset

proc writeBitsU64*(w: var BitWriter; value: uint64; nBits: int) =
  ## Write the lowest nBits of `value`, MSB first.
  if nBits <= 0: return
  var i = nBits - 1
  while i >= 0:
    let b = ((value shr i) and 1'u64) != 0
    w.writeBit(b)
    if i == 0: break
    dec i

proc bytes*(w: BitWriter): seq[byte] = w.buf
proc bitLen*(w: BitWriter): int = w.bitOffset

# --------- BitReader ---------

type BitReader* = object
  buf*: seq[byte]
  bitOffset*: int
  bitLimit*: int

proc newBitReader*(buf: seq[byte]; bitLen: int): BitReader =
  BitReader(buf: buf, bitOffset: 0, bitLimit: bitLen)

proc readBit*(r: var BitReader): bool =
  if r.bitOffset >= r.bitLimit:
    raise newException(IOError, "bit reader past EOF")
  let byteIdx = r.bitOffset shr 3
  let inByte  = r.bitOffset and 7
  let bit     = (r.buf[byteIdx] shr (7 - inByte)) and 1'u8
  inc r.bitOffset
  bit != 0'u8

proc readBitsU64*(r: var BitReader; nBits: int): uint64 =
  if nBits <= 0: return 0'u64
  var v: uint64 = 0
  for _ in 0 ..< nBits:
    v = (v shl 1) or (if r.readBit(): 1'u64 else: 0'u64)
  v

# --------- zigzag ---------

proc zigzag*(x: int64): uint64 {.inline.} =
  cast[uint64]((x shl 1) xor (x shr 63))

proc unzigzag*(u: uint64): int64 {.inline.} =
  cast[int64]((u shr 1)) xor -(cast[int64](u and 1))

# --------- delta-of-delta ---------

proc encodeDoD*(w: var BitWriter; dod: int64) =
  # 7  bits → zigzag 0..127  → dod ∈ [-64,  63]
  # 9  bits → zigzag 0..511  → dod ∈ [-256, 255]
  # 12 bits → zigzag 0..4095 → dod ∈ [-2048, 2047]
  if dod == 0:
    w.writeBit(false)
    return
  if dod >= -64 and dod <= 63:
    w.writeBit(true); w.writeBit(false)
    w.writeBitsU64(zigzag(dod), 7)
    return
  if dod >= -256 and dod <= 255:
    w.writeBit(true); w.writeBit(true); w.writeBit(false)
    w.writeBitsU64(zigzag(dod), 9)
    return
  if dod >= -2048 and dod <= 2047:
    w.writeBit(true); w.writeBit(true); w.writeBit(true); w.writeBit(false)
    w.writeBitsU64(zigzag(dod), 12)
    return
  # '1111' + 32 bits raw zigzag (covers dod in [-2^31, 2^31-1])
  w.writeBit(true); w.writeBit(true); w.writeBit(true); w.writeBit(true)
  w.writeBitsU64(zigzag(dod) and 0xFFFFFFFF'u64, 32)

proc decodeDoD*(r: var BitReader): int64 =
  if not r.readBit(): return 0
  if not r.readBit():
    let bits = r.readBitsU64(7);  return unzigzag(bits)
  if not r.readBit():
    let bits = r.readBitsU64(9);  return unzigzag(bits)
  if not r.readBit():
    let bits = r.readBitsU64(12); return unzigzag(bits)
  let bits = r.readBitsU64(32);   return unzigzag(bits)

# --------- xor (Gorilla) ---------

type XorState* = object
  prevLeading*:  int
  prevTrailing*: int
  hasPrev*:      bool

proc countLeadingZeros64*(x: uint64): int =
  if x == 0: return 64
  var n = 0
  var v = x
  while (v and (1'u64 shl 63)) == 0:
    inc n; v = v shl 1
  n

proc countTrailingZeros64*(x: uint64): int =
  if x == 0: return 64
  var n = 0
  var v = x
  while (v and 1'u64) == 0:
    inc n; v = v shr 1
  n

proc encodeXor*(w: var BitWriter; xstate: var XorState; xord: uint64) =
  if xord == 0'u64:
    w.writeBit(false); return
  w.writeBit(true)
  let leading  = min(countLeadingZeros64(xord), 31)   # cap at 31, fits in 5 bits
  let trailing = countTrailingZeros64(xord)
  if xstate.hasPrev and leading >= xstate.prevLeading and trailing >= xstate.prevTrailing:
    # reuse previous block
    w.writeBit(false)
    let blockBits = 64 - xstate.prevLeading - xstate.prevTrailing
    let mask = if blockBits == 64: high(uint64) else: (1'u64 shl blockBits) - 1'u64
    let meaningful = (xord shr xstate.prevTrailing) and mask
    w.writeBitsU64(meaningful, blockBits)
  else:
    w.writeBit(true)
    w.writeBitsU64(uint64(leading), 5)
    let blockBits = 64 - leading - trailing
    let bb = if blockBits == 64: 0 else: blockBits   # 6 bits encodes 0..63; 64 wraps to 0
    w.writeBitsU64(uint64(bb), 6)
    let bits = if blockBits == 64: 64 else: blockBits
    let meaningful =
      (xord shr trailing) and
      (if bits == 64: high(uint64) else: (1'u64 shl bits) - 1'u64)
    w.writeBitsU64(meaningful, bits)
    xstate.prevLeading = leading
    xstate.prevTrailing = trailing
    xstate.hasPrev = true

proc decodeXor*(r: var BitReader; xstate: var XorState): uint64 =
  if not r.readBit(): return 0'u64
  if not r.readBit():
    let blockBits = 64 - xstate.prevLeading - xstate.prevTrailing
    let meaningful = r.readBitsU64(blockBits)
    return meaningful shl xstate.prevTrailing
  let leading = int(r.readBitsU64(5))
  let bb6 = int(r.readBitsU64(6))
  let blockBits = if bb6 == 0: 64 else: bb6
  let trailing = 64 - leading - blockBits
  let meaningful = r.readBitsU64(blockBits)
  xstate.prevLeading = leading
  xstate.prevTrailing = trailing
  xstate.hasPrev = true
  meaningful shl trailing

# --------- FNV-1a 32-bit checksum ---------

proc fnv1a32*(buf: openArray[byte]): uint32 =
  var h: uint32 = 0x811C9DC5'u32
  for b in buf:
    h = (h xor uint32(b)) * 0x01000193'u32
  h
