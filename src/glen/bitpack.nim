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
#
# Reader uses a 64-bit register (`cache`) holding the next up-to-64 bits in
# MSB-aligned form: bit 63 is the next bit to consume. `cacheBits` tracks
# how many of those bits are still valid.
#
# Refill pulls the next byte from `buf`, ORs it into the empty low bits of
# `cache`, and bumps `cacheBits` by 8. We refill only when needed by the
# caller-requested width — the common case for `readBit`/`readBitsU64(<=8)`
# is a single shift+mask with no refill cost.
#
# Original semantics preserved: same MSB-first bit ordering, same EOF
# behaviour (`readBit`/`readBitsU64` raise IOError past `bitLimit`).

type BitReader* = object
  buf*: seq[byte]
  bytePos*:   int       # next byte to refill from
  cache*:     uint64    # holds the next `cacheBits` bits, MSB-aligned
  cacheBits*: int       # 0..64
  bitLimit*:  int       # total bits in this stream
  bitsRead*:  int       # how many we've already consumed

proc newBitReader*(buf: seq[byte]; bitLen: int): BitReader =
  result = BitReader(buf: buf, bytePos: 0, cache: 0'u64, cacheBits: 0,
                     bitLimit: bitLen, bitsRead: 0)

proc bitOffset*(r: BitReader): int {.inline.} = r.bitsRead

proc fillCache(r: var BitReader) {.inline.} =
  ## Pull whole bytes into the cache until either the cache is full (>=56
  ## bits, leaving room for 8 more) or the buffer is exhausted.
  while r.cacheBits <= 56 and r.bytePos < r.buf.len:
    r.cache = r.cache or (uint64(r.buf[r.bytePos]) shl (56 - r.cacheBits))
    r.cacheBits += 8
    inc r.bytePos

proc readBit*(r: var BitReader): bool =
  if r.bitsRead >= r.bitLimit:
    raise newException(IOError, "bit reader past EOF")
  if r.cacheBits == 0: r.fillCache()
  let bit = (r.cache shr 63) and 1'u64
  r.cache = r.cache shl 1
  dec r.cacheBits
  inc r.bitsRead
  bit != 0'u64

proc takeFromCache(r: var BitReader; nBits: int): uint64 {.inline.} =
  ## Caller has verified `1 <= nBits <= r.cacheBits`. Reads the top nBits
  ## from the cache and consumes them. The `nBits == 64` case is special-
  ## cased because `x shl 64` is undefined in C (shift count masked to 6
  ## bits on x86, leaving stale high bits). After a take, the bottom of
  ## `cache` is zero — important so `fillCache` can OR new bytes in cleanly.
  if nBits == 64:
    result = r.cache
    r.cache = 0'u64
  else:
    result = r.cache shr (64 - nBits)
    r.cache = r.cache shl nBits
  r.cacheBits -= nBits
  r.bitsRead += nBits

proc readBitsU64*(r: var BitReader; nBits: int): uint64 =
  ## Read up to 64 bits in one shot, MSB-first. May span a refill: drains the
  ## cache, refills, then takes the remainder. Same EOF semantics as the old
  ## bit-by-bit reader.
  if nBits <= 0: return 0'u64
  doAssert nBits <= 64, "BitReader: readBitsU64 capped at 64 bits"
  if r.bitsRead + nBits > r.bitLimit:
    raise newException(IOError, "bit reader past EOF")
  if nBits <= r.cacheBits:
    return r.takeFromCache(nBits)
  # Two-phase: drain whatever's currently cached, refill, then take the rest.
  let first = r.cacheBits
  var hi: uint64 = 0
  if first > 0:
    hi = r.takeFromCache(first)
  let remaining = nBits - first
  r.fillCache()
  if remaining > r.cacheBits:
    raise newException(IOError, "bit reader past EOF")
  let lo = r.takeFromCache(remaining)
  if first == 0: return lo
  (hi shl remaining) or lo

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

when defined(gcc) or defined(clang):
  # gcc/clang builtins compile to lzcnt/tzcnt on x86-64 (BMI), clz/rbit+clz on
  # arm64. Both ~3-cycle hardware operations; the scalar fallback below is a
  # 32× slower bit-shift loop. The `0` guard is needed because both builtins
  # are undefined on zero input.
  proc builtinClzll(x: culonglong): cint {.importc: "__builtin_clzll", nodecl.}
  proc builtinCtzll(x: culonglong): cint {.importc: "__builtin_ctzll", nodecl.}

  proc countLeadingZeros64*(x: uint64): int {.inline.} =
    if x == 0: 64 else: int(builtinClzll(culonglong(x)))

  proc countTrailingZeros64*(x: uint64): int {.inline.} =
    if x == 0: 64 else: int(builtinCtzll(culonglong(x)))
else:
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

proc decodeXor*(r: var BitReader; xstate: var XorState): uint64

proc decodeXorRun*(r: var BitReader; xstate: var XorState;
                   nValues: int): seq[uint64] =
  ## Bulk-decode `nValues` consecutive XOR values into a buffer. Equivalent
  ## to calling `decodeXor` `nValues` times, but uses `countLeadingZeros64`
  ## on the cached bit register to detect runs of zero-XOR values (the
  ## dominant case for radar/raster fields with constant cells) and skip
  ## past them in O(1) per run instead of O(N) per individual readBit.
  ##
  ## A zero-XOR encoded as `0` (one bit) leaves `prevLeading`/`prevTrailing`
  ## unchanged, which matches the standalone `decodeXor` semantics, so the
  ## XorState passed in/out behaves identically across batched and
  ## per-call decoders.
  result = newSeq[uint64](nValues)
  var i = 0
  while i < nValues:
    if r.cacheBits == 0: r.fillCache()
    if r.cacheBits == 0:
      raise newException(IOError, "bit reader past EOF")
    if (r.cache shr 63) == 0'u64:
      # The next bit is 0 → next XOR is 0 → all bits up to the first 1-bit
      # are also zero-XOR markers. clz on the cache tells us how many.
      let availBits = r.cacheBits
      var nZeros: int
      if r.cache == 0'u64:
        nZeros = availBits
      else:
        let clz = countLeadingZeros64(r.cache)
        nZeros = if clz < availBits: clz else: availBits
      let take = min(nZeros, nValues - i)
      # Skip `take` bits in the cache. Result entries are already 0 from
      # newSeq, so zero-XOR positions need no further work.
      if take == 64:
        r.cache = 0'u64
      else:
        r.cache = r.cache shl take
      r.cacheBits -= take
      r.bitsRead += take
      i += take
    else:
      # Non-zero XOR; defer to the standard decoder for this one value.
      result[i] = decodeXor(r, xstate)
      inc i

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
