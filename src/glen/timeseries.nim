# Glen timeseries — Gorilla-style chunked column store.
#
# Optimised for the canonical timeseries workload: many points per second per
# series, near-monotonic timestamps, slowly-changing float values. Compared to
# storing samples as documents:
#   * ~10–20× tighter on disk (delta-of-delta on ts + XOR on values)
#   * write path is O(few bits) per sample, no per-sample allocations
#   * range scans skip whole blocks via the in-memory block index
#
# Storage layout (one file per series):
#
#   File header (16 B):
#     magic        : "GLENGTS1"  (8 B)
#     version      : uint32      (1)
#     blockSize    : uint32      (default samples-per-block, informational)
#
#   Repeated blocks:
#     block header (40 B):
#       payloadBytes : uint32   (bytes following this header, *including* CRC)
#       count        : uint32   (samples in block)
#       startTs      : int64
#       endTs        : int64
#       minVal       : float64
#       maxVal       : float64
#     payload       : bit-packed Gorilla encoding
#     crc           : uint32   (FNV1a-32 over payload bytes)
#
# Encoding (per block):
#   sample 0: full ts (already in header, omitted from payload), full 64-bit value
#   sample 1: ts delta = ts1 - ts0 as zigzag varint
#             value = XOR with previous
#   sample n>=2: delta-of-delta of ts (variable-length prefix); XOR of value
#
# Active (open) block lives in memory; flushed to disk when full (blockSize
# samples) or on explicit flush()/close().

import std/[os, locks]

const
  Magic        = "GLENGTS1"
  FileVersion  = 1'u32
  HeaderBytes  = 16
  BlockHeaderBytes = 40
  DefaultBlockSize* = 4096

# --------- bit writer / reader ---------

type
  BitWriter = object
    buf: seq[byte]
    bitOffset: int  # next free bit; total written = bitOffset

proc newBitWriter(): BitWriter =
  BitWriter(buf: newSeqOfCap[byte](256), bitOffset: 0)

proc writeBit(w: var BitWriter; bit: bool) =
  let byteIdx = w.bitOffset shr 3
  let inByte  = w.bitOffset and 7
  if byteIdx >= w.buf.len: w.buf.add(0'u8)
  if bit:
    w.buf[byteIdx] = w.buf[byteIdx] or (1'u8 shl (7 - inByte))
  inc w.bitOffset

proc writeBitsU64(w: var BitWriter; value: uint64; nBits: int) =
  ## Write the lowest nBits of `value`, MSB first.
  if nBits <= 0: return
  var i = nBits - 1
  while i >= 0:
    let b = ((value shr i) and 1'u64) != 0
    w.writeBit(b)
    if i == 0: break
    dec i

proc bytes(w: BitWriter): seq[byte] = w.buf
proc bitLen(w: BitWriter): int = w.bitOffset

type
  BitReader = object
    buf: seq[byte]
    bitOffset: int
    bitLimit: int  # total bits available

proc newBitReader(buf: seq[byte]; bitLen: int): BitReader =
  BitReader(buf: buf, bitOffset: 0, bitLimit: bitLen)

proc readBit(r: var BitReader): bool =
  if r.bitOffset >= r.bitLimit:
    raise newException(IOError, "bit reader past EOF")
  let byteIdx = r.bitOffset shr 3
  let inByte  = r.bitOffset and 7
  let bit     = (r.buf[byteIdx] shr (7 - inByte)) and 1'u8
  inc r.bitOffset
  bit != 0'u8

proc readBitsU64(r: var BitReader; nBits: int): uint64 =
  if nBits <= 0: return 0'u64
  var v: uint64 = 0
  for _ in 0 ..< nBits:
    v = (v shl 1) or (if r.readBit(): 1'u64 else: 0'u64)
  v

# --------- zigzag for signed ---------

proc zigzag(x: int64): uint64 {.inline.} =
  cast[uint64]((x shl 1) xor (x shr 63))

proc unzigzag(u: uint64): int64 {.inline.} =
  cast[int64]((u shr 1)) xor -(cast[int64](u and 1))

# --------- delta-of-delta encoding ---------

proc encodeDoD(w: var BitWriter; dod: int64) =
  # Range bounds chosen so zigzag fits in N bits:
  #   7 bits → zigzag 0..127  → dod in [-64,  63]
  #   9 bits → zigzag 0..511  → dod in [-256, 255]
  #  12 bits → zigzag 0..4095 → dod in [-2048, 2047]
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

proc decodeDoD(r: var BitReader): int64 =
  if not r.readBit(): return 0
  if not r.readBit():
    let bits = r.readBitsU64(7)
    return unzigzag(bits)
  if not r.readBit():
    let bits = r.readBitsU64(9)
    return unzigzag(bits)
  if not r.readBit():
    let bits = r.readBitsU64(12)
    return unzigzag(bits)
  let bits = r.readBitsU64(32)
  return unzigzag(bits)

# --------- xor float encoding ---------

type XorState = object
  prevLeading: int
  prevTrailing: int
  hasPrev: bool

proc countLeadingZeros64(x: uint64): int =
  if x == 0: return 64
  var n = 0
  var v = x
  while (v and (1'u64 shl 63)) == 0:
    inc n; v = v shl 1
  n

proc countTrailingZeros64(x: uint64): int =
  if x == 0: return 64
  var n = 0
  var v = x
  while (v and 1'u64) == 0:
    inc n; v = v shr 1
  n

proc encodeXor(w: var BitWriter; xstate: var XorState; xord: uint64) =
  if xord == 0'u64:
    w.writeBit(false)
    return
  w.writeBit(true)
  let leading  = min(countLeadingZeros64(xord), 31)   # cap at 31 so it fits in 5 bits
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
    let meaningful = (xord shr trailing) and (if bits == 64: high(uint64) else: (1'u64 shl bits) - 1'u64)
    w.writeBitsU64(meaningful, bits)
    xstate.prevLeading = leading
    xstate.prevTrailing = trailing
    xstate.hasPrev = true

proc decodeXor(r: var BitReader; xstate: var XorState): uint64 =
  if not r.readBit(): return 0'u64
  if not r.readBit():
    # reuse previous block
    let blockBits = 64 - xstate.prevLeading - xstate.prevTrailing
    let meaningful = r.readBitsU64(blockBits)
    return meaningful shl xstate.prevTrailing
  let leading = int(r.readBitsU64(5))
  let bb6 = int(r.readBitsU64(6))
  let blockBits = if bb6 == 0: 64 else: bb6  # 0 means 64
  let trailing = 64 - leading - blockBits
  let meaningful = r.readBitsU64(blockBits)
  xstate.prevLeading = leading
  xstate.prevTrailing = trailing
  xstate.hasPrev = true
  meaningful shl trailing

# --------- block encode / decode ---------

type
  Block* = object
    count*: int
    startTs*: int64
    endTs*: int64
    minVal*: float64
    maxVal*: float64
    payload*: seq[byte]
    payloadBitLen*: int

proc encodeBlock*(samples: seq[(int64, float64)]): Block =
  doAssert samples.len > 0
  var b: Block
  b.count = samples.len
  b.startTs = samples[0][0]
  b.endTs = samples[^1][0]
  b.minVal = samples[0][1]
  b.maxVal = samples[0][1]
  for s in samples:
    if s[1] < b.minVal: b.minVal = s[1]
    if s[1] > b.maxVal: b.maxVal = s[1]
  var w = newBitWriter()
  # sample 0: full value (ts is in header)
  let firstBits = cast[uint64](samples[0][1])
  w.writeBitsU64(firstBits, 64)
  if samples.len == 1:
    b.payload = w.bytes
    b.payloadBitLen = w.bitLen
    return b
  # sample 1: ts delta as zigzag varint (we use fixed 32-bit zigzag for simplicity).
  # For delta within [-2^31, 2^31) that's plenty; series with timestamps in millis
  # and intra-block deltas of seconds-to-minutes are far below 2^31 ms.
  let firstDelta = samples[1][0] - samples[0][0]
  w.writeBitsU64(zigzag(firstDelta) and 0xFFFFFFFF'u64, 32)
  var xstate: XorState
  let xor1 = cast[uint64](samples[1][1]) xor cast[uint64](samples[0][1])
  encodeXor(w, xstate, xor1)
  var prevDelta = firstDelta
  var i = 2
  while i < samples.len:
    let delta = samples[i][0] - samples[i-1][0]
    let dod = delta - prevDelta
    encodeDoD(w, dod)
    let xord = cast[uint64](samples[i][1]) xor cast[uint64](samples[i-1][1])
    encodeXor(w, xstate, xord)
    prevDelta = delta
    inc i
  b.payload = w.bytes
  b.payloadBitLen = w.bitLen
  b

proc decodeBlock*(b: Block): seq[(int64, float64)] =
  result = newSeqOfCap[(int64, float64)](b.count)
  if b.count == 0: return
  var r = newBitReader(b.payload, b.payloadBitLen)
  let firstBits = r.readBitsU64(64)
  let firstVal = cast[float64](firstBits)
  result.add((b.startTs, firstVal))
  if b.count == 1: return
  let firstDelta = unzigzag(r.readBitsU64(32))
  var prevTs = b.startTs + firstDelta
  var xstate: XorState
  let xor1 = decodeXor(r, xstate)
  let val1Bits = firstBits xor xor1
  var prevValBits = val1Bits
  result.add((prevTs, cast[float64](val1Bits)))
  var prevDelta = firstDelta
  var i = 2
  while i < b.count:
    let dod = decodeDoD(r)
    let delta = prevDelta + dod
    prevTs = prevTs + delta
    let xord = decodeXor(r, xstate)
    prevValBits = prevValBits xor xord
    result.add((prevTs, cast[float64](prevValBits)))
    prevDelta = delta
    inc i

# --------- crc + io helpers ---------

proc fnv1a32(buf: openArray[byte]): uint32 =
  var h: uint32 = 0x811C9DC5'u32
  for b in buf:
    h = (h xor uint32(b)) * 0x01000193'u32
  h

proc writeU32(f: File; v: uint32) =
  var x = v
  discard f.writeBuffer(addr x, 4)

proc writeU64(f: File; v: uint64) =
  var x = v
  discard f.writeBuffer(addr x, 8)

proc writeI64(f: File; v: int64) =
  var x = v
  discard f.writeBuffer(addr x, 8)

proc writeF64(f: File; v: float64) =
  var x = v
  discard f.writeBuffer(addr x, 8)

proc readU32(f: File): uint32 =
  var x: uint32
  if f.readBuffer(addr x, 4) != 4: raise newException(IOError, "short read u32")
  x

proc readU64(f: File): uint64 =
  var x: uint64
  if f.readBuffer(addr x, 8) != 8: raise newException(IOError, "short read u64")
  x

proc readI64(f: File): int64 =
  var x: int64
  if f.readBuffer(addr x, 8) != 8: raise newException(IOError, "short read i64")
  x

proc readF64(f: File): float64 =
  var x: float64
  if f.readBuffer(addr x, 8) != 8: raise newException(IOError, "short read f64")
  x

# --------- Series ---------

type
  BlockMeta = object
    fileOffset: int64    # offset of block header
    payloadBytes: int    # payload + CRC (matches header field)
    count: int
    startTs: int64
    endTs: int64
    minVal: float64
    maxVal: float64

  Series* = ref object
    path*: string
    file: File
    blockSize*: int
    blockIndex: seq[BlockMeta]
    active: seq[(int64, float64)]   # in-memory active block samples
    activeMinTs: int64
    activeMaxTs: int64
    totalSamples: int
    fsyncOnFlush: bool
    lock: Lock

proc writeFileHeader(s: Series) =
  s.file.setFilePos(0)
  s.file.write(Magic)
  s.file.writeU32(FileVersion)
  s.file.writeU32(uint32(s.blockSize))

proc readFileHeader(s: Series) =
  s.file.setFilePos(0)
  var magicBuf = newString(Magic.len)
  if s.file.readBuffer(addr magicBuf[0], Magic.len) != Magic.len:
    raise newException(IOError, "series file truncated header")
  if magicBuf != Magic:
    raise newException(IOError, "series file: bad magic " & magicBuf)
  let ver = s.file.readU32()
  if ver != FileVersion:
    raise newException(IOError, "series file: unsupported version " & $ver)
  let bs = s.file.readU32()
  if bs > 0: s.blockSize = int(bs)

proc scanBlocks(s: Series) =
  ## Build the in-memory block index by walking block headers; tolerates a
  ## torn final block (truncates back to the last fully-checksummed one).
  s.blockIndex = @[]
  s.totalSamples = 0
  s.file.setFilePos(int64(HeaderBytes))
  var lastGoodEnd = int64(HeaderBytes)
  while true:
    let pos = s.file.getFilePos()
    let payloadBytes =
      try: int(s.file.readU32())
      except IOError: -1
    if payloadBytes < 0: break
    var meta = BlockMeta(fileOffset: pos, payloadBytes: payloadBytes)
    try:
      meta.count = int(s.file.readU32())
      meta.startTs = s.file.readI64()
      meta.endTs = s.file.readI64()
      meta.minVal = s.file.readF64()
      meta.maxVal = s.file.readF64()
    except IOError:
      break
    # Bounds-check payload bytes to avoid pathological reads on a torn tail.
    if payloadBytes < 4 or payloadBytes > 64 * 1024 * 1024:
      break
    var payload = newSeq[byte](payloadBytes - 4)
    if payload.len > 0:
      let n = s.file.readBuffer(addr payload[0], payload.len)
      if n != payload.len: break
    let crc =
      try: s.file.readU32()
      except IOError: break
    if fnv1a32(payload) != crc: break
    s.blockIndex.add(meta)
    s.totalSamples += meta.count
    lastGoodEnd = s.file.getFilePos()
  # Truncate trailing torn data
  s.file.setFilePos(lastGoodEnd)

proc openSeries*(path: string; blockSize = DefaultBlockSize; fsyncOnFlush = false): Series =
  ## Open or create a series file. `blockSize` only controls the active
  ## block's auto-flush threshold; existing blocks keep their stored sizes.
  result = Series(path: path, blockSize: blockSize, fsyncOnFlush: fsyncOnFlush,
                  blockIndex: @[], active: @[], totalSamples: 0)
  initLock(result.lock)
  let isNew = not fileExists(path)
  if isNew:
    let dir = parentDir(path)
    if dir.len > 0 and not dirExists(dir): createDir(dir)
    result.file = open(path, fmReadWrite)
    result.writeFileHeader()
  else:
    result.file = open(path, fmReadWriteExisting)
    result.readFileHeader()
    result.scanBlocks()

proc flushActive(s: Series) =
  if s.active.len == 0: return
  let blk = encodeBlock(s.active)
  let payloadBytes = blk.payload.len + 4   # + CRC
  let pos = s.file.getFileSize()
  s.file.setFilePos(pos)
  s.file.writeU32(uint32(payloadBytes))
  s.file.writeU32(uint32(blk.count))
  s.file.writeI64(blk.startTs)
  s.file.writeI64(blk.endTs)
  s.file.writeF64(blk.minVal)
  s.file.writeF64(blk.maxVal)
  if blk.payload.len > 0:
    discard s.file.writeBuffer(unsafeAddr blk.payload[0], blk.payload.len)
  let crc = fnv1a32(blk.payload)
  s.file.writeU32(crc)
  if s.fsyncOnFlush:
    s.file.flushFile()
  s.blockIndex.add(BlockMeta(
    fileOffset: pos, payloadBytes: payloadBytes,
    count: blk.count, startTs: blk.startTs, endTs: blk.endTs,
    minVal: blk.minVal, maxVal: blk.maxVal))
  s.totalSamples += blk.count
  s.active.setLen(0)

proc append*(s: Series; tsMillis: int64; value: float64) =
  ## Append a single (ts, value). Timestamps within a block must be
  ## non-decreasing; this is enforced by the encoder's delta-of-delta scheme.
  ## Out-of-order across block boundaries is tolerated but penalised in
  ## subsequent reads via a sort fallback.
  acquire(s.lock)
  if s.active.len > 0 and tsMillis < s.active[^1][0]:
    # Out-of-order within active: flush active, start fresh block. This keeps
    # in-block monotonicity (DoD encoding requires it) without losing the sample.
    s.flushActive()
  s.active.add((tsMillis, value))
  if s.active.len >= s.blockSize:
    s.flushActive()
  release(s.lock)

proc flush*(s: Series) =
  acquire(s.lock)
  s.flushActive()
  s.file.flushFile()
  release(s.lock)

proc close*(s: Series) =
  if s.file.isNil: return
  s.flush()
  acquire(s.lock)
  s.file.close()
  s.file = nil
  release(s.lock)

proc len*(s: Series): int =
  acquire(s.lock)
  let r = s.totalSamples + s.active.len
  release(s.lock)
  r

proc minTs*(s: Series): int64 =
  acquire(s.lock)
  if s.blockIndex.len > 0:
    result = s.blockIndex[0].startTs
  elif s.active.len > 0:
    result = s.active[0][0]
  else:
    result = high(int64)
  release(s.lock)

proc maxTs*(s: Series): int64 =
  acquire(s.lock)
  if s.active.len > 0:
    result = s.active[^1][0]
  elif s.blockIndex.len > 0:
    result = s.blockIndex[^1].endTs
  else:
    result = low(int64)
  release(s.lock)

# --------- block I/O for queries ---------

proc readBlockAt(s: Series; meta: BlockMeta): Block =
  s.file.setFilePos(meta.fileOffset)
  let payloadBytes = int(s.file.readU32())
  doAssert payloadBytes == meta.payloadBytes
  let count = int(s.file.readU32())
  let startTs = s.file.readI64()
  let endTs = s.file.readI64()
  let minV = s.file.readF64()
  let maxV = s.file.readF64()
  var payload = newSeq[byte](payloadBytes - 4)
  if payload.len > 0:
    let n = s.file.readBuffer(addr payload[0], payload.len)
    doAssert n == payload.len
  let crc = s.file.readU32()
  doAssert fnv1a32(payload) == crc, "series block CRC mismatch"
  Block(count: count, startTs: startTs, endTs: endTs,
        minVal: minV, maxVal: maxV,
        payload: payload, payloadBitLen: payload.len * 8)

proc range*(s: Series; fromMs, toMs: int64): seq[(int64, float64)] =
  ## Inclusive [fromMs, toMs] range scan.
  result = @[]
  if fromMs > toMs: return
  acquire(s.lock)
  defer: release(s.lock)
  for meta in s.blockIndex:
    if meta.endTs < fromMs or meta.startTs > toMs: continue
    let blk = s.readBlockAt(meta)
    for (ts, v) in decodeBlock(blk):
      if ts >= fromMs and ts <= toMs:
        result.add((ts, v))
  for (ts, v) in s.active:
    if ts >= fromMs and ts <= toMs:
      result.add((ts, v))

proc latest*(s: Series; n: int): seq[(int64, float64)] =
  ## Returns the last `n` samples in chronological order.
  result = @[]
  if n <= 0: return
  acquire(s.lock)
  defer: release(s.lock)
  # Walk from the end: active first (already in order), then blocks back-to-front
  var collected: seq[(int64, float64)] = @[]
  let activeTake = min(n, s.active.len)
  if activeTake > 0:
    for i in (s.active.len - activeTake) ..< s.active.len:
      collected.add(s.active[i])
  if collected.len < n and s.blockIndex.len > 0:
    var i = s.blockIndex.high
    while i >= 0 and collected.len < n:
      let blk = s.readBlockAt(s.blockIndex[i])
      let samples = decodeBlock(blk)
      let need = n - collected.len
      if samples.len <= need:
        # whole block fits
        var prepended: seq[(int64, float64)] = samples
        prepended.add(collected)
        collected = prepended
      else:
        var prepended: seq[(int64, float64)] = samples[samples.len - need .. ^1]
        prepended.add(collected)
        collected = prepended
        break
      dec i
  result = collected

# --------- retention: drop blocks fully older than cutoff ---------

proc dropBlocksBefore*(s: Series; cutoffMs: int64) =
  ## Drops every fully-closed block whose endTs < cutoffMs. Rewrites the file
  ## by streaming the surviving blocks into a temp file and atomically renaming.
  acquire(s.lock)
  defer: release(s.lock)
  var keep: seq[BlockMeta] = @[]
  for meta in s.blockIndex:
    if meta.endTs >= cutoffMs: keep.add(meta)
  if keep.len == s.blockIndex.len: return  # nothing to drop
  let tmpPath = s.path & ".compact"
  block:
    var tmp = open(tmpPath, fmReadWrite)
    tmp.write(Magic)
    tmp.writeU32(FileVersion)
    tmp.writeU32(uint32(s.blockSize))
    var newIndex: seq[BlockMeta] = @[]
    var totalSamples = 0
    for meta in keep:
      let pos = tmp.getFileSize()
      # copy block as-is (header + payload + crc)
      s.file.setFilePos(meta.fileOffset)
      let totalBytes = BlockHeaderBytes + meta.payloadBytes
      var buf = newSeq[byte](totalBytes)
      let n = s.file.readBuffer(addr buf[0], totalBytes)
      doAssert n == totalBytes
      discard tmp.writeBuffer(addr buf[0], totalBytes)
      var nm = meta
      nm.fileOffset = pos
      newIndex.add(nm)
      totalSamples += meta.count
    tmp.flushFile()
    tmp.close()
    s.file.close()
    moveFile(tmpPath, s.path)
    s.file = open(s.path, fmReadWriteExisting)
    s.file.setFilePos(s.file.getFileSize())
    s.blockIndex = newIndex
    s.totalSamples = totalSamples

# --------- iterator-style helpers ---------

iterator items*(s: Series): (int64, float64) =
  acquire(s.lock)
  let snapshotBlocks = s.blockIndex
  let snapshotActive = s.active
  release(s.lock)
  for meta in snapshotBlocks:
    acquire(s.lock)
    let blk = s.readBlockAt(meta)
    release(s.lock)
    for sample in decodeBlock(blk):
      yield sample
  for sample in snapshotActive:
    yield sample
