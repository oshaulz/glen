# Glen tile time-stack — chunked, Gorilla-compressed storage for a 2-D
# raster that evolves through time. Designed for radar reflectivity sweeps,
# satellite-derived rasters, gridded weather model output, animated heatmaps
# of LLM-emitted probability fields — anything shaped like
# `(time, row, col, channel) → float64` over a fixed geographic bbox.
#
# ============================================================================
# When to use this vs frame-per-doc + GeoMesh
# ============================================================================
#
# Frame-per-doc + GeoMesh (recommended starting point):
#   * Fastest "show me the latest scan" / "animate the last hour"
#   * Simplest API — just `db.put` and `db.rangeBy("byTs", ...)`
#   * Each frame is independent, atomic, easy to subscribe / replicate
#   * Storage per frame = raw size (no inter-frame compression)
#
# TileStack (this module):
#   * Wins decisively on:
#       1. Disk cost for long-lived archives — Gorilla XOR + DoD across time
#          gives 3-10× compression on smoothly-varying values like radar dBZ
#          or temperature
#       2. "One cell over a long window" queries — load a single tile chunk,
#          decode just the cells you need, return the per-timestamp series.
#          Frame-per-doc can't do this well because every cell-history query
#          fetches every frame's full mesh.
#   * Loses to frame-per-doc on:
#       1. "Latest frame" — must gather all tiles' newest chunks and reassemble
#       2. Append throughput — splits each frame into K writes (one per tile)
#       3. Per-frame metadata (model name, asof, etc.) — TileStack has one
#          manifest for the whole stack; frame-per-doc can carry per-doc fields
#
# Mix-and-match patterns:
#   * Live radar + archive: frame-per-doc for the last 24h (hot, frequent
#     "latest" queries), background process drains older frames into a
#     TileStack for the long tail.
#   * Per-station tiles: one TileStack per radar station. Use a polygon index
#     in the document collection to map (lon, lat) → station, then open the
#     station's stack for point-history queries.
#
# ============================================================================
# Worked use cases
# ============================================================================
#
# 1) Radar reflectivity, NEXRAD-style 1km grid, 1 channel:
#      let stack = newTileStack(
#        "./radar/KMUX",
#        bbox       = bbox(-122.7, 36.7, -120.7, 38.7),
#        rows       = 200, cols = 200, channels = 1,
#        tileSize   = 64, chunkSize = 128,
#        labels     = @["dbz"])
#      # ingest one frame every 5 minutes
#      stack.appendFrame(scanTimeMs, mesh)
#      # show me the storm at 12:34
#      let (ok, frame) = stack.readFrame(scanTimeMs)
#      # what was reflectivity right over my house for the last hour?
#      let history = stack.readPointHistory(myLon, myLat,
#                                           nowMs - 3_600_000, nowMs)
#
# 2) Multi-class probability raster from an LLM (rain, snow, clear), one
#    forecast every 10 minutes:
#      let stack = newTileStack(dir,
#        bbox = ..., rows = 64, cols = 64, channels = 3,
#        labels = @["rain", "snow", "clear"], tileSize = 32, chunkSize = 144)
#      # per-cell trend of "rain probability" over the past day
#      let series = stack.readPointHistory(lon, lat, dayAgo, now,
#                                          channel = stack.channelIndex("rain"))
#
# 3) Monthly satellite NDVI mosaic (slowly-changing values → very high
#    compression). Set chunkSize larger because frames are sparse:
#      let stack = newTileStack(dir, bbox = ..., rows = 1024, cols = 1024,
#        channels = 1, tileSize = 128, chunkSize = 60)
#
# ============================================================================
# Storage layout
# ============================================================================
#
#   <dir>/manifest.tsm           manifest (text, line-per-field; bbox, dims, etc.)
#   <dir>/tile_<r>_<c>.tts       one append-only file per tile
#
# Each tile file:
#   file header (16 B):
#     magic "GLENTTS1" (8 B)
#     version           uint32   currently 1
#     reserved          uint32
#   repeated chunks:
#     chunk header (40 B):
#       payloadBytes    uint32   bytes after this header, including CRC
#       frameCount      uint32   ≤ chunkSize
#       startTs         int64
#       endTs           int64
#       minVal          float64  (over all cells & channels in the chunk)
#       maxVal          float64
#     payload (bit-packed):
#       — timestamps: ts0 already in header; ts1 zigzag-32; ts2.. DoD
#       — for each cell index i in 0..tileCells-1:
#           for each channel ch in 0..channels-1:
#             v0 raw 64 bits; v1.. Gorilla XOR (fresh XorState per stream)
#     crc32 (FNV-1a)
#
# Active (in-memory) chunks live per tile until they reach chunkSize frames or
# `flush()` is called. `appendFrame` decomposes a full GeoMesh into per-tile
# slices and pushes each slice into its tile's active buffer.

import std/[os, locks, strutils, tables, algorithm]
import glen/geo
import glen/geomesh
import glen/bitpack
import glen/simple8b
import glen/chunkcache

const
  TileFileMagic    = "GLENTTS1"
  TileFileVersion  = 2'u32   # v1 = Gorilla-only; v2 = adds constant-chunk RLE
  TileFileHdrBytes = 16
  ChunkHdrBytes    = 40
  ManifestFileName = "manifest.tsm"
  DefaultTileSize* = 64
  DefaultChunkSize* = 128
  # Bits 30 and 31 of the payloadBytes field encode flags. Legacy v1 chunks
  # always had both bits clear (cap was 256 MiB = 0x10000000) so old files
  # transparently decode as ckGorilla / DoD timestamps.
  ChunkKindBit     = 0x80000000'u32   # bit 31: 1 = ckConstant
  TsCodecBit       = 0x40000000'u32   # bit 30: 1 = timestamps via Simple-8b
  PayloadBytesMask = 0x3FFFFFFF'u32

type
  ChunkKind = enum
    ckGorilla = 0   # full per-cell-channel XOR streams
    ckConstant = 1  # whole chunk, every cell-channel-frame == minVal == maxVal

  TsCodec = enum
    tsDoD = 0       # legacy delta-of-delta with 1/2/3/4-bit prefix codes
    tsSimple8b = 1  # zigzag → Simple-8b on per-frame DoDs

# --------- types ---------

type
  TileStackManifest = object
    bbox*:      BBox
    rows*:      int       # full grid
    cols*:      int
    channels*:  int
    tileSize*:  int
    chunkSize*: int
    labels*:    seq[string]   # length 0 or == channels

  ChunkMeta = object
    fileOffset:   int64
    payloadBytes: int
    frameCount:   int
    startTs:      int64
    endTs:        int64
    minVal:       float64
    maxVal:       float64
    kind:         ChunkKind
    tsCodec:      TsCodec

  TileFile = ref object
    path:    string
    file:    File
    chunks:  seq[ChunkMeta]
    # Active (unflushed) frames. Flat row-major channel-interleaved per frame.
    activeTs:    seq[int64]
    activeData:  seq[seq[float64]]   # one entry per active frame
    # Decoded-chunk LRU. Repeated readPointHistory / readFrameRange / readFrame
    # walk the same chunks; caching their decoded form turns a chunk decode
    # (potentially MB of bit-packed data) into a table hit.
    decodedCache: ChunkCache[int, (seq[int64], seq[seq[float64]])]

  TileStack* = ref object
    dir*:       string
    manifest*:  TileStackManifest
    tiles:      Table[(int, int), TileFile]
    lock:       Lock

# Cells per tile — varies for edge tiles.
proc tileExtent(s: TileStack; tr, tc: int): (int, int) =
  let r0 = tr * s.manifest.tileSize
  let c0 = tc * s.manifest.tileSize
  let r1 = min(r0 + s.manifest.tileSize, s.manifest.rows)
  let c1 = min(c0 + s.manifest.tileSize, s.manifest.cols)
  (r1 - r0, c1 - c0)

proc tileGridDims*(s: TileStack): (int, int) =
  ((s.manifest.rows + s.manifest.tileSize - 1) div s.manifest.tileSize,
   (s.manifest.cols + s.manifest.tileSize - 1) div s.manifest.tileSize)

# --------- manifest ---------

proc serializeManifest(m: TileStackManifest): string =
  ## Plain key=value lines; one labels line with comma-separated values.
  var lines: seq[string] = @[]
  lines.add("bbox=" & $m.bbox.minX & "," & $m.bbox.minY & "," &
                       $m.bbox.maxX & "," & $m.bbox.maxY)
  lines.add("rows=" & $m.rows)
  lines.add("cols=" & $m.cols)
  lines.add("channels=" & $m.channels)
  lines.add("tileSize=" & $m.tileSize)
  lines.add("chunkSize=" & $m.chunkSize)
  if m.labels.len > 0:
    lines.add("labels=" & m.labels.join(","))
  lines.join("\n") & "\n"

proc parseManifest(s: string): (bool, TileStackManifest) =
  var m = TileStackManifest()
  for raw in s.split('\n'):
    let line = raw.strip()
    if line.len == 0 or line.startsWith("#"): continue
    let eq = line.find('=')
    if eq < 0: return (false, m)
    let key = line[0 ..< eq]
    let val = line[eq + 1 .. ^1]
    case key
    of "bbox":
      let parts = val.split(',')
      if parts.len != 4: return (false, m)
      try:
        m.bbox.minX = parseFloat(parts[0]); m.bbox.minY = parseFloat(parts[1])
        m.bbox.maxX = parseFloat(parts[2]); m.bbox.maxY = parseFloat(parts[3])
      except ValueError: return (false, m)
    of "rows":      m.rows      = parseInt(val)
    of "cols":      m.cols      = parseInt(val)
    of "channels":  m.channels  = parseInt(val)
    of "tileSize":  m.tileSize  = parseInt(val)
    of "chunkSize": m.chunkSize = parseInt(val)
    of "labels":    m.labels    = val.split(',')
    else: discard
  if m.rows <= 0 or m.cols <= 0 or m.channels < 1 or
     m.tileSize <= 0 or m.chunkSize <= 0:
    return (false, m)
  if m.labels.len > 0 and m.labels.len != m.channels: return (false, m)
  (true, m)

proc writeManifest(dir: string; m: TileStackManifest) =
  let path = dir / ManifestFileName
  let tmp = path & ".tmp"
  writeFile(tmp, serializeManifest(m))
  moveFile(tmp, path)

proc readManifest(dir: string): (bool, TileStackManifest) =
  let path = dir / ManifestFileName
  if not fileExists(path):
    return (false, TileStackManifest())
  parseManifest(readFile(path))

# --------- tile file io ---------

proc tileFilePath(stackDir: string; tr, tc: int): string =
  stackDir / ("tile_" & $tr & "_" & $tc & ".tts")

proc writeFileHeader(f: File) =
  f.setFilePos(0)
  f.write(TileFileMagic)
  var ver = TileFileVersion
  discard f.writeBuffer(addr ver, 4)
  var reserved: uint32 = 0
  discard f.writeBuffer(addr reserved, 4)

proc readFileHeader(f: File): bool =
  f.setFilePos(0)
  var magicBuf = newString(TileFileMagic.len)
  if f.readBuffer(addr magicBuf[0], TileFileMagic.len) != TileFileMagic.len:
    return false
  if magicBuf != TileFileMagic: return false
  var ver: uint32
  if f.readBuffer(addr ver, 4) != 4: return false
  # Accept v1 (legacy Gorilla-only) and v2 (adds constant-chunk RLE).
  # Constant chunks set bit 31 of payloadBytes; v1 files never had that bit
  # set so they decode transparently with the v2 reader.
  if ver != 1'u32 and ver != TileFileVersion: return false
  var reserved: uint32
  if f.readBuffer(addr reserved, 4) != 4: return false
  true

proc writeU32(f: File; v: uint32) =
  var x = v
  discard f.writeBuffer(addr x, 4)

proc writeI64(f: File; v: int64) =
  var x = v
  discard f.writeBuffer(addr x, 8)

proc writeF64(f: File; v: float64) =
  var x = v
  discard f.writeBuffer(addr x, 8)

proc readU32(f: File): uint32 =
  if f.readBuffer(addr result, 4) != 4: raise newException(IOError, "short u32 read")

proc readI64(f: File): int64 =
  if f.readBuffer(addr result, 8) != 8: raise newException(IOError, "short i64 read")

proc readF64(f: File): float64 =
  if f.readBuffer(addr result, 8) != 8: raise newException(IOError, "short f64 read")

# --------- chunk encode / decode ---------

proc encodeTimestampsDoDToWriter(w: var BitWriter; timestamps: seq[int64]) =
  ## Legacy DoD timestamp encoding: 32-bit zigzag first delta + DoD prefix
  ## codes for the rest.
  let n = timestamps.len
  if n >= 2:
    let firstDelta = timestamps[1] - timestamps[0]
    w.writeBitsU64(zigzag(firstDelta) and 0xFFFFFFFF'u64, 32)
    var prevDelta = firstDelta
    for i in 2 ..< n:
      let delta = timestamps[i] - timestamps[i - 1]
      let dod = delta - prevDelta
      encodeDoD(w, dod)
      prevDelta = delta

proc decodeTimestampsDoDFromReader(r: var BitReader; n: int; startTs: int64): seq[int64] =
  result = newSeq[int64](n)
  result[0] = startTs
  if n >= 2:
    let firstDelta = unzigzag(r.readBitsU64(32))
    result[1] = startTs + firstDelta
    var prevDelta = firstDelta
    for i in 2 ..< n:
      let dod = decodeDoD(r)
      let delta = prevDelta + dod
      result[i] = result[i - 1] + delta
      prevDelta = delta

proc timestampsToZigzagDods(timestamps: seq[int64]): seq[uint64] =
  ## Build the (n-1)-element vector of zigzag(DoD) values that Simple-8b will
  ## pack. The first element holds the first delta itself (encoded as if its
  ## "previous delta" was 0). After the first, true DoDs.
  let n = timestamps.len
  if n < 2: return @[]
  result = newSeqOfCap[uint64](n - 1)
  let firstDelta = timestamps[1] - timestamps[0]
  result.add(zigzag64(firstDelta))
  var prevDelta = firstDelta
  for i in 2 ..< n:
    let delta = timestamps[i] - timestamps[i - 1]
    let dod = delta - prevDelta
    result.add(zigzag64(dod))
    prevDelta = delta

proc encodeTimestampsS8bToWriter(w: var BitWriter; timestamps: seq[int64]): int =
  ## Pack zigzag-DoD timestamps via Simple-8b. We write a single varuint with
  ## the number of S8b words, then each word as a 64-bit chunk. Returns the
  ## number of S8b words emitted.
  ##
  ## The reader reconstructs (n-1) values from `nWords` words; n is in the
  ## chunk header so we don't need to write it.
  let dods = timestampsToZigzagDods(timestamps)
  if dods.len == 0:
    # No timestamps to encode beyond ts0 → write 0 words.
    w.writeBitsU64(0'u64, 16)
    return 0
  let words = encodeSimple8b(dods)
  w.writeBitsU64(uint64(words.len), 16)
  for word in words: w.writeBitsU64(word, 64)
  words.len

proc decodeTimestampsS8bFromReader(r: var BitReader; n: int; startTs: int64): seq[int64] =
  result = newSeq[int64](n)
  result[0] = startTs
  let nWords = int(r.readBitsU64(16))
  if n < 2: return
  var words = newSeq[uint64](nWords)
  for i in 0 ..< nWords: words[i] = r.readBitsU64(64)
  let dods = decodeSimple8b(words, n - 1)
  let firstDelta = unzigzag64(dods[0])
  result[1] = startTs + firstDelta
  var prevDelta = firstDelta
  for i in 2 ..< n:
    let delta = prevDelta + unzigzag64(dods[i - 1])
    result[i] = result[i - 1] + delta
    prevDelta = delta

proc estimateDoDBits(timestamps: seq[int64]): int =
  ## Encode timestamps via DoD into a throwaway writer to measure the bit cost.
  ## Used to pick the smaller of DoD vs Simple-8b for the chunk-header flag.
  var w = newBitWriter()
  encodeTimestampsDoDToWriter(w, timestamps)
  w.bitLen

proc estimateS8bBits(timestamps: seq[int64]): int =
  ## 16-bit length prefix + 64 bits per word.
  let dods = timestampsToZigzagDods(timestamps)
  if dods.len == 0: return 16
  let words = encodeSimple8b(dods)
  16 + words.len * 64

proc isWholeChunkConstant(frames: seq[seq[float64]]): (bool, float64) =
  ## Whole-chunk-constant means every cell of every frame is bit-identical to
  ## frames[0][0]. Bit-compare via cast — covers signed-zero and NaN edge cases
  ## consistently (two NaNs with different bit patterns are treated as
  ## different, which is the safe choice).
  if frames.len == 0 or frames[0].len == 0: return (false, 0.0)
  let v0Bits = cast[uint64](frames[0][0])
  for f in frames:
    for x in f:
      if cast[uint64](x) != v0Bits: return (false, 0.0)
  (true, frames[0][0])

proc encodeTileChunk(timestamps: seq[int64];
                     frames: seq[seq[float64]];
                     tileCells, channels: int): (seq[byte], int, float64, float64, ChunkKind, TsCodec) =
  ## frames[i] is row-major channel-interleaved tileCells*channels floats.
  ## Returns (payload_bytes_with_zero_crc, payloadBitLen, minVal, maxVal,
  ## kind, tsCodec). Caller is responsible for appending the CRC trailer.
  ##
  ## When every cell of every frame is bit-identical, emits a tiny constant
  ## chunk: timestamps + 1 × float64. Otherwise the legacy Gorilla layout.
  ##
  ## Timestamps are encoded as DoD or via Simple-8b — whichever produces a
  ## smaller bit count for *this* chunk's timestamps. The choice is recorded
  ## in the per-chunk tsCodec flag (bit 30 of payloadBytes on disk).
  doAssert frames.len == timestamps.len
  doAssert frames.len > 0
  # Pick smaller timestamp codec. For chunkSize=1 (n=1) both produce 0 bits
  # past the header, so it doesn't matter; default to DoD.
  let dodBits = estimateDoDBits(timestamps)
  let s8bBits = estimateS8bBits(timestamps)
  let tsCodec = if s8bBits < dodBits: tsSimple8b else: tsDoD
  template writeTs(w: var BitWriter) =
    case tsCodec
    of tsDoD:
      encodeTimestampsDoDToWriter(w, timestamps)
    of tsSimple8b:
      discard encodeTimestampsS8bToWriter(w, timestamps)
  # Fast path: whole-chunk-constant (clear-sky pixels, flat probability maps).
  let (allEqual, constVal) = isWholeChunkConstant(frames)
  if allEqual:
    var w = newBitWriter()
    writeTs(w)
    w.writeBitsU64(cast[uint64](constVal), 64)
    return (w.bytes, w.bitLen, constVal, constVal, ckConstant, tsCodec)
  let n = frames.len
  let cellChans = tileCells * channels
  var w = newBitWriter()
  writeTs(w)
  # Per-stream Gorilla. Each cell-channel is independent (its own XorState).
  var minV =  Inf
  var maxV = -Inf
  for cc in 0 ..< cellChans:
    let v0 = frames[0][cc]
    if v0 < minV: minV = v0
    if v0 > maxV: maxV = v0
    w.writeBitsU64(cast[uint64](v0), 64)
    var xstate: XorState
    var prevBits = cast[uint64](v0)
    for i in 1 ..< n:
      let v = frames[i][cc]
      if v < minV: minV = v
      if v > maxV: maxV = v
      let bits = cast[uint64](v)
      encodeXor(w, xstate, prevBits xor bits)
      prevBits = bits
  if n == 1:
    # min/max from the one frame
    if minV == Inf: minV = 0.0
    if maxV == NegInf: maxV = 0.0
  (w.bytes, w.bitLen, minV, maxV, ckGorilla, tsCodec)

proc decodeTileChunk(payload: seq[byte];
                     payloadBitLen: int;
                     n, tileCells, channels: int;
                     startTs: int64;
                     kind: ChunkKind = ckGorilla;
                     tsCodec: TsCodec = tsDoD): (seq[int64], seq[seq[float64]]) =
  ## Returns (timestamps, frames). frames[i] is tileCells*channels floats.
  doAssert n >= 1
  var r = newBitReader(payload, payloadBitLen)
  let timestamps =
    case tsCodec
    of tsDoD:      decodeTimestampsDoDFromReader(r, n, startTs)
    of tsSimple8b: decodeTimestampsS8bFromReader(r, n, startTs)
  let cellChans = tileCells * channels
  var frames = newSeq[seq[float64]](n)
  for i in 0 ..< n:
    frames[i] = newSeq[float64](cellChans)
  case kind
  of ckConstant:
    let constVal = cast[float64](r.readBitsU64(64))
    for i in 0 ..< n:
      for cc in 0 ..< cellChans:
        frames[i][cc] = constVal
  of ckGorilla:
    # Per-cell-channel decode using decodeXorRun, which uses clz to bulk-
    # skip runs of zero-XOR values (the dominant case for radar / raster
    # fields with constant cells). The reconstruction (prefix-XOR scan)
    # stays scalar — measurement showed NEON 2-way pairing didn't beat
    # scalar here; the lane-shuffling overhead canceled the parallelism.
    for cc in 0 ..< cellChans:
      let bits0 = r.readBitsU64(64)
      frames[0][cc] = cast[float64](bits0)
      var xstate: XorState
      let xors = decodeXorRun(r, xstate, n - 1)
      var prevBits = bits0
      for i in 0 ..< (n - 1):
        prevBits = prevBits xor xors[i]
        frames[i + 1][cc] = cast[float64](prevBits)
  (timestamps, frames)

# --------- tile file open / scan ---------

const DefaultTileChunkCacheSize* = 16
  ## Per-tile decoded-chunk LRU capacity. Each cached chunk holds
  ## `chunkSize × tileSize² × channels` floats, so a 64×64×1×128 chunk is ~4 MB
  ## decoded — 16 slots is ~64 MB worst-case per actively-queried tile.

proc openTileFile(path: string;
                  cacheSize = DefaultTileChunkCacheSize): TileFile =
  ## Open or create a tile file. Scans existing chunk headers, tolerates
  ## torn tail (truncates back to last verified chunk).
  result = TileFile(path: path, chunks: @[],
                    activeTs: @[], activeData: @[],
                    decodedCache: newChunkCache[int, (seq[int64], seq[seq[float64]])](cacheSize))
  let isNew = not fileExists(path)
  if isNew:
    let parent = parentDir(path)
    if parent.len > 0 and not dirExists(parent): createDir(parent)
    result.file = open(path, fmReadWrite)
    writeFileHeader(result.file)
    return
  result.file = open(path, fmReadWriteExisting)
  if not readFileHeader(result.file):
    raise newException(IOError, "tile file " & path & ": bad header")
  var lastGoodEnd = int64(TileFileHdrBytes)
  while true:
    let pos = result.file.getFilePos()
    var meta = ChunkMeta(fileOffset: pos)
    var pbRaw: uint32
    try: pbRaw = readU32(result.file)
    except IOError: break
    meta.kind    = if (pbRaw and ChunkKindBit) != 0: ckConstant else: ckGorilla
    meta.tsCodec = if (pbRaw and TsCodecBit) != 0: tsSimple8b else: tsDoD
    let pb = pbRaw and PayloadBytesMask
    meta.payloadBytes = int(pb)
    if meta.payloadBytes < 4 or meta.payloadBytes > 256 * 1024 * 1024: break
    try:
      meta.frameCount = int(readU32(result.file))
      meta.startTs    = readI64(result.file)
      meta.endTs      = readI64(result.file)
      meta.minVal     = readF64(result.file)
      meta.maxVal     = readF64(result.file)
    except IOError: break
    if meta.frameCount <= 0 or meta.frameCount > 1_000_000: break
    var payload = newSeq[byte](meta.payloadBytes - 4)
    if payload.len > 0:
      if result.file.readBuffer(addr payload[0], payload.len) != payload.len: break
    var crc: uint32
    try: crc = readU32(result.file)
    except IOError: break
    if fnv1a32(payload) != crc: break
    result.chunks.add(meta)
    lastGoodEnd = result.file.getFilePos()
  result.file.setFilePos(lastGoodEnd)

proc tileCellsFor(s: TileStack; tr, tc: int): int =
  let (h, w) = s.tileExtent(tr, tc)
  h * w

proc flushTile(s: TileStack; tr, tc: int; tf: TileFile) =
  ## Flush the active buffer to one chunk on disk. No-op if empty.
  if tf.activeTs.len == 0: return
  let tc2 = s.tileCellsFor(tr, tc)
  let (payload, bitLen, minV, maxV, kind, tsCodec) =
    encodeTileChunk(tf.activeTs, tf.activeData, tc2, s.manifest.channels)
  let payloadBytes = payload.len + 4
  # Bit 31 → ckConstant; bit 30 → Simple-8b ts codec.
  var pbRaw = uint32(payloadBytes)
  if kind == ckConstant: pbRaw = pbRaw or ChunkKindBit
  if tsCodec == tsSimple8b: pbRaw = pbRaw or TsCodecBit
  let pos = tf.file.getFileSize()
  tf.file.setFilePos(pos)
  writeU32(tf.file, pbRaw)
  writeU32(tf.file, uint32(tf.activeTs.len))
  writeI64(tf.file, tf.activeTs[0])
  writeI64(tf.file, tf.activeTs[^1])
  writeF64(tf.file, minV)
  writeF64(tf.file, maxV)
  if payload.len > 0:
    discard tf.file.writeBuffer(unsafeAddr payload[0], payload.len)
  let crc = fnv1a32(payload)
  writeU32(tf.file, crc)
  tf.chunks.add(ChunkMeta(
    fileOffset: pos, payloadBytes: payloadBytes,
    frameCount: tf.activeTs.len, startTs: tf.activeTs[0],
    endTs: tf.activeTs[^1], minVal: minV, maxVal: maxV, kind: kind,
    tsCodec: tsCodec))
  tf.activeTs.setLen(0)
  tf.activeData.setLen(0)
  discard bitLen   # only used during encode; payloadBitLen on read = bytes*8

# --------- public api ---------

proc getOrOpenTile(s: TileStack; tr, tc: int): TileFile =
  if (tr, tc) in s.tiles: return s.tiles[(tr, tc)]
  let tf = openTileFile(tileFilePath(s.dir, tr, tc))
  s.tiles[(tr, tc)] = tf
  tf

proc newTileStack*(dir: string;
                   bbox: BBox; rows, cols, channels: int;
                   tileSize = DefaultTileSize;
                   chunkSize = DefaultChunkSize;
                   labels: seq[string] = @[]): TileStack =
  ## Create or open a TileStack rooted at `dir`. If a manifest already exists
  ## there with the same dimensions, the call attaches to it; otherwise a new
  ## manifest is written. To open without specifying dimensions, use
  ## `openTileStack(dir)`.
  doAssert rows > 0 and cols > 0 and channels >= 1
  doAssert tileSize > 0 and chunkSize > 0
  if labels.len > 0 and labels.len != channels:
    raise newException(ValueError,
      "newTileStack: labels.len " & $labels.len & " != channels " & $channels)
  if not dirExists(dir): createDir(dir)
  let m = TileStackManifest(
    bbox: bbox, rows: rows, cols: cols, channels: channels,
    tileSize: tileSize, chunkSize: chunkSize, labels: @labels)
  let (existed, prev) = readManifest(dir)
  if existed:
    if prev.rows != m.rows or prev.cols != m.cols or
       prev.channels != m.channels or prev.tileSize != m.tileSize:
      raise newException(ValueError,
        "TileStack manifest mismatch at " & dir & "; refusing to clobber")
    result = TileStack(dir: dir, manifest: prev, tiles: initTable[(int,int), TileFile]())
  else:
    writeManifest(dir, m)
    result = TileStack(dir: dir, manifest: m, tiles: initTable[(int,int), TileFile]())
  initLock(result.lock)

proc openTileStack*(dir: string): TileStack =
  ## Attach to an existing TileStack. Raises if no manifest exists.
  let (ok, m) = readManifest(dir)
  if not ok:
    raise newException(IOError, "TileStack: no valid manifest in " & dir)
  result = TileStack(dir: dir, manifest: m, tiles: initTable[(int,int), TileFile]())
  initLock(result.lock)

proc channelIndex*(s: TileStack; label: string): int =
  for i, l in s.manifest.labels:
    if l == label: return i
  -1

proc bbox*(s: TileStack): BBox = s.manifest.bbox
proc rows*(s: TileStack): int = s.manifest.rows
proc cols*(s: TileStack): int = s.manifest.cols
proc channels*(s: TileStack): int = s.manifest.channels
proc tileSize*(s: TileStack): int = s.manifest.tileSize
proc chunkSize*(s: TileStack): int = s.manifest.chunkSize

proc latestTs*(s: TileStack): int64 =
  acquire(s.lock)
  defer: release(s.lock)
  result = low(int64)
  for _, tf in s.tiles:
    if tf.activeTs.len > 0:
      if tf.activeTs[^1] > result: result = tf.activeTs[^1]
    elif tf.chunks.len > 0:
      if tf.chunks[^1].endTs > result: result = tf.chunks[^1].endTs
  # tiles not yet opened in memory aren't counted; user can call latestTsOnDisk
  # if they want to scan all tile files. For radar use cases all tiles are
  # touched per frame, so this is fine.

proc appendFrame*(s: TileStack; tsMillis: int64; mesh: GeoMesh) =
  ## Decompose mesh into tile slices, append each slice to its tile's active
  ## buffer. Flushes any tile whose buffer reaches chunkSize.
  ##
  ## The mesh must match the TileStack's bbox / dimensions / channels exactly.
  if mesh.rows != s.manifest.rows or mesh.cols != s.manifest.cols or
     mesh.channels != s.manifest.channels:
    raise newException(ValueError, "appendFrame: mesh shape mismatch")
  acquire(s.lock)
  defer: release(s.lock)
  let (gridR, gridC) = s.tileGridDims()
  let ts = s.manifest.tileSize
  let cs = s.manifest.chunkSize
  let ch = s.manifest.channels
  for tr in 0 ..< gridR:
    for tc in 0 ..< gridC:
      let r0 = tr * ts
      let c0 = tc * ts
      let r1 = min(r0 + ts, s.manifest.rows)
      let c1 = min(c0 + ts, s.manifest.cols)
      let tH = r1 - r0
      let tW = c1 - c0
      var tileBuf = newSeq[float64](tH * tW * ch)
      for r in 0 ..< tH:
        let srcRowBase = (r0 + r) * s.manifest.cols * ch
        let dstRowBase = r * tW * ch
        for c in 0 ..< tW:
          let srcBase = srcRowBase + (c0 + c) * ch
          let dstBase = dstRowBase + c * ch
          for k in 0 ..< ch:
            tileBuf[dstBase + k] = mesh.data[srcBase + k]
      let tf = s.getOrOpenTile(tr, tc)
      # Out-of-order frames force a flush so each chunk stays monotonic.
      if tf.activeTs.len > 0 and tsMillis < tf.activeTs[^1]:
        flushTile(s, tr, tc, tf)
      tf.activeTs.add(tsMillis)
      tf.activeData.add(tileBuf)
      if tf.activeTs.len >= cs:
        flushTile(s, tr, tc, tf)

proc readChunkFramesUncached(tf: TileFile; idx: int;
                             tileCells, channels: int): (seq[int64], seq[seq[float64]]) =
  ## Decode a chunk fully — both timestamps and per-frame cell data.
  let m = tf.chunks[idx]
  tf.file.setFilePos(m.fileOffset)
  discard readU32(tf.file)            # payloadBytes (incl. kind bit)
  discard readU32(tf.file)            # frameCount
  discard readI64(tf.file)            # startTs
  discard readI64(tf.file)            # endTs
  discard readF64(tf.file)            # minVal
  discard readF64(tf.file)            # maxVal
  let payloadLen = m.payloadBytes - 4
  var payload = newSeq[byte](payloadLen)
  if payloadLen > 0:
    if tf.file.readBuffer(addr payload[0], payloadLen) != payloadLen:
      raise newException(IOError, "tilestack: short payload read")
  let crc = readU32(tf.file)
  if fnv1a32(payload) != crc:
    raise newException(IOError, "tilestack: chunk CRC mismatch")
  decodeTileChunk(payload, payload.len * 8, m.frameCount,
                  tileCells, channels, m.startTs, m.kind, m.tsCodec)

proc readChunkFrames(tf: TileFile; idx: int;
                     tileCells, channels: int): (seq[int64], seq[seq[float64]]) =
  ## LRU-cached variant. Same return as readChunkFramesUncached, just cheaper
  ## on repeat access.
  let (hit, cached) = tf.decodedCache.get(idx)
  if hit: return cached
  result = readChunkFramesUncached(tf, idx, tileCells, channels)
  tf.decodedCache.put(idx, result)

proc readFrame*(s: TileStack; tsMillis: int64): (bool, GeoMesh) =
  ## Reconstruct the frame whose timestamp == tsMillis (exact match). Returns
  ## (false, _) if no chunk + active buffer in any tile contains exactly that ts.
  ## The caller-visible behaviour is "if you appended a frame at T and haven't
  ## modified the stack since, readFrame(T) returns it bit-exact".
  acquire(s.lock)
  defer: release(s.lock)
  var mesh = newGeoMesh(s.manifest.bbox, s.manifest.rows, s.manifest.cols,
                        channels = s.manifest.channels,
                        labels = s.manifest.labels)
  let (gridR, gridC) = s.tileGridDims()
  let ch = s.manifest.channels
  var anyHit = false
  for tr in 0 ..< gridR:
    for tc in 0 ..< gridC:
      let tf = s.getOrOpenTile(tr, tc)
      let (tH, tW) = s.tileExtent(tr, tc)
      let tileCells = tH * tW
      var localTs: int64 = low(int64)
      var localData: seq[float64]
      var found = false
      # active buffer
      for i in 0 ..< tf.activeTs.len:
        if tf.activeTs[i] == tsMillis:
          localTs = tf.activeTs[i]; localData = tf.activeData[i]; found = true; break
      # closed chunks
      if not found:
        for ci, m in tf.chunks:
          if tsMillis < m.startTs or tsMillis > m.endTs: continue
          let (timestamps, frames) =
            readChunkFrames(tf, ci, tileCells, ch)
          for i, t in timestamps:
            if t == tsMillis:
              localTs = t; localData = frames[i]; found = true; break
          if found: break
      if not found: continue
      anyHit = true
      let r0 = tr * s.manifest.tileSize
      let c0 = tc * s.manifest.tileSize
      for r in 0 ..< tH:
        let dstRowBase = (r0 + r) * s.manifest.cols * ch
        let srcRowBase = r * tW * ch
        for c in 0 ..< tW:
          let dstBase = dstRowBase + (c0 + c) * ch
          let srcBase = srcRowBase + c * ch
          for k in 0 ..< ch:
            mesh.data[dstBase + k] = localData[srcBase + k]
      discard localTs
  (anyHit, mesh)

proc readFrameRange*(s: TileStack; fromMs, toMs: int64): seq[(int64, GeoMesh)] =
  ## All frames whose ts ∈ [fromMs, toMs], in ascending order.
  result = @[]
  if fromMs > toMs: return
  acquire(s.lock)
  # Collect distinct timestamps that any tile saw in the range.
  var tsSet: seq[int64] = @[]
  proc addUnique(ts: int64) =
    for x in tsSet:
      if x == ts: return
    tsSet.add(ts)
  let (gridR, gridC) = s.tileGridDims()
  for tr in 0 ..< gridR:
    for tc in 0 ..< gridC:
      let tf = s.getOrOpenTile(tr, tc)
      for i in 0 ..< tf.activeTs.len:
        let t = tf.activeTs[i]
        if t >= fromMs and t <= toMs: addUnique(t)
      for ci in 0 ..< tf.chunks.len:
        let m = tf.chunks[ci]
        if m.endTs < fromMs or m.startTs > toMs: continue
        let (tH, tW) = s.tileExtent(tr, tc)
        let (timestamps, _) = readChunkFrames(tf, ci, tH * tW, s.manifest.channels)
        for t in timestamps:
          if t >= fromMs and t <= toMs: addUnique(t)
  release(s.lock)
  # sort
  tsSet.sort()
  for t in tsSet:
    let (ok, m) = s.readFrame(t)
    if ok: result.add((t, m))

proc readPointHistory*(s: TileStack;
                      lon, lat: float64;
                      fromMs, toMs: int64;
                      channel = 0): seq[(int64, float64)] =
  ## All (ts, value) samples for the cell containing (lon, lat), within
  ## [fromMs, toMs], on the given channel. Decodes only the single tile that
  ## covers the cell.
  result = @[]
  if channel < 0 or channel >= s.manifest.channels:
    raise newException(ValueError, "readPointHistory: channel out of range")
  if fromMs > toMs: return
  let mesh = newGeoMesh(s.manifest.bbox, s.manifest.rows, s.manifest.cols,
                        channels = s.manifest.channels)
  let (ok, row, col) = mesh.cellAt(lon, lat)
  if not ok: return
  let ts = s.manifest.tileSize
  let tr = row div ts
  let tc = col div ts
  let rInTile = row mod ts
  let cInTile = col mod ts
  acquire(s.lock)
  defer: release(s.lock)
  let tf = s.getOrOpenTile(tr, tc)
  let (tH, tW) = s.tileExtent(tr, tc)
  let cellIdxInTile = rInTile * tW + cInTile
  let ch = s.manifest.channels
  let cellChanIdx = cellIdxInTile * ch + channel
  # active buffer
  for i in 0 ..< tf.activeTs.len:
    let t = tf.activeTs[i]
    if t < fromMs or t > toMs: continue
    result.add((t, tf.activeData[i][cellChanIdx]))
  # closed chunks
  for ci, m in tf.chunks:
    if m.endTs < fromMs or m.startTs > toMs: continue
    let (timestamps, frames) =
      readChunkFrames(tf, ci, tH * tW, ch)
    for i, t in timestamps:
      if t >= fromMs and t <= toMs:
        result.add((t, frames[i][cellChanIdx]))
  # sort by ts
  result.sort(proc (a, b: (int64, float64)): int = cmp(a[0], b[0]))

proc flush*(s: TileStack) =
  ## Force any active per-tile buffers out to disk.
  acquire(s.lock)
  defer: release(s.lock)
  for k, tf in s.tiles:
    let (tr, tc) = k
    flushTile(s, tr, tc, tf)
    tf.file.flushFile()

proc close*(s: TileStack) =
  acquire(s.lock)
  defer: release(s.lock)
  for k, tf in s.tiles:
    let (tr, tc) = k
    flushTile(s, tr, tc, tf)
    if not tf.file.isNil:
      tf.file.close()
      tf.file = nil
  s.tiles.clear()

# --------- diagnostics ---------

proc countChunks*(s: TileStack): int =
  ## Total chunks across every tile that's been opened (touched).
  acquire(s.lock)
  defer: release(s.lock)
  for _, tf in s.tiles: result += tf.chunks.len

proc countActiveFrames*(s: TileStack): int =
  ## Frames buffered in memory across all tiles, not yet flushed.
  acquire(s.lock)
  defer: release(s.lock)
  for _, tf in s.tiles: result += tf.activeTs.len
