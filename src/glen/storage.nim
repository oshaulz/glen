# Glen storage snapshots
#
# Two on-disk formats are supported:
#
#   v1 (legacy): varuint numDocs; repeated (varuint idLen idStr | binaryValue)
#                Loaded by streaming read into a Table; backward-compatible.
#
#   v2 (default for new writes): a header + index + body layout that supports
#                random access via mmap, enabling lazy / spillable loading:
#                  magic    "GLENSNP2" (8 B)
#                  version  uint32 (2)
#                  docCount uint32
#                  index entries (docCount), each:
#                    idLen      uint32
#                    id         [idLen bytes]
#                    bodyOffset uint64  (absolute file offset)
#                    bodyLength uint32
#                  body section: encoded values concatenated; index points in.
#
# The format auto-detects on load: v2 snapshots start with the 8-byte magic;
# anything else is treated as v1.

import std/[os, streams, tables, memfiles, syncio, algorithm]
when defined(windows):
  import std/winlean
import glen/types, glen/codec
import glen/config
import glen/errors

proc writeVarUint(s: Stream; x: uint64) =
  var v = x
  while true:
    var b = uint8(v and 0x7F)
    v = v shr 7
    if v != 0: b = b or 0x80'u8
    s.write(b)
    if v == 0: break

proc readVarUint(s: Stream): uint64 =
  var shift: uint32
  var iterations = 0
  while true:
    let b = s.readUint8()
    result = result or (uint64(b and 0x7F) shl shift)
    if (b and 0x80) == 0: break
    shift += 7
    inc iterations
    if iterations > 10: raiseSnapshot("varuint too long")

const
  SnapshotV2Magic*   = "GLENSNP2"
  SnapshotV2Version* = 2'u32
  SnapshotV3Magic*   = "GLENSNP3"
  SnapshotV3Version* = 3'u32
  SnapshotV4Magic*   = "GLENSNP4"
  SnapshotV4Version* = 4'u32
  # v3 header layout: magic(8) + version(4) + docCount(4) + bodiesStart(8) +
  #                   entriesStart(8) + offsetsStart(8) = 40 bytes
  SnapshotV3HeaderBytes = 40
  # v4 adds dictStart (8) + flags (4) + reserved (4) before bodiesStart =
  # 40 + 16 = 56 bytes. Layout:
  #   magic(8) + version(4) + docCount(4) + dictStart(8) +
  #   bodiesStart(8) + entriesStart(8) + offsetsStart(8) +
  #   flags(4) + reserved(4) = 56 bytes
  SnapshotV4HeaderBytes = 56
  # v4 flags
  V4FlagHasKeyDict*   = 0x00000001'u32
  V4FlagHasValueDict* = 0x00000002'u32

  # Dictionary build thresholds: keys / (field, value) pairs appearing in
  # fewer than this many docs aren't dictionarised, reducing dict size when
  # the schema has long tails.
  DefaultKeyDictThreshold*   = 4
  DefaultValueDictThreshold* = 8

proc snapshotPath(dir, collection: string): string = dir / (collection & ".snap")

proc flushDir(dir: string) =
  when defined(windows):
    let w = newWideCString(dir)
    let h = createFileW(w, GENERIC_READ, FILE_SHARE_READ or FILE_SHARE_WRITE, nil, OPEN_EXISTING, FILE_FLAG_BACKUP_SEMANTICS, 0)
    if h != INVALID_HANDLE_VALUE:
      discard flushFileBuffers(h)
      discard closeHandle(h)
  else:
    discard

## Write a snapshot for a collection atomically: write to a temp file then rename.
proc writeSnapshot*(dir, collection: string; docs: Table[string, Value]) =
  createDir(dir)
  let finalPath = snapshotPath(dir, collection)
  let tmpPath = finalPath & ".tmp"
  # write to temp file
  var f = syncio.open(tmpPath, fmWrite)
  var fs = newFileStream(f)
  writeVarUint(fs, uint64(docs.len))
  for id, v in docs:
    writeVarUint(fs, uint64(id.len)); fs.write(id)
    let enc = encode(v)
    writeVarUint(fs, uint64(enc.len)); fs.write(enc)
  f.flushFile()
  f.close()
  # atomic replace
  when defined(windows):
    # Nim's moveFile raises if dest exists on Windows; remove first.
    if fileExists(finalPath): removeFile(finalPath)
    moveFile(tmpPath, finalPath)
  else:
    # POSIX rename(2) is atomic and replaces an existing destination — do not
    # remove first; that creates a window where readers see no snapshot.
    moveFile(tmpPath, finalPath)
  # best-effort directory flush (no-op on non-Windows for now)
  flushDir(dir)

## Load a snapshot eagerly into memory. v1 and v2 are auto-detected.
## Returns an empty table if no snapshot exists.
proc loadSnapshot*(dir, collection: string): Table[string, Value] =
  result = initTable[string, Value]()
  let path = snapshotPath(dir, collection)
  if not fileExists(path): return
  var f = syncio.open(path, fmRead)
  defer: f.close()
  # Sniff magic. v3 (paged on-disk index) and v2 (in-memory index) use the
  # same first 7 chars "GLENSNP", differ in the 8th. v1 has no magic.
  var magicBuf = newString(8)
  let nMagic = f.readBuffer(addr magicBuf[0], 8)
  if nMagic == 8 and magicBuf == SnapshotV4Magic:
    # v4 path: header → dict (if present) → walk every entry → decode body
    # using dict when bound.
    var ver: uint32
    if f.readBuffer(addr ver, 4) != 4: raiseSnapshot("v4 truncated header")
    if ver != SnapshotV4Version: raiseSnapshot("v4 unsupported version " & $ver)
    var docCount: uint32
    if f.readBuffer(addr docCount, 4) != 4: raiseSnapshot("v4 truncated header")
    var dictStart, bodiesStart, entriesStart, offsetsStart: uint64
    if f.readBuffer(addr dictStart, 8) != 8: raiseSnapshot("v4 truncated header")
    if f.readBuffer(addr bodiesStart, 8) != 8: raiseSnapshot("v4 truncated header")
    if f.readBuffer(addr entriesStart, 8) != 8: raiseSnapshot("v4 truncated header")
    if f.readBuffer(addr offsetsStart, 8) != 8: raiseSnapshot("v4 truncated header")
    var flags: uint32
    if f.readBuffer(addr flags, 4) != 4: raiseSnapshot("v4 truncated header")
    var reservedHdr: uint32
    if f.readBuffer(addr reservedHdr, 4) != 4: raiseSnapshot("v4 truncated header")
    let cfg = loadConfig()
    var dict: KeyDict
    if dictStart != 0:
      f.setFilePos(int64(dictStart))
      var keys: seq[string] = @[]
      if (flags and V4FlagHasKeyDict) != 0:
        var keyCount: uint32
        if f.readBuffer(addr keyCount, 4) != 4: raiseSnapshot("v4 dict truncated")
        keys = newSeqOfCap[string](int(keyCount))
        for _ in 0 ..< int(keyCount):
          var kLen: uint32
          if f.readBuffer(addr kLen, 4) != 4: raiseSnapshot("v4 dict entry trunc")
          if int(kLen) > cfg.maxStringOrBytes: raiseSnapshot("v4 dict key too large")
          var k = newString(int(kLen))
          if int(kLen) > 0 and f.readBuffer(addr k[0], int(kLen)) != int(kLen):
            raiseSnapshot("v4 dict key body truncated")
          keys.add(k)
      dict = newKeyDict(keys)
      if (flags and V4FlagHasValueDict) != 0:
        var fieldCount: uint32
        if f.readBuffer(addr fieldCount, 4) != 4: raiseSnapshot("v4 vd fields trunc")
        for _ in 0 ..< int(fieldCount):
          var fkLen: uint32
          if f.readBuffer(addr fkLen, 4) != 4: raiseSnapshot("v4 vd field-name trunc")
          if int(fkLen) > cfg.maxStringOrBytes: raiseSnapshot("v4 vd field too large")
          var fk = newString(int(fkLen))
          if int(fkLen) > 0 and f.readBuffer(addr fk[0], int(fkLen)) != int(fkLen):
            raiseSnapshot("v4 vd field body trunc")
          var valueCount: uint32
          if f.readBuffer(addr valueCount, 4) != 4: raiseSnapshot("v4 vd values trunc")
          var vals = newSeqOfCap[string](int(valueCount))
          for _ in 0 ..< int(valueCount):
            var vLen: uint32
            if f.readBuffer(addr vLen, 4) != 4: raiseSnapshot("v4 vd value trunc")
            if int(vLen) > cfg.maxStringOrBytes: raiseSnapshot("v4 vd value too large")
            var vs = newString(int(vLen))
            if int(vLen) > 0 and f.readBuffer(addr vs[0], int(vLen)) != int(vLen):
              raiseSnapshot("v4 vd value body trunc")
            vals.add(vs)
          dict.valueDictByField[fk] = newValueDict(vals)
    f.setFilePos(int64(entriesStart))
    for _ in 0 ..< int(docCount):
      var idLen: uint32
      if f.readBuffer(addr idLen, 4) != 4: raiseSnapshot("v4 truncated entry")
      if int(idLen) > cfg.maxStringOrBytes: raiseSnapshot("v4 id too large")
      var id = newString(int(idLen))
      if int(idLen) > 0 and f.readBuffer(addr id[0], int(idLen)) != int(idLen):
        raiseSnapshot("v4 truncated id")
      var bodyOff: uint64
      if f.readBuffer(addr bodyOff, 8) != 8: raiseSnapshot("v4 truncated bodyOffset")
      var bodyLen: uint32
      if f.readBuffer(addr bodyLen, 4) != 4: raiseSnapshot("v4 truncated bodyLen")
      if int(bodyLen) > cfg.maxStringOrBytes: raiseSnapshot("v4 body too large")
      let savedPos = f.getFilePos()
      f.setFilePos(int64(bodyOff))
      var enc = newString(int(bodyLen))
      if int(bodyLen) > 0 and f.readBuffer(addr enc[0], int(bodyLen)) != int(bodyLen):
        raiseSnapshot("v4 truncated body")
      result[id] =
        if not dict.isNil: decodeWithDict(enc, dict)
        else:              decode(enc)
      f.setFilePos(savedPos)
    return
  if nMagic == 8 and magicBuf == SnapshotV3Magic:
    # v3 path: header → walk every entry sequentially → decode body for each.
    var ver: uint32
    if f.readBuffer(addr ver, 4) != 4: raiseSnapshot("v3 truncated header")
    if ver != SnapshotV3Version: raiseSnapshot("v3 unsupported version " & $ver)
    var docCount: uint32
    if f.readBuffer(addr docCount, 4) != 4: raiseSnapshot("v3 truncated header")
    var bodiesStart, entriesStart, offsetsStart: uint64
    if f.readBuffer(addr bodiesStart, 8) != 8: raiseSnapshot("v3 truncated header")
    if f.readBuffer(addr entriesStart, 8) != 8: raiseSnapshot("v3 truncated header")
    if f.readBuffer(addr offsetsStart, 8) != 8: raiseSnapshot("v3 truncated header")
    let cfg = loadConfig()
    f.setFilePos(int64(entriesStart))
    for _ in 0 ..< int(docCount):
      var idLen: uint32
      if f.readBuffer(addr idLen, 4) != 4: raiseSnapshot("v3 truncated entry")
      if int(idLen) > cfg.maxStringOrBytes: raiseSnapshot("v3 id too large")
      var id = newString(int(idLen))
      if int(idLen) > 0 and f.readBuffer(addr id[0], int(idLen)) != int(idLen):
        raiseSnapshot("v3 truncated id")
      var bodyOff: uint64
      if f.readBuffer(addr bodyOff, 8) != 8: raiseSnapshot("v3 truncated bodyOffset")
      var bodyLen: uint32
      if f.readBuffer(addr bodyLen, 4) != 4: raiseSnapshot("v3 truncated bodyLen")
      if int(bodyLen) > cfg.maxStringOrBytes: raiseSnapshot("v3 body too large")
      let savedPos = f.getFilePos()
      f.setFilePos(int64(bodyOff))
      var enc = newString(int(bodyLen))
      if int(bodyLen) > 0 and f.readBuffer(addr enc[0], int(bodyLen)) != int(bodyLen):
        raiseSnapshot("v3 truncated body")
      result[id] = decode(enc)
      f.setFilePos(savedPos)
    return
  if nMagic == 8 and magicBuf == SnapshotV2Magic:
    # v2 path: read version + docCount, then index, then dereference into body.
    var ver: uint32
    if f.readBuffer(addr ver, 4) != 4: raiseSnapshot("v2 truncated header")
    if ver != SnapshotV2Version: raiseSnapshot("v2 unsupported version " & $ver)
    var docCount: uint32
    if f.readBuffer(addr docCount, 4) != 4: raiseSnapshot("v2 truncated header")
    let cfg = loadConfig()
    type IndexEntry = object
      id: string
      bodyOffset: uint64
      bodyLength: uint32
    var entries = newSeq[IndexEntry](int(docCount))
    for i in 0 ..< int(docCount):
      var idLen: uint32
      if f.readBuffer(addr idLen, 4) != 4: raiseSnapshot("v2 truncated index")
      if int(idLen) > cfg.maxStringOrBytes: raiseSnapshot("v2 index id too large")
      var id = newString(int(idLen))
      if int(idLen) > 0 and f.readBuffer(addr id[0], int(idLen)) != int(idLen):
        raiseSnapshot("v2 truncated id")
      var off: uint64
      if f.readBuffer(addr off, 8) != 8: raiseSnapshot("v2 truncated offset")
      var blen: uint32
      if f.readBuffer(addr blen, 4) != 4: raiseSnapshot("v2 truncated length")
      if int(blen) > cfg.maxStringOrBytes: raiseSnapshot("v2 body too large")
      entries[i] = IndexEntry(id: id, bodyOffset: off, bodyLength: blen)
    # Walk the index and decode each value's body.
    for e in entries:
      f.setFilePos(int64(e.bodyOffset))
      var enc = newString(int(e.bodyLength))
      if int(e.bodyLength) > 0 and
         f.readBuffer(addr enc[0], int(e.bodyLength)) != int(e.bodyLength):
        raiseSnapshot("v2 truncated body")
      result[e.id] = decode(enc)
    return
  # v1 fallback: rewind and stream-decode.
  f.setFilePos(0)
  var fs = newFileStream(f)
  let cfg = loadConfig()
  let n = int(readVarUint(fs))
  if n < 0 or n > 10_000_000: raiseSnapshot("snapshot doc count too large")
  for i in 0..<n:
    let idLen = int(readVarUint(fs));
    if idLen < 0 or idLen > cfg.maxStringOrBytes: raiseSnapshot("snapshot id too large")
    let id = fs.readStr(idLen)
    let vLen = int(readVarUint(fs));
    if vLen < 0 or vLen > cfg.maxStringOrBytes: raiseSnapshot("snapshot value too large")
    let enc = fs.readStr(vLen)
    result[id] = decode(enc)

# ---- v2 writer + lazy / spillable loaders ----

## Write a snapshot in v2 (indexed) format.
proc writeSnapshotV2*(dir, collection: string; docs: Table[string, Value]) =
  createDir(dir)
  let finalPath = snapshotPath(dir, collection)
  let tmpPath = finalPath & ".tmp"
  # First pass: encode every value, compute total body size, build index entries.
  type IndexEntry = object
    id: string
    bodyLength: uint32
    enc: string
  var entries: seq[IndexEntry] = @[]
  entries.setLen(docs.len)
  var i = 0
  for id, v in docs:
    let enc = encode(v)
    entries[i] = IndexEntry(id: id, bodyLength: uint32(enc.len), enc: enc)
    inc i
  # Compute the file offset where the body section begins:
  #   header: 8 (magic) + 4 (version) + 4 (docCount)            = 16
  #   index : sum over entries of (4 + idLen + 8 + 4)
  var indexBytes: int = 0
  for e in entries:
    indexBytes += 4 + e.id.len + 8 + 4
  let bodyStart = uint64(16 + indexBytes)
  var f = syncio.open(tmpPath, fmWrite)
  # Write header
  f.write(SnapshotV2Magic)
  var ver = SnapshotV2Version
  discard f.writeBuffer(addr ver, 4)
  var docCount = uint32(entries.len)
  discard f.writeBuffer(addr docCount, 4)
  # Write index (with computed body offsets)
  var bodyOff = bodyStart
  for e in entries:
    var idLen = uint32(e.id.len)
    discard f.writeBuffer(addr idLen, 4)
    if e.id.len > 0:
      discard f.writeBuffer(unsafeAddr e.id[0], e.id.len)
    var off = bodyOff
    discard f.writeBuffer(addr off, 8)
    var blen = e.bodyLength
    discard f.writeBuffer(addr blen, 4)
    bodyOff += uint64(e.bodyLength)
  # Write body
  for e in entries:
    if e.enc.len > 0:
      discard f.writeBuffer(unsafeAddr e.enc[0], e.enc.len)
  f.flushFile()
  f.close()
  when defined(windows):
    if fileExists(finalPath): removeFile(finalPath)
    moveFile(tmpPath, finalPath)
  else:
    moveFile(tmpPath, finalPath)
  flushDir(dir)

# ---- Lazy / spillable loader ----

type
  SnapshotIndexEntry* = object
    bodyOffset*: uint64
    bodyLength*: uint32

  SnapshotMmap* = ref object
    ## A mmap'd snapshot file kept open for random body reads.
    ##
    ## v2: the `index` Table is fully populated in RAM at open time.
    ## v3: only `docCount` + `offsetsStart` are kept in RAM; the index lives
    ##     on disk as a sorted offsets table mapped via the OS page cache.
    ## v4: like v3 plus an in-memory `keyDict` loaded from the file's dict
    ##     section once at open. Decode reads dispatch via decodeWithDict.
    ## v1: legacy; isV2/isV3/isV4 all false; callers should fall back to the
    ##     eager `loadSnapshot` path.
    path*:           string
    file*:           MemFile
    isV2*:           bool
    isV3*:           bool
    isV4*:           bool
    # v2 fields (only populated when isV2):
    index*:          Table[string, SnapshotIndexEntry]
    # v3/v4 shared paged-index fields:
    docCount*:       uint32
    bodiesStart*:    uint64
    entriesStart*:   uint64
    offsetsStart*:   uint64
    # v4 only:
    keyDict*:        KeyDict

proc openSnapshotMmap*(dir, collection: string): SnapshotMmap =
  ## Memory-map the snapshot file and read just the index (v2). On a v1 file,
  ## returns an SnapshotMmap with `isV2 = false` and an empty index — callers
  ## must fall back to `loadSnapshot` (eager) for v1 collections.
  let path = snapshotPath(dir, collection)
  if not fileExists(path): return nil
  result = SnapshotMmap(path: path,
                        index: initTable[string, SnapshotIndexEntry]())
  result.file = memfiles.open(path, mode = fmRead)
  let raw = cast[ptr UncheckedArray[byte]](result.file.mem)
  let total = result.file.size
  if total < 16: return result
  # Sniff: v2 and v3 share "GLENSNP" prefix; differ in 8th char.
  var prefixOk = true
  for i in 0 ..< 7:
    if char(raw[i]) != "GLENSNP"[i]:
      prefixOk = false; break
  if not prefixOk: return result   # v1 or unknown — caller falls back
  let kindChar = char(raw[7])
  if kindChar == '4':
    # v4 path: like v3, plus a leading dict section we read into memory once.
    if total < SnapshotV4HeaderBytes:
      raiseSnapshot("v4 mmap truncated header")
    var off = 8
    let ver = cast[ptr uint32](addr raw[off])[]; off += 4
    if ver != SnapshotV4Version: raiseSnapshot("v4 unsupported version " & $ver)
    let docCount = cast[ptr uint32](addr raw[off])[]; off += 4
    let dictStart = cast[ptr uint64](addr raw[off])[]; off += 8
    let bodiesStart = cast[ptr uint64](addr raw[off])[]; off += 8
    let entriesStart = cast[ptr uint64](addr raw[off])[]; off += 8
    let offsetsStart = cast[ptr uint64](addr raw[off])[]; off += 8
    let flags = cast[ptr uint32](addr raw[off])[]; off += 4
    discard cast[ptr uint32](addr raw[off])[]   # reserved
    if int(offsetsStart) + int(docCount) * 8 > total:
      raiseSnapshot("v4 mmap offsets section out of bounds")
    result.isV4 = true
    result.docCount = docCount
    result.bodiesStart = bodiesStart
    result.entriesStart = entriesStart
    result.offsetsStart = offsetsStart
    if dictStart != 0:
      var dpos = int(dictStart)
      let cfgK = loadConfig()
      var keys: seq[string] = @[]
      if (flags and V4FlagHasKeyDict) != 0:
        if dpos + 4 > total: raiseSnapshot("v4 mmap key-dict header oob")
        let keyCount = cast[ptr uint32](addr raw[dpos])[]; dpos += 4
        keys = newSeqOfCap[string](int(keyCount))
        for _ in 0 ..< int(keyCount):
          if dpos + 4 > total: raiseSnapshot("v4 mmap key-dict entry oob")
          let kLen = cast[ptr uint32](addr raw[dpos])[]; dpos += 4
          if int(kLen) > cfgK.maxStringOrBytes: raiseSnapshot("v4 dict key too large")
          if dpos + int(kLen) > total: raiseSnapshot("v4 mmap dict key body oob")
          var k = newString(int(kLen))
          if kLen > 0: copyMem(addr k[0], addr raw[dpos], int(kLen))
          dpos += int(kLen)
          keys.add(k)
      result.keyDict = newKeyDict(keys)
      if (flags and V4FlagHasValueDict) != 0:
        if dpos + 4 > total: raiseSnapshot("v4 mmap value-dict header oob")
        let fieldCount = cast[ptr uint32](addr raw[dpos])[]; dpos += 4
        for _ in 0 ..< int(fieldCount):
          if dpos + 4 > total: raiseSnapshot("v4 mmap value-dict field oob")
          let fkLen = cast[ptr uint32](addr raw[dpos])[]; dpos += 4
          if int(fkLen) > cfgK.maxStringOrBytes: raiseSnapshot("v4 vd field too large")
          if dpos + int(fkLen) > total: raiseSnapshot("v4 mmap vd field body oob")
          var fk = newString(int(fkLen))
          if fkLen > 0: copyMem(addr fk[0], addr raw[dpos], int(fkLen))
          dpos += int(fkLen)
          if dpos + 4 > total: raiseSnapshot("v4 mmap vd value-count oob")
          let valueCount = cast[ptr uint32](addr raw[dpos])[]; dpos += 4
          var vals = newSeqOfCap[string](int(valueCount))
          for _ in 0 ..< int(valueCount):
            if dpos + 4 > total: raiseSnapshot("v4 mmap vd value entry oob")
            let vLen = cast[ptr uint32](addr raw[dpos])[]; dpos += 4
            if int(vLen) > cfgK.maxStringOrBytes: raiseSnapshot("v4 vd value too large")
            if dpos + int(vLen) > total: raiseSnapshot("v4 mmap vd value body oob")
            var vs = newString(int(vLen))
            if vLen > 0: copyMem(addr vs[0], addr raw[dpos], int(vLen))
            dpos += int(vLen)
            vals.add(vs)
          result.keyDict.valueDictByField[fk] = newValueDict(vals)
    return
  if kindChar == '3':
    # v3 path: read header into the SnapshotMmap fields; defer entry/offset
    # access to lookup-time (paged via OS).
    if total < SnapshotV3HeaderBytes:
      raiseSnapshot("v3 mmap truncated header")
    var off = 8
    let ver = cast[ptr uint32](addr raw[off])[]; off += 4
    if ver != SnapshotV3Version: raiseSnapshot("v3 unsupported version " & $ver)
    let docCount = cast[ptr uint32](addr raw[off])[]; off += 4
    let bodiesStart = cast[ptr uint64](addr raw[off])[]; off += 8
    let entriesStart = cast[ptr uint64](addr raw[off])[]; off += 8
    let offsetsStart = cast[ptr uint64](addr raw[off])[]; off += 8
    if int(offsetsStart) + int(docCount) * 8 > total:
      raiseSnapshot("v3 mmap offsets section out of bounds")
    result.isV3 = true
    result.docCount = docCount
    result.bodiesStart = bodiesStart
    result.entriesStart = entriesStart
    result.offsetsStart = offsetsStart
    return
  if kindChar != '2':
    # Unknown version letter — treat as v1 / unsupported; fall through.
    return result
  result.isV2 = true
  var off = 8
  let ver = cast[ptr uint32](addr raw[off])[]
  off += 4
  if ver != SnapshotV2Version:
    raiseSnapshot("v2 unsupported version " & $ver)
  let docCount = cast[ptr uint32](addr raw[off])[]
  off += 4
  let cfg = loadConfig()
  for _ in 0 ..< int(docCount):
    if off + 4 > total: raiseSnapshot("v2 mmap truncated index header")
    let idLen = cast[ptr uint32](addr raw[off])[]
    off += 4
    if int(idLen) > cfg.maxStringOrBytes: raiseSnapshot("v2 mmap id too large")
    if off + int(idLen) > total: raiseSnapshot("v2 mmap truncated id")
    var id = newString(int(idLen))
    if idLen > 0:
      copyMem(addr id[0], addr raw[off], int(idLen))
    off += int(idLen)
    if off + 12 > total: raiseSnapshot("v2 mmap truncated entry tail")
    let bodyOffset = cast[ptr uint64](addr raw[off])[]
    off += 8
    let bodyLength = cast[ptr uint32](addr raw[off])[]
    off += 4
    if int(bodyLength) > cfg.maxStringOrBytes:
      raiseSnapshot("v2 mmap body too large")
    result.index[id] = SnapshotIndexEntry(
      bodyOffset: bodyOffset, bodyLength: bodyLength)

# ---- v3 raw-byte helpers (mmap dereferences) ----

proc raw(s: SnapshotMmap): ptr UncheckedArray[byte] {.inline.} =
  cast[ptr UncheckedArray[byte]](s.file.mem)

proc readU32At(s: SnapshotMmap; off: int): uint32 {.inline.} =
  let r = s.raw
  result = (uint32(r[off])) or
           (uint32(r[off + 1]) shl 8) or
           (uint32(r[off + 2]) shl 16) or
           (uint32(r[off + 3]) shl 24)

proc readU64At(s: SnapshotMmap; off: int): uint64 {.inline.} =
  let r = s.raw
  result = (uint64(r[off]))      or
           (uint64(r[off+1]) shl  8) or
           (uint64(r[off+2]) shl 16) or
           (uint64(r[off+3]) shl 24) or
           (uint64(r[off+4]) shl 32) or
           (uint64(r[off+5]) shl 40) or
           (uint64(r[off+6]) shl 48) or
           (uint64(r[off+7]) shl 56)

proc compareEntryIdAt(s: SnapshotMmap; entryOff: uint64; key: string): int =
  ## Compare the entry's id at `entryOff` with `key`. Returns -1/0/1 like cmp.
  let idLen = readU32At(s, int(entryOff))
  let r = s.raw
  let n = min(int(idLen), key.len)
  for i in 0 ..< n:
    let a = char(r[int(entryOff) + 4 + i])
    let b = key[i]
    if a < b: return -1
    if a > b: return 1
  if int(idLen) < key.len: return -1
  if int(idLen) > key.len: return 1
  0

proc readEntryAt(s: SnapshotMmap; entryOff: uint64): (string, uint64, uint32) =
  let idLen = readU32At(s, int(entryOff))
  var id = newString(int(idLen))
  if idLen > 0:
    let r = s.raw
    copyMem(addr id[0], addr r[int(entryOff) + 4], int(idLen))
  let bodyOff = readU64At(s, int(entryOff) + 4 + int(idLen))
  let bodyLen = readU32At(s, int(entryOff) + 4 + int(idLen) + 8)
  (id, bodyOff, bodyLen)

proc lookupV3(s: SnapshotMmap; docId: string): (bool, uint64, uint32) =
  ## Binary-search the v3/v4 offsets table for `docId`. Returns
  ## (found, bodyOffset, bodyLength). The offsets array and the entries it
  ## points into are paged in by the OS as we touch them.
  result = (false, 0'u64, 0'u32)
  if not (s.isV3 or s.isV4) or s.docCount == 0: return
  var lo = 0
  var hi = int(s.docCount)
  while lo < hi:
    let mid = (lo + hi) shr 1
    let entryOff = readU64At(s, int(s.offsetsStart) + mid * 8)
    let cmpRes = compareEntryIdAt(s, entryOff, docId)
    if cmpRes == 0:
      let (_, bodyOff, bodyLen) = readEntryAt(s, entryOff)
      return (true, bodyOff, bodyLen)
    if cmpRes < 0:   # entry's id < query → search right half
      lo = mid + 1
    else:
      hi = mid

proc loadDocFromMmap*(s: SnapshotMmap; docId: string): Value =
  ## Returns nil if the doc isn't in the index. Otherwise decodes and returns
  ## the Value by reading directly from the mapped region. Handles v2
  ## (in-memory Table lookup) and v3 (binary search on disk-paged offsets).
  if s.isNil: return nil
  let raw = cast[ptr UncheckedArray[byte]](s.file.mem)
  if s.isV2:
    if docId notin s.index: return nil
    let e = s.index[docId]
    if e.bodyOffset.int + e.bodyLength.int > s.file.size:
      raiseSnapshot("v2 mmap body out of range")
    var enc = newString(int(e.bodyLength))
    if e.bodyLength > 0:
      copyMem(addr enc[0], addr raw[int(e.bodyOffset)], int(e.bodyLength))
    return decode(enc)
  if s.isV3:
    let (found, bodyOff, bodyLen) = lookupV3(s, docId)
    if not found: return nil
    if int(bodyOff) + int(bodyLen) > s.file.size:
      raiseSnapshot("v3 mmap body out of range")
    var enc = newString(int(bodyLen))
    if bodyLen > 0:
      copyMem(addr enc[0], addr raw[int(bodyOff)], int(bodyLen))
    return decode(enc)
  if s.isV4:
    let (found, bodyOff, bodyLen) = lookupV3(s, docId)
    if not found: return nil
    if int(bodyOff) + int(bodyLen) > s.file.size:
      raiseSnapshot("v4 mmap body out of range")
    var enc = newString(int(bodyLen))
    if bodyLen > 0:
      copyMem(addr enc[0], addr raw[int(bodyOff)], int(bodyLen))
    if not s.keyDict.isNil:
      return decodeWithDict(enc, s.keyDict)
    return decode(enc)
  return nil

proc containsId*(s: SnapshotMmap; docId: string): bool =
  ## Existence check that works for v2, v3, and v4.
  if s.isNil: return false
  if s.isV2: return docId in s.index
  if s.isV3 or s.isV4:
    let (found, _, _) = lookupV3(s, docId)
    return found
  false

iterator iterIds*(s: SnapshotMmap): string =
  ## Yields every docId in the snapshot. Order: insertion order for v2,
  ## sorted (lexicographic) for v3 / v4.
  if not s.isNil:
    if s.isV2:
      for id in s.index.keys: yield id
    elif s.isV3 or s.isV4:
      for i in 0 ..< int(s.docCount):
        let entryOff = readU64At(s, int(s.offsetsStart) + i * 8)
        let (id, _, _) = readEntryAt(s, entryOff)
        yield id

proc snapshotDocCount*(s: SnapshotMmap): int =
  ## Total docs in the mapped snapshot (works across v2/v3/v4).
  if s.isNil: return 0
  if s.isV2: return s.index.len
  if s.isV3 or s.isV4: return int(s.docCount)
  0

proc closeSnapshotMmap*(s: SnapshotMmap) =
  if s.isNil: return
  close(s.file)

# ---- Snapshot v3: paged on-disk index, mmap-backed binary search ----
#
# Layout:
#   header (40 B):
#     magic         "GLENSNP3" (8 B)
#     version       uint32     (4 B, = 3)
#     docCount      uint32     (4 B)
#     bodiesStart   uint64     (8 B)  abs file offset
#     entriesStart  uint64     (8 B)  abs file offset
#     offsetsStart  uint64     (8 B)  abs file offset
#   bodies section: encoded values, concatenated. Stored in docId-sorted order
#                   so sequential iteration is also sequential I/O.
#   entries section: variable-size records, sorted by docId:
#     idLen      uint32
#     id         [idLen bytes]
#     bodyOffset uint64    (abs file offset of this doc's body)
#     bodyLength uint32
#   offsets section: docCount × uint64, each is the abs file offset of the
#                    matching entry (also in docId-sorted order).
#
# Lookup: binary-search the offsets table; for each midpoint, deref to the
# entry, read its idLen+id, compare to the queried docId. Found entries
# yield (bodyOffset, bodyLength), which point into the bodies section.
#
# Memory footprint: only the file pages actually touched by lookups stay
# resident (handled by the OS page cache via mmap). For random key lookups,
# log₂(docCount) page faults per lookup; for iteration, a sequential scan
# of the offsets and entries sections.

proc writeSnapshotV3*(dir, collection: string; docs: Table[string, Value]) =
  ## Write a v3 (paged) snapshot atomically: write to temp file then rename.
  createDir(dir)
  let finalPath = snapshotPath(dir, collection)
  let tmpPath = finalPath & ".tmp"
  # Sort entries by docId so bodies, entries, and offsets all align.
  type Pair = tuple[id: string, enc: string]
  var pairs = newSeqOfCap[Pair](docs.len)
  for id, v in docs:
    pairs.add((id, encode(v)))
  pairs.sort(proc (a, b: Pair): int = cmp(a.id, b.id))

  var f = syncio.open(tmpPath, fmReadWrite)
  # Reserve header bytes (we'll patch them after writing all sections).
  var hdrPad = newString(SnapshotV3HeaderBytes)
  discard f.writeBuffer(addr hdrPad[0], SnapshotV3HeaderBytes)

  # Bodies section: write each encoded value, capturing offsets.
  let bodiesStart = uint64(f.getFilePos())
  var bodyOffsets = newSeq[uint64](pairs.len)
  var bodyLens = newSeq[uint32](pairs.len)
  for i, p in pairs:
    bodyOffsets[i] = uint64(f.getFilePos())
    bodyLens[i] = uint32(p.enc.len)
    if p.enc.len > 0:
      discard f.writeBuffer(unsafeAddr p.enc[0], p.enc.len)

  # Entries section: write each (idLen, id, bodyOffset, bodyLength).
  let entriesStart = uint64(f.getFilePos())
  var entryOffsets = newSeq[uint64](pairs.len)
  for i, p in pairs:
    entryOffsets[i] = uint64(f.getFilePos())
    var idLen = uint32(p.id.len)
    discard f.writeBuffer(addr idLen, 4)
    if p.id.len > 0:
      discard f.writeBuffer(unsafeAddr p.id[0], p.id.len)
    var bo = bodyOffsets[i]
    discard f.writeBuffer(addr bo, 8)
    var bl = bodyLens[i]
    discard f.writeBuffer(addr bl, 4)

  # Offsets section: docCount × uint64.
  let offsetsStart = uint64(f.getFilePos())
  for off in entryOffsets:
    var x = off
    discard f.writeBuffer(addr x, 8)

  # Patch the header.
  f.setFilePos(0)
  f.write(SnapshotV3Magic)
  var ver = SnapshotV3Version
  discard f.writeBuffer(addr ver, 4)
  var dc = uint32(pairs.len)
  discard f.writeBuffer(addr dc, 4)
  var bs = bodiesStart
  discard f.writeBuffer(addr bs, 8)
  var es = entriesStart
  discard f.writeBuffer(addr es, 8)
  var os2 = offsetsStart
  discard f.writeBuffer(addr os2, 8)
  f.flushFile()
  f.close()
  when defined(windows):
    if fileExists(finalPath): removeFile(finalPath)
    moveFile(tmpPath, finalPath)
  else:
    moveFile(tmpPath, finalPath)
  flushDir(dir)

# (v3 raw read helpers were hoisted above lookupV3.)

# ---- Snapshot v4: paged on-disk index + per-snapshot key dictionary ----
#
# Layout (extends v3 with a leading dict section):
#   header (56 B):
#     magic         "GLENSNP4" (8)
#     version       uint32     (4, = 4)
#     docCount      uint32     (4)
#     dictStart     uint64     (8)  abs file offset (0 if no dict)
#     bodiesStart   uint64     (8)
#     entriesStart  uint64     (8)
#     offsetsStart  uint64     (8)
#     flags         uint32     (4)  bit 0 = V4FlagHasKeyDict
#     reserved      uint32     (4)
#   (optional) dict section:
#     keyCount      uint32     (4)
#     for each key:
#       keyLen      uint32     (4)
#       keyBytes    [keyLen]
#   bodies section: same shape as v3 but bodies MAY use TAG_OBJECT_DICT
#                   referring to the dict above.
#   entries section: same as v3
#   offsets section: same as v3
#
# v3 readers reject v4 (different magic). v4 readers accept v4 only — v3
# files keep working through the v3 path.

proc writeSnapshotV4*(dir, collection: string; docs: Table[string, Value];
                     keyDictThreshold = DefaultKeyDictThreshold;
                     valueDictThreshold = DefaultValueDictThreshold) =
  ## Write a v4 snapshot atomically. Builds a per-collection key dictionary
  ## from `docs` (recursing into nested objects) plus optional per-field
  ## value dictionaries, and uses them to compress bodies.
  ##
  ## `keyDictThreshold = 0` disables key dictionarisation entirely.
  ## `valueDictThreshold = 0` disables value dictionarisation.
  createDir(dir)
  let finalPath = snapshotPath(dir, collection)
  let tmpPath = finalPath & ".tmp"
  # Build the dictionaries.
  var dict: KeyDict
  if keyDictThreshold > 0 and docs.len > 0:
    dict = buildKeyDict(docs, keyDictThreshold)
  if valueDictThreshold > 0 and docs.len > 0:
    if dict.isNil: dict = newKeyDict()
    populateValueDicts(dict, docs, valueDictThreshold)
  # Encode every body once, sorted by docId for in-order bodies/entries/offsets.
  type Pair = tuple[id: string, enc: string]
  var pairs = newSeqOfCap[Pair](docs.len)
  let useDict = not dict.isNil and (
    dict.idToKey.len > 0 or dict.valueDictByField.len > 0)
  for id, v in docs:
    let enc =
      if useDict: encodeWithDict(v, dict)
      else: encode(v)
    pairs.add((id, enc))
  pairs.sort(proc (a, b: Pair): int = cmp(a.id, b.id))

  var f = syncio.open(tmpPath, fmReadWrite)
  # Reserve header.
  var hdrPad = newString(SnapshotV4HeaderBytes)
  discard f.writeBuffer(addr hdrPad[0], SnapshotV4HeaderBytes)

  # Dict section.
  #
  # Layout (when present):
  #   [key-dict]    — written if V4FlagHasKeyDict
  #     keyCount   uint32
  #     for each key: keyLen uint32 + bytes
  #   [value-dict] — written if V4FlagHasValueDict, immediately after key-dict
  #     fieldCount uint32
  #     for each field:
  #       fieldNameLen uint32 + bytes
  #       valueCount   uint32
  #       for each value: valueLen uint32 + bytes
  var dictStart: uint64 = 0
  var flags: uint32 = 0
  let hasKeyDict = not dict.isNil and dict.idToKey.len > 0
  let hasValueDict = not dict.isNil and dict.valueDictByField.len > 0
  if hasKeyDict or hasValueDict:
    dictStart = uint64(f.getFilePos())
  if hasKeyDict:
    flags = flags or V4FlagHasKeyDict
    var keyCount = uint32(dict.idToKey.len)
    discard f.writeBuffer(addr keyCount, 4)
    for k in dict.idToKey:
      var kLen = uint32(k.len)
      discard f.writeBuffer(addr kLen, 4)
      if k.len > 0:
        discard f.writeBuffer(unsafeAddr k[0], k.len)
  if hasValueDict:
    flags = flags or V4FlagHasValueDict
    var fieldCount = uint32(dict.valueDictByField.len)
    discard f.writeBuffer(addr fieldCount, 4)
    for fk, vd in dict.valueDictByField:
      var fkLen = uint32(fk.len)
      discard f.writeBuffer(addr fkLen, 4)
      if fk.len > 0:
        discard f.writeBuffer(unsafeAddr fk[0], fk.len)
      var valueCount = uint32(vd.idToVal.len)
      discard f.writeBuffer(addr valueCount, 4)
      for v in vd.idToVal:
        var vLen = uint32(v.len)
        discard f.writeBuffer(addr vLen, 4)
        if v.len > 0:
          discard f.writeBuffer(unsafeAddr v[0], v.len)

  # Bodies section.
  let bodiesStart = uint64(f.getFilePos())
  var bodyOffsets = newSeq[uint64](pairs.len)
  var bodyLens = newSeq[uint32](pairs.len)
  for i, p in pairs:
    bodyOffsets[i] = uint64(f.getFilePos())
    bodyLens[i] = uint32(p.enc.len)
    if p.enc.len > 0:
      discard f.writeBuffer(unsafeAddr p.enc[0], p.enc.len)

  # Entries section.
  let entriesStart = uint64(f.getFilePos())
  var entryOffsets = newSeq[uint64](pairs.len)
  for i, p in pairs:
    entryOffsets[i] = uint64(f.getFilePos())
    var idLen = uint32(p.id.len)
    discard f.writeBuffer(addr idLen, 4)
    if p.id.len > 0:
      discard f.writeBuffer(unsafeAddr p.id[0], p.id.len)
    var bo = bodyOffsets[i]
    discard f.writeBuffer(addr bo, 8)
    var bl = bodyLens[i]
    discard f.writeBuffer(addr bl, 4)

  # Offsets section.
  let offsetsStart = uint64(f.getFilePos())
  for off in entryOffsets:
    var x = off
    discard f.writeBuffer(addr x, 8)

  # Patch header.
  f.setFilePos(0)
  f.write(SnapshotV4Magic)
  var ver = SnapshotV4Version
  discard f.writeBuffer(addr ver, 4)
  var dc = uint32(pairs.len)
  discard f.writeBuffer(addr dc, 4)
  var ds = dictStart
  discard f.writeBuffer(addr ds, 8)
  var bs = bodiesStart
  discard f.writeBuffer(addr bs, 8)
  var es = entriesStart
  discard f.writeBuffer(addr es, 8)
  var os2 = offsetsStart
  discard f.writeBuffer(addr os2, 8)
  var fl = flags
  discard f.writeBuffer(addr fl, 4)
  var rsv: uint32 = 0
  discard f.writeBuffer(addr rsv, 4)
  f.flushFile()
  f.close()
  when defined(windows):
    if fileExists(finalPath): removeFile(finalPath)
    moveFile(tmpPath, finalPath)
  else:
    moveFile(tmpPath, finalPath)
  flushDir(dir)
