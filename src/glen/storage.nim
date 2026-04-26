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

import std/[os, streams, tables, memfiles, syncio]
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
  # Sniff magic for v2.
  var magicBuf = newString(SnapshotV2Magic.len)
  let nMagic = f.readBuffer(addr magicBuf[0], SnapshotV2Magic.len)
  if nMagic == SnapshotV2Magic.len and magicBuf == SnapshotV2Magic:
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
    ## A mmap'd snapshot file kept open for random body reads. The `index`
    ## maps docId → location of the encoded body within the file.
    path*:   string
    file*:   MemFile
    index*:  Table[string, SnapshotIndexEntry]
    isV2*:   bool       # false if the snapshot is v1 — index will be empty

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
  if total < SnapshotV2Magic.len + 8: return result
  # Magic check
  for i in 0 ..< SnapshotV2Magic.len:
    if char(raw[i]) != SnapshotV2Magic[i]:
      result.isV2 = false
      return result
  result.isV2 = true
  var off = SnapshotV2Magic.len
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

proc loadDocFromMmap*(s: SnapshotMmap; docId: string): Value =
  ## Returns nil if the doc isn't in the index. Otherwise decodes and returns
  ## the Value by reading directly from the mapped region.
  if s.isNil or not s.isV2: return nil
  if docId notin s.index: return nil
  let e = s.index[docId]
  let raw = cast[ptr UncheckedArray[byte]](s.file.mem)
  if e.bodyOffset.int + e.bodyLength.int > s.file.size:
    raiseSnapshot("v2 mmap body out of range")
  var enc = newString(int(e.bodyLength))
  if e.bodyLength > 0:
    copyMem(addr enc[0], addr raw[int(e.bodyOffset)], int(e.bodyLength))
  decode(enc)

proc closeSnapshotMmap*(s: SnapshotMmap) =
  if s.isNil: return
  close(s.file)
