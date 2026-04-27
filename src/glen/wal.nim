# Glen Write-Ahead Log
# Simple segmented append log: glen.wal.0, glen.wal.1, ...
# Each record: varuint length | uint32 checksum(fnv1a of body) | body
# Body layout: varuint recordType | collection | docId | version | valueLen(+value)
# recordType: 1 = Put {collection, id, version, value}; 2 = Delete {collection,id,version}

import std/[os, streams, locks]
when defined(windows):
  import std/winlean
import glen/types, glen/codec

const WAL_PREFIX = "glen.wal."
const WAL_MAGIC = "GLENWAL1"
const WAL_VERSION = 2'u32
const MAX_WAL_RECORD_SIZE = 64 * 1024 * 1024  # 64 MiB safety cap

type
  WalRecordType* = enum wrPut = 1, wrDelete = 2

  WalSyncMode* = enum wsmAlways, wsmInterval, wsmNone

  WalRecord* = object
    kind*: WalRecordType
    collection*: string
    docId*: string
    version*: uint64
    value*: Value  # only for put
    # Replication metadata (v2+). Optional when reading v1 segments.
    changeId*: string
    originNode*: string
    hlc*: Hlc

  WriteAheadLog* = ref object
    dir*: string
    maxSegmentSize*: int
    currentIndex: int
    currentSize: int
    fs: File
    syncMode: WalSyncMode
    flushEveryBytes: int
    bytesSinceFlush: int
    lock: Lock

proc fnv1a32(data: string): uint32 =
  var h: uint32 = 0x811C9DC5'u32
  for ch in data:
    h = h xor uint32(uint8(ch))
    h = h * 0x01000193'u32
  h

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
    if iterations > 10: raise newException(IOError, "varuint too long")

proc segmentPath(dir: string; idx: int): string =
  dir / (WAL_PREFIX & $idx)

proc flushDir(dir: string) =
  when defined(windows):
    let w = newWideCString(dir)
    let h = createFileW(w, GENERIC_READ, FILE_SHARE_READ or FILE_SHARE_WRITE, nil, OPEN_EXISTING, FILE_FLAG_BACKUP_SEMANTICS, 0)
    if h != INVALID_HANDLE_VALUE:
      discard flushFileBuffers(h)
      discard closeHandle(h)
  else:
    discard

proc openSegment(wal: WriteAheadLog) =
  if wal.fs != nil:
    wal.fs.flushFile()
    wal.fs.close()
    wal.fs = nil
  wal.fs = open(segmentPath(wal.dir, wal.currentIndex), fmAppend)
  if wal.fs.getFileSize == 0:
    # new file: write magic + version header
    var hs = newStringStream()
    hs.write(WAL_MAGIC)
    hs.write(WAL_VERSION)
    wal.fs.write(hs.data)
    wal.fs.flushFile()
    flushDir(wal.dir)
  wal.currentSize = int(wal.fs.getFileSize())

proc openWriteAheadLog*(dir: string; maxSegmentSize = 8 * 1024 * 1024; syncMode: WalSyncMode = wsmAlways; flushEveryBytes = 1 * 1024 * 1024): WriteAheadLog =
  createDir(dir)
  result = WriteAheadLog(dir: dir, maxSegmentSize: maxSegmentSize, syncMode: syncMode, flushEveryBytes: flushEveryBytes, bytesSinceFlush: 0)
  initLock(result.lock)
  # find last segment
  var idx = 0
  while fileExists(segmentPath(dir, idx + 1)): inc idx
  result.currentIndex = idx
  # If existing last segment has older header version, start a new segment to upgrade
  if fileExists(segmentPath(dir, idx)):
    var fs: Stream = nil
    var f: File
    var fOpened = false
    try:
      f = open(segmentPath(dir, idx), fmRead)
      fOpened = true
      fs = newFileStream(f)
      let magic = fs.readStr(WAL_MAGIC.len)
      if magic == WAL_MAGIC:
        let ver = fs.readUint32()
        if ver != WAL_VERSION:
          result.currentIndex = idx + 1
    except IOError:
      discard
    finally:
      if fs != nil:
        fs.close()
      elif fOpened:
        f.close()
  result.openSegment()

proc rotateIfNeeded(wal: WriteAheadLog; needed: int) =
  if wal.currentSize + needed > wal.maxSegmentSize:
    inc wal.currentIndex
    wal.openSegment()

proc size*(wal: WriteAheadLog): int = wal.currentSize

proc reset*(wal: WriteAheadLog) =
  if wal.fs != nil:
    wal.fs.flushFile()
    wal.fs.close()
    wal.fs = nil
  var idx = 0
  while true:
    let path = segmentPath(wal.dir, idx)
    if not fileExists(path): break
    removeFile(path)
    inc idx
  wal.currentIndex = 0
  wal.openSegment()

proc totalSize*(wal: WriteAheadLog): int =
  var idx = 0
  var sum = 0
  while true:
    let path = segmentPath(wal.dir, idx)
    if not fileExists(path): break
    if idx == wal.currentIndex:
      sum += wal.currentSize
    else:
      try:
        let f = open(path, fmRead)
        sum += int(f.getFileSize())
        f.close()
      except IOError:
        discard
    inc idx
  sum

proc flush*(wal: WriteAheadLog) =
  ## Force flush the current WAL file to disk regardless of sync policy.
  acquire(wal.lock)
  defer: release(wal.lock)
  if wal.fs != nil:
    wal.fs.flushFile()

# Thread-local body buffer reused across append() calls to avoid per-record
# StringStream and result-string heap allocations on the hot WAL write path.
var bodyStream {.threadvar.}: StringStream

proc encodeRecordBody(rec: WalRecord; ms: Stream) {.inline.} =
  writeVarUint(ms, uint64(rec.kind.ord))
  writeVarUint(ms, uint64(rec.collection.len)); ms.write(rec.collection)
  writeVarUint(ms, uint64(rec.docId.len)); ms.write(rec.docId)
  ms.write(rec.version)
  if rec.kind == wrPut:
    # Stash a placeholder for the payload length, then encode the value
    # directly into `ms` and patch the length in. Saves both the temporary
    # `payload` string allocation AND a second copy into the body buffer.
    let lenPos = ms.getPosition()
    writeVarUint(ms, 0'u64) # placeholder (varuint of 0 = 1 byte)
    let valStart = ms.getPosition()
    encodeInto(ms, rec.value)
    let valEnd = ms.getPosition()
    let valLen = uint64(valEnd - valStart)
    # Re-encode the length as a varuint at lenPos. If the new varuint is
    # longer than 1 byte we shift the payload to make room.
    var lenBuf: array[10, byte]
    var lenN = 0
    var v = valLen
    while true:
      var b = uint8(v and 0x7F)
      v = v shr 7
      if v != 0: b = b or 0x80'u8
      lenBuf[lenN] = b
      inc lenN
      if v == 0: break
    if lenN == 1:
      ms.setPosition(lenPos)
      ms.write(lenBuf[0])
      ms.setPosition(valEnd)
    else:
      # Shift payload right by (lenN - 1) bytes, then patch the length.
      let shift = lenN - 1
      let dataRef = StringStream(ms)
      let oldLen = dataRef.data.len
      dataRef.data.setLen(oldLen + shift)
      # memmove payload bytes
      let buf = addr dataRef.data[0]
      moveMem(cast[pointer](cast[uint](buf) + uint(valStart + shift)),
              cast[pointer](cast[uint](buf) + uint(valStart)),
              valEnd - valStart)
      copyMem(cast[pointer](cast[uint](buf) + uint(lenPos)),
              addr lenBuf[0], lenN)
      dataRef.setPosition(oldLen + shift)
  else:
    writeVarUint(ms, 0)
  writeVarUint(ms, uint64(rec.changeId.len)); if rec.changeId.len > 0: ms.write(rec.changeId)
  writeVarUint(ms, uint64(rec.originNode.len)); if rec.originNode.len > 0: ms.write(rec.originNode)
  ms.write(rec.hlc.wallMillis)
  ms.write(rec.hlc.counter)
  writeVarUint(ms, uint64(rec.hlc.nodeId.len)); if rec.hlc.nodeId.len > 0: ms.write(rec.hlc.nodeId)

proc resetBodyStream() {.inline.} =
  if bodyStream.isNil:
    bodyStream = newStringStream()
  else:
    bodyStream.setPosition(0)
    bodyStream.data.setLen(0)

proc writeFramedRecord(wal: WriteAheadLog; body: string): int {.inline.} =
  ## Writes header(varuint length) + crc(4 LE bytes) + body to wal.fs.
  ## Returns total bytes written.
  let crc = fnv1a32(body)
  # Header: varuint of body length. Worst case 10 bytes; stack-allocated.
  var hdr: array[14, byte]
  var hdrN = 0
  var v = uint64(body.len)
  while true:
    var b = uint8(v and 0x7F)
    v = v shr 7
    if v != 0: b = b or 0x80'u8
    hdr[hdrN] = b
    inc hdrN
    if v == 0: break
  hdr[hdrN]   = byte(crc and 0xFF)
  hdr[hdrN+1] = byte((crc shr 8) and 0xFF)
  hdr[hdrN+2] = byte((crc shr 16) and 0xFF)
  hdr[hdrN+3] = byte((crc shr 24) and 0xFF)
  let prefixLen = hdrN + 4
  wal.rotateIfNeeded(prefixLen + body.len)
  discard wal.fs.writeBuffer(addr hdr[0], prefixLen)
  if body.len > 0:
    discard wal.fs.writeBuffer(unsafeAddr body[0], body.len)
  prefixLen + body.len

proc append*(wal: WriteAheadLog; rec: WalRecord) =
  # Encode the record body OUTSIDE wal.lock — bodyStream is thread-local, so
  # parallel writers can serialize their record bytes concurrently and only
  # contend on the lock during the actual file write + flush.
  resetBodyStream()
  encodeRecordBody(rec, bodyStream)
  let body = bodyStream.data
  acquire(wal.lock)
  defer: release(wal.lock)
  let written = writeFramedRecord(wal, body)
  wal.currentSize += written
  case wal.syncMode
  of wsmAlways:
    wal.fs.flushFile()
    wal.bytesSinceFlush = 0
  of wsmInterval:
    wal.bytesSinceFlush += written
    if wal.bytesSinceFlush >= wal.flushEveryBytes:
      wal.fs.flushFile()
      wal.bytesSinceFlush = 0
  of wsmNone:
    discard

proc appendMany*(wal: WriteAheadLog; recs: openArray[WalRecord]) =
  # Encode every record outside wal.lock; only the file write + flush span
  # holds the lock. bodyStream is thread-local, so each per-record encode
  # reuses the same buffer (we snapshot via $bodyStream.data into a fresh
  # string before resetting on the next iteration).
  var bodies = newSeqOfCap[string](recs.len)
  for rec in recs:
    resetBodyStream()
    encodeRecordBody(rec, bodyStream)
    bodies.add(bodyStream.data)
  acquire(wal.lock)
  defer: release(wal.lock)
  var totalWritten = 0
  for body in bodies:
    let written = writeFramedRecord(wal, body)
    wal.currentSize += written
    totalWritten += written
  case wal.syncMode
  of wsmAlways:
    wal.fs.flushFile()
    wal.bytesSinceFlush = 0
  of wsmInterval:
    wal.bytesSinceFlush += totalWritten
    if wal.bytesSinceFlush >= wal.flushEveryBytes:
      wal.fs.flushFile()
      wal.bytesSinceFlush = 0
  of wsmNone:
    discard

iterator replay*(dir: string): WalRecord =
  var idx = 0
  while true:
    let path = segmentPath(dir, idx)
    if not fileExists(path): break
    var f = open(path, fmRead)
    var fs = newFileStream(f)
    # Validate segment header
    let magic = fs.readStr(WAL_MAGIC.len)
    if magic != WAL_MAGIC:
      fs.close()
      inc idx
      continue
    let ver = fs.readUint32()
    # Support v1 (legacy) and v2 (current). Skip unknown versions.
    if ver != 1'u32 and ver != 2'u32:
      fs.close()
      inc idx
      continue
    var ok = true
    while not fs.atEnd:
      try:
        let recLen = readVarUint(fs)
        # Guard record size to avoid pathological allocations
        if recLen > uint64(MAX_WAL_RECORD_SIZE):
          ok = false
          break
        let crcOnDisk = fs.readUint32()
        let body = if recLen > 0: fs.readStr(int(recLen)) else: ""
        if fnv1a32(body) != crcOnDisk:
          ok = false
          break
        var rs = newStringStream(body)
        let rType = WalRecordType(readVarUint(rs).int)
        let clen = int(readVarUint(rs)); let collection = rs.readStr(clen)
        let dlen = int(readVarUint(rs)); let docId = rs.readStr(dlen)
        let version = rs.readUint64()
        let vlen = int(readVarUint(rs))
        var val: Value
        if rType == wrPut and vlen > 0:
          let enc = rs.readStr(vlen)
          val = decode(enc)
        if ver == 1'u32:
          yield WalRecord(kind: rType, collection: collection, docId: docId, version: version, value: val)
        else:
          # v2: read replication metadata
          let cidLen = int(readVarUint(rs)); let changeId = if cidLen > 0: rs.readStr(cidLen) else: ""
          let onLen = int(readVarUint(rs)); let originNode = if onLen > 0: rs.readStr(onLen) else: ""
          let wall = rs.readInt64()
          let ctr = rs.readUint32()
          let hnLen = int(readVarUint(rs)); let hlcNode = if hnLen > 0: rs.readStr(hnLen) else: ""
          let rec = WalRecord(kind: rType, collection: collection, docId: docId, version: version, value: val, changeId: changeId, originNode: originNode, hlc: Hlc(wallMillis: wall, counter: ctr, nodeId: hlcNode))
          yield rec
      except IOError:
        ok = false
        break
    fs.close()
    # proceed to next segment even if this one had tail corruption
    inc idx

proc close*(wal: WriteAheadLog) =
  if wal.fs != nil:
    wal.fs.close()
    wal.fs = nil

proc setSyncPolicy*(wal: WriteAheadLog; mode: WalSyncMode; flushEveryBytes = 0) =
  wal.syncMode = mode
  if mode == wsmInterval and flushEveryBytes > 0:
    wal.flushEveryBytes = flushEveryBytes
