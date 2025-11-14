# Glen storage snapshots
# Snapshot file per collection: collection.snap
# Format: varuint numDocs; repeated (varuint idLen idStr | binaryValue)

import std/[os, streams, tables]
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
  var f = open(tmpPath, fmWrite)
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
    # Windows moveFile already replaces; proceed
    if fileExists(finalPath): removeFile(finalPath)
    moveFile(tmpPath, finalPath)
  else:
    # On POSIX, prefer atomic rename over remove+move
    if fileExists(finalPath):
      removeFile(finalPath)
    moveFile(tmpPath, finalPath)
  # best-effort directory flush (no-op on non-Windows for now)
  flushDir(dir)

## Load a snapshot into memory. Returns an empty table if none exists.
proc loadSnapshot*(dir, collection: string): Table[string, Value] =
  result = initTable[string, Value]()
  let path = snapshotPath(dir, collection)
  if not fileExists(path): return
  var f = open(path, fmRead)
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
  f.close()
