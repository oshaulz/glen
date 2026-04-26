# Glen DB high-level API

import std/[os, tables, locks, strutils, streams, hashes, algorithm, parseutils, random, sets]
import glen/types, glen/wal, glen/storage, glen/cache, glen/subscription, glen/txn
import glen/rwlock
import glen/index
import glen/geo
import glen/config
import glen/util
type
  ReplOp* = enum roPut, roDelete

  ReplChange* = object
    collection*: string
    docId*: string
    op*: ReplOp
    version*: uint64
    value*: Value
    changeId*: string
    originNode*: string
    hlc*: Hlc


# Per-collection state. All Tables here are mutated only under the relevant
# stripe write-lock (or, for repl metadata, under db.replLock); they are
# safe to share across threads once the owning ref is in db.collections.
type
  CollectionStore = ref object
    docs:             Table[string, Value]
    versions:         Table[string, uint64]
    indexes:          IndexesByName
    geoIndexes:       GeoIndexesByName
    polygonIndexes:   PolygonIndexesByName
    replMetaHlc:      Table[string, Hlc]
    replMetaChangeId: Table[string, string]
    # ---- Spillable mode (lazy / mmap'd snapshot) ----
    # When `snapshot` is non-nil we mirror the on-disk layout: `docs` only
    # holds the hot working set, the rest is faulted in on demand from the
    # mmap'd snapshot. `dirty` tracks docs whose in-memory value differs from
    # the snapshot (must not be evicted; must be written on next compact).
    # `deleted` tracks tombstones for snapshot-only docs that were removed.
    snapshot:         storage.SnapshotMmap
    dirty:            HashSet[string]
    deleted:          HashSet[string]
    hotDocCap:        int   # 0 = unbounded
    maxDirtyDocs:     int   # 0 = unbounded; otherwise cap on cs.dirty.len

  IndexKind = enum ikEq, ikGeo, ikPoly

  IndexManifestEntry = object
    kind: IndexKind
    collection: string
    name: string
    spec: string                  # eq: field expr; geo: "lon:lat"; poly: polygon field

  GlenDB* = ref object
    dir*: string
    wal*: WriteAheadLog
    collections: Table[string, CollectionStore]
    structLock: RwLock           # protects ONLY the outer 'collections' map
    cache: LruCache
    subs: SubscriptionManager
    lockStripes: seq[RwLock]
    # Replication sequencing / clock / log lock (separate from data locks)
    replLock: Lock
    # Replication
    nodeId*: string
    replSeq: uint64
    # change log (in-memory): seq -> change
    replLog: seq[(uint64, ReplChange)]
    # per-collection in-memory change log for fast export filtering
    replLogByCollection: Table[string, seq[(uint64, ReplChange)]]
    # local HLC state
    localHlc: Hlc
    # Replication peers cursors (peerId -> last exported/applied seq)
    peersCursors: Table[string, uint64]
    peersStatePath: string
    peersDirty: bool
    peersLastWriteMs: int64
    # Index manifest: persisted definitions of all indexes; rebuilt on open.
    indexManifestPath: string
    indexManifestLock: Lock

# ---- Index manifest persistence ----
#
# Format (text, one entry per line):
#   <kind>\t<collection>\t<name>\t<spec>
#
# kind ∈ "eq" | "geo" | "poly"
#   eq:   spec is the field expression ("name" or "name,profile.age")
#   geo:  spec is "<lonField>:<latField>"
#   poly: spec is the polygon field name
#
# Field names containing tabs are not supported. Lines starting with '#' are
# ignored. Atomic rewrites: write to a temp file then rename.

const IndexManifestFile = "indexes.manifest"

proc geoIndexFilePath(dir, collection, name: string): string {.inline.} =
  dir / (collection & "." & name & ".gri")

proc polygonIndexFilePath(dir, collection, name: string): string {.inline.} =
  dir / (collection & "." & name & ".gpi")

proc serializeManifestEntry(e: IndexManifestEntry): string =
  let kindStr = case e.kind
    of ikEq:   "eq"
    of ikGeo:  "geo"
    of ikPoly: "poly"
  kindStr & "\t" & e.collection & "\t" & e.name & "\t" & e.spec

proc parseManifestEntry(line: string): (bool, IndexManifestEntry) =
  let parts = line.split('\t')
  if parts.len != 4: return (false, IndexManifestEntry())
  let kind = case parts[0]
    of "eq":   ikEq
    of "geo":  ikGeo
    of "poly": ikPoly
    else: return (false, IndexManifestEntry())
  (true, IndexManifestEntry(kind: kind, collection: parts[1],
                            name: parts[2], spec: parts[3]))

proc loadIndexManifest(path: string): seq[IndexManifestEntry] =
  result = @[]
  if not fileExists(path): return
  try:
    for raw in readFile(path).split('\n'):
      let line = raw.strip()
      if line.len == 0 or line.startsWith("#"): continue
      let (ok, e) = parseManifestEntry(line)
      if ok: result.add(e)
  except IOError: discard

proc saveIndexManifestAtomic(path: string; entries: openArray[IndexManifestEntry]) =
  var lines: seq[string] = @[]
  for e in entries: lines.add(serializeManifestEntry(e))
  let payload = lines.join("\n") & (if lines.len > 0: "\n" else: "")
  let tmp = path & ".tmp"
  writeFile(tmp, payload)
  moveFile(tmp, path)

proc addManifestEntry(db: GlenDB; e: IndexManifestEntry) =
  acquire(db.indexManifestLock)
  defer: release(db.indexManifestLock)
  var entries = loadIndexManifest(db.indexManifestPath)
  # Replace if same (kind, collection, name) already exists
  var replaced = false
  for i in 0 ..< entries.len:
    if entries[i].kind == e.kind and entries[i].collection == e.collection and entries[i].name == e.name:
      entries[i] = e; replaced = true; break
  if not replaced: entries.add(e)
  saveIndexManifestAtomic(db.indexManifestPath, entries)

proc removeManifestEntry(db: GlenDB; kind: IndexKind; collection, name: string) =
  acquire(db.indexManifestLock)
  defer: release(db.indexManifestLock)
  var entries = loadIndexManifest(db.indexManifestPath)
  var kept: seq[IndexManifestEntry] = @[]
  for e in entries:
    if e.kind == kind and e.collection == collection and e.name == name: continue
    kept.add(e)
  saveIndexManifestAtomic(db.indexManifestPath, kept)

proc newCollectionStore(): CollectionStore =
  CollectionStore(
    docs:             initTable[string, Value](),
    versions:         initTable[string, uint64](),
    indexes:          initTable[string, Index](),
    geoIndexes:       initTable[string, GeoIndex](),
    polygonIndexes:   initTable[string, PolygonIndex](),
    replMetaHlc:      initTable[string, Hlc](),
    replMetaChangeId: initTable[string, string](),
    snapshot:         nil,
    dirty:            initHashSet[string](),
    deleted:          initHashSet[string](),
    hotDocCap:        0,
    maxDirtyDocs:     0
  )

# ---- Spillable mode helpers ----
#
# `lookupDoc` is the read primitive replacing direct `cs.docs[id]` access. It
# returns nil for missing/deleted docs; otherwise returns the value, faulting
# from the mmap'd snapshot when needed and populating cs.docs as a write-through
# cache. Eviction (if hotDocCap > 0) drops a non-dirty entry on overflow.
proc evictColdDoc(cs: CollectionStore) =
  if cs.hotDocCap <= 0 or cs.docs.len <= cs.hotDocCap: return
  if cs.snapshot.isNil: return     # nowhere to evict to
  # Find a non-dirty doc that's also in the snapshot index (so we can re-fetch).
  var victim = ""
  for id in cs.docs.keys:
    if id notin cs.dirty and id in cs.snapshot.index:
      victim = id; break
  if victim.len > 0:
    cs.docs.del(victim)

proc lookupDoc(cs: CollectionStore; docId: string): Value {.inline.} =
  ## Read with cache-fill on snapshot fault. Eager mode (no snapshot) collapses
  ## to a single Table lookup — same hot path as before spillable mode landed.
  if cs.isNil: return nil
  if cs.snapshot.isNil:
    return if docId in cs.docs: cs.docs[docId] else: nil
  if docId in cs.deleted: return nil
  if docId in cs.docs: return cs.docs[docId]
  result = loadDocFromMmap(cs.snapshot, docId)
  if not result.isNil:
    cs.docs[docId] = result
    if cs.hotDocCap > 0 and cs.docs.len > cs.hotDocCap:
      evictColdDoc(cs)

proc lookupDocBypass(cs: CollectionStore; docId: string): Value {.inline.} =
  ## Read variant that does NOT populate cs.docs. For bulk paths (getAll,
  ## index rebuild) where caching every doc would defeat the point of spill
  ## mode and trash the hot working set. Eager mode (no snapshot) also takes
  ## the single-Table-lookup fast path.
  if cs.isNil: return nil
  if cs.snapshot.isNil:
    return if docId in cs.docs: cs.docs[docId] else: nil
  if docId in cs.deleted: return nil
  if docId in cs.docs: return cs.docs[docId]
  loadDocFromMmap(cs.snapshot, docId)

proc hasDoc(cs: CollectionStore; docId: string): bool {.inline.} =
  if cs.isNil: return false
  if cs.snapshot.isNil: return docId in cs.docs
  if docId in cs.deleted: return false
  if docId in cs.docs: return true
  docId in cs.snapshot.index

proc markDirty(cs: CollectionStore; docId: string) {.inline.} =
  cs.dirty.incl(docId)
  cs.deleted.excl(docId)

proc markDeleted(cs: CollectionStore; docId: string) {.inline.} =
  cs.dirty.excl(docId)
  if not cs.snapshot.isNil and docId in cs.snapshot.index:
    cs.deleted.incl(docId)

iterator allDocIds(cs: CollectionStore): string =
  ## Walks every visible (live) docId across both the snapshot and the in-memory
  ## modifications. Order is unspecified; do not rely on it.
  if not cs.snapshot.isNil:
    for id in cs.snapshot.index.keys:
      if id notin cs.deleted: yield id
  for id in cs.docs.keys:
    if cs.snapshot.isNil or id notin cs.snapshot.index:
      yield id   # in-memory doc that didn't exist in the snapshot

proc materializeAllDocs(cs: CollectionStore): Table[string, Value] =
  ## Build a complete docId → Value map combining in-memory dirty docs with
  ## any snapshot-only docs (faulted via the bypass read so the hot table is
  ## not perturbed). Used by compact() and snapshotAll().
  result = initTable[string, Value]()
  for id in allDocIds(cs):
    let v = lookupDocBypass(cs, id)
    if not v.isNil:
      result[id] = v

proc checkDirtyBudget(cs: CollectionStore; addCount: int) =
  ## Raise ValueError if accepting `addCount` more dirty docs would push
  ## past the configured cap. No-op when maxDirtyDocs == 0 (unbounded).
  if cs.maxDirtyDocs <= 0: return
  if cs.dirty.len + addCount > cs.maxDirtyDocs:
    raise newException(ValueError,
      "Glen spill: would exceed maxDirtyDocs (" & $cs.dirty.len &
      " currently dirty + " & $addCount & " incoming > cap " &
      $cs.maxDirtyDocs & "). Call db.compact() to flush dirty entries to a " &
      "fresh snapshot, or chunk the operation into smaller batches.")

# Generate a stable, unique-ish node id and persist it to disk when needed.
proc bytesToHex(bytes: openArray[byte]): string =
  const hexdigits = "0123456789abcdef"
  result = newString(bytes.len * 2)
  var j = 0
  for b in bytes:
    let v = int(b)
    result[j] = hexdigits[(v shr 4) and 0xF]
    inc j
    result[j] = hexdigits[v and 0xF]
    inc j

proc generateStableNodeId(): string =
  ## Uses wall-clock millis and 16 random bytes hex-encoded.
  ## Randomness is seeded from time to avoid duplicate IDs across processes.
  randomize()
  var bytes = newSeq[byte](16)
  for i in 0 ..< bytes.len:
    bytes[i] = byte(rand(255))
  result = $nowMillis() & ":" & bytesToHex(bytes)

## Create or open a Glen database at the given directory.
## Loads snapshots, replays the WAL, and initializes cache and subscriptions.
proc newGlenDB*(dir: string;
                cacheCapacity = 64*1024*1024;
                cacheShards = 16;
                walSync: WalSyncMode = wsmInterval;
                walFlushEveryBytes = 8*1024*1024;
                lockStripesCount = 32;
                spillableMode = false;
                hotDocCap = 0;
                maxDirtyDocs = 0): GlenDB =
  ## `spillableMode = true` opens any existing snapshot v2 file via mmap and
  ## defers loading docs until they're touched. Combined with `hotDocCap > 0`,
  ## cold non-dirty docs are evicted from RAM under pressure and faulted back
  ## from the mmap'd snapshot on next access. Use this when your dataset is
  ## bigger than RAM or when you only need to query a small fraction of it.
  ##
  ## Mutations: writes still go to memory (and the WAL) and are pinned there
  ## as "dirty" — they can't be evicted until the next compact() folds them
  ## into a fresh snapshot. `maxDirtyDocs > 0` caps the per-collection dirty
  ## set; multi-doc operations (commit, applyChanges, putMany, deleteMany)
  ## that would exceed the cap raise ValueError with a clear message asking
  ## you to compact or chunk.
  result = GlenDB(
    dir: dir,
    wal: openWriteAheadLog(dir, syncMode = walSync, flushEveryBytes = walFlushEveryBytes),
    collections: initTable[string, CollectionStore](),
    cache: newLruCache(cacheCapacity, numShards = cacheShards),
    subs: newSubscriptionManager(),
    replSeq: 0,
    replLog: @[],
    replLogByCollection: initTable[string, seq[(uint64, ReplChange)]]()
  )
  initLock(result.replLock)
  initLock(result.indexManifestLock)
  initRwLock(result.structLock)
  result.lockStripes.setLen(lockStripesCount)
  for i in 0 ..< lockStripesCount:
    initRwLock(result.lockStripes[i])
  result.indexManifestPath = dir / IndexManifestFile
  # derive nodeId if provided
  let cfg = loadConfig()
  # Persisted/stable node id
  let nodeIdPath = dir / "node.id"
  if cfg.nodeId.len > 0:
    result.nodeId = cfg.nodeId
    # if no file exists, persist
    if not fileExists(nodeIdPath): writeFile(nodeIdPath, result.nodeId)
  elif fileExists(nodeIdPath):
    try:
      result.nodeId = readFile(nodeIdPath).strip()
      if result.nodeId.len == 0:
        result.nodeId = generateStableNodeId()
        writeFile(nodeIdPath, result.nodeId)
    except IOError:
      result.nodeId = generateStableNodeId()
      writeFile(nodeIdPath, result.nodeId)
  else:
    result.nodeId = generateStableNodeId()
    writeFile(nodeIdPath, result.nodeId)
  # init local HLC
  result.localHlc = Hlc(wallMillis: nowMillis(), counter: 0'u32, nodeId: result.nodeId)
  # load snapshots — eager unless spillableMode is on, in which case we mmap
  # the v2 snapshot's index and leave cs.docs empty (faulted in lazily).
  for kind, path in walkDir(dir):
    if kind == pcFile and path.endsWith(".snap"):
      let name = splitFile(path).name
      let cs = newCollectionStore()
      cs.hotDocCap = hotDocCap
      cs.maxDirtyDocs = maxDirtyDocs
      if spillableMode:
        let mm = openSnapshotMmap(dir, name)
        if not mm.isNil and mm.isV2:
          cs.snapshot = mm
          # cs.docs starts empty; lookupDoc will fault from mm on demand.
        else:
          # v1 file or no snapshot — fall back to eager load. Spill won't kick
          # in until the next compact() upgrades the snapshot to v2.
          cs.docs = loadSnapshot(dir, name)
      else:
        cs.docs = loadSnapshot(dir, name)
      result.collections[name] = cs
  # peers state path
  result.peersStatePath = dir / "peers.state"
  result.peersCursors = initTable[string, uint64]()
  result.peersDirty = false
  result.peersLastWriteMs = 0
  if fileExists(result.peersStatePath):
    try:
      let content = readFile(result.peersStatePath)
      for line in content.split('\n'):
        let ln = line.strip()
        if ln.len == 0: continue
        let parts = ln.splitWhitespace()
        if parts.len == 2:
          var n: int
          if parseInt(parts[1], n) == parts[1].len and n >= 0:
            result.peersCursors[parts[0]] = uint64(n)
    except IOError:
      discard
  # Read the index manifest BEFORE replay so we can pre-load any persisted
  # spatial indexes (.gri/.gpi) and let the WAL replay incrementally update
  # them via the existing hooks. Index entries without a dump file (or with a
  # corrupted one) get bulk-built post-replay from the loaded docs.
  let manifestEntries = loadIndexManifest(result.indexManifestPath)
  var loadedFromDump = initTable[string, bool]()
  for entry in manifestEntries:
    if entry.collection notin result.collections:
      result.collections[entry.collection] = newCollectionStore()
    let cs = result.collections[entry.collection]
    let key = $ord(entry.kind) & "|" & entry.collection & "|" & entry.name
    case entry.kind
    of ikEq:
      discard   # equality indexes don't have binary dumps; bulk-build below
    of ikGeo:
      let parts = entry.spec.split(':')
      if parts.len != 2: continue
      let gix = newGeoIndex(entry.name, parts[0], parts[1])
      let path = geoIndexFilePath(result.dir, entry.collection, entry.name)
      if tryLoadGeoIndex(gix, path):
        cs.geoIndexes[entry.name] = gix
        loadedFromDump[key] = true
    of ikPoly:
      let pix = newPolygonIndex(entry.name, entry.spec)
      let path = polygonIndexFilePath(result.dir, entry.collection, entry.name)
      if tryLoadPolygonIndex(pix, path):
        cs.polygonIndexes[entry.name] = pix
        loadedFromDump[key] = true
  # replay WAL (also rebuild versions/indexes and in-memory replication log).
  for rec in replay(dir):
    if rec.collection notin result.collections:
      result.collections[rec.collection] = newCollectionStore()
    let cs = result.collections[rec.collection]
    if rec.kind == wrPut:
      cs.docs[rec.docId] = rec.value
      markDirty(cs, rec.docId)   # post-snapshot mutation; do not evict
      cs.versions[rec.docId] = rec.version
      for _, idx in cs.indexes:
        idx.indexDoc(rec.docId, rec.value)
      for _, gix in cs.geoIndexes:
        gix.indexDoc(rec.docId, rec.value)
      for _, pix in cs.polygonIndexes:
        pix.indexDoc(rec.docId, rec.value)
    else:
      let oldDoc = lookupDoc(cs, rec.docId)
      if not oldDoc.isNil:
        for _, idx in cs.indexes:
          idx.unindexDoc(rec.docId, oldDoc)
        for _, gix in cs.geoIndexes:
          gix.unindexDoc(rec.docId, oldDoc)
        for _, pix in cs.polygonIndexes:
          pix.unindexDoc(rec.docId, oldDoc)
        if rec.docId in cs.docs: cs.docs.del(rec.docId)
        markDeleted(cs, rec.docId)
      if rec.docId in cs.versions:
        cs.versions.del(rec.docId)
    # Rebuild in-memory repl log cursor from WAL (uses v2 metadata when present)
    inc result.replSeq
    var ch: ReplChange
    ch.collection = rec.collection
    ch.docId = rec.docId
    ch.op = (if rec.kind == wrPut: roPut else: roDelete)
    ch.version = rec.version
    ch.value = rec.value
    ch.changeId = rec.changeId
    ch.originNode = rec.originNode
    ch.hlc = rec.hlc
    result.replLog.add((result.replSeq, ch))
    if ch.collection notin result.replLogByCollection:
      result.replLogByCollection[ch.collection] = @[]
    result.replLogByCollection[ch.collection].add((result.replSeq, ch))
    # Restore per-doc replication metadata so HLC-based conflict resolution
    # works correctly across restarts.
    if ch.changeId.len > 0:
      cs.replMetaHlc[ch.docId] = ch.hlc
      cs.replMetaChangeId[ch.docId] = ch.changeId
      # Advance local HLC past the highest seen value.
      if ch.hlc.wallMillis > result.localHlc.wallMillis:
        result.localHlc.wallMillis = ch.hlc.wallMillis
        result.localHlc.counter = ch.hlc.counter
      elif ch.hlc.wallMillis == result.localHlc.wallMillis and ch.hlc.counter > result.localHlc.counter:
        result.localHlc.counter = ch.hlc.counter
  # Build any indexes that didn't get pre-loaded from a dump file.
  for entry in manifestEntries:
    let key = $ord(entry.kind) & "|" & entry.collection & "|" & entry.name
    if key in loadedFromDump: continue
    if entry.collection notin result.collections: continue
    let cs = result.collections[entry.collection]
    # Materialise the full doc set (snapshot + in-memory) into a transient
    # Table so eq/geo/polygon index builders see EVERY doc, including
    # snapshot-only ones in spill mode. Bypass-read so we don't fault them
    # all into cs.docs.
    var allDocs = initTable[string, Value]()
    for id in allDocIds(cs):
      let v = lookupDocBypass(cs, id)
      if not v.isNil: allDocs[id] = v
    case entry.kind
    of ikEq:
      let idx = newIndex(entry.name, entry.spec)
      for id, v in allDocs: idx.indexDoc(id, v)
      cs.indexes[entry.name] = idx
    of ikGeo:
      let parts = entry.spec.split(':')
      if parts.len != 2: continue
      let gix = newGeoIndex(entry.name, parts[0], parts[1])
      gix.bulkBuild(allDocs)
      cs.geoIndexes[entry.name] = gix
    of ikPoly:
      let pix = newPolygonIndex(entry.name, entry.spec)
      pix.bulkBuild(allDocs)
      cs.polygonIndexes[entry.name] = pix

# ---- Replication peers state and log GC ----
const PeersStateFlushDebounceMs = 500

proc snapshotPeersStateLocked(db: GlenDB): string =
  var lines: seq[string] = @[]
  for peer, seq in db.peersCursors:
    lines.add(peer & " " & $seq)
  lines.join("\n")

proc flushPeersState(db: GlenDB; force = false) =
  var payload = ""
  var shouldWrite = false
  acquire(db.replLock)
  if db.peersDirty:
    let now = nowMillis()
    if force or db.peersLastWriteMs == 0 or (now - db.peersLastWriteMs) >= PeersStateFlushDebounceMs:
      payload = snapshotPeersStateLocked(db)
      db.peersDirty = false
      db.peersLastWriteMs = now
      shouldWrite = true
  release(db.replLock)
  if shouldWrite:
    writeFile(db.peersStatePath, payload)

proc setPeerCursor*(db: GlenDB; peerId: string; seq: uint64) =
  acquire(db.replLock)
  db.peersCursors[peerId] = seq
  db.peersDirty = true
  release(db.replLock)
  db.flushPeersState()

proc getPeerCursor*(db: GlenDB; peerId: string): uint64 =
  acquire(db.replLock)
  let res = (if peerId in db.peersCursors: db.peersCursors[peerId] else: 0'u64)
  release(db.replLock)
  res

proc minPeerCursor(db: GlenDB): uint64 =
  var minVal: uint64 = high(uint64)
  if db.peersCursors.len == 0: return 0'u64
  for _, seq in db.peersCursors:
    if seq < minVal: minVal = seq
  minVal

proc gcReplLog*(db: GlenDB) =
  ## Trim in-memory replication log up to the minimum acknowledged cursor across peers.
  acquire(db.replLock)
  defer: release(db.replLock)
  if db.peersCursors.len == 0: return
  var cutoff: uint64 = high(uint64)
  for _, seq in db.peersCursors:
    if seq < cutoff: cutoff = seq
  if cutoff == 0'u64: return
  # seqNo is appended monotonically under db.replLock, so binary search the
  # first index whose seqNo > cutoff and slice off the prefix.
  proc firstAfter(s: seq[(uint64, ReplChange)]; cutoff: uint64): int =
    var lo = 0
    var hi = s.len
    while lo < hi:
      let mid = (lo + hi) shr 1
      if s[mid][0] <= cutoff: lo = mid + 1
      else: hi = mid
    lo
  let cut = firstAfter(db.replLog, cutoff)
  if cut > 0:
    db.replLog = db.replLog[cut .. ^1]
  for coll, seqs in db.replLogByCollection.mpairs:
    let cutc = firstAfter(seqs, cutoff)
    if cutc > 0:
      seqs = seqs[cutc .. ^1]

# ---- Collection lifecycle ----
#
# Two access patterns:
#
#   tryGetCollection(db, name) -> CollectionStore or nil
#     Read-only. Cheap; takes structLock in read mode briefly. The returned
#     ref stays valid even if the outer table later rehashes.
#
#   getOrCreateCollection(db, name) -> CollectionStore
#     For write paths. Creates the collection if missing under structLock
#     write mode (using double-checked locking to keep the common case on
#     the read fast path).
#
# Once you hold the CollectionStore ref, the outer table is irrelevant and
# subsequent operations can proceed under the per-collection stripe lock.

proc tryGetCollection(db: GlenDB; collection: string): CollectionStore {.inline.} =
  acquireRead(db.structLock)
  if collection in db.collections:
    result = db.collections[collection]
  else:
    result = nil
  releaseRead(db.structLock)

proc getOrCreateCollection(db: GlenDB; collection: string): CollectionStore =
  # Fast path: most ops touch existing collections.
  acquireRead(db.structLock)
  if collection in db.collections:
    result = db.collections[collection]
    releaseRead(db.structLock)
    return
  releaseRead(db.structLock)
  # Slow path: structurally insert, double-checked under write mode.
  acquireWrite(db.structLock)
  if collection notin db.collections:
    db.collections[collection] = newCollectionStore()
  result = db.collections[collection]
  releaseWrite(db.structLock)

proc ensureCollection*(db: GlenDB; collection: string) =
  ## Pre-create a collection. Optional — `put`/`delete`/`commit`/`applyChanges`
  ## all create lazily and are safe under arbitrary concurrent first-writes.
  ## Useful as a hint, e.g. when bulk-loading and you want to materialise the
  ## indexes table before fanning out.
  discard db.getOrCreateCollection(collection)

# ---- Stripe locking helpers ----
proc stripeIndex(db: GlenDB; collection: string): int =
  if db.lockStripes.len == 0: return 0
  abs(hash(collection).int) mod db.lockStripes.len

proc acquireStripeRead(db: GlenDB; collection: string) =
  acquireRead(db.lockStripes[db.stripeIndex(collection)])

proc releaseStripeRead(db: GlenDB; collection: string) =
  releaseRead(db.lockStripes[db.stripeIndex(collection)])

proc acquireStripeWrite(db: GlenDB; collection: string) =
  acquireWrite(db.lockStripes[db.stripeIndex(collection)])

proc releaseStripeWrite(db: GlenDB; collection: string) =
  releaseWrite(db.lockStripes[db.stripeIndex(collection)])

proc acquireAllStripesWrite(db: GlenDB) =
  for i in 0 ..< db.lockStripes.len:
    acquireWrite(db.lockStripes[i])

proc releaseAllStripesWrite(db: GlenDB) =
  var i = db.lockStripes.len - 1
  while i >= 0:
    releaseWrite(db.lockStripes[i])
    if i == 0: break
    dec i

proc acquireStripesWrite(db: GlenDB; stripes: seq[int]) =
  var uniq = stripes
  uniq.sort(system.cmp[int])
  var idx = 0
  while idx < uniq.len:
    let i = uniq[idx]
    acquireWrite(db.lockStripes[i])
    inc idx
    while idx < uniq.len and uniq[idx] == i:
      inc idx

proc releaseStripesWrite(db: GlenDB; stripes: seq[int]) =
  var uniq = stripes
  uniq.sort(system.cmp[int])
  if uniq.len == 0: return
  var idx = uniq.len - 1
  while true:
    let i = uniq[idx]
    # skip duplicates while moving backward
    var j = idx
    while j > 0 and uniq[j-1] == i:
      dec j
    releaseWrite(db.lockStripes[i])
    if j == 0: break
    idx = j - 1

# ---- HLC helpers ----
proc nextLocalHlc(db: GlenDB): Hlc =
  let now = nowMillis()
  if now > db.localHlc.wallMillis:
    db.localHlc.wallMillis = now
    db.localHlc.counter = 0'u32
  else:
    db.localHlc.counter = db.localHlc.counter + 1
  db.localHlc

proc mergeRemoteHlc(db: GlenDB; remote: Hlc) =
  # Advance local clock on receipt to maintain monotonicity
  if remote.wallMillis > db.localHlc.wallMillis:
    db.localHlc.wallMillis = remote.wallMillis
    db.localHlc.counter = remote.counter
  elif remote.wallMillis == db.localHlc.wallMillis and remote.counter > db.localHlc.counter:
    db.localHlc.counter = remote.counter

## Get the current value of a document by collection and id.
## Returns nil if not found. Cached reads are served from the LRU cache.
proc faultInDocFromSnapshot(db: GlenDB; cs: CollectionStore;
                            collection, docId: string): Value =
  ## Spillable-mode helper. Acquires the stripe write lock, double-checks the
  ## hot table, then loads from the mmap'd snapshot and populates cs.docs.
  ## Returns nil if the doc isn't in the snapshot or has been deleted.
  if cs.snapshot.isNil: return nil
  if docId in cs.deleted: return nil
  if docId notin cs.snapshot.index: return nil
  db.acquireStripeWrite(collection)
  defer: db.releaseStripeWrite(collection)
  if docId in cs.docs: return cs.docs[docId]   # raced with another faulter
  if docId in cs.deleted: return nil
  let v = loadDocFromMmap(cs.snapshot, docId)
  if v.isNil: return nil
  cs.docs[docId] = v
  if cs.hotDocCap > 0 and cs.docs.len > cs.hotDocCap:
    evictColdDoc(cs)
  return v

proc get*(db: GlenDB; collection, docId: string): Value {.inline.} =
  let key = collection & ":" & docId
  let cached = db.cache.get(key)
  if cached != nil:
    return cached.clone()
  let cs = db.tryGetCollection(collection)
  if cs.isNil: return nil
  db.acquireStripeRead(collection)
  if docId in cs.deleted:
    db.releaseStripeRead(collection)
    return nil
  if docId in cs.docs:
    let v = cs.docs[docId]
    db.cache.put(key, v)
    let cloned = v.clone()
    db.releaseStripeRead(collection)
    return cloned
  let needFault = (not cs.snapshot.isNil) and docId in cs.snapshot.index
  db.releaseStripeRead(collection)
  if needFault:
    let v = db.faultInDocFromSnapshot(cs, collection, docId)
    if not v.isNil:
      db.cache.put(key, v)
      return v.clone()
  return nil

# Borrowed (non-cloned) read. Caller must not mutate returned Value.
proc getBorrowed*(db: GlenDB; collection, docId: string): Value {.inline.} =
  let key = collection & ":" & docId
  let cached = db.cache.get(key)
  if cached != nil:
    return cached
  let cs = db.tryGetCollection(collection)
  if cs.isNil: return nil
  db.acquireStripeRead(collection)
  if docId in cs.deleted:
    db.releaseStripeRead(collection)
    return nil
  if docId in cs.docs:
    let v = cs.docs[docId]
    db.cache.put(key, v)
    db.releaseStripeRead(collection)
    return v
  let needFault = (not cs.snapshot.isNil) and docId in cs.snapshot.index
  db.releaseStripeRead(collection)
  if needFault:
    let v = db.faultInDocFromSnapshot(cs, collection, docId)
    if not v.isNil:
      db.cache.put(key, v)
      return v
  return nil

# Internal: read the current version of (c, docId), 0 if missing. Caller must
# already hold the appropriate stripe lock OR the txn-commit phase (which
# blocks all writers to the relevant stripes).
proc currentVersionFromStore(cs: CollectionStore; docId: string): uint64 {.inline.} =
  if cs.isNil: return 0
  if docId in cs.versions: cs.versions[docId] else: 0'u64

# Transaction-aware read: records version for OCC
## Transaction-aware get. Records the version read into the provided transaction
## for optimistic concurrency control.
proc get*(db: GlenDB; collection, docId: string; t: Txn): Value =
  let v = db.get(collection, docId)
  if v != nil:
    let cs = db.tryGetCollection(collection)
    let ver = currentVersionFromStore(cs, docId)
    t.recordRead(Id(collection: collection, docId: docId, version: ver))
  return v

## Batch get by ids. Returns pairs of (docId, Value) for those found.
## Uses the cache when available and acquires the DB read lock once per call.
## In spillable mode, snapshot-only docs are decoded straight from the mmap'd
## region without populating cs.docs (so a giant getMany doesn't trash the
## hot working set).
proc getMany*(db: GlenDB; collection: string; docIds: openArray[string]): seq[(string, Value)] =
  result = @[]
  var missing: seq[string] = @[]
  for id in docIds:
    let key = collection & ":" & id
    let cached = db.cache.get(key)
    if cached != nil:
      result.add((id, cached.clone()))
    else:
      missing.add(id)
  if missing.len == 0: return
  let cs = db.tryGetCollection(collection)
  if cs.isNil: return
  db.acquireStripeRead(collection)
  for id in missing:
    let v = lookupDocBypass(cs, id)
    if not v.isNil:
      db.cache.put(collection & ":" & id, v)
      result.add((id, v.clone()))
  db.releaseStripeRead(collection)

## Borrowed batch get: returns refs without cloning. Caller must not mutate.
proc getBorrowedMany*(db: GlenDB; collection: string; docIds: openArray[string]): seq[(string, Value)] =
  result = @[]
  var missing: seq[string] = @[]
  for id in docIds:
    let key = collection & ":" & id
    let cached = db.cache.get(key)
    if cached != nil:
      result.add((id, cached))
    else:
      missing.add(id)
  if missing.len == 0: return
  let cs = db.tryGetCollection(collection)
  if cs.isNil: return
  db.acquireStripeRead(collection)
  for id in missing:
    let v = lookupDocBypass(cs, id)
    if not v.isNil:
      db.cache.put(collection & ":" & id, v)
      result.add((id, v))
  db.releaseStripeRead(collection)

## Transaction-aware batch get. Records read versions for OCC.
proc getMany*(db: GlenDB; collection: string; docIds: openArray[string]; t: Txn): seq[(string, Value)] =
  let pairs = db.getMany(collection, docIds)
  let cs = db.tryGetCollection(collection)
  for (id, _) in pairs:
    let ver = currentVersionFromStore(cs, id)
    t.recordRead(Id(collection: collection, docId: id, version: ver))
  return pairs

## Get all documents in a collection. Returns pairs of (docId, Value).
## In spillable mode, walks both the in-memory and snapshot-only docs without
## faulting the snapshot ones into cs.docs.
proc getAll*(db: GlenDB; collection: string): seq[(string, Value)] =
  result = @[]
  let cs = db.tryGetCollection(collection)
  if cs.isNil: return
  db.acquireStripeRead(collection)
  for id in allDocIds(cs):
    let v = lookupDocBypass(cs, id)
    if not v.isNil:
      db.cache.put(collection & ":" & id, v)
      result.add((id, v.clone()))
  db.releaseStripeRead(collection)

## Borrowed getAll: returns refs without cloning. Caller must not mutate.
proc getBorrowedAll*(db: GlenDB; collection: string): seq[(string, Value)] =
  result = @[]
  let cs = db.tryGetCollection(collection)
  if cs.isNil: return
  db.acquireStripeRead(collection)
  for id in allDocIds(cs):
    let v = lookupDocBypass(cs, id)
    if not v.isNil:
      db.cache.put(collection & ":" & id, v)
      result.add((id, v))
  db.releaseStripeRead(collection)

## Transaction-aware getAll. Records read versions for all returned docs.
proc getAll*(db: GlenDB; collection: string; t: Txn): seq[(string, Value)] =
  let pairs = db.getAll(collection)
  let cs = db.tryGetCollection(collection)
  for (id, _) in pairs:
    let ver = currentVersionFromStore(cs, id)
    t.recordRead(Id(collection: collection, docId: id, version: ver))
  return pairs

## Upsert a document value. Appends to WAL, updates in-memory state, versions,
## cache, and notifies subscribers.
proc put*(db: GlenDB; collection, docId: string; value: Value) =
  var notifications: seq[(Id, Value)] = @[]
  var fieldNotifications: seq[(Id, Value, Value)] = @[]
  let cs = db.getOrCreateCollection(collection)
  db.acquireStripeWrite(collection)
  let oldVer = currentVersionFromStore(cs, docId)
  let newVer = oldVer + 1
  var stored = value.clone()
  # assign replication metadata (under replLock)
  acquire(db.replLock)
  inc db.replSeq
  let chHlc = db.nextLocalHlc()
  let chChangeId = $db.replSeq & ":" & db.nodeId
  let ch = ReplChange(collection: collection, docId: docId, op: roPut, version: newVer, value: stored, changeId: chChangeId, originNode: db.nodeId, hlc: chHlc)
  db.replLog.add((db.replSeq, ch))
  if collection notin db.replLogByCollection: db.replLogByCollection[collection] = @[]
  db.replLogByCollection[collection].add((db.replSeq, ch))
  release(db.replLock)
  # append to WAL before mutating (write-ahead)
  db.wal.append(WalRecord(kind: wrPut, collection: collection, docId: docId, version: newVer, value: stored, changeId: chChangeId, originNode: db.nodeId, hlc: chHlc))
  cs.replMetaHlc[docId] = chHlc
  cs.replMetaChangeId[docId] = chChangeId
  let oldDoc = lookupDoc(cs, docId)   # faults from snapshot if needed
  cs.docs[docId] = stored
  markDirty(cs, docId)
  cs.versions[docId] = newVer
  db.cache.put(collection & ":" & docId, stored)
  for _, idx in cs.indexes:
    idx.reindexDoc(docId, oldDoc, stored)
  for _, gix in cs.geoIndexes:
    gix.reindexDoc(docId, oldDoc, stored)
  for _, pix in cs.polygonIndexes:
    pix.reindexDoc(docId, oldDoc, stored)
  notifications.add((Id(collection: collection, docId: docId, version: newVer), stored))
  fieldNotifications.add((Id(collection: collection, docId: docId, version: newVer), oldDoc, stored))
  db.releaseStripeWrite(collection)
  for it in notifications:
    db.subs.notify(it[0], it[1])
  for it in fieldNotifications:
    db.subs.notifyFieldChanges(it[0], it[1], it[2])

## Delete a document if it exists. Appends a delete to WAL, removes from
## in-memory state and cache, bumps the version, and notifies subscribers.
proc delete*(db: GlenDB; collection, docId: string) =
  var notifications: seq[(Id, Value)] = @[]
  var fieldNotifications: seq[(Id, Value, Value)] = @[]
  let cs = db.tryGetCollection(collection)
  if cs.isNil: return
  db.acquireStripeWrite(collection)
  let oldDoc = lookupDoc(cs, docId)   # may fault from snapshot
  if not oldDoc.isNil:
    let ver = currentVersionFromStore(cs, docId) + 1
    # assign replication metadata (under replLock)
    acquire(db.replLock)
    inc db.replSeq
    let chHlc = db.nextLocalHlc()
    let chChangeId = $db.replSeq & ":" & db.nodeId
    let ch = ReplChange(collection: collection, docId: docId, op: roDelete, version: ver, value: nil, changeId: chChangeId, originNode: db.nodeId, hlc: chHlc)
    db.replLog.add((db.replSeq, ch))
    if collection notin db.replLogByCollection: db.replLogByCollection[collection] = @[]
    db.replLogByCollection[collection].add((db.replSeq, ch))
    release(db.replLock)
    # append to WAL before mutating (write-ahead)
    db.wal.append(WalRecord(kind: wrDelete, collection: collection, docId: docId, version: ver, changeId: chChangeId, originNode: db.nodeId, hlc: chHlc))
    cs.replMetaHlc[docId] = chHlc
    cs.replMetaChangeId[docId] = chChangeId
    for _, idx in cs.indexes:
      idx.unindexDoc(docId, oldDoc)
    for _, gix in cs.geoIndexes:
      gix.unindexDoc(docId, oldDoc)
    for _, pix in cs.polygonIndexes:
      pix.unindexDoc(docId, oldDoc)
    if docId in cs.docs: cs.docs.del(docId)
    markDeleted(cs, docId)
    if docId in cs.versions: cs.versions.del(docId)
    db.cache.del(collection & ":" & docId)
    notifications.add((Id(collection: collection, docId: docId, version: ver), VNull()))
    fieldNotifications.add((Id(collection: collection, docId: docId, version: ver), oldDoc, nil))
  db.releaseStripeWrite(collection)
  for it in notifications:
    db.subs.notify(it[0], it[1])
  for it in fieldNotifications:
    db.subs.notifyFieldChanges(it[0], it[1], it[2])

# Transaction support
## Begin a new transaction (optimistic).
proc beginTxn*(db: GlenDB): Txn = newTxn()

## Return the current version for a (collection, docId), or 0 if not present.
proc currentVersion*(db: GlenDB; collection, docId: string): uint64 =
  let cs = db.tryGetCollection(collection)
  currentVersionFromStore(cs, docId)

## Attempt to commit the provided transaction using optimistic concurrency.
## Validates recorded read versions against current versions and applies staged
## writes on success. Returns a CommitResult with status and message.
proc commit*(db: GlenDB; t: Txn): CommitResult =
  var notifications: seq[(Id, Value)] = @[]
  var fieldNotifications: seq[(Id, Value, Value)] = @[]
  # Pre-flight: per-collection dirty budget check (spillable mode only).
  # Returns csInvalid (not csOk) on overflow so the caller sees a clear
  # message; the txn isn't applied at all.
  var perColl = initTable[string, int]()
  for key, _ in t.writes:
    perColl[key[0]] = perColl.getOrDefault(key[0], 0) + 1
  for c, count in perColl:
    let cs = db.tryGetCollection(c)
    if not cs.isNil and cs.maxDirtyDocs > 0 and
       cs.dirty.len + count > cs.maxDirtyDocs:
      t.state = tsRolledBack
      return CommitResult(status: csInvalid,
        message: "spill: would exceed maxDirtyDocs in collection " & c &
          " (" & $cs.dirty.len & " currently dirty + " & $count &
          " incoming > cap " & $cs.maxDirtyDocs &
          "). Compact() or chunk the txn.")
  # Acquire all needed collection stripes in sorted order. We must include
  # stripes for read-only keys too, otherwise a concurrent writer to a
  # collection we read from (but did not write) can change the version
  # between our recordRead and this validation, causing a missed conflict.
  var stripes: seq[int] = @[]
  for key, _ in t.writes:
    stripes.add(db.stripeIndex(key[0]))
  for k, _ in t.readVersions:
    stripes.add(db.stripeIndex(k[0]))
  db.acquireStripesWrite(stripes)
  if t.state != tsActive:
    db.releaseStripesWrite(stripes)
    return CommitResult(status: csInvalid, message: "Transaction not active")
  # validate under stripes; writers to these keys are blocked
  for k, ver in t.readVersions:
    let cur = db.currentVersion(k[0], k[1])
    if cur != ver:
      t.state = tsRolledBack
      db.releaseStripesWrite(stripes)
      return CommitResult(status: csConflict, message: "Version mismatch for " & k[0] & ":" & k[1])
  # apply writes (compute WAL with replication metadata, then mutate, then log)
  var walRecs: seq[WalRecord] = @[]
  var replEntries: seq[(uint64, ReplChange)] = @[]
  for key, w in t.writes:
    let collection = key[0]
    let docId = key[1]
    if w.kind == twDelete:
      let cs = db.tryGetCollection(collection)
      let oldDoc = if cs.isNil: nil else: lookupDoc(cs, docId)
      if not cs.isNil and not oldDoc.isNil:
        let newVer = currentVersionFromStore(cs, docId) + 1
        # replication metadata under replLock
        acquire(db.replLock)
        inc db.replSeq
        let chHlc = db.nextLocalHlc()
        let chChangeId = $db.replSeq & ":" & db.nodeId
        let ch = ReplChange(collection: collection, docId: docId, op: roDelete, version: newVer, value: nil, changeId: chChangeId, originNode: db.nodeId, hlc: chHlc)
        replEntries.add((db.replSeq, ch))
        cs.replMetaHlc[docId] = chHlc
        cs.replMetaChangeId[docId] = chChangeId
        release(db.replLock)
        # write-ahead
        walRecs.add(WalRecord(kind: wrDelete, collection: collection, docId: docId, version: newVer, changeId: chChangeId, originNode: db.nodeId, hlc: chHlc))
        for _, idx in cs.indexes:
          idx.unindexDoc(docId, oldDoc)
        for _, gix in cs.geoIndexes:
          gix.unindexDoc(docId, oldDoc)
        for _, pix in cs.polygonIndexes:
          pix.unindexDoc(docId, oldDoc)
        if docId in cs.docs: cs.docs.del(docId)
        markDeleted(cs, docId)
        if docId in cs.versions: cs.versions.del(docId)
        db.cache.del(collection & ":" & docId)
        notifications.add((Id(collection: collection, docId: docId, version: newVer), VNull()))
        fieldNotifications.add((Id(collection: collection, docId: docId, version: newVer), oldDoc, nil))
    else:
      let cs = db.getOrCreateCollection(collection)
      let newVer = currentVersionFromStore(cs, docId) + 1
      # replication metadata under replLock
      acquire(db.replLock)
      inc db.replSeq
      let chHlc = db.nextLocalHlc()
      let chChangeId = $db.replSeq & ":" & db.nodeId
      let ch = ReplChange(collection: collection, docId: docId, op: roPut, version: newVer, value: w.value, changeId: chChangeId, originNode: db.nodeId, hlc: chHlc)
      replEntries.add((db.replSeq, ch))
      cs.replMetaHlc[docId] = chHlc
      cs.replMetaChangeId[docId] = chChangeId
      release(db.replLock)
      # write-ahead
      walRecs.add(WalRecord(kind: wrPut, collection: collection, docId: docId, version: newVer, value: w.value, changeId: chChangeId, originNode: db.nodeId, hlc: chHlc))
      let oldDoc = lookupDoc(cs, docId)   # spill-aware: faults if needed
      cs.docs[docId] = w.value
      markDirty(cs, docId)
      cs.versions[docId] = newVer
      db.cache.put(collection & ":" & docId, w.value)
      for _, idx in cs.indexes:
        idx.reindexDoc(docId, oldDoc, w.value)
      for _, gix in cs.geoIndexes:
        gix.reindexDoc(docId, oldDoc, w.value)
      for _, pix in cs.polygonIndexes:
        pix.reindexDoc(docId, oldDoc, w.value)
      notifications.add((Id(collection: collection, docId: docId, version: newVer), w.value))
      fieldNotifications.add((Id(collection: collection, docId: docId, version: newVer), oldDoc, w.value))
  if walRecs.len > 0:
    db.wal.appendMany(walRecs)
  # record replication changes (with precomputed seq) under replLock to avoid
  # racing with concurrent put/delete/applyChanges on disjoint stripes.
  if replEntries.len > 0:
    acquire(db.replLock)
    for entry in replEntries:
      db.replLog.add(entry)
      let coll = entry[1].collection
      if coll notin db.replLogByCollection: db.replLogByCollection[coll] = @[]
      db.replLogByCollection[coll].add(entry)
    release(db.replLock)
  t.state = tsCommitted
  db.releaseStripesWrite(stripes)
  for it in notifications:
    db.subs.notify(it[0], it[1])
  for it in fieldNotifications:
    db.subs.notifyFieldChanges(it[0], it[1], it[2])
  return CommitResult(status: csOk)

## Subscribe to updates for a specific (collection, docId). Returns a handle
## that can be passed to unsubscribe.
proc subscribe*(db: GlenDB; collection, docId: string; cb: SubscriberCallback): SubscriptionHandle =
  result = db.subs.subscribe(collection, docId, cb)

## Remove a previously registered subscription using its handle.
proc unsubscribe*(db: GlenDB; h: SubscriptionHandle) =
  db.subs.unsubscribe(h)

## Subscribe and stream updates as framed events into the provided Stream.
proc subscribeStream*(db: GlenDB; collection, docId: string; s: Stream): SubscriptionHandle =
  result = db.subs.subscribeStream(collection, docId, s)

## Field-level subscriptions
proc subscribeField*(db: GlenDB; collection, docId: string; fieldPath: string; cb: FieldCallback): FieldSubscriptionHandle =
  result = db.subs.subscribeField(collection, docId, fieldPath, cb)

proc unsubscribeField*(db: GlenDB; h: FieldSubscriptionHandle) =
  db.subs.unsubscribeField(h)

proc subscribeFieldStream*(db: GlenDB; collection, docId: string; fieldPath: string; s: Stream): FieldSubscriptionHandle =
  result = db.subs.subscribeFieldStream(collection, docId, fieldPath, s)

proc subscribeFieldDelta*(db: GlenDB; collection, docId: string; fieldPath: string; cb: FieldDeltaCallback): FieldDeltaSubscriptionHandle =
  result = db.subs.subscribeFieldDelta(collection, docId, fieldPath, cb)

proc unsubscribeFieldDelta*(db: GlenDB; h: FieldDeltaSubscriptionHandle) =
  db.subs.unsubscribeFieldDelta(h)

proc subscribeFieldDeltaStream*(db: GlenDB; collection, docId: string; fieldPath: string; s: Stream): FieldDeltaSubscriptionHandle =
  result = db.subs.subscribeFieldDeltaStream(collection, docId, fieldPath, s)

## Batch put: apply multiple upserts under one write lock, batch WAL appends, update indexes and cache.
proc putMany*(db: GlenDB; collection: string; items: openArray[(string, Value)]) =
  if items.len == 0: return
  type PendingPut = object
    docId: string
    stored: Value
    oldDoc: Value
    version: uint64
    seqNo: uint64
    hlc: Hlc
    changeId: string
  var notifications: seq[(Id, Value)] = @[]
  var fieldNotifications: seq[(Id, Value, Value)] = @[]
  let cs = db.getOrCreateCollection(collection)
  checkDirtyBudget(cs, items.len)
  db.acquireStripeWrite(collection)
  var walRecs: seq[WalRecord] = @[]
  var pending: seq[PendingPut] = @[]
  acquire(db.replLock)
  for (docId, value) in items:
    let oldVer = currentVersionFromStore(cs, docId)
    let newVer = oldVer + 1
    var stored = value.clone()
    inc db.replSeq
    let seqNo = db.replSeq
    let chHlc = db.nextLocalHlc()
    let chChangeId = $seqNo & ":" & db.nodeId
    walRecs.add(WalRecord(kind: wrPut, collection: collection, docId: docId, version: newVer, value: stored, changeId: chChangeId, originNode: db.nodeId, hlc: chHlc))
    let oldDoc = lookupDoc(cs, docId)   # spill-aware
    pending.add(PendingPut(docId: docId, stored: stored, oldDoc: oldDoc, version: newVer, seqNo: seqNo, hlc: chHlc, changeId: chChangeId))
  if walRecs.len > 0:
    db.wal.appendMany(walRecs)
  # replication log and metadata updates
  for entry in pending:
    let change = ReplChange(collection: collection, docId: entry.docId, op: roPut, version: entry.version, value: entry.stored, changeId: entry.changeId, originNode: db.nodeId, hlc: entry.hlc)
    db.replLog.add((entry.seqNo, change))
    if collection notin db.replLogByCollection: db.replLogByCollection[collection] = @[]
    db.replLogByCollection[collection].add((entry.seqNo, change))
    cs.replMetaHlc[entry.docId] = entry.hlc
    cs.replMetaChangeId[entry.docId] = entry.changeId
  release(db.replLock)
  for entry in pending:
    cs.docs[entry.docId] = entry.stored
    markDirty(cs, entry.docId)
    cs.versions[entry.docId] = entry.version
    db.cache.put(collection & ":" & entry.docId, entry.stored)
    for _, idx in cs.indexes:
      idx.reindexDoc(entry.docId, entry.oldDoc, entry.stored)
    for _, gix in cs.geoIndexes:
      gix.reindexDoc(entry.docId, entry.oldDoc, entry.stored)
    for _, pix in cs.polygonIndexes:
      pix.reindexDoc(entry.docId, entry.oldDoc, entry.stored)
    let idObj = Id(collection: collection, docId: entry.docId, version: entry.version)
    notifications.add((idObj, entry.stored))
    fieldNotifications.add((idObj, entry.oldDoc, entry.stored))
  db.releaseStripeWrite(collection)
  for it in notifications:
    db.subs.notify(it[0], it[1])
  for it in fieldNotifications:
    db.subs.notifyFieldChanges(it[0], it[1], it[2])

## Batch delete: delete multiple documents under one write lock, batch WAL appends.
proc deleteMany*(db: GlenDB; collection: string; docIds: openArray[string]) =
  if docIds.len == 0: return
  type PendingDelete = object
    docId: string
    oldDoc: Value
    version: uint64
    seqNo: uint64
    hlc: Hlc
    changeId: string
  var notifications: seq[(Id, Value)] = @[]
  var fieldNotifications: seq[(Id, Value, Value)] = @[]
  let cs = db.tryGetCollection(collection)
  if cs.isNil: return
  checkDirtyBudget(cs, docIds.len)
  db.acquireStripeWrite(collection)
  var walRecs: seq[WalRecord] = @[]
  var pending: seq[PendingDelete] = @[]
  acquire(db.replLock)
  for docId in docIds:
    let oldDoc = lookupDoc(cs, docId)   # spill-aware
    if not oldDoc.isNil:
      let ver = currentVersionFromStore(cs, docId) + 1
      inc db.replSeq
      let seqNo = db.replSeq
      let chHlc = db.nextLocalHlc()
      let chChangeId = $seqNo & ":" & db.nodeId
      walRecs.add(WalRecord(kind: wrDelete, collection: collection, docId: docId, version: ver, changeId: chChangeId, originNode: db.nodeId, hlc: chHlc))
      pending.add(PendingDelete(docId: docId, oldDoc: oldDoc, version: ver, seqNo: seqNo, hlc: chHlc, changeId: chChangeId))
  if walRecs.len > 0:
    db.wal.appendMany(walRecs)
  for entry in pending:
    let change = ReplChange(collection: collection, docId: entry.docId, op: roDelete, version: entry.version, value: nil, changeId: entry.changeId, originNode: db.nodeId, hlc: entry.hlc)
    db.replLog.add((entry.seqNo, change))
    if collection notin db.replLogByCollection: db.replLogByCollection[collection] = @[]
    db.replLogByCollection[collection].add((entry.seqNo, change))
    cs.replMetaHlc[entry.docId] = entry.hlc
    cs.replMetaChangeId[entry.docId] = entry.changeId
  release(db.replLock)
  for entry in pending:
    for _, idx in cs.indexes:
      idx.unindexDoc(entry.docId, entry.oldDoc)
    for _, gix in cs.geoIndexes:
      gix.unindexDoc(entry.docId, entry.oldDoc)
    for _, pix in cs.polygonIndexes:
      pix.unindexDoc(entry.docId, entry.oldDoc)
    if entry.docId in cs.docs: cs.docs.del(entry.docId)
    markDeleted(cs, entry.docId)
    if entry.docId in cs.versions: cs.versions.del(entry.docId)
    db.cache.del(collection & ":" & entry.docId)
    let idObj = Id(collection: collection, docId: entry.docId, version: entry.version)
    notifications.add((idObj, VNull()))
    fieldNotifications.add((idObj, entry.oldDoc, nil))
  db.releaseStripeWrite(collection)
  for it in notifications:
    db.subs.notify(it[0], it[1])
  for it in fieldNotifications:
    db.subs.notifyFieldChanges(it[0], it[1], it[2])

# Snapshot trigger (simple: write all collections)
## Write snapshots for all collections to durable storage.
proc snapshotAll*(db: GlenDB) =
  db.acquireAllStripesWrite()
  defer: db.releaseAllStripesWrite()
  acquireRead(db.structLock)
  # Snapshot each collection's docs. structLock blocks new-collection inserts;
  # all-stripes-write blocks any in-flight mutations.
  var pairs: seq[(string, CollectionStore)] = @[]
  for name, cs in db.collections:
    pairs.add((name, cs))
  releaseRead(db.structLock)
  for (name, cs) in pairs:
    let docs = if cs.snapshot.isNil: cs.docs else: materializeAllDocs(cs)
    writeSnapshotV2(db.dir, name, docs)

## Create an equality index on a field path (e.g., "name" or "profile.age").
## Persisted in the manifest; auto-rebuilt on reopen.
proc createIndex*(db: GlenDB; collection: string; name: string; fieldPath: string) =
  let cs = db.getOrCreateCollection(collection)
  db.acquireStripeWrite(collection)
  let idx = newIndex(name, fieldPath)
  if cs.snapshot.isNil:
    # Eager fast path: direct iteration, matches pre-spill perf.
    for id, v in cs.docs:
      idx.indexDoc(id, v)
  else:
    # Spill path: walk every doc, including snapshot-only ones, without
    # populating the hot table.
    for id in allDocIds(cs):
      let v = lookupDocBypass(cs, id)
      if not v.isNil: idx.indexDoc(id, v)
  cs.indexes[name] = idx
  db.releaseStripeWrite(collection)
  db.addManifestEntry(IndexManifestEntry(
    kind: ikEq, collection: collection, name: name, spec: fieldPath))

## Drop an existing index by name.
proc dropIndex*(db: GlenDB; collection: string; name: string) =
  let cs = db.tryGetCollection(collection)
  if cs.isNil:
    db.removeManifestEntry(ikEq, collection, name); return
  db.acquireStripeWrite(collection)
  if name in cs.indexes:
    cs.indexes.del(name)
  db.releaseStripeWrite(collection)
  db.removeManifestEntry(ikEq, collection, name)

## Create a geospatial (R-tree) index on two numeric fields treated as (lon, lat).
## Bulk-loaded with STR for tight MBRs; updated incrementally on every put/delete.
## Documents missing either field, or with non-numeric values there, are skipped.
## Persisted in the manifest; auto-rebuilt on reopen.
proc createGeoIndex*(db: GlenDB; collection: string; name: string; lonField, latField: string) =
  let cs = db.getOrCreateCollection(collection)
  db.acquireStripeWrite(collection)
  let gix = newGeoIndex(name, lonField, latField)
  if cs.snapshot.isNil:
    # Eager fast path: bulkBuild reads cs.docs directly.
    gix.bulkBuild(cs.docs)
  else:
    # Spill path: materialise full doc set (snapshot + in-memory) once,
    # bypass-loading any snapshot-only entries.
    var allDocs = initTable[string, Value]()
    for id in allDocIds(cs):
      let v = lookupDocBypass(cs, id)
      if not v.isNil: allDocs[id] = v
    gix.bulkBuild(allDocs)
  cs.geoIndexes[name] = gix
  db.releaseStripeWrite(collection)
  db.addManifestEntry(IndexManifestEntry(
    kind: ikGeo, collection: collection, name: name,
    spec: lonField & ":" & latField))

## Drop a geospatial index by name.
proc dropGeoIndex*(db: GlenDB; collection: string; name: string) =
  let cs = db.tryGetCollection(collection)
  if cs.isNil:
    db.removeManifestEntry(ikGeo, collection, name); return
  db.acquireStripeWrite(collection)
  if name in cs.geoIndexes:
    cs.geoIndexes.del(name)
  db.releaseStripeWrite(collection)
  db.removeManifestEntry(ikGeo, collection, name)

## Find docs whose indexed point falls inside the given bounding box.
proc findInBBox*(db: GlenDB; collection, indexName: string;
                 minLon, minLat, maxLon, maxLat: float64;
                 limit = 0): seq[(string, Value)] =
  let cs = db.tryGetCollection(collection)
  if cs.isNil: return @[]
  db.acquireStripeRead(collection)
  defer: db.releaseStripeRead(collection)
  if indexName notin cs.geoIndexes: return @[]
  let gix = cs.geoIndexes[indexName]
  let q = bbox(minLon, minLat, maxLon, maxLat)
  result = @[]
  for id in gix.tree.searchBBox(q, limit):
    let v = lookupDocBypass(cs, id)
    if not v.isNil: result.add((id, v.clone()))

## K-nearest neighbours by Euclidean distance (treats coords as planar).
## Results are sorted ascending by distance; second tuple element is the distance.
proc findNearest*(db: GlenDB; collection, indexName: string;
                  lon, lat: float64; k: int;
                  metric = gmPlanar): seq[(string, float64, Value)] =
  ## K-nearest neighbours. With `metric = gmPlanar` (default) distances are
  ## Euclidean over raw coords (degrees). With `metric = gmGeographic`,
  ## distances are haversine metres — coords are treated as (lon, lat) degrees.
  let cs = db.tryGetCollection(collection)
  if cs.isNil: return @[]
  db.acquireStripeRead(collection)
  defer: db.releaseStripeRead(collection)
  if indexName notin cs.geoIndexes: return @[]
  let gix = cs.geoIndexes[indexName]
  result = @[]
  let pairs =
    if metric == gmGeographic: gix.tree.nearestGeo(lon, lat, k)
    else: gix.tree.nearest(lon, lat, k)
  for (id, dist) in pairs:
    let v = lookupDocBypass(cs, id)
    if not v.isNil: result.add((id, dist, v.clone()))

## Find docs within `radiusMeters` of (lon, lat) using haversine distance.
## Uses an R-tree bbox prefilter, then exact haversine post-filter and sort.
proc findWithinRadius*(db: GlenDB; collection, indexName: string;
                       lon, lat: float64; radiusMeters: float64;
                       limit = 0): seq[(string, float64, Value)] =
  let cs = db.tryGetCollection(collection)
  if cs.isNil: return @[]
  db.acquireStripeRead(collection)
  defer: db.releaseStripeRead(collection)
  if indexName notin cs.geoIndexes: return @[]
  let gix = cs.geoIndexes[indexName]
  let bb = radiusBBox(lon, lat, radiusMeters)
  var candidates: seq[(string, float64)] = @[]
  # Materialise candidate docs once: bypass-load any snapshot-only ones so
  # the haversine post-filter and the result clone don't have to read twice.
  var docByCandidate = initTable[string, Value]()
  for id in gix.tree.searchBBox(bb, 0):
    let doc = lookupDocBypass(cs, id)
    if doc.isNil: continue
    let (ok, plon, plat) = gix.extractPoint(doc)
    if not ok: continue
    let d = haversineMeters(lon, lat, plon, plat)
    if d <= radiusMeters:
      candidates.add((id, d))
      docByCandidate[id] = doc
  candidates.sort(proc (a, b: (string, float64)): int = cmp(a[1], b[1]))
  result = @[]
  for (id, d) in candidates:
    result.add((id, d, docByCandidate[id].clone()))
    if limit > 0 and result.len >= limit: break

# ---- Polygon indexes ----

## Create a polygon (R-tree of MBRs) index. The named field must hold a
## polygon as `VArray([VArray([VFloat(x), VFloat(y)]), ...])`.
## Persisted in the manifest; auto-rebuilt on reopen.
proc createPolygonIndex*(db: GlenDB; collection: string; name: string;
                         polygonField: string) =
  let cs = db.getOrCreateCollection(collection)
  db.acquireStripeWrite(collection)
  let pix = newPolygonIndex(name, polygonField)
  if cs.snapshot.isNil:
    pix.bulkBuild(cs.docs)
  else:
    var allDocs = initTable[string, Value]()
    for id in allDocIds(cs):
      let v = lookupDocBypass(cs, id)
      if not v.isNil: allDocs[id] = v
    pix.bulkBuild(allDocs)
  cs.polygonIndexes[name] = pix
  db.releaseStripeWrite(collection)
  db.addManifestEntry(IndexManifestEntry(
    kind: ikPoly, collection: collection, name: name, spec: polygonField))

## Drop a polygon index by name.
proc dropPolygonIndex*(db: GlenDB; collection: string; name: string) =
  let cs = db.tryGetCollection(collection)
  if cs.isNil:
    db.removeManifestEntry(ikPoly, collection, name); return
  db.acquireStripeWrite(collection)
  if name in cs.polygonIndexes:
    cs.polygonIndexes.del(name)
  db.releaseStripeWrite(collection)
  db.removeManifestEntry(ikPoly, collection, name)

## Find every polygon in the index whose interior contains the point (x, y).
## Uses an R-tree bbox prefilter, then exact ray-cast point-in-polygon test.
proc findPolygonsContaining*(db: GlenDB; collection, indexName: string;
                             x, y: float64; limit = 0): seq[(string, Value)] =
  let cs = db.tryGetCollection(collection)
  if cs.isNil: return @[]
  db.acquireStripeRead(collection)
  defer: db.releaseStripeRead(collection)
  if indexName notin cs.polygonIndexes: return @[]
  let pix = cs.polygonIndexes[indexName]
  result = @[]
  for id in pix.polygonsContainingPoint(x, y, limit):
    let v = lookupDocBypass(cs, id)
    if not v.isNil: result.add((id, v.clone()))

## Polygons whose MBR intersects a query bounding box. This is a fast prefilter
## (O(log n + k)); the result is a superset of the geometric-intersection set.
proc findPolygonsIntersecting*(db: GlenDB; collection, indexName: string;
                               minX, minY, maxX, maxY: float64;
                               limit = 0): seq[(string, Value)] =
  let cs = db.tryGetCollection(collection)
  if cs.isNil: return @[]
  db.acquireStripeRead(collection)
  defer: db.releaseStripeRead(collection)
  if indexName notin cs.polygonIndexes: return @[]
  let pix = cs.polygonIndexes[indexName]
  let q = bbox(minX, minY, maxX, maxY)
  result = @[]
  for id in pix.polygonsIntersectingBBox(q, limit):
    let v = lookupDocBypass(cs, id)
    if not v.isNil: result.add((id, v.clone()))

## Query a *point* (geo) index using a polygon: returns all indexed points
## that lie inside `polygon`. Bbox prefilter + ray-cast.
proc findPointsInPolygon*(db: GlenDB; collection, geoIndexName: string;
                          polygon: Polygon;
                          limit = 0): seq[(string, Value)] =
  let cs = db.tryGetCollection(collection)
  if cs.isNil: return @[]
  db.acquireStripeRead(collection)
  defer: db.releaseStripeRead(collection)
  if geoIndexName notin cs.geoIndexes: return @[]
  let gix = cs.geoIndexes[geoIndexName]
  let bb = polygonBBox(polygon)
  result = @[]
  for id in gix.tree.searchBBox(bb, 0):
    let doc = lookupDocBypass(cs, id)
    if doc.isNil: continue
    let (ok, x, y) = gix.extractPoint(doc)
    if not ok: continue
    if pointInPolygon(polygon, x, y):
      result.add((id, doc.clone()))
      if limit > 0 and result.len >= limit: return

## Query documents by equality on an indexed field. Optional limit.
proc findBy*(db: GlenDB; collection: string; indexName: string; keyValue: Value; limit = 0): seq[(string, Value)] =
  let cs = db.tryGetCollection(collection)
  if cs.isNil: return @[]
  db.acquireStripeRead(collection)
  defer: db.releaseStripeRead(collection)
  if indexName notin cs.indexes: return @[]
  let idx = cs.indexes[indexName]
  result = @[]
  for id in idx.findEq(keyValue, limit):
    let v = lookupDocBypass(cs, id)
    if not v.isNil: result.add((id, v.clone()))

## Range query on a single-field index, with order and limit.
proc rangeBy*(db: GlenDB; collection: string; indexName: string; minVal, maxVal: Value; inclusiveMin = true; inclusiveMax = true; limit = 0; asc = true): seq[(string, Value)] =
  let cs = db.tryGetCollection(collection)
  if cs.isNil: return @[]
  db.acquireStripeRead(collection)
  defer: db.releaseStripeRead(collection)
  if indexName notin cs.indexes: return @[]
  let idx = cs.indexes[indexName]
  result = @[]
  for id in idx.findRange(minVal, maxVal, inclusiveMin, inclusiveMax, limit, asc):
    let v = lookupDocBypass(cs, id)
    if not v.isNil: result.add((id, v.clone()))

# Compaction: snapshot all collections and truncate WAL
## Snapshot all collections and truncate the WAL so that recovery can start
## from the snapshots and a fresh log.
proc compact*(db: GlenDB) =
  db.acquireAllStripesWrite()
  defer: db.releaseAllStripesWrite()
  acquireRead(db.structLock)
  var pairs: seq[(string, CollectionStore)] = @[]
  for name, cs in db.collections:
    pairs.add((name, cs))
  releaseRead(db.structLock)
  for (name, cs) in pairs:
    # Always write v2 (the spillable-friendly indexed format). Eager-mode
    # readers handle v2 transparently.
    let docs = if cs.snapshot.isNil: cs.docs else: materializeAllDocs(cs)
    writeSnapshotV2(db.dir, name, docs)
    # Persist each spatial index alongside the snapshot. WAL is reset below,
    # so on next open these dumps reflect a state with an empty WAL — the
    # replay loop won't double-apply.
    for idxName, gix in cs.geoIndexes:
      dumpGeoIndex(gix, geoIndexFilePath(db.dir, name, idxName))
    for idxName, pix in cs.polygonIndexes:
      dumpPolygonIndex(pix, polygonIndexFilePath(db.dir, name, idxName))
    # If this DB was opened in spillable mode, swap in the new snapshot's
    # mmap and reset dirty/deleted state so future evictions can re-fetch
    # from the new file.
    if not cs.snapshot.isNil:
      closeSnapshotMmap(cs.snapshot)
      cs.snapshot = openSnapshotMmap(db.dir, name)
      cs.dirty.clear()
      cs.deleted.clear()
      # Optionally clear the hot table to reclaim memory; a non-zero hotDocCap
      # would do this organically on next access. We keep cs.docs as-is here.
  # Reset WAL after snapshot to start a new, empty log
  if db.wal != nil:
    db.wal.reset()

# Close database resources
## Close database resources (WAL file handles, snapshot mmaps).
proc close*(db: GlenDB) =
  db.acquireAllStripesWrite()
  defer: db.releaseAllStripesWrite()
  if db.peersDirty:
    db.flushPeersState(force = true)
  acquireRead(db.structLock)
  for _, cs in db.collections:
    if not cs.snapshot.isNil:
      closeSnapshotMmap(cs.snapshot)
      cs.snapshot = nil
  releaseRead(db.structLock)
  if db.wal != nil:
    db.wal.close()

proc cacheStats*(db: GlenDB): CacheStats =
  db.cache.stats()

proc setWalSync*(db: GlenDB; mode: WalSyncMode; flushEveryBytes = 0) =
  db.acquireAllStripesWrite()
  defer: db.releaseAllStripesWrite()
  if db.wal != nil:
    db.wal.setSyncPolicy(mode, flushEveryBytes)

proc newGlenDBFromEnv*(dir: string): GlenDB =
  let cfg = loadConfig()
  var mode = wsmInterval
  if cfg.walSyncMode == "always": mode = wsmAlways
  elif cfg.walSyncMode == "none": mode = wsmNone
  result = newGlenDB(
    dir,
    cacheCapacity = cfg.cacheCapacityBytes,
    cacheShards = cfg.cacheShards,
    walSync = mode,
    walFlushEveryBytes = cfg.walFlushEveryBytes
  )

# -------- Streaming iterators --------
#
# Each iterator captures its target ID set under a short stripe-read lock,
# releases the lock, then yields one doc at a time — re-acquiring the lock
# briefly on each yield to bypass-load the value. This means:
#   * Caller's RAM footprint is one Value at a time (plus ~30 B per ID
#     in the upfront snapshot, which is unavoidable without on-disk paging).
#   * Writers can proceed between yields. A doc deleted mid-iteration is
#     simply skipped.
#   * In spillable mode, snapshot-only docs are decoded straight from the
#     mmap'd region without populating cs.docs — true bounded-memory streaming.
#
# Borrowed variants yield the underlying Value ref without cloning; callers
# must not mutate. Use them in tight read-only loops.

proc snapshotAllIds(db: GlenDB; cs: CollectionStore; collection: string): seq[string] =
  ## Capture the live doc-id set under a brief stripe read lock.
  result = @[]
  db.acquireStripeRead(collection)
  for id in allDocIds(cs): result.add(id)
  db.releaseStripeRead(collection)

proc fetchOneBypass(db: GlenDB; cs: CollectionStore;
                    collection, docId: string): Value =
  ## Bypass-load a doc under a stripe read lock that only covers this fetch.
  ## Returns nil if the doc was deleted between the ID snapshot and now.
  db.acquireStripeRead(collection)
  result = lookupDocBypass(cs, docId)
  db.releaseStripeRead(collection)

iterator getAllStream*(db: GlenDB; collection: string): (string, Value) =
  ## Stream every visible doc as (id, clone). Bounded memory in spill mode.
  let cs = db.tryGetCollection(collection)
  if not cs.isNil:
    let ids = db.snapshotAllIds(cs, collection)
    for id in ids:
      let v = db.fetchOneBypass(cs, collection, id)
      if not v.isNil: yield (id, v.clone())

iterator getBorrowedAllStream*(db: GlenDB; collection: string): (string, Value) =
  ## Same as getAllStream but yields raw refs. Caller must not mutate.
  let cs = db.tryGetCollection(collection)
  if not cs.isNil:
    let ids = db.snapshotAllIds(cs, collection)
    for id in ids:
      let v = db.fetchOneBypass(cs, collection, id)
      if not v.isNil: yield (id, v)

iterator getManyStream*(db: GlenDB; collection: string;
                        docIds: openArray[string]): (string, Value) =
  let cs = db.tryGetCollection(collection)
  if not cs.isNil:
    for id in docIds:
      let v = db.fetchOneBypass(cs, collection, id)
      if not v.isNil: yield (id, v.clone())

iterator getBorrowedManyStream*(db: GlenDB; collection: string;
                                docIds: openArray[string]): (string, Value) =
  let cs = db.tryGetCollection(collection)
  if not cs.isNil:
    for id in docIds:
      let v = db.fetchOneBypass(cs, collection, id)
      if not v.isNil: yield (id, v)

proc snapshotEqMatch(db: GlenDB; cs: CollectionStore; collection, indexName: string;
                     keyValue: Value; limit: int): seq[string] =
  result = @[]
  db.acquireStripeRead(collection)
  if indexName in cs.indexes:
    result = cs.indexes[indexName].findEq(keyValue, limit)
  db.releaseStripeRead(collection)

iterator findByStream*(db: GlenDB; collection, indexName: string;
                       keyValue: Value; limit = 0): (string, Value) =
  let cs = db.tryGetCollection(collection)
  if not cs.isNil:
    let ids = db.snapshotEqMatch(cs, collection, indexName, keyValue, limit)
    for id in ids:
      let v = db.fetchOneBypass(cs, collection, id)
      if not v.isNil: yield (id, v.clone())

proc snapshotRangeMatch(db: GlenDB; cs: CollectionStore; collection, indexName: string;
                        minVal, maxVal: Value;
                        inclusiveMin, inclusiveMax: bool;
                        limit: int; asc: bool): seq[string] =
  result = @[]
  db.acquireStripeRead(collection)
  if indexName in cs.indexes:
    result = cs.indexes[indexName].findRange(minVal, maxVal,
                                             inclusiveMin, inclusiveMax,
                                             limit, asc)
  db.releaseStripeRead(collection)

iterator rangeByStream*(db: GlenDB; collection, indexName: string;
                        minVal, maxVal: Value;
                        inclusiveMin = true; inclusiveMax = true;
                        limit = 0; asc = true): (string, Value) =
  let cs = db.tryGetCollection(collection)
  if not cs.isNil:
    let ids = db.snapshotRangeMatch(cs, collection, indexName,
                                    minVal, maxVal,
                                    inclusiveMin, inclusiveMax,
                                    limit, asc)
    for id in ids:
      let v = db.fetchOneBypass(cs, collection, id)
      if not v.isNil: yield (id, v.clone())

proc snapshotBBoxMatch(db: GlenDB; cs: CollectionStore; collection, indexName: string;
                       q: BBox; limit: int): seq[string] =
  result = @[]
  db.acquireStripeRead(collection)
  if indexName in cs.geoIndexes:
    result = cs.geoIndexes[indexName].tree.searchBBox(q, limit)
  db.releaseStripeRead(collection)

iterator findInBBoxStream*(db: GlenDB; collection, indexName: string;
                           minLon, minLat, maxLon, maxLat: float64;
                           limit = 0): (string, Value) =
  let cs = db.tryGetCollection(collection)
  if not cs.isNil:
    let ids = db.snapshotBBoxMatch(cs, collection, indexName,
                                   bbox(minLon, minLat, maxLon, maxLat), limit)
    for id in ids:
      let v = db.fetchOneBypass(cs, collection, id)
      if not v.isNil: yield (id, v.clone())

proc snapshotNearestPairs(db: GlenDB; cs: CollectionStore; collection, indexName: string;
                          lon, lat: float64; k: int;
                          metric: GeoMetric): seq[(string, float64)] =
  result = @[]
  db.acquireStripeRead(collection)
  if indexName in cs.geoIndexes:
    let gix = cs.geoIndexes[indexName]
    result =
      if metric == gmGeographic: gix.tree.nearestGeo(lon, lat, k)
      else: gix.tree.nearest(lon, lat, k)
  db.releaseStripeRead(collection)

iterator findNearestStream*(db: GlenDB; collection, indexName: string;
                            lon, lat: float64; k: int;
                            metric = gmPlanar): (string, float64, Value) =
  let cs = db.tryGetCollection(collection)
  if not cs.isNil:
    let pairs = db.snapshotNearestPairs(cs, collection, indexName,
                                        lon, lat, k, metric)
    for (id, dist) in pairs:
      let v = db.fetchOneBypass(cs, collection, id)
      if not v.isNil: yield (id, dist, v.clone())

proc snapshotRadiusPairs(db: GlenDB; cs: CollectionStore; collection, indexName: string;
                         lon, lat, radiusMeters: float64;
                         limit: int): seq[(string, float64)] =
  result = @[]
  db.acquireStripeRead(collection)
  if indexName in cs.geoIndexes:
    let gix = cs.geoIndexes[indexName]
    let bb = radiusBBox(lon, lat, radiusMeters)
    var candidates: seq[(string, float64)] = @[]
    for id in gix.tree.searchBBox(bb, 0):
      let doc = lookupDocBypass(cs, id)
      if doc.isNil: continue
      let (ok, plon, plat) = gix.extractPoint(doc)
      if not ok: continue
      let d = haversineMeters(lon, lat, plon, plat)
      if d <= radiusMeters: candidates.add((id, d))
    candidates.sort(proc (a, b: (string, float64)): int = cmp(a[1], b[1]))
    if limit > 0 and candidates.len > limit:
      candidates = candidates[0 ..< limit]
    result = candidates
  db.releaseStripeRead(collection)

iterator findWithinRadiusStream*(db: GlenDB; collection, indexName: string;
                                 lon, lat: float64; radiusMeters: float64;
                                 limit = 0): (string, float64, Value) =
  let cs = db.tryGetCollection(collection)
  if not cs.isNil:
    let pairs = db.snapshotRadiusPairs(cs, collection, indexName,
                                       lon, lat, radiusMeters, limit)
    for (id, d) in pairs:
      let v = db.fetchOneBypass(cs, collection, id)
      if not v.isNil: yield (id, d, v.clone())

proc snapshotPolyContainsIds(db: GlenDB; cs: CollectionStore;
                             collection, indexName: string;
                             x, y: float64; limit: int): seq[string] =
  result = @[]
  db.acquireStripeRead(collection)
  if indexName in cs.polygonIndexes:
    result = cs.polygonIndexes[indexName].polygonsContainingPoint(x, y, limit)
  db.releaseStripeRead(collection)

iterator findPolygonsContainingStream*(db: GlenDB; collection, indexName: string;
                                       x, y: float64; limit = 0): (string, Value) =
  let cs = db.tryGetCollection(collection)
  if not cs.isNil:
    let ids = db.snapshotPolyContainsIds(cs, collection, indexName, x, y, limit)
    for id in ids:
      let v = db.fetchOneBypass(cs, collection, id)
      if not v.isNil: yield (id, v.clone())

proc snapshotPolyIntersectIds(db: GlenDB; cs: CollectionStore;
                              collection, indexName: string;
                              q: BBox; limit: int): seq[string] =
  result = @[]
  db.acquireStripeRead(collection)
  if indexName in cs.polygonIndexes:
    result = cs.polygonIndexes[indexName].polygonsIntersectingBBox(q, limit)
  db.releaseStripeRead(collection)

iterator findPolygonsIntersectingStream*(db: GlenDB; collection, indexName: string;
                                         minX, minY, maxX, maxY: float64;
                                         limit = 0): (string, Value) =
  let cs = db.tryGetCollection(collection)
  if not cs.isNil:
    let ids = db.snapshotPolyIntersectIds(cs, collection, indexName,
                                          bbox(minX, minY, maxX, maxY), limit)
    for id in ids:
      let v = db.fetchOneBypass(cs, collection, id)
      if not v.isNil: yield (id, v.clone())

proc snapshotPointsInPolygonIds(db: GlenDB; cs: CollectionStore;
                                collection, geoIndexName: string;
                                polygon: Polygon; limit: int): seq[string] =
  result = @[]
  db.acquireStripeRead(collection)
  if geoIndexName in cs.geoIndexes:
    let gix = cs.geoIndexes[geoIndexName]
    let bb = polygonBBox(polygon)
    for id in gix.tree.searchBBox(bb, 0):
      let doc = lookupDocBypass(cs, id)
      if doc.isNil: continue
      let (ok, x, y) = gix.extractPoint(doc)
      if not ok: continue
      if pointInPolygon(polygon, x, y):
        result.add(id)
        if limit > 0 and result.len >= limit: break
  db.releaseStripeRead(collection)

iterator findPointsInPolygonStream*(db: GlenDB; collection, geoIndexName: string;
                                    polygon: Polygon;
                                    limit = 0): (string, Value) =
  let cs = db.tryGetCollection(collection)
  if not cs.isNil:
    let ids = db.snapshotPointsInPolygonIds(cs, collection, geoIndexName,
                                            polygon, limit)
    for id in ids:
      let v = db.fetchOneBypass(cs, collection, id)
      if not v.isNil: yield (id, v.clone())

# -------- Replication (multi-master) API --------

type ReplExportCursor* = uint64

proc exportChanges*(db: GlenDB; since: ReplExportCursor; includeCollections: seq[string] = @[]; excludeCollections: seq[string] = @[]): (ReplExportCursor, seq[ReplChange]) =
  ## Export changes after the provided cursor, filtered by collection allow/deny lists.
  ## Returns the new cursor and a sequence of changes.
  var allow = initTable[string, bool]()
  if includeCollections.len > 0:
    for c in includeCollections: allow[c] = true
  var deny = initTable[string, bool]()
  for c in excludeCollections: deny[c] = true
  var changesOut: seq[ReplChange] = @[]
  var nextCursor = since
  acquire(db.replLock)
  if includeCollections.len == 0 and excludeCollections.len == 0:
    # Fast path: no filters, scan global log
    for (seqNo, ch) in db.replLog:
      if seqNo <= since: continue
      if ch.changeId.len == 0: continue
      changesOut.add(ch)
      if seqNo > nextCursor: nextCursor = seqNo
  else:
    # Filtered path: iterate per-collection logs
    var filtered: seq[(uint64, ReplChange)] = @[]
    if includeCollections.len > 0:
      for coll, _ in allow:
        if coll in db.replLogByCollection:
          for (seqNo, ch) in db.replLogByCollection[coll]:
            if seqNo <= since: continue
            if ch.collection in deny: continue
            if ch.changeId.len == 0: continue
            filtered.add((seqNo, ch))
    else:
      # no explicit include list -> iterate all collections except denied
      for coll, seqs in db.replLogByCollection:
        if coll in deny: continue
        for (seqNo, ch) in seqs:
          if seqNo <= since: continue
          if ch.changeId.len == 0: continue
          filtered.add((seqNo, ch))
    filtered.sort(proc (a, b: (uint64, ReplChange)): int = cmp(a[0], b[0]))
    for (seqNo, ch) in filtered:
      changesOut.add(ch)
      if seqNo > nextCursor: nextCursor = seqNo
  let res = (nextCursor, changesOut)
  release(db.replLock)
  return res

proc applyChanges*(db: GlenDB; changes: openArray[ReplChange]) =
  ## Apply a batch of changes from a remote node. Idempotent via changeId; resolves conflicts using HLC (LWW semantics).
  ## Raises ValueError in spillable mode if the batch would push any
  ## collection's dirty set past its maxDirtyDocs cap.
  if changes.len == 0: return
  # Pre-flight per-collection dirty budget check
  block:
    var perColl = initTable[string, int]()
    for ch in changes:
      perColl[ch.collection] = perColl.getOrDefault(ch.collection, 0) + 1
    for c, count in perColl:
      let cs = db.tryGetCollection(c)
      if not cs.isNil:
        checkDirtyBudget(cs, count)
  type PendingApply = object
    change: ReplChange
    seqNo: uint64
    oldDoc: Value
    newDoc: Value
  var notifications: seq[(Id, Value)] = @[]
  var fieldNotifications: seq[(Id, Value, Value)] = @[]
  # Materialise every collection touched by this batch up front so we can
  # operate on stable CollectionStore refs once we hold the stripes.
  var csByColl = initTable[string, CollectionStore]()
  var stripes: seq[int] = @[]
  for ch in changes:
    if ch.collection notin csByColl:
      csByColl[ch.collection] = db.getOrCreateCollection(ch.collection)
    stripes.add(db.stripeIndex(ch.collection))
  db.acquireStripesWrite(stripes)
  acquire(db.replLock)
  var walRecs: seq[WalRecord] = @[]
  var pendingDocs = initTable[(string, string), Value]()
  var pendingHlc = initTable[(string, string), Hlc]()
  var pendingChangeIds = initTable[(string, string), string]()
  var actions: seq[PendingApply] = @[]
  for ch in changes:
    let coll = ch.collection
    let docId = ch.docId
    let cs = csByColl[coll]
    let key = (coll, docId)
    # In spill mode, the "current" doc may live only in the snapshot; the
    # bypass read decodes from the mmap'd region without faulting it into
    # cs.docs (we'll write the new value imminently anyway).
    let curDoc =
      if key in pendingDocs: pendingDocs[key]
      else: lookupDocBypass(cs, docId)
    var curHlc: Hlc
    var hasHlc = false
    if key in pendingHlc:
      curHlc = pendingHlc[key]
      hasHlc = true
    elif docId in cs.replMetaHlc:
      curHlc = cs.replMetaHlc[docId]
      hasHlc = true
    var curChangeId = ""
    if key in pendingChangeIds:
      curChangeId = pendingChangeIds[key]
    elif docId in cs.replMetaChangeId:
      curChangeId = cs.replMetaChangeId[docId]
    if curChangeId.len > 0 and ch.changeId.len > 0 and curChangeId == ch.changeId:
      db.mergeRemoteHlc(ch.hlc)
      continue
    var accept = true
    if hasHlc and hlcCompare(ch.hlc, curHlc) < 0:
      accept = false
    if accept:
      inc db.replSeq
      let seqNo = db.replSeq
      var stored = if ch.op == roDelete: nil else: (if ch.value.isNil: VNull() else: ch.value)
      walRecs.add(WalRecord(kind: (if ch.op == roPut: wrPut else: wrDelete), collection: coll, docId: docId, version: ch.version, value: stored, changeId: ch.changeId, originNode: ch.originNode, hlc: ch.hlc))
      pendingDocs[key] = stored
      pendingHlc[key] = ch.hlc
      pendingChangeIds[key] = ch.changeId
      actions.add(PendingApply(change: ch, seqNo: seqNo, oldDoc: curDoc, newDoc: stored))
    db.mergeRemoteHlc(ch.hlc)
  if walRecs.len > 0:
    db.wal.appendMany(walRecs)
  for act in actions:
    let ch = act.change
    let coll = ch.collection
    let docId = ch.docId
    let key = (coll, docId)
    let cs = csByColl[coll]
    if ch.op == roDelete:
      if not act.oldDoc.isNil:
        for _, idx in cs.indexes:
          idx.unindexDoc(docId, act.oldDoc)
        for _, gix in cs.geoIndexes:
          gix.unindexDoc(docId, act.oldDoc)
        for _, pix in cs.polygonIndexes:
          pix.unindexDoc(docId, act.oldDoc)
      if docId in cs.docs: cs.docs.del(docId)
      markDeleted(cs, docId)
      if docId in cs.versions: cs.versions.del(docId)
      db.cache.del(coll & ":" & docId)
      notifications.add((Id(collection: coll, docId: docId, version: ch.version), VNull()))
      fieldNotifications.add((Id(collection: coll, docId: docId, version: ch.version), act.oldDoc, nil))
    else:
      var stored = act.newDoc
      if stored.isNil:
        stored = VNull()
      cs.docs[docId] = stored
      markDirty(cs, docId)
      cs.versions[docId] = ch.version
      db.cache.put(coll & ":" & docId, stored)
      for _, idx in cs.indexes:
        idx.reindexDoc(docId, act.oldDoc, stored)
      for _, gix in cs.geoIndexes:
        gix.reindexDoc(docId, act.oldDoc, stored)
      for _, pix in cs.polygonIndexes:
        pix.reindexDoc(docId, act.oldDoc, stored)
      notifications.add((Id(collection: coll, docId: docId, version: ch.version), stored))
      fieldNotifications.add((Id(collection: coll, docId: docId, version: ch.version), act.oldDoc, stored))
    cs.replMetaHlc[docId] = pendingHlc[key]
    cs.replMetaChangeId[docId] = pendingChangeIds[key]
    let entry = (act.seqNo, ch)
    db.replLog.add(entry)
    if coll notin db.replLogByCollection: db.replLogByCollection[coll] = @[]
    db.replLogByCollection[coll].add(entry)
  release(db.replLock)
  db.releaseStripesWrite(stripes)
  for it in notifications:
    db.subs.notify(it[0], it[1])
  for it in fieldNotifications:
    db.subs.notifyFieldChanges(it[0], it[1], it[2])

