## HNSW vector index for Glen.
##
## Hierarchical Navigable Small World graph (Malkov & Yashunin, 2018) over
## per-document embedding vectors. Each indexed document contributes one
## point; the graph supports approximate k-nearest-neighbour queries with
## sub-linear average complexity.
##
## Trade-offs:
##   * Approximate (recall < 100%); tunable via efSearch.
##   * In-memory graph + flat vector store. Persisted to a `.vri` dump on
##     compact() so reopen doesn't need to re-insert every doc.
##   * Bulk build: just iterates inserts; HNSW has no STR-equivalent.
##
## Distance metrics:
##   * vmCosine — 1 − cos(a, b). Vectors normalised to unit length on insert
##     so the cosine reduces to 1 − dot.
##   * vmL2     — Euclidean distance, ‖a − b‖.
##   * vmDot    — negative dot product (smaller = closer).
##
## On-disk format (.vri, written atomically on compact):
##   header (16 B):
##     magic "GLENVRI1" (8 B)
##     version  uint32 (1)
##     reserved uint32
##   meta (32 B):
##     M               uint32
##     efConstruction  uint32
##     efSearch        uint32
##     dim             uint32
##     metric          uint32     (0=cosine, 1=L2, 2=dot)
##     entryPoint      int32      (-1 if empty)
##     maxLevel        int32
##     nodeCount       uint32
##   per-node:
##     level           uint32
##     vector          dim × float32
##     for level..0:
##       neighbour count  uint32
##       neighbour ids    n × uint32
##   docId table:
##     for nodeCount entries: varuint length + utf-8 bytes
##   trailer:
##     fnv1a32 over everything before the trailer    uint32

import std/[heapqueue, math, random, tables, sets, os, algorithm]
import glen/types

type
  VectorMetric* = enum
    vmCosine = 0
    vmL2     = 1
    vmDot    = 2

  HnswNode = object
    level:   int                # max layer this node is on
    vec:     seq[float32]       # length == dim
    # neighbours[lvl] is the adjacency at layer lvl (lvl ≤ level)
    neighbours: seq[seq[uint32]]

  Hnsw* = ref object
    dim*:            int
    M*:              int        # neighbours kept per layer (and 2× at layer 0)
    Mmax0:           int        # cap at layer 0 (== 2*M typical)
    Mmax:            int        # cap at higher layers
    efConstruction*: int
    efSearch*:       int
    metric*:         VectorMetric
    levelMult:       float64    # 1 / ln(M)
    nodes*:          seq[HnswNode]
    entryPoint*:     int32      # -1 if empty
    maxLevel*:       int        # max level among existing nodes
    rng:             Rand

  VectorIndex* = ref object
    name*:           string
    embeddingField*: string
    dim*:            int
    hnsw*:           Hnsw
    docToNode*:      Table[string, uint32]
    nodeToDoc*:      seq[string]   # nodeId -> docId

  VectorIndexesByName* = Table[string, VectorIndex]

# ---- Distance ----

proc dotF32(a, b: openArray[float32]): float32 {.inline.} =
  var s: float32 = 0
  for i in 0 ..< a.len: s += a[i] * b[i]
  s

proc l2sqF32(a, b: openArray[float32]): float32 {.inline.} =
  var s: float32 = 0
  for i in 0 ..< a.len:
    let d = a[i] - b[i]
    s += d * d
  s

proc l2NormF32(v: var seq[float32]) =
  var n: float32 = 0
  for x in v: n += x * x
  if n <= 0: return
  let inv = 1.0'f32 / sqrt(n)
  for i in 0 ..< v.len: v[i] = v[i] * inv

proc distance*(h: Hnsw; a, b: openArray[float32]): float32 {.inline.} =
  ## Smaller = closer for every metric.
  case h.metric
  of vmCosine:
    # vectors are stored unit-normalised, so 1 - dot is cosine distance.
    1.0'f32 - dotF32(a, b)
  of vmL2:
    sqrt(l2sqF32(a, b))
  of vmDot:
    -dotF32(a, b)

# ---- Construction ----

proc newHnsw*(dim: int;
              M = 16;
              efConstruction = 200;
              efSearch = 64;
              metric: VectorMetric = vmCosine;
              seed: int64 = 1): Hnsw =
  doAssert dim > 0 and M > 0
  result = Hnsw(
    dim: dim, M: M, Mmax: M, Mmax0: 2 * M,
    efConstruction: efConstruction, efSearch: efSearch,
    metric: metric, levelMult: 1.0 / ln(float64(M)),
    nodes: @[], entryPoint: -1, maxLevel: -1,
    rng: initRand(seed))

proc preprocessVec(h: Hnsw; v: openArray[float32]): seq[float32] =
  ## Copy + normalise (cosine) the input.
  result = newSeq[float32](v.len)
  for i in 0 ..< v.len: result[i] = v[i]
  if h.metric == vmCosine:
    l2NormF32(result)

proc randomLevel(h: var Hnsw): int =
  let r = h.rng.rand(1.0)
  result = int(floor(-ln(max(r, 1e-12)) * h.levelMult))

# ---- Search primitives ----

# Min-heap over (distance, nodeId). For "candidates" — we always want the
# closest (smallest dist) candidate to expand next.
type DistNode = tuple[dist: float32, id: uint32]

proc `<`(a, b: DistNode): bool {.inline.} = a.dist < b.dist

# Result set is a "max-heap" (we evict the *farthest* when over capacity).
# heapqueue is min-heap, so we negate.
type FarFirst = tuple[negDist: float32, id: uint32]
proc `<`(a, b: FarFirst): bool {.inline.} = a.negDist < b.negDist

proc searchLayer(h: Hnsw; query: openArray[float32];
                 entry: uint32; ef, level: int): seq[DistNode] =
  ## Returns the ef nearest nodes at `level`, starting from `entry`.
  ## Output is unsorted (caller sorts when needed).
  var visited = initHashSet[uint32]()
  visited.incl(entry)
  let dEntry = distance(h, query, h.nodes[entry].vec)
  var candidates: HeapQueue[DistNode]
  candidates.push((dist: dEntry, id: entry))
  var results: HeapQueue[FarFirst]
  results.push((negDist: -dEntry, id: entry))
  while candidates.len > 0:
    let cur = candidates.pop()
    # If the closest candidate is already farther than our worst result, stop.
    if results.len >= ef:
      let worstNeg = results[0].negDist
      if cur.dist > -worstNeg: break
    if level >= h.nodes[cur.id].neighbours.len: continue
    for nb in h.nodes[cur.id].neighbours[level]:
      if nb in visited: continue
      visited.incl(nb)
      let dNb = distance(h, query, h.nodes[nb].vec)
      let worstNeg = if results.len > 0: results[0].negDist else: float32.high
      if results.len < ef or dNb < -worstNeg:
        candidates.push((dist: dNb, id: nb))
        results.push((negDist: -dNb, id: nb))
        if results.len > ef: discard results.pop()
  result = @[]
  while results.len > 0:
    let r = results.pop()
    result.add((dist: -r.negDist, id: r.id))

proc selectNeighborsSimple(h: Hnsw;
                           candidates: seq[DistNode];
                           m: int): seq[uint32] =
  ## Take the m closest candidates.
  var local = candidates
  local.sort(proc (a, b: DistNode): int = cmp(a.dist, b.dist))
  let k = min(m, local.len)
  result = newSeqOfCap[uint32](k)
  for i in 0 ..< k: result.add(local[i].id)

proc connectBidirectional(h: var Hnsw;
                          newNode: uint32;
                          neighbours: seq[uint32];
                          level: int) =
  ## Wire `newNode` ↔ each neighbour at the given level. Prune neighbours
  ## that go over the per-level cap by keeping only the M closest.
  let cap = if level == 0: h.Mmax0 else: h.Mmax
  # link new -> existing
  for nb in neighbours:
    h.nodes[newNode].neighbours[level].add(nb)
  # link existing -> new, with pruning
  for nb in neighbours:
    h.nodes[nb].neighbours[level].add(newNode)
    if h.nodes[nb].neighbours[level].len > cap:
      # prune: keep cap closest to nb
      var dists: seq[DistNode] = @[]
      for x in h.nodes[nb].neighbours[level]:
        dists.add((dist: distance(h, h.nodes[nb].vec, h.nodes[x].vec), id: x))
      dists.sort(proc (a, b: DistNode): int = cmp(a.dist, b.dist))
      h.nodes[nb].neighbours[level].setLen(0)
      for i in 0 ..< cap:
        h.nodes[nb].neighbours[level].add(dists[i].id)

proc insert*(h: var Hnsw; vec: openArray[float32]): uint32 =
  ## Insert a new vector into the graph. Returns the new node id.
  doAssert vec.len == h.dim
  let v = preprocessVec(h, vec)
  let level = randomLevel(h)
  let nodeId = uint32(h.nodes.len)
  var node = HnswNode(level: level, vec: v,
                      neighbours: newSeq[seq[uint32]](level + 1))
  for lvl in 0 .. level:
    node.neighbours[lvl] = @[]
  h.nodes.add(node)
  if h.entryPoint < 0:
    h.entryPoint = int32(nodeId)
    h.maxLevel = level
    return nodeId
  var entry: uint32 = uint32(h.entryPoint)
  # Greedy descent from above `level` down to layer level+1. At each layer
  # we keep only the single best as the next descent's entry.
  var lvl = h.maxLevel
  while lvl > level:
    let cands = searchLayer(h,v, entry, ef = 1, level = lvl)
    if cands.len > 0:
      var best = cands[0]
      for c in cands:
        if c.dist < best.dist: best = c
      entry = best.id
    dec lvl
  # From min(level, maxLevel) down to 0: full ef_construction search + connect.
  lvl = min(level, h.maxLevel)
  while lvl >= 0:
    let cands = searchLayer(h,v, entry, ef = h.efConstruction, level = lvl)
    let nbs = selectNeighborsSimple(h, cands, h.M)
    connectBidirectional(h, nodeId, nbs, lvl)
    if cands.len > 0:
      var best = cands[0]
      for c in cands:
        if c.dist < best.dist: best = c
      entry = best.id
    dec lvl
  if level > h.maxLevel:
    h.maxLevel = level
    h.entryPoint = int32(nodeId)
  nodeId

proc knnSearch*(h: Hnsw; query: openArray[float32]; k: int): seq[DistNode] =
  ## Return up to k (distance, nodeId) pairs, sorted ascending by distance.
  if h.entryPoint < 0 or k <= 0:
    return @[]
  doAssert query.len == h.dim
  var q: seq[float32] = newSeq[float32](query.len)
  for i in 0 ..< query.len: q[i] = query[i]
  if h.metric == vmCosine:
    l2NormF32(q)
  var entry: uint32 = uint32(h.entryPoint)
  var lvl = h.maxLevel
  while lvl > 0:
    let cands = searchLayer(h, q, entry, ef = 1, level = lvl)
    if cands.len > 0:
      var best = cands[0]
      for c in cands:
        if c.dist < best.dist: best = c
      entry = best.id
    dec lvl
  let ef = max(h.efSearch, k)
  let cands = searchLayer(h, q, entry, ef = ef, level = 0)
  result = cands
  result.sort(proc (a, b: DistNode): int = cmp(a.dist, b.dist))
  if result.len > k: result.setLen(k)

# ---- VectorIndex (Glen-facing wrapper) ----

proc extractEmbedding*(idx: VectorIndex; doc: Value): (bool, seq[float32]) =
  if doc.isNil or doc.kind != vkObject: return (false, @[])
  let v = doc[idx.embeddingField]
  if v.isNil or v.kind != vkArray: return (false, @[])
  if v.arr.len != idx.dim: return (false, @[])
  var vec = newSeq[float32](idx.dim)
  for i, e in v.arr:
    case e.kind
    of vkFloat: vec[i] = float32(e.f)
    of vkInt:   vec[i] = float32(e.i)
    else:        return (false, @[])
  (true, vec)

proc newVectorIndex*(name, embeddingField: string; dim: int;
                    metric: VectorMetric = vmCosine;
                    M = 16; efConstruction = 200; efSearch = 64): VectorIndex =
  VectorIndex(name: name, embeddingField: embeddingField, dim: dim,
              hnsw: newHnsw(dim, M = M, efConstruction = efConstruction,
                            efSearch = efSearch, metric = metric),
              docToNode: initTable[string, uint32](),
              nodeToDoc: @[])

proc indexDoc*(idx: VectorIndex; docId: string; doc: Value) =
  let (ok, vec) = idx.extractEmbedding(doc)
  if not ok: return
  if docId in idx.docToNode:
    # Tombstone+re-add semantics. HNSW lacks delete; we insert a new node
    # and the old one is left as an orphan target. Cheap on small mutation
    # rates; compact() rebuilds when called.
    discard
  let nodeId = idx.hnsw.insert(vec)
  idx.docToNode[docId] = nodeId
  if int(nodeId) >= idx.nodeToDoc.len:
    idx.nodeToDoc.setLen(int(nodeId) + 1)
  idx.nodeToDoc[int(nodeId)] = docId

proc unindexDoc*(idx: VectorIndex; docId: string; doc: Value) =
  ## Soft-delete: drop the docId mapping. The node remains in the graph but
  ## won't be returned by knnSearch (we filter via nodeToDoc lookup).
  if docId in idx.docToNode:
    let nid = idx.docToNode[docId]
    idx.docToNode.del(docId)
    if int(nid) < idx.nodeToDoc.len:
      idx.nodeToDoc[int(nid)] = ""

proc reindexDoc*(idx: VectorIndex; docId: string; oldDoc, newDoc: Value) =
  if not oldDoc.isNil:
    idx.unindexDoc(docId, oldDoc)
  if not newDoc.isNil:
    idx.indexDoc(docId, newDoc)

proc bulkBuild*(idx: VectorIndex; docs: Table[string, Value]) =
  for id, v in docs:
    idx.indexDoc(id, v)

proc findNearest*(idx: VectorIndex; query: openArray[float32]; k: int): seq[(string, float32)] =
  ## Returns (docId, distance) pairs sorted ascending. May return fewer than k
  ## if the graph has fewer documents (or if some matches were soft-deleted).
  result = @[]
  if query.len != idx.dim or k <= 0: return
  # Search a bit deeper than k to compensate for soft-deleted slots.
  let raw = idx.hnsw.knnSearch(query, k * 2 + 8)
  for (dist, nid) in raw:
    if int(nid) >= idx.nodeToDoc.len: continue
    let id = idx.nodeToDoc[int(nid)]
    if id.len == 0: continue   # soft-deleted
    result.add((id, dist))
    if result.len >= k: break

proc findNearestWithin*(idx: VectorIndex; query: openArray[float32];
                       maxDistance: float32;
                       maxResults = 0): seq[(string, float32)] =
  ## Find every doc whose distance from `query` is ≤ `maxDistance`.
  ##
  ## Useful for "find anything more similar than X" workflows where you
  ## care about a threshold rather than a fixed top-k. With cosine
  ## metric, distance is `1 - cos(a, b)` so `maxDistance = 0.2` means
  ## "cosine similarity ≥ 0.8". With L2, distance is Euclidean. With
  ## dot, distance is `-dot(a, b)` (smaller = closer for all metrics).
  ##
  ## `maxResults` caps the result count (0 = unlimited). Internally we
  ## keep doubling the search depth until either the next batch all
  ## exceeds the threshold or we run out of nodes — so for "rare match"
  ## queries this stays efficient.
  result = @[]
  if query.len != idx.dim: return
  if idx.hnsw.entryPoint < 0: return
  var depth = max(idx.hnsw.efSearch, 32)
  let totalNodes = idx.nodeToDoc.len
  while true:
    let raw = idx.hnsw.knnSearch(query, depth)
    var lastDist: float32 = NegInf
    result.setLen(0)
    var truncated = false
    for (dist, nid) in raw:
      lastDist = dist
      if dist > maxDistance: continue
      if int(nid) >= idx.nodeToDoc.len: continue
      let id = idx.nodeToDoc[int(nid)]
      if id.len == 0: continue
      result.add((id, dist))
      if maxResults > 0 and result.len >= maxResults:
        truncated = true; break
    # We're done when (a) the deepest match still fits the threshold or
    # (b) we've already scanned the whole graph. Otherwise double depth
    # and re-scan; HNSW search results are sorted, so re-scanning is the
    # only way to be sure we didn't miss something at the boundary.
    if truncated: break
    if lastDist > maxDistance: break
    if depth >= totalNodes: break
    depth *= 2

# ---- Persistence ----

const VRIMagic    = "GLENVRI1"
const VRIVersion  = 1'u32

proc fnv1a32(data: openArray[byte]): uint32 =
  var h: uint32 = 0x811C9DC5'u32
  for b in data:
    h = h xor uint32(b)
    h = h * 0x01000193'u32
  h

proc writeU32(buf: var seq[byte]; v: uint32) =
  buf.add(byte(v and 0xFF))
  buf.add(byte((v shr 8) and 0xFF))
  buf.add(byte((v shr 16) and 0xFF))
  buf.add(byte((v shr 24) and 0xFF))

proc writeI32(buf: var seq[byte]; v: int32) {.inline.} =
  writeU32(buf, cast[uint32](v))

proc writeF32(buf: var seq[byte]; v: float32) {.inline.} =
  writeU32(buf, cast[uint32](v))

proc writeVarUint(buf: var seq[byte]; x: uint64) =
  var v = x
  while true:
    var b = uint8(v and 0x7F)
    v = v shr 7
    if v != 0: b = b or 0x80'u8
    buf.add(byte(b))
    if v == 0: break

proc readU32(buf: openArray[byte]; pos: var int): uint32 =
  result = uint32(buf[pos]) or (uint32(buf[pos+1]) shl 8) or
           (uint32(buf[pos+2]) shl 16) or (uint32(buf[pos+3]) shl 24)
  pos += 4

proc readI32(buf: openArray[byte]; pos: var int): int32 {.inline.} =
  cast[int32](readU32(buf, pos))

proc readF32(buf: openArray[byte]; pos: var int): float32 {.inline.} =
  cast[float32](readU32(buf, pos))

proc readVarUint(buf: openArray[byte]; pos: var int): uint64 =
  var shift = 0'u32
  while true:
    let b = buf[pos]; inc pos
    result = result or (uint64(b and 0x7F) shl shift)
    if (b and 0x80) == 0: break
    shift += 7
    if shift > 63: raise newException(IOError, "varuint too long")

proc dumpVectorIndex*(idx: VectorIndex; path: string) =
  ## Atomically write the graph + docId table to `path`.
  var buf: seq[byte] = @[]
  for ch in VRIMagic: buf.add(byte(ord(ch)))
  writeU32(buf, VRIVersion)
  writeU32(buf, 0'u32)  # reserved
  let h = idx.hnsw
  writeU32(buf, uint32(h.M))
  writeU32(buf, uint32(h.efConstruction))
  writeU32(buf, uint32(h.efSearch))
  writeU32(buf, uint32(idx.dim))
  writeU32(buf, uint32(ord(h.metric)))
  writeI32(buf, h.entryPoint)
  writeI32(buf, int32(h.maxLevel))
  writeU32(buf, uint32(h.nodes.len))
  for n in h.nodes:
    writeU32(buf, uint32(n.level))
    for x in n.vec: writeF32(buf, x)
    for lvl in 0 .. n.level:
      writeU32(buf, uint32(n.neighbours[lvl].len))
      for nb in n.neighbours[lvl]: writeU32(buf, nb)
  for s in idx.nodeToDoc:
    writeVarUint(buf, uint64(s.len))
    for c in s: buf.add(byte(ord(c)))
  let trailer = fnv1a32(buf)
  writeU32(buf, trailer)
  let tmp = path & ".tmp"
  let parent = parentDir(path)
  if parent.len > 0 and not dirExists(parent): createDir(parent)
  writeFile(tmp, cast[string](buf))
  moveFile(tmp, path)

proc tryLoadVectorIndex*(idx: VectorIndex; path: string): bool =
  if not fileExists(path): return false
  var raw: string
  try: raw = readFile(path)
  except IOError: return false
  if raw.len < 16 + 32 + 4: return false
  var buf = newSeq[byte](raw.len)
  for i, c in raw: buf[i] = byte(c)
  # Verify trailer
  let trailerOnDisk = (uint32(buf[buf.len - 4]) or
                       (uint32(buf[buf.len - 3]) shl 8) or
                       (uint32(buf[buf.len - 2]) shl 16) or
                       (uint32(buf[buf.len - 1]) shl 24))
  if fnv1a32(toOpenArray(buf, 0, buf.len - 5)) != trailerOnDisk: return false
  var pos = 0
  for i, ch in VRIMagic:
    if buf[pos] != byte(ord(ch)): return false
    inc pos
  let ver = readU32(buf, pos)
  if ver != VRIVersion: return false
  discard readU32(buf, pos)  # reserved
  let m         = int(readU32(buf, pos))
  let efC       = int(readU32(buf, pos))
  let efS       = int(readU32(buf, pos))
  let dim       = int(readU32(buf, pos))
  let metricRaw = readU32(buf, pos)
  let entry     = readI32(buf, pos)
  let maxLevel  = int(readI32(buf, pos))
  let nNodes    = int(readU32(buf, pos))
  if dim != idx.dim: return false
  let metric = case metricRaw
    of 0'u32: vmCosine
    of 1'u32: vmL2
    of 2'u32: vmDot
    else:    return false
  idx.hnsw = newHnsw(dim, M = m, efConstruction = efC, efSearch = efS,
                     metric = metric)
  idx.hnsw.entryPoint = entry
  idx.hnsw.maxLevel = maxLevel
  idx.hnsw.nodes.setLen(nNodes)
  for i in 0 ..< nNodes:
    let level = int(readU32(buf, pos))
    var node = HnswNode(level: level, vec: newSeq[float32](dim))
    for j in 0 ..< dim:
      node.vec[j] = readF32(buf, pos)
    node.neighbours = newSeq[seq[uint32]](level + 1)
    for lvl in 0 .. level:
      let nNb = int(readU32(buf, pos))
      node.neighbours[lvl] = newSeq[uint32](nNb)
      for j in 0 ..< nNb:
        node.neighbours[lvl][j] = readU32(buf, pos)
    idx.hnsw.nodes[i] = node
  idx.nodeToDoc = newSeq[string](nNodes)
  idx.docToNode.clear()
  for i in 0 ..< nNodes:
    let l = int(readVarUint(buf, pos))
    var s = newString(l)
    for j in 0 ..< l:
      s[j] = char(buf[pos]); inc pos
    idx.nodeToDoc[i] = s
    if s.len > 0:
      idx.docToNode[s] = uint32(i)
  true
