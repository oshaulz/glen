# Glen geospatial index — R-tree with STR bulk-load + Guttman incremental insert.
#
# Design choices for performance:
#   * In-memory only; rebuilt by `createGeoIndex` (same pattern as equality indexes).
#   * STR (Sort-Tile-Recursive) bulk loader for createGeoIndex-on-existing-data:
#     near-optimal MBR packing, O(n log n) build.
#   * Guttman linear split for incremental inserts. Cheaper than quadratic/R*
#     and "good enough" for online updates.
#   * Best-first KNN with a min-heap on bbox-to-point distance — the standard
#     optimal KNN traversal (Hjaltason & Samet).
#   * Fanout (M=16, m=6) is conservative; tweak after profiling.

import std/[algorithm, math, tables, heapqueue, os]
import glen/types

type
  GeoMetric* = enum
    gmPlanar      ## Euclidean distance over raw coords (degrees, units, anything).
    gmGeographic  ## Haversine distance in meters; coords interpreted as (lon, lat) degrees.

  BBox* = object
    minX*, minY*, maxX*, maxY*: float64

  RTreeNode = ref object
    bbox: BBox
    isLeaf: bool
    # exactly one of the next two is populated
    entries: seq[LeafEntry]      # if isLeaf
    children: seq[RTreeNode]     # if not isLeaf

  LeafEntry = object
    bbox: BBox
    docId: string

  RTree* = ref object
    root: RTreeNode
    maxEntries: int
    minEntries: int
    size: int
    docBBox: Table[string, BBox]   # for delete/reindex

  GeoIndex* = ref object
    name*: string
    lonField*: string
    latField*: string
    tree*: RTree

  Polygon* = object
    ## Simple polygon: ordered list of vertices, implicit closing edge.
    ## No holes, no self-intersection enforcement (callers' responsibility).
    vertices*: seq[(float64, float64)]

  PolygonIndex* = ref object
    name*: string
    polygonField*: string
    tree*: RTree
    polygons*: Table[string, Polygon]    # docId -> polygon, for exact tests

# --------- BBox ---------

const InfBBox = BBox(minX: Inf, minY: Inf, maxX: NegInf, maxY: NegInf)

proc isEmpty(b: BBox): bool {.inline.} = b.minX > b.maxX or b.minY > b.maxY

proc point*(x, y: float64): BBox {.inline.} =
  BBox(minX: x, minY: y, maxX: x, maxY: y)

proc bbox*(minX, minY, maxX, maxY: float64): BBox {.inline.} =
  BBox(minX: minX, minY: minY, maxX: maxX, maxY: maxY)

proc area*(b: BBox): float64 {.inline.} =
  if b.isEmpty: return 0.0
  (b.maxX - b.minX) * (b.maxY - b.minY)

proc union*(a, b: BBox): BBox {.inline.} =
  if a.isEmpty: return b
  if b.isEmpty: return a
  BBox(
    minX: min(a.minX, b.minX), minY: min(a.minY, b.minY),
    maxX: max(a.maxX, b.maxX), maxY: max(a.maxY, b.maxY))

proc enlargement(a, withB: BBox): float64 {.inline.} =
  ## How much area `a` grows by to contain `withB`.
  union(a, withB).area - a.area

proc intersects*(a, b: BBox): bool {.inline.} =
  not (a.maxX < b.minX or a.minX > b.maxX or
       a.maxY < b.minY or a.minY > b.maxY)

proc containsPoint(b: BBox; x, y: float64): bool {.inline.} =
  x >= b.minX and x <= b.maxX and y >= b.minY and y <= b.maxY

proc minDistSq(b: BBox; x, y: float64): float64 {.inline.} =
  ## Squared euclidean distance from point (x,y) to nearest point of bbox.
  ## Zero if the point is inside.
  let dx =
    if x < b.minX: b.minX - x
    elif x > b.maxX: x - b.maxX
    else: 0.0
  let dy =
    if y < b.minY: b.minY - y
    elif y > b.maxY: y - b.maxY
    else: 0.0
  dx*dx + dy*dy

# Haversine distance (meters), for radius queries on lon/lat data.
const EarthRadiusM = 6_371_008.8

proc haversineMeters*(lon1, lat1, lon2, lat2: float64): float64 =
  let toRad = PI / 180.0
  let dLat = (lat2 - lat1) * toRad
  let dLon = (lon2 - lon1) * toRad
  let a = sin(dLat / 2) * sin(dLat / 2) +
          cos(lat1 * toRad) * cos(lat2 * toRad) *
          sin(dLon / 2) * sin(dLon / 2)
  let c = 2.0 * arctan2(sqrt(a), sqrt(1.0 - a))
  EarthRadiusM * c

# Convert a meter radius around (lon, lat) to a bounding-box of degrees.
# Slight over-approximation; the post-filter does exact haversine.
proc radiusBBox*(lon, lat, meters: float64): BBox =
  let latDeg = meters / 111_320.0
  let cosLat = cos(lat * PI / 180.0)
  let lonDeg =
    if cosLat < 1e-9: 180.0
    else: meters / (111_320.0 * cosLat)
  BBox(
    minX: lon - lonDeg, minY: lat - latDeg,
    maxX: lon + lonDeg, maxY: lat + latDeg)

# --------- node helpers ---------

proc newLeaf(): RTreeNode =
  RTreeNode(bbox: InfBBox, isLeaf: true, entries: @[], children: @[])

proc newInternal(): RTreeNode =
  RTreeNode(bbox: InfBBox, isLeaf: false, entries: @[], children: @[])

proc recomputeBBox(n: RTreeNode) =
  var b = InfBBox
  if n.isLeaf:
    for e in n.entries: b = union(b, e.bbox)
  else:
    for c in n.children: b = union(b, c.bbox)
  n.bbox = b

proc childCount(n: RTreeNode): int {.inline.} =
  if n.isLeaf: n.entries.len else: n.children.len

# --------- newRTree ---------

proc newRTree*(maxEntries = 16; minEntries = 6): RTree =
  ## Default fanout 16 / fill ratio ~3/8. Bump max for static datasets, cut for
  ## very dynamic ones.
  doAssert maxEntries >= 4
  doAssert minEntries >= 2 and minEntries <= maxEntries div 2
  RTree(
    root: newLeaf(),
    maxEntries: maxEntries,
    minEntries: minEntries,
    size: 0,
    docBBox: initTable[string, BBox]())

proc len*(t: RTree): int {.inline.} = t.size

# --------- linear split (Guttman) ---------

proc pickSeedsLinearLeaf(entries: seq[LeafEntry]): (int, int) =
  ## Linear-time seed selection: greatest normalised separation on either axis.
  var minXLow = Inf; var maxXLow = NegInf
  var minXHigh = Inf; var maxXHigh = NegInf
  var minYLow = Inf; var maxYLow = NegInf
  var minYHigh = Inf; var maxYHigh = NegInf
  var lowXIdx = 0; var highXIdx = 0
  var lowYIdx = 0; var highYIdx = 0
  for i, e in entries:
    if e.bbox.minX > maxXLow: maxXLow = e.bbox.minX; lowXIdx = i
    if e.bbox.minX < minXLow: minXLow = e.bbox.minX
    if e.bbox.maxX < minXHigh: minXHigh = e.bbox.maxX; highXIdx = i
    if e.bbox.maxX > maxXHigh: maxXHigh = e.bbox.maxX
    if e.bbox.minY > maxYLow: maxYLow = e.bbox.minY; lowYIdx = i
    if e.bbox.minY < minYLow: minYLow = e.bbox.minY
    if e.bbox.maxY < minYHigh: minYHigh = e.bbox.maxY; highYIdx = i
    if e.bbox.maxY > maxYHigh: maxYHigh = e.bbox.maxY
  let widthX = max(maxXHigh - minXLow, 1e-12)
  let widthY = max(maxYHigh - minYLow, 1e-12)
  let sepX = abs(maxXLow - minXHigh) / widthX
  let sepY = abs(maxYLow - minYHigh) / widthY
  var a, b: int
  if sepX >= sepY:
    a = lowXIdx; b = highXIdx
  else:
    a = lowYIdx; b = highYIdx
  if a == b:
    # degenerate; pick first two
    a = 0; b = if entries.len > 1: 1 else: 0
  if a > b: swap a, b
  (a, b)

proc pickSeedsLinearChildren(children: seq[RTreeNode]): (int, int) =
  var minXLow = Inf; var maxXLow = NegInf
  var minXHigh = Inf; var maxXHigh = NegInf
  var minYLow = Inf; var maxYLow = NegInf
  var minYHigh = Inf; var maxYHigh = NegInf
  var lowXIdx = 0; var highXIdx = 0
  var lowYIdx = 0; var highYIdx = 0
  for i, c in children:
    let bb = c.bbox
    if bb.minX > maxXLow: maxXLow = bb.minX; lowXIdx = i
    if bb.minX < minXLow: minXLow = bb.minX
    if bb.maxX < minXHigh: minXHigh = bb.maxX; highXIdx = i
    if bb.maxX > maxXHigh: maxXHigh = bb.maxX
    if bb.minY > maxYLow: maxYLow = bb.minY; lowYIdx = i
    if bb.minY < minYLow: minYLow = bb.minY
    if bb.maxY < minYHigh: minYHigh = bb.maxY; highYIdx = i
    if bb.maxY > maxYHigh: maxYHigh = bb.maxY
  let widthX = max(maxXHigh - minXLow, 1e-12)
  let widthY = max(maxYHigh - minYLow, 1e-12)
  let sepX = abs(maxXLow - minXHigh) / widthX
  let sepY = abs(maxYLow - minYHigh) / widthY
  var a, b: int
  if sepX >= sepY:
    a = lowXIdx; b = highXIdx
  else:
    a = lowYIdx; b = highYIdx
  if a == b:
    a = 0; b = if children.len > 1: 1 else: 0
  if a > b: swap a, b
  (a, b)

proc splitLeaf(n: RTreeNode; minE: int): RTreeNode =
  ## Splits `n` (a leaf) in place; returns the sibling.
  let (s1, s2) = pickSeedsLinearLeaf(n.entries)
  let entries = n.entries
  var aEntries: seq[LeafEntry] = @[entries[s1]]
  var bEntries: seq[LeafEntry] = @[entries[s2]]
  var aBox = entries[s1].bbox
  var bBox = entries[s2].bbox
  var remaining: seq[int] = @[]
  for i in 0 ..< entries.len:
    if i != s1 and i != s2: remaining.add(i)
  let total = entries.len
  while remaining.len > 0:
    let needA = minE - aEntries.len
    let needB = minE - bEntries.len
    let leftover = remaining.len
    if needA >= leftover:
      for j in remaining: aEntries.add(entries[j]); aBox = union(aBox, entries[j].bbox)
      remaining.setLen(0); break
    if needB >= leftover:
      for j in remaining: bEntries.add(entries[j]); bBox = union(bBox, entries[j].bbox)
      remaining.setLen(0); break
    # pick next: take the entry with greatest cost difference and assign
    var pickIdx = 0
    var pickDiff = NegInf
    for k, idx in remaining:
      let d = abs(enlargement(aBox, entries[idx].bbox) - enlargement(bBox, entries[idx].bbox))
      if d > pickDiff: pickDiff = d; pickIdx = k
    let chosen = remaining[pickIdx]
    remaining.del(pickIdx)
    let dA = enlargement(aBox, entries[chosen].bbox)
    let dB = enlargement(bBox, entries[chosen].bbox)
    if dA < dB or (dA == dB and aEntries.len <= bEntries.len):
      aEntries.add(entries[chosen]); aBox = union(aBox, entries[chosen].bbox)
    else:
      bEntries.add(entries[chosen]); bBox = union(bBox, entries[chosen].bbox)
  doAssert aEntries.len + bEntries.len == total
  n.entries = aEntries
  n.bbox = aBox
  result = newLeaf()
  result.entries = bEntries
  result.bbox = bBox

proc splitInternal(n: RTreeNode; minE: int): RTreeNode =
  let (s1, s2) = pickSeedsLinearChildren(n.children)
  let kids = n.children
  var aKids: seq[RTreeNode] = @[kids[s1]]
  var bKids: seq[RTreeNode] = @[kids[s2]]
  var aBox = kids[s1].bbox
  var bBox = kids[s2].bbox
  var remaining: seq[int] = @[]
  for i in 0 ..< kids.len:
    if i != s1 and i != s2: remaining.add(i)
  let total = kids.len
  while remaining.len > 0:
    let needA = minE - aKids.len
    let needB = minE - bKids.len
    let leftover = remaining.len
    if needA >= leftover:
      for j in remaining: aKids.add(kids[j]); aBox = union(aBox, kids[j].bbox)
      remaining.setLen(0); break
    if needB >= leftover:
      for j in remaining: bKids.add(kids[j]); bBox = union(bBox, kids[j].bbox)
      remaining.setLen(0); break
    var pickIdx = 0
    var pickDiff = NegInf
    for k, idx in remaining:
      let d = abs(enlargement(aBox, kids[idx].bbox) - enlargement(bBox, kids[idx].bbox))
      if d > pickDiff: pickDiff = d; pickIdx = k
    let chosen = remaining[pickIdx]
    remaining.del(pickIdx)
    let dA = enlargement(aBox, kids[chosen].bbox)
    let dB = enlargement(bBox, kids[chosen].bbox)
    if dA < dB or (dA == dB and aKids.len <= bKids.len):
      aKids.add(kids[chosen]); aBox = union(aBox, kids[chosen].bbox)
    else:
      bKids.add(kids[chosen]); bBox = union(bBox, kids[chosen].bbox)
  doAssert aKids.len + bKids.len == total
  n.children = aKids
  n.bbox = aBox
  result = newInternal()
  result.children = bKids
  result.bbox = bBox

# --------- insert ---------

proc insertRec(n: RTreeNode; entry: LeafEntry; maxE, minE: int): RTreeNode =
  ## Inserts entry into subtree rooted at n. Returns a non-nil sibling if `n`
  ## was split (caller must add the sibling at its level).
  if n.isLeaf:
    n.entries.add(entry)
    n.bbox = union(n.bbox, entry.bbox)
    if n.entries.len > maxE:
      return splitLeaf(n, minE)
    return nil
  # internal node: choose subtree minimising enlargement (tie: smaller area)
  var bestIdx = 0
  var bestEnlarge = Inf
  var bestArea = Inf
  for i, c in n.children:
    let e = enlargement(c.bbox, entry.bbox)
    let a = c.bbox.area
    if e < bestEnlarge or (e == bestEnlarge and a < bestArea):
      bestEnlarge = e; bestArea = a; bestIdx = i
  let sibling = insertRec(n.children[bestIdx], entry, maxE, minE)
  # update child bbox after recursive descent
  n.children[bestIdx].recomputeBBox()
  n.bbox = union(n.bbox, entry.bbox)
  if not sibling.isNil:
    n.children.add(sibling)
    n.bbox = union(n.bbox, sibling.bbox)
    if n.children.len > maxE:
      return splitInternal(n, minE)
  return nil

proc insert*(t: RTree; docId: string; b: BBox) =
  if docId in t.docBBox:
    discard  # caller should call remove first; we tolerate by removing below
  let entry = LeafEntry(bbox: b, docId: docId)
  let sibling = insertRec(t.root, entry, t.maxEntries, t.minEntries)
  if not sibling.isNil:
    # grow tree upward
    let newRoot = newInternal()
    newRoot.children = @[t.root, sibling]
    newRoot.recomputeBBox()
    t.root = newRoot
  t.docBBox[docId] = b
  inc t.size

# --------- delete (CondenseTree) ---------

proc findLeafAndRemove(n: RTreeNode; docId: string; targetBBox: BBox): bool =
  ## Recursive remove. Returns true if entry was removed somewhere under n.
  ## Underflow handling is delegated to condense().
  if n.isLeaf:
    var idx = -1
    for i, e in n.entries:
      if e.docId == docId:
        idx = i; break
    if idx < 0: return false
    n.entries.del(idx)
    n.recomputeBBox()
    return true
  for i in countdown(n.children.len - 1, 0):
    let c = n.children[i]
    if not intersects(c.bbox, targetBBox): continue
    if findLeafAndRemove(c, docId, targetBBox):
      n.recomputeBBox()
      return true
  return false

proc condense(t: RTree) =
  ## Walk down/up to find any node with too few children and reinsert orphans.
  # Simple approach: collect underflowing nodes' contents into orphans, rebuild
  # the affected subtree by reinserting orphans. We recurse from root.
  var orphans: seq[LeafEntry] = @[]
  proc walk(n: RTreeNode; depth: int): bool =
    # returns true if n itself should be removed (became empty)
    if n.isLeaf:
      if n.entries.len < t.minEntries and depth > 0:
        for e in n.entries: orphans.add(e)
        return true
      return false
    var i = 0
    while i < n.children.len:
      let c = n.children[i]
      let drop = walk(c, depth + 1)
      if drop:
        n.children.del(i)
      else:
        inc i
    n.recomputeBBox()
    if n.children.len < t.minEntries and depth > 0:
      # collect leaves under n into orphans
      proc collect(x: RTreeNode) =
        if x.isLeaf:
          for e in x.entries: orphans.add(e)
        else:
          for c in x.children: collect(c)
      collect(n)
      return true
    return false
  discard walk(t.root, 0)
  # Root cleanup: if root is internal with single child, contract
  while not t.root.isLeaf and t.root.children.len == 1:
    t.root = t.root.children[0]
  # Reinsert orphans
  for e in orphans:
    let sibling = insertRec(t.root, e, t.maxEntries, t.minEntries)
    if not sibling.isNil:
      let newRoot = newInternal()
      newRoot.children = @[t.root, sibling]
      newRoot.recomputeBBox()
      t.root = newRoot

proc remove*(t: RTree; docId: string): bool =
  if docId notin t.docBBox: return false
  let b = t.docBBox[docId]
  let removed = findLeafAndRemove(t.root, docId, b)
  if not removed:
    # bbox may have been wrong; do full scan as fallback
    proc scanRemove(n: RTreeNode): bool =
      if n.isLeaf:
        for i, e in n.entries:
          if e.docId == docId:
            n.entries.del(i); n.recomputeBBox(); return true
        return false
      for c in n.children:
        if scanRemove(c):
          n.recomputeBBox(); return true
      return false
    if not scanRemove(t.root):
      return false
  t.docBBox.del(docId)
  dec t.size
  condense(t)
  return true

# --------- queries ---------

proc searchBBox*(t: RTree; q: BBox; limit = 0): seq[string] =
  result = @[]
  if t.size == 0: return
  if not intersects(t.root.bbox, q): return
  # Explicit stack avoids closure-capture of `result`.
  var stack: seq[RTreeNode] = @[t.root]
  while stack.len > 0:
    let n = stack.pop()
    if n.isLeaf:
      for e in n.entries:
        if intersects(e.bbox, q):
          result.add(e.docId)
          if limit > 0 and result.len >= limit: return
    else:
      for c in n.children:
        if intersects(c.bbox, q):
          stack.add(c)

# --- KNN with priority queue (best-first) ---

type
  KnnItem = object
    distSq: float64
    isLeaf: bool       # true if entry, false if internal node ref
    docId: string      # populated when isLeaf
    node: RTreeNode    # populated when not isLeaf

proc `<`(a, b: KnnItem): bool {.inline.} = a.distSq < b.distSq

proc nearest*(t: RTree; x, y: float64; k: int): seq[(string, float64)] =
  ## Returns up to k nearest doc ids with their euclidean distances (sqrt-ed).
  result = @[]
  if t.size == 0 or k <= 0: return
  var pq = initHeapQueue[KnnItem]()
  pq.push(KnnItem(distSq: minDistSq(t.root.bbox, x, y), isLeaf: false, node: t.root))
  while pq.len > 0:
    let it = pq.pop()
    if it.isLeaf:
      result.add((it.docId, sqrt(it.distSq)))
      if result.len >= k: return
      continue
    let n = it.node
    if n.isLeaf:
      for e in n.entries:
        let d = minDistSq(e.bbox, x, y)
        pq.push(KnnItem(distSq: d, isLeaf: true, docId: e.docId))
    else:
      for c in n.children:
        let d = minDistSq(c.bbox, x, y)
        pq.push(KnnItem(distSq: d, isLeaf: false, node: c))

proc nearestWithin*(t: RTree; x, y: float64; maxDist: float64; limit = 0): seq[(string, float64)] =
  ## Best-first traversal that stops when bbox min-distance exceeds `maxDist`.
  result = @[]
  if t.size == 0 or maxDist < 0: return
  let cutoff = maxDist * maxDist
  var pq = initHeapQueue[KnnItem]()
  pq.push(KnnItem(distSq: minDistSq(t.root.bbox, x, y), isLeaf: false, node: t.root))
  while pq.len > 0:
    let it = pq.pop()
    if it.distSq > cutoff: return
    if it.isLeaf:
      result.add((it.docId, sqrt(it.distSq)))
      if limit > 0 and result.len >= limit: return
      continue
    let n = it.node
    if n.isLeaf:
      for e in n.entries:
        let d = minDistSq(e.bbox, x, y)
        if d <= cutoff:
          pq.push(KnnItem(distSq: d, isLeaf: true, docId: e.docId))
    else:
      for c in n.children:
        let d = minDistSq(c.bbox, x, y)
        if d <= cutoff:
          pq.push(KnnItem(distSq: d, isLeaf: false, node: c))

# --- Geographic KNN (haversine) ---
#
# bbox lower-bound for haversine: clamp the query point's lon/lat into the bbox
# range and compute haversine to the clamped point. This is exact when the
# nearest point on the bbox is a corner or edge intersection; conservative
# (returns ≤ true min) otherwise — perfect for KNN pruning.
# Antimeridian-spanning bboxes are NOT handled specially; treat the index as
# operating on bboxes that don't cross 180°/-180°.

proc haversineMinMeters*(b: BBox; lon, lat: float64): float64 =
  ## Lower bound on haversine distance from (lon, lat) to nearest point of b.
  ## Zero if (lon, lat) is inside b.
  let clampedLon =
    if lon < b.minX: b.minX
    elif lon > b.maxX: b.maxX
    else: lon
  let clampedLat =
    if lat < b.minY: b.minY
    elif lat > b.maxY: b.maxY
    else: lat
  haversineMeters(lon, lat, clampedLon, clampedLat)

proc nearestGeo*(t: RTree; lon, lat: float64; k: int): seq[(string, float64)] =
  ## K-nearest by haversine distance (metres). The R-tree is assumed to index
  ## points or bboxes in (lon, lat) degrees.
  result = @[]
  if t.size == 0 or k <= 0: return
  var pq = initHeapQueue[KnnItem]()
  pq.push(KnnItem(distSq: haversineMinMeters(t.root.bbox, lon, lat),
                  isLeaf: false, node: t.root))
  while pq.len > 0:
    let it = pq.pop()
    if it.isLeaf:
      result.add((it.docId, it.distSq))
      if result.len >= k: return
      continue
    let n = it.node
    if n.isLeaf:
      for e in n.entries:
        let d = haversineMinMeters(e.bbox, lon, lat)
        pq.push(KnnItem(distSq: d, isLeaf: true, docId: e.docId))
    else:
      for c in n.children:
        let d = haversineMinMeters(c.bbox, lon, lat)
        pq.push(KnnItem(distSq: d, isLeaf: false, node: c))

proc nearestGeoWithin*(t: RTree; lon, lat: float64;
                       maxMeters: float64; limit = 0): seq[(string, float64)] =
  ## Best-first traversal stopping when bbox haversine lower-bound > maxMeters.
  result = @[]
  if t.size == 0 or maxMeters < 0: return
  var pq = initHeapQueue[KnnItem]()
  pq.push(KnnItem(distSq: haversineMinMeters(t.root.bbox, lon, lat),
                  isLeaf: false, node: t.root))
  while pq.len > 0:
    let it = pq.pop()
    if it.distSq > maxMeters: return
    if it.isLeaf:
      result.add((it.docId, it.distSq))
      if limit > 0 and result.len >= limit: return
      continue
    let n = it.node
    if n.isLeaf:
      for e in n.entries:
        let d = haversineMinMeters(e.bbox, lon, lat)
        if d <= maxMeters:
          pq.push(KnnItem(distSq: d, isLeaf: true, docId: e.docId))
    else:
      for c in n.children:
        let d = haversineMinMeters(c.bbox, lon, lat)
        if d <= maxMeters:
          pq.push(KnnItem(distSq: d, isLeaf: false, node: c))

# --------- STR bulk loader ---------
#
# Sort-Tile-Recursive: sort by x, partition into vertical "slices", sort each
# by y, partition each slice into pages (leaves). Repeat at parent levels.
# Produces nodes packed to maxEntries with very tight MBRs.

proc strBuildLevel(items: seq[LeafEntry]; maxE: int): seq[RTreeNode] =
  result = @[]
  if items.len == 0: return
  # number of leaves
  let P = (items.len + maxE - 1) div maxE
  let S = max(1, int(ceil(sqrt(P.float))))
  var sorted = items
  sorted.sort(proc (a, b: LeafEntry): int =
    let ax = (a.bbox.minX + a.bbox.maxX) * 0.5
    let bx = (b.bbox.minX + b.bbox.maxX) * 0.5
    cmp(ax, bx))
  let perSlice = (sorted.len + S - 1) div S
  var idx = 0
  while idx < sorted.len:
    var slice = sorted[idx ..< min(idx + perSlice, sorted.len)]
    slice.sort(proc (a, b: LeafEntry): int =
      let ay = (a.bbox.minY + a.bbox.maxY) * 0.5
      let by = (b.bbox.minY + b.bbox.maxY) * 0.5
      cmp(ay, by))
    var j = 0
    while j < slice.len:
      let leaf = newLeaf()
      let stop = min(j + maxE, slice.len)
      for k in j ..< stop: leaf.entries.add(slice[k])
      leaf.recomputeBBox()
      result.add(leaf)
      j = stop
    idx += perSlice

proc strBuildInternal(nodes: seq[RTreeNode]; maxE: int): seq[RTreeNode] =
  result = @[]
  if nodes.len == 0: return
  let P = (nodes.len + maxE - 1) div maxE
  let S = max(1, int(ceil(sqrt(P.float))))
  var sorted = nodes
  sorted.sort(proc (a, b: RTreeNode): int =
    let ax = (a.bbox.minX + a.bbox.maxX) * 0.5
    let bx = (b.bbox.minX + b.bbox.maxX) * 0.5
    cmp(ax, bx))
  let perSlice = (sorted.len + S - 1) div S
  var idx = 0
  while idx < sorted.len:
    var slice = sorted[idx ..< min(idx + perSlice, sorted.len)]
    slice.sort(proc (a, b: RTreeNode): int =
      let ay = (a.bbox.minY + a.bbox.maxY) * 0.5
      let by = (b.bbox.minY + b.bbox.maxY) * 0.5
      cmp(ay, by))
    var j = 0
    while j < slice.len:
      let parent = newInternal()
      let stop = min(j + maxE, slice.len)
      for k in j ..< stop: parent.children.add(slice[k])
      parent.recomputeBBox()
      result.add(parent)
      j = stop
    idx += perSlice

proc bulkLoad*(t: RTree; items: openArray[(string, BBox)]) =
  ## Bulk-load entries with STR. Resets the tree. Use this when building from
  ## scratch — it's both faster and produces tighter MBRs than incremental insert.
  t.docBBox = initTable[string, BBox]()
  for (id, b) in items: t.docBBox[id] = b
  t.size = items.len
  if items.len == 0:
    t.root = newLeaf(); return
  var entries: seq[LeafEntry] = @[]
  for (id, b) in items: entries.add(LeafEntry(bbox: b, docId: id))
  var level = strBuildLevel(entries, t.maxEntries)
  while level.len > 1:
    level = strBuildInternal(level, t.maxEntries)
  t.root = level[0]

# --------- GeoIndex (extracts lon/lat from Value) ---------

proc readFloat(v: Value): (bool, float64) {.inline.} =
  if v.isNil: return (false, 0.0)
  case v.kind
  of vkFloat: (true, v.f)
  of vkInt:   (true, float64(v.i))
  else:       (false, 0.0)

proc extractPoint*(idx: GeoIndex; doc: Value): (bool, float64, float64) =
  if doc.isNil or doc.kind != vkObject: return (false, 0.0, 0.0)
  let lon = doc[idx.lonField]
  let lat = doc[idx.latField]
  let (okLon, fLon) = readFloat(lon)
  let (okLat, fLat) = readFloat(lat)
  if not okLon or not okLat: return (false, 0.0, 0.0)
  (true, fLon, fLat)

proc newGeoIndex*(name, lonField, latField: string): GeoIndex =
  GeoIndex(name: name, lonField: lonField, latField: latField, tree: newRTree())

proc indexDoc*(idx: GeoIndex; docId: string; doc: Value) =
  let (ok, lon, lat) = idx.extractPoint(doc)
  if not ok: return
  if docId in idx.tree.docBBox:
    discard idx.tree.remove(docId)
  idx.tree.insert(docId, point(lon, lat))

proc unindexDoc*(idx: GeoIndex; docId: string; doc: Value) =
  ## docId-only signature is enough since we track docBBox.
  discard idx.tree.remove(docId)

proc reindexDoc*(idx: GeoIndex; docId: string; oldDoc, newDoc: Value) =
  if not oldDoc.isNil:
    discard idx.tree.remove(docId)
  if not newDoc.isNil:
    let (ok, lon, lat) = idx.extractPoint(newDoc)
    if ok: idx.tree.insert(docId, point(lon, lat))

proc bulkBuild*(idx: GeoIndex; docs: Table[string, Value]) =
  var pairs: seq[(string, BBox)] = @[]
  for id, v in docs:
    let (ok, lon, lat) = idx.extractPoint(v)
    if ok: pairs.add((id, point(lon, lat)))
  idx.tree.bulkLoad(pairs)

type GeoIndexesByName* = Table[string, GeoIndex]

# --------- Polygon helpers ---------

proc polygonBBox*(p: Polygon): BBox =
  ## MBR of a polygon. Empty BBox if no vertices.
  if p.vertices.len == 0: return InfBBox
  result = BBox(
    minX: p.vertices[0][0], minY: p.vertices[0][1],
    maxX: p.vertices[0][0], maxY: p.vertices[0][1])
  for v in p.vertices:
    if v[0] < result.minX: result.minX = v[0]
    if v[0] > result.maxX: result.maxX = v[0]
    if v[1] < result.minY: result.minY = v[1]
    if v[1] > result.maxY: result.maxY = v[1]

proc pointInPolygon*(p: Polygon; x, y: float64): bool =
  ## Ray-casting (Crossing Number) test. Edge cases: a point exactly on an
  ## edge may resolve either way — we don't define edge semantics rigorously.
  let n = p.vertices.len
  if n < 3: return false
  var inside = false
  var j = n - 1
  for i in 0 ..< n:
    let (xi, yi) = p.vertices[i]
    let (xj, yj) = p.vertices[j]
    if (yi > y) != (yj > y):
      let xIntersect = (xj - xi) * (y - yi) / (yj - yi) + xi
      if x < xIntersect:
        inside = not inside
    j = i
  inside

proc readPolygonFromValue*(v: Value): (bool, Polygon) =
  ## Polygons are encoded as `VArray([VArray([VFloat(x), VFloat(y)]), ...])`.
  ## Returns (false, _) on shape mismatch. Accepts vkInt for coords too.
  if v.isNil or v.kind != vkArray: return (false, Polygon())
  var p = Polygon(vertices: @[])
  for vert in v.arr:
    if vert.isNil or vert.kind != vkArray or vert.arr.len < 2:
      return (false, Polygon())
    let (okX, x) = readFloat(vert.arr[0])
    let (okY, y) = readFloat(vert.arr[1])
    if not okX or not okY: return (false, Polygon())
    p.vertices.add((x, y))
  if p.vertices.len < 3: return (false, Polygon())
  (true, p)

# --------- PolygonIndex ---------

proc newPolygonIndex*(name, polygonField: string): PolygonIndex =
  PolygonIndex(
    name: name, polygonField: polygonField,
    tree: newRTree(),
    polygons: initTable[string, Polygon]())

proc extractPolygon*(idx: PolygonIndex; doc: Value): (bool, Polygon) =
  if doc.isNil or doc.kind != vkObject: return (false, Polygon())
  readPolygonFromValue(doc[idx.polygonField])

proc indexDoc*(idx: PolygonIndex; docId: string; doc: Value) =
  let (ok, poly) = idx.extractPolygon(doc)
  if not ok: return
  if docId in idx.tree.docBBox:
    discard idx.tree.remove(docId)
    idx.polygons.del(docId)
  idx.tree.insert(docId, polygonBBox(poly))
  idx.polygons[docId] = poly

proc unindexDoc*(idx: PolygonIndex; docId: string; doc: Value) =
  discard idx.tree.remove(docId)
  idx.polygons.del(docId)

proc reindexDoc*(idx: PolygonIndex; docId: string; oldDoc, newDoc: Value) =
  if not oldDoc.isNil:
    discard idx.tree.remove(docId)
    idx.polygons.del(docId)
  if not newDoc.isNil:
    let (ok, poly) = idx.extractPolygon(newDoc)
    if ok:
      idx.tree.insert(docId, polygonBBox(poly))
      idx.polygons[docId] = poly

proc bulkBuild*(idx: PolygonIndex; docs: Table[string, Value]) =
  var pairs: seq[(string, BBox)] = @[]
  idx.polygons = initTable[string, Polygon]()
  for id, v in docs:
    let (ok, poly) = idx.extractPolygon(v)
    if ok:
      pairs.add((id, polygonBBox(poly)))
      idx.polygons[id] = poly
  idx.tree.bulkLoad(pairs)

# Public R-tree exact-test queries

proc polygonsContainingPoint*(idx: PolygonIndex; x, y: float64;
                              limit = 0): seq[string] =
  ## Find every indexed polygon whose interior contains (x, y).
  result = @[]
  let q = point(x, y)
  for id in idx.tree.searchBBox(q, 0):
    if id in idx.polygons and pointInPolygon(idx.polygons[id], x, y):
      result.add(id)
      if limit > 0 and result.len >= limit: return

proc polygonsIntersectingBBox*(idx: PolygonIndex; q: BBox;
                               limit = 0): seq[string] =
  ## Bbox-level intersection (cheap; not exact polygon-polygon intersection).
  idx.tree.searchBBox(q, limit)

type PolygonIndexesByName* = Table[string, PolygonIndex]

# --------- binary persistence ---------
#
# We persist the (docId -> BBox) mapping plus, for polygon indexes, the
# polygon vertex lists. The R-tree itself is rebuilt via bulkLoad — STR over
# already-known entries is much faster than walking the tree for serialization
# and produces tighter MBRs.
#
# Format:
#   magic "GLENGRI1" (8 B)
#   version          uint32 (currently 1)
#   kind             uint32 (1 = geo points, 2 = polygons)
#   entryCount       uint64
#   For each entry:
#     idLen          uint32
#     id             <idLen> bytes
#     minX, minY, maxX, maxY   4 × float64
#   If kind == 2, then:
#     polyCount      uint64
#     For each polygon:
#       idLen        uint32
#       id           <idLen> bytes
#       vertCount    uint32
#       vertices     vertCount × (float64, float64)
#   crc32            uint32  (FNV-1a over all bytes after the magic+version+kind header)

const
  GriMagic        = "GLENGRI1"
  GriVersion      = 1'u32
  GriKindGeo      = 1'u32
  GriKindPolygon  = 2'u32

proc griFnv1a32(buf: openArray[byte]): uint32 =
  var h: uint32 = 0x811C9DC5'u32
  for b in buf:
    h = (h xor uint32(b)) * 0x01000193'u32
  h

proc writeBytes(s: var seq[byte]; data: openArray[byte]) =
  let off = s.len
  s.setLen(off + data.len)
  for i, b in data: s[off + i] = b

proc writeStr(s: var seq[byte]; v: string) =
  if v.len == 0: return
  let off = s.len
  s.setLen(off + v.len)
  for i, c in v: s[off + i] = byte(c)

proc writeU32(s: var seq[byte]; v: uint32) =
  let off = s.len
  s.setLen(off + 4)
  for i in 0 ..< 4:
    s[off + i] = byte((v shr (i * 8)) and 0xFF'u32)

proc writeU64(s: var seq[byte]; v: uint64) =
  let off = s.len
  s.setLen(off + 8)
  for i in 0 ..< 8:
    s[off + i] = byte((v shr (i * 8)) and 0xFF'u64)

proc writeF64(s: var seq[byte]; v: float64) =
  let bits = cast[uint64](v)
  writeU64(s, bits)

type ByteReader = object
  buf: seq[byte]
  off: int

proc readBytes(r: var ByteReader; n: int): seq[byte] =
  if r.off + n > r.buf.len:
    raise newException(IOError, "short read")
  result = r.buf[r.off ..< r.off + n]
  r.off += n

proc readU32(r: var ByteReader): uint32 =
  if r.off + 4 > r.buf.len: raise newException(IOError, "short read u32")
  result = 0
  for i in 0 ..< 4:
    result = result or (uint32(r.buf[r.off + i]) shl (i * 8))
  r.off += 4

proc readU64(r: var ByteReader): uint64 =
  if r.off + 8 > r.buf.len: raise newException(IOError, "short read u64")
  result = 0
  for i in 0 ..< 8:
    result = result or (uint64(r.buf[r.off + i]) shl (i * 8))
  r.off += 8

proc readF64(r: var ByteReader): float64 =
  cast[float64](readU64(r))

proc readStr(r: var ByteReader; n: int): string =
  if r.off + n > r.buf.len: raise newException(IOError, "short read str")
  result = newString(n)
  for i in 0 ..< n:
    result[i] = char(r.buf[r.off + i])
  r.off += n

proc dumpRTreeEntries(t: RTree): seq[(string, BBox)] =
  result = @[]
  for id, b in t.docBBox: result.add((id, b))

proc dumpGeoIndex*(idx: GeoIndex; path: string) =
  ## Atomically write the geo index to disk (entries-only; tree is rebuilt on load).
  var body: seq[byte] = @[]
  let entries = dumpRTreeEntries(idx.tree)
  writeU64(body, uint64(entries.len))
  for (id, b) in entries:
    writeU32(body, uint32(id.len))
    writeStr(body, id)
    writeF64(body, b.minX); writeF64(body, b.minY)
    writeF64(body, b.maxX); writeF64(body, b.maxY)
  let crc = griFnv1a32(body)
  var out2: seq[byte] = @[]
  writeStr(out2, GriMagic)
  writeU32(out2, GriVersion)
  writeU32(out2, GriKindGeo)
  for b in body: out2.add(b)
  writeU32(out2, crc)
  let tmp = path & ".tmp"
  writeFile(tmp, cast[string](out2))
  moveFile(tmp, path)

proc dumpPolygonIndex*(idx: PolygonIndex; path: string) =
  var body: seq[byte] = @[]
  let entries = dumpRTreeEntries(idx.tree)
  writeU64(body, uint64(entries.len))
  for (id, b) in entries:
    writeU32(body, uint32(id.len))
    writeStr(body, id)
    writeF64(body, b.minX); writeF64(body, b.minY)
    writeF64(body, b.maxX); writeF64(body, b.maxY)
  writeU64(body, uint64(idx.polygons.len))
  for id, poly in idx.polygons:
    writeU32(body, uint32(id.len))
    writeStr(body, id)
    writeU32(body, uint32(poly.vertices.len))
    for (x, y) in poly.vertices:
      writeF64(body, x); writeF64(body, y)
  let crc = griFnv1a32(body)
  var out2: seq[byte] = @[]
  writeStr(out2, GriMagic)
  writeU32(out2, GriVersion)
  writeU32(out2, GriKindPolygon)
  for b in body: out2.add(b)
  writeU32(out2, crc)
  let tmp = path & ".tmp"
  writeFile(tmp, cast[string](out2))
  moveFile(tmp, path)

proc loadIndexFile(path: string; expectedKind: uint32): (bool, seq[(string, BBox)], Table[string, Polygon]) =
  ## Reads the file and validates magic / version / kind / CRC.
  ## Returns (ok, entries, polygons). polygons is empty for geo-point indexes.
  if not fileExists(path):
    return (false, @[], initTable[string, Polygon]())
  let raw = cast[seq[byte]](readFile(path))
  if raw.len < GriMagic.len + 4 + 4 + 4: return (false, @[], initTable[string, Polygon]())
  var r = ByteReader(buf: raw, off: 0)
  let magic = readStr(r, GriMagic.len)
  if magic != GriMagic: return (false, @[], initTable[string, Polygon]())
  let ver = readU32(r)
  if ver != GriVersion: return (false, @[], initTable[string, Polygon]())
  let kind = readU32(r)
  if kind != expectedKind: return (false, @[], initTable[string, Polygon]())
  let bodyStart = r.off
  if raw.len < bodyStart + 4: return (false, @[], initTable[string, Polygon]())
  let bodyEnd = raw.len - 4
  let body = raw[bodyStart ..< bodyEnd]
  let crcStored = (uint32(raw[bodyEnd])) or
                  (uint32(raw[bodyEnd + 1]) shl 8) or
                  (uint32(raw[bodyEnd + 2]) shl 16) or
                  (uint32(raw[bodyEnd + 3]) shl 24)
  if griFnv1a32(body) != crcStored:
    return (false, @[], initTable[string, Polygon]())
  var entries: seq[(string, BBox)] = @[]
  var polys = initTable[string, Polygon]()
  var br = ByteReader(buf: body, off: 0)
  try:
    let entryCount = int(readU64(br))
    for _ in 0 ..< entryCount:
      let idLen = int(readU32(br))
      let id = readStr(br, idLen)
      let b = BBox(minX: readF64(br), minY: readF64(br),
                   maxX: readF64(br), maxY: readF64(br))
      entries.add((id, b))
    if kind == GriKindPolygon:
      let polyCount = int(readU64(br))
      for _ in 0 ..< polyCount:
        let idLen = int(readU32(br))
        let id = readStr(br, idLen)
        let vc = int(readU32(br))
        var p = Polygon(vertices: @[])
        for _ in 0 ..< vc:
          p.vertices.add((readF64(br), readF64(br)))
        polys[id] = p
  except IOError:
    return (false, @[], initTable[string, Polygon]())
  (true, entries, polys)

proc tryLoadGeoIndex*(idx: GeoIndex; path: string): bool =
  let (ok, entries, _) = loadIndexFile(path, GriKindGeo)
  if not ok: return false
  idx.tree.bulkLoad(entries)
  true

proc tryLoadPolygonIndex*(idx: PolygonIndex; path: string): bool =
  let (ok, entries, polys) = loadIndexFile(path, GriKindPolygon)
  if not ok: return false
  idx.tree.bulkLoad(entries)
  idx.polygons = polys
  true
