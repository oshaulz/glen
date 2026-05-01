# Glen geomesh — a 2-D grid of values (or per-cell vectors) tied to a
# geographic bounding box. Designed for things like LLM-emitted probability
# rasters: a model emits a (rows × cols × classes) tensor over an area, you
# store it as one document, and you can sample it at (lon, lat).
#
# Storage choices for size:
#   * data is held flat as seq[float64], row-major, channel-interleaved:
#       data[(row*cols + col)*channels + ch]
#   * Serialised to disk as vkBytes (raw little-endian float64), not
#     VArray-of-VFloat. A 1000×1000×5 mesh = 5M floats = 40 MB packed,
#     vs ~hundreds of MB as nested arrays.
#
# Conventions:
#   * row 0   == top of bbox (maxLat). row rows-1 == bottom (minLat).
#   * col 0   == left of bbox (minLon). col cols-1 == right (maxLon).
#     Matches image / tile conventions; useful when the mesh comes from
#     rasterised input.

import std/strutils
import glen/types
import glen/geo
import glen/linalg

type
  GeoMesh* = object
    bbox*:     BBox
    rows*:     int
    cols*:     int
    channels*: int           # 1 for scalar mesh, ≥1 per-cell vector
    labels*:   seq[string]   # length 0 or == channels (optional names)
    data*:     seq[float64]  # rows*cols*channels values, row-major, channel-interleaved

# --------- Constructors ---------

proc newGeoMesh*(bbox: BBox; rows, cols: int;
                 channels = 1; labels: seq[string] = @[]): GeoMesh =
  doAssert rows >= 0 and cols >= 0 and channels >= 1
  if labels.len > 0 and labels.len != channels:
    raise newException(ValueError,
      "newGeoMesh: labels.len " & $labels.len & " != channels " & $channels)
  GeoMesh(
    bbox: bbox, rows: rows, cols: cols, channels: channels,
    labels: @labels,
    data: newSeq[float64](rows * cols * channels))

proc fromMatrix*(bbox: BBox; m: Matrix): GeoMesh =
  ## Wrap an existing Matrix (single-channel scalar mesh). Copies data.
  result = newGeoMesh(bbox, m.rows, m.cols, channels = 1)
  for i in 0 ..< m.data.len: result.data[i] = m.data[i]

# --------- Cell math ---------

proc cellAt*(m: GeoMesh; lon, lat: float64): (bool, int, int) =
  ## Returns (inBounds, row, col). Out-of-bounds points return (false, 0, 0).
  ## Lon/lat lying exactly on a max edge are clamped to the last cell.
  if m.rows == 0 or m.cols == 0: return (false, 0, 0)
  if lon < m.bbox.minX or lon > m.bbox.maxX or
     lat < m.bbox.minY or lat > m.bbox.maxY:
    return (false, 0, 0)
  let widthX = m.bbox.maxX - m.bbox.minX
  let widthY = m.bbox.maxY - m.bbox.minY
  if widthX <= 0.0 or widthY <= 0.0: return (false, 0, 0)
  let fx = (lon - m.bbox.minX) / widthX            # [0..1]
  let fy = (m.bbox.maxY - lat) / widthY            # [0..1]; row 0 == top
  var col = int(fx * float64(m.cols))
  var row = int(fy * float64(m.rows))
  if col >= m.cols: col = m.cols - 1
  if row >= m.rows: row = m.rows - 1
  (true, row, col)

proc cellCenter*(m: GeoMesh; row, col: int): (float64, float64) =
  ## Geographic center of a (row, col) cell.
  let widthX = m.bbox.maxX - m.bbox.minX
  let widthY = m.bbox.maxY - m.bbox.minY
  let lon = m.bbox.minX + (float64(col) + 0.5) * widthX / float64(m.cols)
  let lat = m.bbox.maxY - (float64(row) + 0.5) * widthY / float64(m.rows)
  (lon, lat)

# --------- Reads / writes ---------

proc valueAt*(m: GeoMesh; lon, lat: float64; channel = 0): float64 =
  ## Single-channel sample at the cell containing (lon, lat). Returns NaN if
  ## out of bounds — callers that care should use `cellAt` first.
  if channel < 0 or channel >= m.channels:
    raise newException(ValueError, "valueAt: channel out of range")
  let (ok, row, col) = m.cellAt(lon, lat)
  if not ok: return NaN
  m.data[(row * m.cols + col) * m.channels + channel]

# --------- Interpolation ---------

type
  InterpolationKind* = enum
    imNearest    ## Pick the cell containing (lon, lat). Cheapest, but
                 ## piecewise-constant — wrong for continuous fields.
    imBilinear   ## 4-cell weighted average. Standard for continuous
                 ## quantities (temperature, pressure, wind speed).

proc cellAtFractional(m: GeoMesh; lon, lat: float64): (bool, float64, float64) =
  ## Returns (inBounds, fracRow, fracCol) where the integer parts are
  ## the cell indices and fractional parts are the offset *within* the
  ## cell from its top-left corner. Used by bilinear sampling.
  if m.rows == 0 or m.cols == 0: return (false, 0.0, 0.0)
  if lon < m.bbox.minX or lon > m.bbox.maxX or
     lat < m.bbox.minY or lat > m.bbox.maxY: return (false, 0.0, 0.0)
  let widthX = m.bbox.maxX - m.bbox.minX
  let widthY = m.bbox.maxY - m.bbox.minY
  if widthX <= 0.0 or widthY <= 0.0: return (false, 0.0, 0.0)
  let fx = (lon - m.bbox.minX) / widthX            # [0..1]
  let fy = (m.bbox.maxY - lat) / widthY            # [0..1]; row 0 == top
  (true, fy * float64(m.rows), fx * float64(m.cols))

proc sampleAt*(m: GeoMesh; lon, lat: float64;
               channel = 0;
               kind: InterpolationKind = imBilinear): float64 =
  ## Sample the mesh at a precise (lon, lat). With `imNearest` this is
  ## equivalent to `valueAt`. With `imBilinear` (the default) it returns
  ## the bilinear weighted average of the 4 surrounding cell centres —
  ## the right thing for continuous fields like temperature, pressure,
  ## wind, or precipitation.
  ##
  ## Returns NaN if (lon, lat) is outside the mesh's bbox.
  if channel < 0 or channel >= m.channels:
    raise newException(ValueError, "sampleAt: channel out of range")
  case kind
  of imNearest:
    return m.valueAt(lon, lat, channel)
  of imBilinear:
    let (ok, fr, fc) = m.cellAtFractional(lon, lat)
    if not ok: return NaN
    # Cells are addressed by their top-left corner. To weight by *cell
    # centres*, shift by 0.5 — a query at exactly the centre of cell (r, c)
    # is just that cell's value, not a 4-way average.
    let r = fr - 0.5
    let c = fc - 0.5
    var r0 = int(r); var c0 = int(c)
    if r < 0.0: r0 = -1   # int() truncates toward zero, not floor
    if c < 0.0: c0 = -1
    let r1 = r0 + 1
    let c1 = c0 + 1
    let dr = r - float64(r0)
    let dc = c - float64(c0)
    # Clamp at edges: query points in the half-cell border just outside
    # cell centres degenerate to the edge cell value.
    proc cellVal(m: GeoMesh; row, col, ch: int): float64 =
      var rr = row
      var cc = col
      if rr < 0: rr = 0
      if cc < 0: cc = 0
      if rr >= m.rows: rr = m.rows - 1
      if cc >= m.cols: cc = m.cols - 1
      m.data[(rr * m.cols + cc) * m.channels + ch]
    let v00 = cellVal(m, r0, c0, channel)
    let v01 = cellVal(m, r0, c1, channel)
    let v10 = cellVal(m, r1, c0, channel)
    let v11 = cellVal(m, r1, c1, channel)
    let top    = v00 * (1.0 - dc) + v01 * dc
    let bottom = v10 * (1.0 - dc) + v11 * dc
    return top * (1.0 - dr) + bottom * dr

proc vectorAt*(m: GeoMesh; lon, lat: float64): Vector =
  ## Returns the full channel-vector at the cell containing (lon, lat).
  ## Empty seq if out of bounds.
  let (ok, row, col) = m.cellAt(lon, lat)
  if not ok: return @[]
  result = newSeq[float64](m.channels)
  let base = (row * m.cols + col) * m.channels
  for k in 0 ..< m.channels: result[k] = m.data[base + k]

proc cellVector*(m: GeoMesh; row, col: int): Vector =
  ## Direct access by integer indices.
  if row < 0 or row >= m.rows or col < 0 or col >= m.cols:
    raise newException(ValueError, "cellVector: out of range")
  result = newSeq[float64](m.channels)
  let base = (row * m.cols + col) * m.channels
  for k in 0 ..< m.channels: result[k] = m.data[base + k]

proc `[]`*(m: GeoMesh; row, col, channel: int): float64 {.inline.} =
  m.data[(row * m.cols + col) * m.channels + channel]

proc `[]=`*(m: var GeoMesh; row, col, channel: int; v: float64) {.inline.} =
  m.data[(row * m.cols + col) * m.channels + channel] = v

proc setCell*(m: var GeoMesh; row, col: int; values: openArray[float64]) =
  ## Overwrite all channels at (row, col).
  if values.len != m.channels:
    raise newException(ValueError,
      "setCell: " & $values.len & " values for " & $m.channels & " channels")
  let base = (row * m.cols + col) * m.channels
  for k in 0 ..< m.channels: m.data[base + k] = values[k]

# Channel name → index. -1 if not labelled / not found.
proc channelIndex*(m: GeoMesh; label: string): int =
  for i, l in m.labels:
    if l == label: return i
  -1

# --------- Polygon aggregation ---------
#
# Reduce all cells whose centre lies inside a polygon to a single
# scalar (mean / min / max / sum) — the canonical "average rainfall
# over this watershed" / "max wind speed in this county" pattern.
#
# Cells are treated atomically: a cell is either fully inside or fully
# outside (decided by its centre). For polygon boundaries that cut
# across cells, this is a fine approximation when cells are small
# relative to the polygon. For sub-cell precision, oversample the mesh
# beforehand.

type
  AggregationKind* = enum
    akMean, akMin, akMax, akSum, akCount

  AggregateResult* = object
    count*: int
    sum*: float64
    minValue*: float64
    maxValue*: float64
    mean*: float64

proc aggregateInPolygonStats*(m: GeoMesh; polygon: Polygon;
                              channel = 0;
                              metric = gmPlanar): AggregateResult =
  ## Reduce mesh cells whose centres lie inside `polygon` to a stats
  ## bundle (count + sum + min + max + mean). With `metric = gmPlanar`
  ## (default) edges are straight in lon/lat space; with `gmGeographic`
  ## edges are great-circle arcs. Cells outside the polygon's MBR are
  ## skipped without the more expensive containment test.
  if channel < 0 or channel >= m.channels:
    raise newException(ValueError, "aggregateInPolygon: channel out of range")
  result = AggregateResult(count: 0, sum: 0.0,
                           minValue: Inf, maxValue: NegInf, mean: NaN)
  if m.rows == 0 or m.cols == 0: return
  let pbb = polygonBBox(polygon)
  # Convert MBR back to (row, col) bounds, expanding by one cell to be
  # safe against clamping in cellAt.
  let widthX = m.bbox.maxX - m.bbox.minX
  let widthY = m.bbox.maxY - m.bbox.minY
  if widthX <= 0.0 or widthY <= 0.0: return
  proc clampRow(r: int): int =
    if r < 0: 0
    elif r >= m.rows: m.rows - 1
    else: r
  proc clampCol(c: int): int =
    if c < 0: 0
    elif c >= m.cols: m.cols - 1
    else: c
  # The polygon's MBR may extend outside the mesh; clamp.
  let lonLo = max(pbb.minX, m.bbox.minX)
  let lonHi = min(pbb.maxX, m.bbox.maxX)
  let latLo = max(pbb.minY, m.bbox.minY)
  let latHi = min(pbb.maxY, m.bbox.maxY)
  if lonLo > lonHi or latLo > latHi: return
  # Note: row 0 is top (maxLat). So latHi → small row, latLo → big row.
  let rowMin = clampRow(int((m.bbox.maxY - latHi) / widthY * float64(m.rows)) - 1)
  let rowMax = clampRow(int((m.bbox.maxY - latLo) / widthY * float64(m.rows)) + 1)
  let colMin = clampCol(int((lonLo - m.bbox.minX) / widthX * float64(m.cols)) - 1)
  let colMax = clampCol(int((lonHi - m.bbox.minX) / widthX * float64(m.cols)) + 1)
  for row in rowMin .. rowMax:
    for col in colMin .. colMax:
      let (cLon, cLat) = m.cellCenter(row, col)
      let inside =
        case metric
        of gmPlanar:     pointInPolygon(polygon, cLon, cLat)
        of gmGeographic: pointInPolygonSpherical(polygon, cLon, cLat)
      if not inside: continue
      let v = m.data[(row * m.cols + col) * m.channels + channel]
      inc result.count
      result.sum += v
      if v < result.minValue: result.minValue = v
      if v > result.maxValue: result.maxValue = v
  if result.count > 0:
    result.mean = result.sum / float64(result.count)
  else:
    result.minValue = NaN
    result.maxValue = NaN

proc aggregateInPolygon*(m: GeoMesh; polygon: Polygon;
                        agg: AggregationKind;
                        channel = 0;
                        metric = gmPlanar): float64 =
  ## Convenience: returns a single scalar from `aggregateInPolygonStats`.
  ## Returns NaN if no cell centres lie inside the polygon (or, for
  ## `akCount`, returns 0).
  let s = aggregateInPolygonStats(m, polygon, channel, metric)
  case agg
  of akMean:  s.mean
  of akMin:   s.minValue
  of akMax:   s.maxValue
  of akSum:   s.sum
  of akCount: float64(s.count)

# --------- Value (de)serialization ---------
#
# Doc shape (one VObject):
#   bbox     : VArray([VFloat(minX), VFloat(minY), VFloat(maxX), VFloat(maxY)])
#   rows     : VInt
#   cols     : VInt
#   channels : VInt
#   labels   : VArray([VString(...)])    (omitted if empty)
#   data     : VBytes (raw little-endian float64, length = rows*cols*channels*8)

proc bytesFromFloats(data: seq[float64]): seq[byte] =
  result = newSeq[byte](data.len * 8)
  for i, f in data:
    let bits = cast[uint64](f)
    let off = i * 8
    for k in 0 ..< 8:
      result[off + k] = byte((bits shr (k * 8)) and 0xFF'u64)

proc floatsFromBytes(buf: seq[byte]; count: int): seq[float64] =
  if buf.len != count * 8:
    raise newException(ValueError,
      "floatsFromBytes: " & $buf.len & " bytes for " & $count & " floats")
  result = newSeq[float64](count)
  for i in 0 ..< count:
    var bits: uint64 = 0
    let off = i * 8
    for k in 0 ..< 8:
      bits = bits or (uint64(buf[off + k]) shl (k * 8))
    result[i] = cast[float64](bits)

proc toValue*(m: GeoMesh): Value =
  result = VObject()
  result["bbox"] = VArray(@[
    VFloat(m.bbox.minX), VFloat(m.bbox.minY),
    VFloat(m.bbox.maxX), VFloat(m.bbox.maxY)])
  result["rows"]     = VInt(int64(m.rows))
  result["cols"]     = VInt(int64(m.cols))
  result["channels"] = VInt(int64(m.channels))
  if m.labels.len > 0:
    var arr: seq[Value] = @[]
    arr.setLen(m.labels.len)
    for i, l in m.labels: arr[i] = VString(l)
    result["labels"] = VArray(arr)
  result["data"] = VBytes(bytesFromFloats(m.data))

proc readBBoxFromValue(v: Value): (bool, BBox) =
  if v.isNil or v.kind != vkArray or v.arr.len != 4: return (false, BBox())
  var f: array[4, float64]
  for i in 0 ..< 4:
    let item = v.arr[i]
    if item.isNil: return (false, BBox())
    case item.kind
    of vkFloat: f[i] = item.f
    of vkInt:   f[i] = float64(item.i)
    else:       return (false, BBox())
  (true, BBox(minX: f[0], minY: f[1], maxX: f[2], maxY: f[3]))

proc readGeoMesh*(v: Value): (bool, GeoMesh) =
  ## Decode a GeoMesh from a VObject in the layout produced by `toValue`.
  ## Permissive: if optional fields are missing or types disagree, returns
  ## (false, _) rather than raising.
  if v.isNil or v.kind != vkObject: return (false, GeoMesh())
  var m: GeoMesh
  let (okB, bb) = readBBoxFromValue(v["bbox"])
  if not okB: return (false, GeoMesh())
  m.bbox = bb
  let r = v["rows"]; let c = v["cols"]; let ch = v["channels"]
  if r.isNil or r.kind != vkInt: return (false, GeoMesh())
  if c.isNil or c.kind != vkInt: return (false, GeoMesh())
  if ch.isNil or ch.kind != vkInt: return (false, GeoMesh())
  m.rows = int(r.i); m.cols = int(c.i); m.channels = int(ch.i)
  if m.rows < 0 or m.cols < 0 or m.channels < 1: return (false, GeoMesh())
  let labV = v["labels"]
  if not labV.isNil and labV.kind == vkArray:
    if labV.arr.len != m.channels: return (false, GeoMesh())
    m.labels = newSeq[string](labV.arr.len)
    for i, item in labV.arr:
      if item.isNil or item.kind != vkString: return (false, GeoMesh())
      m.labels[i] = item.s
  let dV = v["data"]
  if dV.isNil or dV.kind != vkBytes: return (false, GeoMesh())
  let n = m.rows * m.cols * m.channels
  if dV.bytes.len != n * 8: return (false, GeoMesh())
  try:
    m.data = floatsFromBytes(dV.bytes, n)
  except ValueError:
    return (false, GeoMesh())
  (true, m)

# --------- Pretty ---------

proc `$`*(m: GeoMesh): string =
  "GeoMesh(bbox=(" & $m.bbox.minX & "," & $m.bbox.minY & "," &
  $m.bbox.maxX & "," & $m.bbox.maxY & "), shape=" & $m.rows & "×" &
  $m.cols & "×" & $m.channels &
  (if m.labels.len > 0: ", labels=[" & m.labels.join(",") & "]" else: "") & ")"
