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
