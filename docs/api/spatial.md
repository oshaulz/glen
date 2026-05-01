# Spatial API — `glen/geo`, `glen/geomesh`

Three spatial primitives:

- **Geo index** — R-tree on `(lon, lat)` points stored on documents
- **Polygon index** — R-tree on polygon MBRs + exact ray-cast point-in-polygon
- **GeoMesh** — value type for a 2-D raster pinned to a geographic bbox

The first two are integrated into the document model (mutations keep them in
sync automatically); GeoMesh is a value type that rides inside ordinary
documents.

## Geo index (points)

R-tree-backed; STR bulk-load on `createGeoIndex`, Guttman linear split for
incremental insert/delete; persisted to `<collection>.<name>.gri`.

### Setup

```nim
import glen/db, glen/types, glen/geo

# Documents need two numeric fields treated as (lon, lat).
proc place(name: string; lon, lat: float64): Value =
  result = VObject()
  result["name"] = VString(name)
  result["lon"]  = VFloat(lon)
  result["lat"]  = VFloat(lat)

db.put("places", "sf",  place("San Francisco", -122.42, 37.77))
db.put("places", "oak", place("Oakland",       -122.27, 37.80))

db.createGeoIndex("places", "byLoc",
                  lonField = "lon", latField = "lat")
```

Docs missing either field, or with non-numeric values there, are silently
skipped. The index is persisted in `indexes.manifest`; auto-rebuilt on reopen.

### Queries

Bounding-box:

```nim
for (id, doc) in db.findInBBox("places", "byLoc",
                               minLon = -123.0, minLat = 37.0,
                               maxLon = -122.0, maxLat = 38.0,
                               limit = 0):
  echo id
```

K-nearest neighbours:

```nim
# Planar Euclidean (over raw degrees) — fast, fine for short ranges
for (id, dist, doc) in db.findNearest("places", "byLoc",
                                      lon = -122.42, lat = 37.77, k = 3,
                                      metric = gmPlanar):
  echo id, " @ ", dist

# Geographic (haversine) — distance in metres
for (id, metres, doc) in db.findNearest("places", "byLoc",
                                        -122.42, 37.77, k = 3,
                                        metric = gmGeographic):
  echo id, " ", metres / 1000.0, "km"
```

Radius (haversine post-filter):

```nim
for (id, metres, doc) in db.findWithinRadius("places", "byLoc",
                                             -122.42, 37.77,
                                             radiusMeters = 50_000.0):
  echo id, " ", metres / 1000.0, "km"
```

Streaming variants — bounded memory:

```nim
for (id, doc) in db.findInBBoxStream(...): ...
for (id, dist, doc) in db.findNearestStream(...): ...
for (id, metres, doc) in db.findWithinRadiusStream(...): ...
```

### Drop

```nim
db.dropGeoIndex("places", "byLoc")
```

### Direct R-tree

If you want the spatial index without the GlenDB overhead — moving objects,
custom data, non-doc payloads — `glen/geo` exposes the R-tree as a normal
data structure:

```nim
import glen/geo

let t = newRTree(maxEntries = 16, minEntries = 6)

# Bulk-load (STR — fastest, tightest MBRs for static data)
var pts: seq[(string, BBox)] = @[]
for (id, x, y) in myData: pts.add((id, point(x, y)))
t.bulkLoad(pts)

# Or incremental
t.insert("p42", point(10.0, 20.0))
discard t.remove("p42")

# Queries
for id in t.searchBBox(bbox(0.0, 0.0, 100.0, 100.0)): echo id
for (id, dist) in t.nearest(50.0, 50.0, k = 10): echo id, " ", dist
for (id, dist) in t.nearestGeo(lon, lat, k = 10): ...     # haversine
for (id, dist) in t.nearestWithin(50.0, 50.0, maxDist = 25.0): ...
for (id, metres) in t.nearestGeoWithin(lon, lat, maxMeters = 5000.0): ...
```

Helpers: `BBox`, `point(x, y)`, `bbox(minX, minY, maxX, maxY)`,
`intersects`, `union`, `area`, `haversineMeters`, `radiusBBox`,
`haversineMinMeters` (bbox lower-bound used internally by `nearestGeo`).

## Polygon index

R-tree on polygon MBRs + per-doc polygons cached for exact ray-cast tests.
Persisted to `<collection>.<name>.gpi`.

### Polygon shape on a document

A polygon is `VArray([VArray([VFloat(x), VFloat(y)]), ...])`:

```nim
proc poly(verts: openArray[(float64, float64)]): Value =
  var arr: seq[Value] = @[]
  for (x, y) in verts: arr.add(VArray(@[VFloat(x), VFloat(y)]))
  VArray(arr)

var doc = VObject()
doc["name"]  = VString("Bay Area-ish")
doc["shape"] = poly([(-123.0, 37.0), (-122.0, 37.0),
                     (-122.0, 38.0), (-123.0, 38.0)])
db.put("zones", "z1", doc)

db.createPolygonIndex("zones", "byShape", "shape")
```

### Queries

```nim
# Which zones contain (x, y)?
for (id, doc) in db.findPolygonsContaining("zones", "byShape",
                                           x = -122.42, y = 37.77):
  echo id

# Polygons whose MBR intersects a query box (cheap superset)
for (id, doc) in db.findPolygonsIntersecting("zones", "byShape",
                                             minX = -123.0, minY = 37.0,
                                             maxX = -122.0, maxY = 38.0):
  echo id

# Which indexed points fall inside a polygon?
let p = Polygon(vertices: @[
  (-123.0, 37.0), (-122.0, 37.0), (-122.0, 38.0), (-123.0, 38.0)])
for (id, doc) in db.findPointsInPolygon("places", "byLoc", p):
  echo id
```

Streaming variants are available for all three:
`findPolygonsContainingStream`, `findPolygonsIntersectingStream`,
`findPointsInPolygonStream`.

### Direct polygon helpers

```nim
import glen/geo

# Build a polygon from vertex tuples
let p = Polygon(vertices: @[(0.0, 0.0), (10.0, 0.0), (10.0, 10.0), (0.0, 10.0)])
let bb = polygonBBox(p)
echo pointInPolygon(p, 5.0, 5.0)        # true
echo readPolygonFromValue(someValue)    # (ok, polygon)
```

## GeoMesh

A 2-D raster pinned to a geographic bbox, with optional per-cell vector
channels (e.g. multi-class probabilities). Stored inside any document.

```nim
import glen/geomesh

var mesh = newGeoMesh(
  bbox(-122.5, 37.5, -122.0, 38.0),
  rows = 64, cols = 64, channels = 3,
  labels = @["rain", "snow", "clear"])

# Fill cells
for r in 0 ..< mesh.rows:
  for c in 0 ..< mesh.cols:
    mesh.setCell(r, c, [0.7, 0.1, 0.2])

# Sample at a point
let probs = mesh.vectorAt(-122.42, 37.77)         # @[0.7, 0.1, 0.2]
let rainP = mesh.valueAt(-122.42, 37.77, channel = mesh.channelIndex("rain"))

# Round-trip into a document
var doc = VObject()
doc["model"]    = VString("rain-v3")
doc["forecast"] = mesh.toValue()                   # data packed as VBytes
db.put("predictions", "today-utc-12", doc)

let stored = db.get("predictions", "today-utc-12")
let (ok, m) = readGeoMesh(stored["forecast"])
```

### Conventions

- **`row 0`** = top of bbox (max lat); **`col 0`** = left (min lon).
- Data layout: `data[(row * cols + col) * channels + channel]`.
- Disk encoding: `data` field uses `VBytes` (packed little-endian float64);
  the rest is plain `VObject` fields. A 1000×1000×5 mesh is ~40 MB on disk
  vs hundreds of MB if stored as nested `VArray`s.

### API

```nim
newGeoMesh(bbox, rows, cols, channels = 1, labels = @[])
fromMatrix(bbox, m: Matrix)             # wrap a single-channel Matrix

cellAt(m, lon, lat) → (inBounds: bool, row, col)
cellCenter(m, row, col) → (lon, lat)
m[row, col, channel]: float64           # direct access
m[row, col, channel] = v
setCell(m, row, col, [v0, v1, ...])     # all channels at once
valueAt(m, lon, lat, channel = 0): float64
vectorAt(m, lon, lat): Vector
cellVector(m, row, col): Vector
channelIndex(m, label: string): int     # -1 if not labelled

# Interpolation
sampleAt(m, lon, lat, channel = 0, kind = imBilinear): float64
# `imNearest` (= valueAt) or `imBilinear` (4-cell weighted average).
# Use bilinear for continuous fields — temperature, pressure, wind,
# precipitation. Returns NaN out of bounds.

# Polygon aggregation
aggregateInPolygon(m, polygon, agg, channel = 0, metric = gmPlanar): float64
# `agg ∈ {akMean, akMin, akMax, akSum, akCount}`. Cells whose centre
# is inside `polygon` contribute. With `metric = gmGeographic`, edges
# are great-circle arcs.
aggregateInPolygonStats(m, polygon, channel = 0, metric = gmPlanar): AggregateResult
# Bundle of (count, sum, minValue, maxValue, mean) — one pass over
# the cells, all stats in one shot.

toValue(m): Value
readGeoMesh(v: Value): (bool, GeoMesh)
```

#### TileStack point-and-region queries

The same `sampleAt` / `aggregateInPolygon` shapes work on a
`TileStack`, scoped to a specific frame timestamp:

```nim
sampleAt(stack, tsMillis, lon, lat, channel = 0, kind = imBilinear): float64
aggregateInPolygon(stack, tsMillis, polygon, agg, channel = 0, metric = gmPlanar): float64
aggregateInPolygonStats(stack, tsMillis, polygon, channel = 0, metric = gmPlanar): AggregateResult

# Time series of polygon aggregates — one (ts, scalar) per frame in [t0, t1].
aggregateInPolygonRange(stack, fromMs, toMs, polygon, agg, channel = 0, metric = gmPlanar): seq[(int64, float64)]
```

These decode the relevant frame and delegate to the GeoMesh procs, so
they have the same bilinear / great-circle behaviour. For a series of
*point* values over time, `readPointHistory` is more efficient — it
only decodes the single tile owning the query point.

## Index persistence

All three index types persist their definition in `indexes.manifest`. Spatial
indexes (geo + polygon) also dump their R-tree binary data to
`<collection>.<name>.gri` / `.gpi` on `compact()`. On reopen:

1. Manifest is read.
2. For each entry, try to load the matching `.gri` / `.gpi` dump.
3. WAL replay applies any post-compact mutations on top of the loaded tree.
4. Indexes whose dump didn't load (or never existed) bulk-rebuild from docs.

CRC mismatch on a dump silently falls back to bulk-rebuild — never crashes.

See [storage.md#snapshot-formats](../storage.md#snapshot-formats) for the
binary dump format.

## Earth-curvature handling

Glen's geo layer is curvature-aware in the cases that matter:

| Operation                                  | Curvature handling |
|--------------------------------------------|---------------------|
| Distance (`haversineMeters`, `findWithinRadius`) | Spherical (Earth radius 6,371,008.8 m) |
| `findNearest` with `metric = gmGeographic` | Haversine, with R-tree pruning via `haversineMinMeters` |
| `findInBBox` / `findWithinRadius` query bbox crossing the antimeridian | **Auto-split** into two halves and unioned (pass `metric = gmGeographic` for `findInBBox`) |
| `findInBBox` / `findWithinRadius` query bbox past ±90° latitude | **Auto-clamped** to ±90° |
| `findPolygonsContaining` with `metric = gmGeographic` | **Spherical PIP** via great-circle winding number (handles antimeridian, poles, continent-scale polygons) |
| Polygons that themselves cross the antimeridian | Indexed with widened MBR `[-180, +180]`; the spherical PIP post-filter rejects false positives |

`metric = gmPlanar` (the default) preserves the legacy behaviour: raw
Euclidean distance, no antimeridian splitting, planar ray-cast PIP.
This is the right choice for non-geographic 2D data (game maps, abstract
coordinate systems, anything that doesn't wrap).

**Spherical polygons require CCW vertex order** around the intended
interior (right-hand rule with the normal pointing out of the sphere).
`pointInPolygonSpherical` reports CW polygons as their *complement*
(the rest of the sphere). When in doubt, run a centroid sanity check.

## What's not there

- **Sphere, not WGS84 ellipsoid**: distances assume a perfect sphere
  with radius 6,371,008.8 m. Error is ~0.3% at extreme latitudes —
  fine for "find pizza nearby," wrong for sub-meter geodesy.
- **No native geographic projections** beyond the haversine path metric.
  Coords are interpreted as `(lon, lat)` degrees in WGS84-ish.
- **No 3D / N-D**. Strictly 2D.
- **No bicubic interpolation** in `GeoMesh.sampleAt` — only nearest and bilinear.
- **No polygon-polygon intersection**. `findPolygonsIntersecting` is bbox-level.
- **No vector index** (HNSW / IVF). Use linear scan + cosine for now (see
  [api/numeric.md](numeric.md)).

## See also

- [Architecture](../architecture.md)
- [api/timeseries.md](timeseries.md) — for tile time-stacks (raster-over-time)
- [api/numeric.md](numeric.md) — Vector + Matrix
