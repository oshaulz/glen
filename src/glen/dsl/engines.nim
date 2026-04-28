## engines — `db`-rooted convenience proxies for the time-series and
## tile-stack engines, so they feel like part of the same library as
## documents instead of three separate APIs.
##
##   # Time-series
##   let s = db.series("temp.sensor1")
##   s.append(nowMillis(), 22.5)
##   for (ts, v) in s.range(t0, t1): plot(ts, v)
##   s.close()
##
##   # Or, auto-close:
##   withSeries(db, "temp.sensor1") do (s):
##     for (ts, v) in s.range(t0, t1): plot(ts, v)
##
##   # Tile-stacks (rasters evolving over time)
##   let bbox = bbox(-180.0, -90.0, 180.0, 90.0)
##   let stack = db.tiles("radar", bbox, rows = 360, cols = 720, channels = 1)
##   stack.appendFrame(nowMillis(), mesh)
##   for (ts, m) in stack.readFrameRange(t0, t1): render(ts, m)
##   stack.close()
##
##   withTiles(db, "radar") do (s):
##     let history = s.readPointHistory(-73.98, 40.75, t0, t1)
##
## Path conventions:
##   * `db.series(name)` opens / creates `<db.dir>/series/<name>.gts`
##   * `db.tiles(name, ...)` and `db.openTiles(name)` use `<db.dir>/tiles/<name>/`
##
## Names may contain dots (`temp.sensor1`) — they're treated as filename
## components, not directory separators. Lifetime is the caller's
## responsibility: `withSeries` / `withTiles` exist for the common
## scoped-use case; for long-lived handles, hold the returned ref and
## call `.close()` when done.

import std/os
import glen/types
import glen/db as glendb
import glen/geo
import glen/timeseries
import glen/tilestack
import glen/geomesh
import glen/vectorindex

export timeseries, tilestack
# Re-export GeoMesh + BBox + Polygon + GeoMetric so users importing
# `glen/dsl` don't need extra imports for the proxy procs below.
export geo.bbox, geo.Polygon, geo.GeoMetric
export geomesh.newGeoMesh, geomesh.fromMatrix
export vectorindex.VectorMetric

# ---- Path conventions ------------------------------------------------------

const
  SeriesSubdir* = "series"
  TilesSubdir*  = "tiles"
  SeriesExt*    = ".gts"

proc seriesPath*(db: glendb.GlenDB; name: string): string {.inline.} =
  ## Filesystem path the named series lives at.
  db.dir / SeriesSubdir / (name & SeriesExt)

proc tilesPath*(db: glendb.GlenDB; name: string): string {.inline.} =
  ## Filesystem path the named tile-stack lives at.
  db.dir / TilesSubdir / name

# ---- Series ----------------------------------------------------------------

proc series*(db: glendb.GlenDB; name: string;
             blockSize = DefaultBlockSize;
             fsyncOnFlush = false): Series =
  ## Open or create the named series under `<db.dir>/series/<name>.gts`.
  ## Caller owns the returned handle and must call `.close()` (or use
  ## `withSeries`). Opening the same name twice concurrently is unsafe —
  ## reuse the same handle across the program.
  let path = seriesPath(db, name)
  let dir = parentDir(path)
  if dir.len > 0 and not dirExists(dir): createDir(dir)
  openSeries(path, blockSize = blockSize, fsyncOnFlush = fsyncOnFlush)

proc seriesExists*(db: glendb.GlenDB; name: string): bool {.inline.} =
  fileExists(seriesPath(db, name))

template withSeries*(db: glendb.GlenDB; name: string;
                     body: untyped): untyped =
  ## Open the named series, run `body` with the handle bound as `s`,
  ## flush + close on the way out (even on exception).
  block:
    let s {.inject.} = series(db, name)
    try:
      body
    finally:
      s.close()

# ---- TileStack -------------------------------------------------------------

proc tiles*(db: glendb.GlenDB; name: string;
            bbox: BBox; rows, cols, channels: int;
            tileSize = DefaultTileSize;
            chunkSize = DefaultChunkSize;
            labels: seq[string] = @[]): TileStack =
  ## Create or attach to the named tile-stack under `<db.dir>/tiles/<name>/`.
  ## If a manifest already exists it must match the provided dimensions;
  ## otherwise a new manifest is written. To attach to an existing stack
  ## without specifying dimensions, use `openTiles`.
  let dir = tilesPath(db, name)
  let parent = parentDir(dir)
  if parent.len > 0 and not dirExists(parent): createDir(parent)
  newTileStack(dir, bbox, rows, cols, channels,
               tileSize = tileSize, chunkSize = chunkSize, labels = labels)

proc openTiles*(db: glendb.GlenDB; name: string): TileStack =
  ## Attach to an existing tile-stack. Raises `IOError` if no manifest is
  ## present at `<db.dir>/tiles/<name>/`.
  openTileStack(tilesPath(db, name))

proc tilesExists*(db: glendb.GlenDB; name: string): bool {.inline.} =
  fileExists(tilesPath(db, name) / "manifest.tsm")

template withTiles*(db: glendb.GlenDB; name: string;
                    body: untyped): untyped =
  ## Open an existing tile-stack, bind it as `s`, close on exit.
  block:
    let s {.inject.} = openTiles(db, name)
    try:
      body
    finally:
      s.close()

# ---- VectorIndex proxy ----------------------------------------------------
#
# Mirrors the `Collection` proxy idea: `db.vectors("docs", "byEmbed")`
# returns a thin `(db, collection, indexName)` handle that routes the
# scattered `findNearestVector` / `findNearestVectorWithin` /
# `createVectorIndex` calls through one tidy surface. Plus a doc-fetching
# variant `.searchDocs` for the common "give me the actual records, not
# just (id, distance) pairs" case.

type
  VectorHandle* = object
    db*: glendb.GlenDB
    collection*: string
    indexName*: string

proc vectors*(db: glendb.GlenDB; collection, indexName: string): VectorHandle {.inline.} =
  ## Get a handle for an existing vector index. The index must have been
  ## created via `createVectorIndex` (or via `schema:` with a vector field).
  VectorHandle(db: db, collection: collection, indexName: indexName)

proc create*(h: VectorHandle; embeddingField: string; dim: int;
             metric: VectorMetric = vmCosine;
             M = 16; efConstruction = 200; efSearch = 64) {.inline.} =
  ## Create the underlying HNSW index. Convenient when you want to
  ## allocate the index lazily off the proxy: `db.vectors("docs", "byEmbed").create("embedding", 384)`.
  h.db.createVectorIndex(h.collection, h.indexName, embeddingField, dim,
                         metric, M, efConstruction, efSearch)

proc drop*(h: VectorHandle) {.inline.} =
  h.db.dropVectorIndex(h.collection, h.indexName)

proc search*(h: VectorHandle; query: openArray[float32];
             k: int): seq[(string, float32)] {.inline.} =
  ## Top-k nearest neighbours by approximate distance.
  h.db.findNearestVector(h.collection, h.indexName, query, k)

proc searchWithin*(h: VectorHandle; query: openArray[float32];
                   maxDistance: float32;
                   maxResults = 0): seq[(string, float32)] {.inline.} =
  ## Every match within `maxDistance`, sorted by distance ascending.
  ## With `metric = vmCosine`, `maxDistance = 0.2` ≈ "cosine similarity ≥ 0.8".
  h.db.findNearestVectorWithin(h.collection, h.indexName, query,
                               maxDistance, maxResults)

proc searchDocs*(h: VectorHandle; query: openArray[float32];
                 k: int): seq[(string, float32, Value)] =
  ## Same as `search` but pre-fetches the matching documents. Useful
  ## when you'll always need the full record (chat messages, embeddings
  ## paired with their source text, etc.) — saves a `db.get` per result.
  result = @[]
  for (id, dist) in h.search(query, k):
    let doc = h.db.get(h.collection, id)
    if not doc.isNil: result.add((id, dist, doc))

proc searchDocsWithin*(h: VectorHandle; query: openArray[float32];
                       maxDistance: float32;
                       maxResults = 0): seq[(string, float32, Value)] =
  result = @[]
  for (id, dist) in h.searchWithin(query, maxDistance, maxResults):
    let doc = h.db.get(h.collection, id)
    if not doc.isNil: result.add((id, dist, doc))

proc upsert*(h: VectorHandle; docId: string; value: Value) {.inline.} =
  ## Add or replace a document. The index automatically picks up the
  ## embedding field on the next put — this proc is just a convenience
  ## wrapper around `db.put` that signals intent ("I'm specifically
  ## indexing this vector").
  h.db.put(h.collection, docId, value)

proc upsertVector*(h: VectorHandle; docId: string;
                   embeddingField: string;
                   embedding: openArray[float32];
                   extra: Value = nil) =
  ## Build a doc with the embedding field set and any caller-provided
  ## additional fields, then put. Convenience for the common pattern of
  ## "store an embedding plus a few metadata fields."
  var v =
    if extra.isNil: VObject()
    elif extra.kind == vkObject: extra
    else: VObject()
  var arr: seq[Value] = newSeqOfCap[Value](embedding.len)
  for f in embedding: arr.add(VFloat(float64(f)))
  v[embeddingField] = VArray(arr)
  h.db.put(h.collection, docId, v)

# ---- GeoIndex proxy -------------------------------------------------------
#
# Mirror of the vector proxy for spatial indexes. `db.geo("stores", "byLoc")`
# bundles all the scattered point-index calls (`findNearest`, `findInBBox`,
# `findWithinRadius`, `findPointsInPolygon`) under one handle.

type
  GeoHandle* = object
    db*: glendb.GlenDB
    collection*: string
    indexName*: string

proc geo*(db: glendb.GlenDB; collection, indexName: string): GeoHandle {.inline.} =
  ## Get a handle for an existing geo (R-tree point) index.
  GeoHandle(db: db, collection: collection, indexName: indexName)

proc create*(h: GeoHandle; lonField, latField: string) {.inline.} =
  h.db.createGeoIndex(h.collection, h.indexName, lonField, latField)

proc drop*(h: GeoHandle) {.inline.} =
  h.db.dropGeoIndex(h.collection, h.indexName)

proc near*(h: GeoHandle; lon, lat, radiusMeters: float64;
           limit = 0): seq[(string, float64, Value)] {.inline.} =
  ## Documents within `radiusMeters` of (lon, lat) using haversine.
  ## Antimeridian-aware. Sorted by distance ascending.
  h.db.findWithinRadius(h.collection, h.indexName, lon, lat,
                        radiusMeters, limit)

proc nearest*(h: GeoHandle; lon, lat: float64; k: int;
              metric = gmGeographic): seq[(string, float64, Value)] {.inline.} =
  ## K-nearest neighbours by haversine (default) or planar Euclidean.
  h.db.findNearest(h.collection, h.indexName, lon, lat, k, metric)

proc inBBox*(h: GeoHandle; minLon, minLat, maxLon, maxLat: float64;
             limit = 0;
             metric = gmPlanar): seq[(string, Value)] {.inline.} =
  ## Documents whose indexed point falls inside the bbox. With
  ## `metric = gmGeographic` an antimeridian-spanning bbox auto-splits.
  h.db.findInBBox(h.collection, h.indexName,
                  minLon, minLat, maxLon, maxLat, limit, metric)

proc inPolygon*(h: GeoHandle; polygon: Polygon;
                limit = 0;
                metric = gmPlanar): seq[(string, Value)] {.inline.} =
  ## Indexed points contained in the given polygon. Pass
  ## `metric = gmGeographic` for great-circle / antimeridian-aware
  ## containment.
  h.db.findPointsInPolygon(h.collection, h.indexName, polygon, limit, metric)
