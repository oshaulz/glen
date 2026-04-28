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
import glen/db as glendb
import glen/geo
import glen/timeseries
import glen/tilestack
import glen/geomesh

export timeseries, tilestack
# Re-export GeoMesh + BBox so users importing `glen/dsl` don't need an
# extra import to construct `bbox(...)` or pass meshes to `appendFrame`.
export geo.bbox, geomesh.newGeoMesh, geomesh.fromMatrix

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
