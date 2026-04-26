## AI Agent Coding and Testing Guide (Glen)

This guide explains how to write code, run tests, and operate effectively as an agent on this repository.

### Principles

- **Safety first**: prefer correctness, durability, and concurrency safety over micro-optimizations.
- **Small, reviewable edits**: scope changes narrowly; keep unrelated code untouched.
- **Always leave the tree green**: run tests after changes and fix regressions before moving on.

## Workflow

### 1) Discovery and context

- Read `README.md`, `glen.nimble`, and relevant modules under `src/glen/`.
- Skim tests under `tests/` to understand guarantees and expected behavior.
- If exploring code, use focused reads; avoid wide, speculative edits.

### 2) Plan with a todo list

- For multi-step tasks, create a short todo list and keep it updated as you progress.
- Keep one item `in_progress`; mark items `completed` as soon as they’re done.

### 3) Making code changes

- Add imports/deps needed for the code to compile; do not reformat unrelated code.
- Preserve existing indentation and whitespace style.
- Follow the project code style (see below) and avoid dead code.
- After each meaningful change, run the test suite.

### 4) Testing

- Run all tests:
  - `nimble test`
- Tests are split by topic. Core:
  - `tests/test_db.nim` — basic ops and transaction paths
  - `tests/test_wal_snapshot.nim` — WAL replay and snapshot round-trip
  - `tests/test_subscriptions.nim` (+ field, stream variants) — subscribe/unsubscribe and notifications
  - `tests/test_cache.nim` — LRU eviction behavior
  - `tests/test_codec.nim` (+ stream variant) — codec and snapshot fuzz
  - `tests/test_soak.nim` / `test_soak_index.nim` — long-running mixed workloads
  - `tests/test_multimaster.nim` — replication export/apply, LWW
  - `tests/test_validators.nim` — Zod-style schema DSL
  - `tests/test_bench.nim` / `test_bench_concurrent.nim` — micro-benchmarks
- Spatial / temporal / numeric extensions:
  - `tests/test_geo.nim` — R-tree primitives, geo index integration
  - `tests/test_polygons.nim` — polygon helpers, polygon index, geographic projection
  - `tests/test_index_persist.nim` — manifest-driven auto-rebuild + `.gri`/`.gpi` round-trip
  - `tests/test_timeseries.nim` — Gorilla TSDB encoding round-trips, retention, torn-tail recovery
  - `tests/test_linalg.nim` — Vector / Matrix arithmetic and Value (de)serialization
  - `tests/test_geomesh.nim` — bbox-anchored raster cell math + packed storage
  - `tests/test_tilestack.nim` — tile time-stack encode/decode, point history, persistence
- Add new tests in a new file under `tests/` with clear suite names; update `glen.nimble` if adding new files.
- Prefer deterministic tests; for fuzz tests, cap iterations and sizes.

## Code style (Glen)

- **Naming**: use descriptive identifiers. Avoid 1–2 char names. Functions should be verbs.
- **Types**: prefer explicit types for public APIs; avoid unsafe casts.
- **Control flow**: handle errors/edge cases early; avoid deep nesting; avoid empty catches.
- **Comments**: keep them short and focused on “why.” Do not narrate actions inside code bodies.
- **Formatting**: match existing style. Prefer multi-line clarity over clever one-liners.

## Glen architecture notes

### Value and codec

- `Value` supports `null/bool/int/float/string/bytes/array/object/id` with constructors like `VString`, `VObject`, etc.
- The binary codec enforces basic size limits; validate external inputs and keep limits in mind when adding features.

### WAL and recovery

- WAL (`src/glen/wal.nim`) uses per-record checksums; replay should stop at the first invalid record in a segment tail.
- Append operations must be flushed; consider platform-specific durability when changing WAL.

### Snapshots

- Snapshots are stored per-collection via `src/glen/storage.nim`.
- When changing snapshot format, add a header (magic + version) and maintain backward compatibility.

### Cache

- LRU cache (`src/glen/cache.nim`) is internally locked and is safe for concurrent access.
- Update the cache consistently on `put`, transactional `commit`, and invalidate on `delete`.

### Subscriptions

- Subscriber type is a closure `proc (id: Id; newValue: Value) {.closure.}` (converter exists for plain procs).
- `subscribe` returns a handle; use `unsubscribe` to detach.
- Notify on put and delete (delete emits `null`).

### Transactions

- Use explicit staged writes: `TxnWriteKind = twPut | twDelete`.
- `commit` validates read versions (OCC), applies writes atomically under the DB lock, updates versions and cache, and notifies subscribers.

## Patterns to follow

- **Concurrency**: hold the DB lock when mutating shared state; the cache has its own lock.
- **Durability**: append to WAL before mutating in-memory state; flush appropriately.
- **Versioning**: bump document versions for each write/delete and propagate into cache and notifications.

## Adding tests

- Create a new `tests/test_<topic>.nim` with one or more `suite` blocks.
- Keep tests independent (use `getTempDir()` for DB/WAL snapshot directories).
- For subscriptions, use closure callbacks; example:

```nim
let h = db.subscribe("users", "u1", proc(id: Id; v: Value) =
  events.add($id & "|" & $v)
)
db.unsubscribe(h)
```

## Useful commands

- Build and run a specific test file:
  - `nim c -r --path:src tests/test_db.nim`
- Run full suite:
  - `nimble test`

## Module-specific guidance

- `db.nim`: keep cache coherence and subscriber notifications aligned with writes; never bypass WAL. When adding new index types, register hooks at every mutation site (replay, `put`, `delete`, `commit`, `putMany`, `deleteMany`, `applyChanges`).
- `wal.nim`: maintain checksum and length encoding; on replay, stop at corruption boundaries.
- `storage.nim`: enforce size limits; consider atomic writes for future improvements.
- `subscription.nim`: keep callbacks closure-based; ensure `unsubscribe` is safe and idempotent.
- `txn.nim`: keep staged writes explicit; avoid overloading special values.
- `index.nim`: equality / range. CritBitTree-backed; persisted via the `indexes.manifest`.
- `geo.nim`: R-tree (geo + polygon), KNN, haversine helpers, `.gri` / `.gpi` binary dumps. STR for bulk-load, Guttman linear split for incremental insert.
- `bitpack.nim`: shared bit-packing primitives (BitWriter / BitReader, zigzag, DoD, Gorilla XOR, FNV-1a32). DoD ranges are symmetric (zigzag-fits-in-N-bits): 7 bits → `[-64, 63]`, 9 → `[-256, 255]`, 12 → `[-2048, 2047]`. The XOR reuse path must use `high(uint64)` for the full-width mask, not `(1 shl 64) - 1`.
- `timeseries.nim`: standalone Gorilla TSDB. One file per series. Active block in memory; flush at `blockSize` or on `flush()/close()`. CRC per chunk; torn tail tolerated.
- `linalg.nim`: pure Nim arithmetic. Stored inside Values as nested `VFloat` arrays (no new ValueKind). Matrix is row-major flat for cache-friendly matmul (ikj loop, skip-on-zero).
- `geomesh.nim`: raster-on-bbox. `data` field in serialised form is `vkBytes` (raw little-endian float64), not nested arrays — this is the size win.
- `tilestack.nim`: tiled time-stacks for raster-over-time. Extends the timeseries idea to N parallel cell-channel streams sharing one timestamp stream per chunk. Each tile is independent on disk; mutations flow to one tile slice each.

## PR/commit guidance (if relevant)

- Small, focused commits with clear messages.
- Include test updates alongside code changes.
- Avoid flaky tests; keep fuzz counts conservative.


