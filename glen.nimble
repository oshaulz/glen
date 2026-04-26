# Glen document database nimble spec
version       = "0.4.4"
author        = "Glen"
description   = "Glen: A wickedly fast embedded document database with subscriptions and transactions"
license       = "MIT"
srcDir        = "src"

# Minimum Nim version
requires "nim >= 1.6.0"

# No external deps for core; future: zstd, etc.

bin           = @["glen"]

proc runTest(file: string) =
  exec "nim c -r --path:src " & file

const testFiles = @[
  "tests/test_db.nim",
  "tests/test_wal_snapshot.nim",
  "tests/test_subscriptions.nim",
  "tests/test_cache.nim",
  "tests/test_codec.nim",
  "tests/test_codec_stream.nim",
  "tests/test_subscriptions_stream.nim",
  "tests/test_subscriptions_field.nim",
  "tests/test_soak.nim",
  "tests/test_soak_index.nim",
  "tests/test_multimaster.nim",
  "tests/test_bench.nim",
  "tests/test_validators.nim",
  "tests/test_geo.nim",
  "tests/test_timeseries.nim",
  "tests/test_polygons.nim",
  "tests/test_index_persist.nim",
  "tests/test_linalg.nim",
  "tests/test_geomesh.nim",
  "tests/test_tilestack.nim",
  "tests/test_spillable.nim",
  "tests/test_streaming.nim"
]

task test, "Run test suite":
  for f in testFiles:
    runTest(f)

task test_release, "Run test suite (release, ORC, O3)":
  for f in testFiles:
    exec "nim c -r -d:release --mm:orc --passC:-O3 --threads:on --path:src " & f

task bench_release, "Run only benchmark (release, ORC, O3)":
  exec "nim c -r -d:release --mm:orc --passC:-O3 --threads:on --path:src tests/test_bench.nim"

task bench_geo, "Geospatial / R-tree benchmarks (release, ORC, O3)":
  exec "nim c -r -d:release --mm:orc --passC:-O3 --threads:on --path:src tests/test_bench_geo.nim"

task bench_timeseries, "Time-series + tilestack benchmarks (release, ORC, O3)":
  exec "nim c -r -d:release --mm:orc --passC:-O3 --threads:on --path:src tests/test_bench_timeseries.nim"

task bench_concurrent, "Run multi-threaded contention benchmark (release, atomicArc, O3)":
  # atomicArc gives thread-safe refcounting (Glen's Value graph is acyclic so
  # we don't need ORC's cycle collector, which is not thread-safe).
  # -d:useMalloc avoids Nim's per-thread heap for cross-thread allocations.
  exec "nim c -r -d:release -d:useMalloc --mm:atomicArc --passC:-O3 --threads:on --path:src tests/test_bench_concurrent.nim"

task docs, "Generate API docs":
  exec "nim doc --project --outdir:docs --path:src src/glen/glen.nim"
