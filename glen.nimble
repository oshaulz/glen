# Glen document database nimble spec
version       = "0.3.0"
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
  "tests/test_validators.nim"
]

task test, "Run test suite":
  for f in testFiles:
    runTest(f)

task test_release, "Run test suite (release, ORC, O3)":
  for f in testFiles:
    exec "nim c -r -d:release --mm:orc --passC:-O3 --threads:on --path:src " & f

task bench_release, "Run only benchmark (release, ORC, O3)":
  exec "nim c -r -d:release --mm:orc --passC:-O3 --threads:on --path:src tests/test_bench.nim"

task docs, "Generate API docs":
  exec "nim doc --project --outdir:docs --path:src src/glen/glen.nim"
