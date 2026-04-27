import std/[os, parseutils, strutils]

type
  GlenConfig* = object
    maxStringOrBytes*: int
    maxArrayLen*: int
    maxObjectFields*: int
    walSyncMode*: string          # always | interval | none
    walFlushEveryBytes*: int      # bytes for interval mode
    cacheCapacityBytes*: int
    cacheShards*: int
    nodeId*: string
    compactWalBytes*: int
    compactIntervalMs*: int
    compactDirtyCount*: int

let defaultMaxStringOrBytes = 16 * 1024 * 1024
let defaultMaxArrayLen = 1_000_000
let defaultMaxObjectFields = 1_000_000

proc parseIntEnv(name: string; defaultValue: int): int =
  let v = getEnv(name)
  if v.len == 0: return defaultValue
  var n: int
  if parseInt(v, n) == v.len and n > 0: n else: defaultValue

proc parseIntEnvAllowZero(name: string; defaultValue: int): int =
  let v = getEnv(name)
  if v.len == 0: return defaultValue
  var n: int
  if parseInt(v, n) == v.len and n >= 0: n else: defaultValue

proc loadConfig*(): GlenConfig =
  GlenConfig(
    maxStringOrBytes: parseIntEnv("GLEN_MAX_STRING_OR_BYTES", defaultMaxStringOrBytes),
    maxArrayLen: parseIntEnv("GLEN_MAX_ARRAY_LEN", defaultMaxArrayLen),
    maxObjectFields: parseIntEnv("GLEN_MAX_OBJECT_FIELDS", defaultMaxObjectFields),
    walSyncMode: getEnv("GLEN_WAL_SYNC").toLowerAscii(),
    walFlushEveryBytes: parseIntEnv("GLEN_WAL_FLUSH_BYTES", 8 * 1024 * 1024),
    cacheCapacityBytes: parseIntEnv("GLEN_CACHE_CAP_BYTES", 64 * 1024 * 1024),
    cacheShards: parseIntEnv("GLEN_CACHE_SHARDS", 16),
    nodeId: getEnv("GLEN_NODE_ID"),
    compactWalBytes: parseIntEnvAllowZero("GLEN_COMPACT_WAL_BYTES", 0),
    compactIntervalMs: parseIntEnvAllowZero("GLEN_COMPACT_INTERVAL_MS", 0),
    compactDirtyCount: parseIntEnvAllowZero("GLEN_COMPACT_DIRTY_COUNT", 0)
  )


