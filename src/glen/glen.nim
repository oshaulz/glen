# Glen root module re-exporting public API

import glen/types
import glen/codec
import glen/wal
import glen/storage
import glen/cache
import glen/subscription
import glen/txn
import glen/db
import glen/util
import glen/index
import glen/geo
import glen/linalg
import glen/geomesh
import glen/tilestack
import glen/codec_stream
import glen/validators
import glen/simple8b
import glen/vectorindex
import glen/query
import glen/dsl

# Public exports

export types, codec, codec_stream, wal, storage, cache, subscription, txn, db, util, index, geo, linalg, geomesh, tilestack, validators, simple8b, vectorindex, query, dsl

# Placeholder openDatabase
proc openDatabase*(path: string): GlenDB =
  ## Open or create a Glen database at the given path.
  result = newGlenDB(path)
