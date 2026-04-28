## Glen DSL — single-import surface for the high-level macros and
## ergonomic wrappers. See `docs/dsl.md` for the full guide.
##
##   import glen/dsl     # everything: %*, glenQuery, glenTxn, glenSchema,
##                       # glenWatch, glenSync, Collection
##
## The DSL is purely additive — every macro emits calls into the existing
## proc-level API, so you can mix and match (e.g. write a query with
## `glenQuery:` but stay on `db.put` for writes).

import glen/dsl/literal
import glen/dsl/collection
import glen/dsl/live
import glen/dsl/query
import glen/dsl/txn
import glen/dsl/schema
import glen/dsl/watch
import glen/dsl/sync

export literal, collection, live, query, txn, schema, watch, sync
