## glenTxn — transaction block with automatic conflict retries.
##
##   let res = glenTxn(db, retries = 3):
##     let acc = txn.get("accounts", srcId)
##     acc["balance"] = %*(acc["balance"].i - amount)
##     txn.put("accounts", srcId, acc)
##
##     let dst = txn.get("accounts", dstId)
##     dst["balance"] = %*(dst["balance"].i + amount)
##     txn.put("accounts", dstId, dst)
##   if not res.ok:
##     echo "transfer failed: ", res.message
##
## Inside the body, the helper `txn` exposes:
##   * `txn.get(coll, id) -> Value`     — records the read version for OCC
##   * `txn.put(coll, id, value)`       — stages a put
##   * `txn.delete(coll, id)`           — stages a delete
##
## On `csConflict`, the body is re-run up to `retries` times (default 3).
## On `csInvalid`, the loop exits with that result. Exceptions raised
## inside the body abort the transaction and surface as `csInvalid` with
## the exception message.

import glen/types
import glen/db as glendb
import glen/txn as glentxn

type
  TxnHelper* = object
    db*: glendb.GlenDB
    t*: glentxn.Txn

proc get*(h: TxnHelper; collection, docId: string): Value {.inline.} =
  ## Read inside a transaction. Records the version for conflict detection.
  h.db.get(collection, docId, h.t)

proc getMany*(h: TxnHelper; collection: string; ids: openArray[string]): seq[(string, Value)] {.inline.} =
  h.db.getMany(collection, ids, h.t)

proc getAll*(h: TxnHelper; collection: string): seq[(string, Value)] {.inline.} =
  h.db.getAll(collection, h.t)

proc put*(h: TxnHelper; collection, docId: string; value: Value) {.inline.} =
  ## Stage a put. The committed version is computed at commit time; the
  ## version field on the staged Id is unused.
  h.t.stagePut(Id(collection: collection, docId: docId, version: 0'u64), value)

proc delete*(h: TxnHelper; collection, docId: string) {.inline.} =
  h.t.stageDelete(collection, docId)

template glenTxn*(db: glendb.GlenDB; retries: int; body: untyped): glentxn.CommitResult =
  ## Run `body` inside a Glen transaction. Retries on `csConflict` up to
  ## `retries` times (so the body executes 1 + retries times in the worst
  ## case). Returns the final `CommitResult`.
  ##
  ## A bare `glenTxn(db): body` form (no retries) is also available below.
  block:
    var glenTxnRes: glentxn.CommitResult =
      glentxn.CommitResult(status: glentxn.csConflict, message: "no attempt")
    var glenTxnDone = false
    var glenTxnAttempt = 0
    while not glenTxnDone and glenTxnAttempt <= retries:
      let glenTxnT = db.beginTxn()
      let txn {.inject.} = TxnHelper(db: db, t: glenTxnT)
      var glenTxnAborted = false
      try:
        body
      except CatchableError as e:
        glenTxnRes = glentxn.CommitResult(
          status: glentxn.csInvalid, message: e.msg)
        glenTxnAborted = true
        glenTxnDone = true
      if not glenTxnAborted:
        glenTxnRes = db.commit(glenTxnT)
        if glenTxnRes.status != glentxn.csConflict:
          glenTxnDone = true
      inc glenTxnAttempt
    glenTxnRes

template glenTxn*(db: glendb.GlenDB; body: untyped): glentxn.CommitResult =
  ## No-retry variant. Equivalent to `glenTxn(db, retries = 0): body`.
  glenTxn(db, 0, body)
