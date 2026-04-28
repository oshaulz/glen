## liveQuery — a query whose result set updates as the underlying
## collection changes. Subscribes once at the collection level and fires
## diff events (added / updated / removed) to registered callbacks.
##
##   let live = liveQuery(db, "users"):
##     where:
##       role == "admin"
##       age >= 30
##
##   let h = live.onChange(proc (ev: LiveQueryEvent) =
##     case ev.kind
##     of lqAdded:   echo "+ ", ev.id, " ", ev.newValue
##     of lqRemoved: echo "- ", ev.id, " ", ev.oldValue
##     of lqUpdated: echo "* ", ev.id
##     of lqRefilled: discard)   # initial seed
##
##   # later
##   live.close()
##
## Semantics
## ---------
##
## On creation, the query is run once and the matching set is captured.
## Each captured doc fires an `lqRefilled` event so callbacks can build
## up initial state. Then a collection-wide subscription is registered.
##
## On every put or delete in the collection:
##   * If the new value matches the predicate set and the doc wasn't in
##     the live set → `lqAdded`, doc is added.
##   * If the new value doesn't match (or doc was deleted) and was in the
##     live set → `lqRemoved`, doc removed from set.
##   * If the doc was in the set, still matches, and the value changed →
##     `lqUpdated`.
##   * Otherwise → no event.
##
## Limitations
## -----------
##
## The MVP supports the `where:` predicate algebra only. `orderBy:` and
## `limit:` are not honoured for live queries (they'd require maintaining
## a sorted heap and recomputing window membership on every change). If
## you need ordered/paged live state, build it on top of the diff events.

import std/[locks, tables]
import glen/types
import glen/db as glendb
import glen/query as glenquery
import glen/subscription

type
  LiveQueryEventKind* = enum
    lqRefilled,   # initial seed: emitted once per matching doc on creation
    lqAdded,      # doc newly enters the result set
    lqUpdated,    # doc was in the set, still matches, value changed
    lqRemoved     # doc left the set (no longer matches, or was deleted)

  LiveQueryEvent* = object
    kind*: LiveQueryEventKind
    id*: string
    oldValue*: Value     # nil for lqAdded / lqRefilled
    newValue*: Value     # nil for lqRemoved

  LiveQueryCallback* = proc (ev: LiveQueryEvent) {.closure.}

  LiveQueryCallbackHandle* = object
    index*: int

  LiveQuery* = ref object
    db*: glendb.GlenDB
    collection*: string
    predicates*: seq[QueryPredicate]
    matched: Table[string, Value]      # docId -> last seen Value
    callbacks: seq[LiveQueryCallback]
    subHandle: CollectionSubscriptionHandle
    subRegistered: bool
    lock: Lock

proc fire(lq: LiveQuery; ev: LiveQueryEvent) =
  ## Fan-out an event under the live-query lock.
  var cbsCopy: seq[LiveQueryCallback] = @[]
  acquire(lq.lock)
  for cb in lq.callbacks:
    if cb != nil: cbsCopy.add(cb)
  release(lq.lock)
  for cb in cbsCopy:
    cb(ev)

proc handleChange(lq: LiveQuery; id: Id; newValue: Value) =
  ## Called from the collection subscription on every put/delete.
  let docId = id.docId
  let matches = (not newValue.isNil) and evalAll(lq.predicates, newValue)
  acquire(lq.lock)
  let wasIn = docId in lq.matched
  let oldVal = if wasIn: lq.matched[docId] else: nil
  if matches:
    lq.matched[docId] = newValue
  elif wasIn:
    lq.matched.del(docId)
  release(lq.lock)

  if wasIn and not matches:
    fire(lq, LiveQueryEvent(kind: lqRemoved, id: docId,
                            oldValue: oldVal, newValue: nil))
  elif not wasIn and matches:
    fire(lq, LiveQueryEvent(kind: lqAdded, id: docId,
                            oldValue: nil, newValue: newValue))
  elif wasIn and matches and oldVal != newValue:
    fire(lq, LiveQueryEvent(kind: lqUpdated, id: docId,
                            oldValue: oldVal, newValue: newValue))

proc newLiveQuery*(db: glendb.GlenDB; collection: string;
                   predicates: seq[QueryPredicate]): LiveQuery =
  ## Construct a live query. Runs an initial scan to seed the matched
  ## set, then registers a collection-wide subscription. Callers receive
  ## `lqRefilled` events for the initial set as soon as they call
  ## `onChange`.
  result = LiveQuery(
    db: db,
    collection: collection,
    predicates: predicates,
    matched: initTable[string, Value](),
    callbacks: @[]
  )
  initLock(result.lock)

  # Seed the matched set from the current contents of the collection.
  for (id, doc) in db.getAll(collection):
    if doc.isNil: continue
    if evalAll(predicates, doc):
      result.matched[id] = doc

  # Register a collection-wide subscription. Captures `result` by ref.
  let lq = result
  let cb: SubscriberCallback = proc (id: Id; newValue: Value) {.closure.} =
    lq.handleChange(id, newValue)
  result.subHandle = db.subscribeCollection(collection, cb)
  result.subRegistered = true

proc onChange*(lq: LiveQuery; cb: LiveQueryCallback): LiveQueryCallbackHandle =
  ## Register a diff-event callback. Immediately replays the current set
  ## as `lqRefilled` events so the callback can hydrate state without a
  ## separate query call.
  acquire(lq.lock)
  lq.callbacks.add(cb)
  let idx = lq.callbacks.len - 1
  # Snapshot current set under the lock so we don't race with concurrent
  # `handleChange` mutations during replay.
  var snapshot: seq[(string, Value)] = newSeqOfCap[(string, Value)](lq.matched.len)
  for k, v in lq.matched.pairs: snapshot.add((k, v))
  release(lq.lock)
  for (id, v) in snapshot:
    cb(LiveQueryEvent(kind: lqRefilled, id: id,
                      oldValue: nil, newValue: v))
  result = LiveQueryCallbackHandle(index: idx)

proc offChange*(lq: LiveQuery; h: LiveQueryCallbackHandle) =
  ## Remove a previously-registered callback.
  acquire(lq.lock)
  if h.index >= 0 and h.index < lq.callbacks.len:
    lq.callbacks[h.index] = nil
  release(lq.lock)

proc snapshot*(lq: LiveQuery): seq[(string, Value)] =
  ## Snapshot the current matched set. Useful for one-shot reads from
  ## the live query state without polling via callbacks.
  acquire(lq.lock)
  result = newSeqOfCap[(string, Value)](lq.matched.len)
  for k, v in lq.matched.pairs: result.add((k, v))
  release(lq.lock)

proc len*(lq: LiveQuery): int =
  acquire(lq.lock)
  result = lq.matched.len
  release(lq.lock)

proc close*(lq: LiveQuery) =
  ## Unsubscribe and clear callbacks. Idempotent.
  if lq.subRegistered:
    lq.db.unsubscribeCollection(lq.subHandle)
    lq.subRegistered = false
  acquire(lq.lock)
  lq.callbacks.setLen(0)
  lq.matched.clear()
  release(lq.lock)
