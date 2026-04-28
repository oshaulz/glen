# Glen subscription system
# Allows subscribing to (collection, docId) changes.

import std/[tables, locks, strutils]
import std/streams
import glen/codec_stream
import glen/types

type
  SubscriberCallback* = proc (id: Id; newValue: Value) {.closure.}

  SubEntry = ref object
    key: string
    callbacks: seq[SubscriberCallback]

  SubscriptionManager* = ref object
    lock: Lock
    subs: Table[string, SubEntry]
    fieldSubs: Table[string, FieldSubEntry]
    fieldDeltaSubs: Table[string, FieldDeltaSubEntry]
    ## Collection-wide ("wildcard") subscriptions. Key is the collection
    ## name; callbacks fire for every put/delete in that collection.
    collectionSubs: Table[string, SubEntry]
    ## Cache of split field paths per (key -> path -> parts)
    pathPartsCache: Table[string, Table[string, seq[string]]]

  SubscriptionHandle* = object
    key*: string
    index*: int

  CollectionSubscriptionHandle* = object
    collection*: string
    index*: int

  FieldCallback* = proc (id: Id; path: string; oldValue: Value; newValue: Value) {.closure.}

  FieldSubEntry = ref object
    key: string
    callbacksByPath: Table[string, seq[FieldCallback]]

  FieldSubscriptionHandle* = object
    key*: string
    path*: string
    index*: int

  FieldDeltaCallback* = proc (id: Id; path: string; deltaEvent: Value) {.closure.}

  FieldDeltaSubEntry = ref object
    key: string
    callbacksByPath: Table[string, seq[FieldDeltaCallback]]

  FieldDeltaSubscriptionHandle* = object
    key*: string
    path*: string
    index*: int

proc newSubscriptionManager*(): SubscriptionManager =
  result = SubscriptionManager(
    subs: initTable[string, SubEntry](),
    fieldSubs: initTable[string, FieldSubEntry](),
    fieldDeltaSubs: initTable[string, FieldDeltaSubEntry](),
    collectionSubs: initTable[string, SubEntry](),
    pathPartsCache: initTable[string, Table[string, seq[string]]]()
  )
  initLock(result.lock)

proc makeKey(collection, docId: string): string = collection & ":" & docId

proc subscribe*(sm: SubscriptionManager; collection, docId: string; cb: SubscriberCallback): SubscriptionHandle =
  acquire(sm.lock)
  let key = makeKey(collection, docId)
  if key notin sm.subs:
    sm.subs[key] = SubEntry(key: key, callbacks: @[])
  sm.subs[key].callbacks.add(cb)
  let idx = sm.subs[key].callbacks.len - 1
  release(sm.lock)
  result = SubscriptionHandle(key: key, index: idx)

converter toSubscriberCallback*(cb: proc (id: Id; newValue: Value)): SubscriberCallback =
  proc (id: Id; newValue: Value) {.closure.} = cb(id, newValue)

proc unsubscribe*(sm: SubscriptionManager; h: SubscriptionHandle) =
  acquire(sm.lock)
  if h.key in sm.subs:
    var entry = sm.subs[h.key]
    if h.index >= 0 and h.index < entry.callbacks.len:
      entry.callbacks[h.index] = nil
      # Compact when too many nils; drop entry if empty
      var newCbs: seq[SubscriberCallback] = @[]
      for cb in entry.callbacks:
        if cb != nil: newCbs.add(cb)
      entry.callbacks = newCbs
      if entry.callbacks.len == 0:
        sm.subs.del(h.key)
  release(sm.lock)

proc notify*(sm: SubscriptionManager; id: Id; newValue: Value) =
  let key = makeKey(id.collection, id.docId)
  var callbacks: seq[SubscriberCallback] = @[]
  acquire(sm.lock)
  if key in sm.subs:
    for cb in sm.subs[key].callbacks:
      if cb != nil:
        callbacks.add(cb)
  if id.collection in sm.collectionSubs:
    for cb in sm.collectionSubs[id.collection].callbacks:
      if cb != nil:
        callbacks.add(cb)
  release(sm.lock)
  for cb in callbacks:
    cb(id, newValue)

proc subscribeCollection*(sm: SubscriptionManager; collection: string;
                          cb: SubscriberCallback): CollectionSubscriptionHandle =
  ## Subscribe to every put/delete inside a collection. Fires after the
  ## same per-doc callbacks `subscribe` already runs.
  acquire(sm.lock)
  if collection notin sm.collectionSubs:
    sm.collectionSubs[collection] = SubEntry(key: collection, callbacks: @[])
  sm.collectionSubs[collection].callbacks.add(cb)
  let idx = sm.collectionSubs[collection].callbacks.len - 1
  release(sm.lock)
  result = CollectionSubscriptionHandle(collection: collection, index: idx)

proc unsubscribeCollection*(sm: SubscriptionManager; h: CollectionSubscriptionHandle) =
  acquire(sm.lock)
  if h.collection in sm.collectionSubs:
    var entry = sm.collectionSubs[h.collection]
    if h.index >= 0 and h.index < entry.callbacks.len:
      entry.callbacks[h.index] = nil
      var newCbs: seq[SubscriberCallback] = @[]
      for cb in entry.callbacks:
        if cb != nil: newCbs.add(cb)
      entry.callbacks = newCbs
      if entry.callbacks.len == 0:
        sm.collectionSubs.del(h.collection)
  release(sm.lock)

## Subscribe and stream updates as framed events into the provided Stream.
## Each event is a Value object encoded via encodeTo with fields:
## { collection: string, docId: string, version: uint64, value: Value }
proc subscribeStream*(sm: SubscriptionManager; collection, docId: string; s: Stream): SubscriptionHandle =
  let cb = proc (id: Id; newValue: Value) {.closure.} =
    var ev = VObject()
    ev["collection"] = VString(id.collection)
    ev["docId"] = VString(id.docId)
    ev["version"] = VInt(int64(id.version))
    ev["value"] = if newValue.isNil: VNull() else: newValue
    encodeTo(s, ev)
  result = sm.subscribe(collection, docId, cb)

proc subscribeField*(sm: SubscriptionManager; collection, docId: string; fieldPath: string; cb: FieldCallback): FieldSubscriptionHandle =
  acquire(sm.lock)
  let key = makeKey(collection, docId)
  if key notin sm.fieldSubs:
    sm.fieldSubs[key] = FieldSubEntry(key: key, callbacksByPath: initTable[string, seq[FieldCallback]]())
  if fieldPath notin sm.fieldSubs[key].callbacksByPath:
    sm.fieldSubs[key].callbacksByPath[fieldPath] = @[]
  sm.fieldSubs[key].callbacksByPath[fieldPath].add(cb)
  let idx = sm.fieldSubs[key].callbacksByPath[fieldPath].len - 1
  release(sm.lock)
  result = FieldSubscriptionHandle(key: key, path: fieldPath, index: idx)

proc unsubscribeField*(sm: SubscriptionManager; h: FieldSubscriptionHandle) =
  acquire(sm.lock)
  if h.key in sm.fieldSubs and h.path in sm.fieldSubs[h.key].callbacksByPath:
    var seqRef = sm.fieldSubs[h.key].callbacksByPath[h.path]
    if h.index >= 0 and h.index < seqRef.len:
      seqRef[h.index] = nil
    # compact path callbacks
    var newCbs: seq[FieldCallback] = @[]
    for cb in seqRef:
      if cb != nil: newCbs.add(cb)
    if newCbs.len == 0:
      sm.fieldSubs[h.key].callbacksByPath.del(h.path)
      if sm.fieldSubs[h.key].callbacksByPath.len == 0:
        sm.fieldSubs.del(h.key)
        if h.key in sm.pathPartsCache: sm.pathPartsCache.del(h.key)
    else:
      sm.fieldSubs[h.key].callbacksByPath[h.path] = newCbs
  release(sm.lock)

proc getFieldByParts(v: Value; parts: seq[string]): Value =
  if v.isNil: return nil
  var cur = v
  for p in parts:
    if cur.isNil or cur.kind != vkObject: return nil
    cur = cur[p]
  return cur

proc getCachedParts(sm: SubscriptionManager; key, path: string): seq[string] =
  ## Returns cached split parts for a path; caches if missing.
  if key notin sm.pathPartsCache:
    sm.pathPartsCache[key] = initTable[string, seq[string]]()
  if path notin sm.pathPartsCache[key]:
    sm.pathPartsCache[key][path] = path.split('.')
  sm.pathPartsCache[key][path]

proc notifyFieldChanges*(sm: SubscriptionManager; id: Id; oldDoc, newDoc: Value) =
  let key = makeKey(id.collection, id.docId)
  var toCall: seq[(FieldCallback, string, Value, Value)] = @[]
  var toDeltaCall: seq[(FieldDeltaCallback, string, Value)] = @[]
  acquire(sm.lock)
  if key in sm.fieldSubs:
    for path, cbs in sm.fieldSubs[key].callbacksByPath.pairs:
      let parts = sm.getCachedParts(key, path)
      let oldV = getFieldByParts(oldDoc, parts)
      let newV = getFieldByParts(newDoc, parts)
      if oldV == newV:
        continue
      for cb in cbs:
        if cb != nil:
          toCall.add((cb, path, oldV, newV))
  if key in sm.fieldDeltaSubs:
    for path, cbs in sm.fieldDeltaSubs[key].callbacksByPath.pairs:
      let parts = sm.getCachedParts(key, path)
      let oldV = getFieldByParts(oldDoc, parts)
      let newV = getFieldByParts(newDoc, parts)
      if oldV == newV:
        continue
      # Build delta event Value with a simple schema:
      # { kind: "append"|"replace"|"delete"|"set", added?: Value, old?: Value, new?: Value }
      var ev = VObject()
      if oldV.isNil and not newV.isNil:
        ev["kind"] = VString("set")
        ev["new"] = newV
      elif not oldV.isNil and newV.isNil:
        ev["kind"] = VString("delete")
        ev["old"] = oldV
      elif oldV.kind == vkString and newV.kind == vkString and newV.s.startsWith(oldV.s):
        ev["kind"] = VString("append")
        ev["added"] = VString(newV.s[oldV.s.len ..< newV.s.len])
      else:
        ev["kind"] = VString("replace")
        ev["old"] = oldV
        ev["new"] = newV
      for cb in cbs:
        if cb != nil:
          toDeltaCall.add((cb, path, ev))
  release(sm.lock)
  for (cb, path, o, n) in toCall:
    cb(id, path, o, n)
  for (cb, path, ev) in toDeltaCall:
    cb(id, path, ev)

proc subscribeFieldStream*(sm: SubscriptionManager; collection, docId: string; fieldPath: string; s: Stream): FieldSubscriptionHandle =
  let cb = proc (id: Id; path: string; oldValue: Value; newValue: Value) {.closure.} =
    var ev = VObject()
    ev["collection"] = VString(id.collection)
    ev["docId"] = VString(id.docId)
    ev["version"] = VInt(int64(id.version))
    ev["fieldPath"] = VString(path)
    ev["old"] = if oldValue.isNil: VNull() else: oldValue
    ev["new"] = if newValue.isNil: VNull() else: newValue
    encodeTo(s, ev)
  result = sm.subscribeField(collection, docId, fieldPath, cb)

proc subscribeFieldDelta*(sm: SubscriptionManager; collection, docId: string; fieldPath: string; cb: FieldDeltaCallback): FieldDeltaSubscriptionHandle =
  acquire(sm.lock)
  let key = makeKey(collection, docId)
  if key notin sm.fieldDeltaSubs:
    sm.fieldDeltaSubs[key] = FieldDeltaSubEntry(key: key, callbacksByPath: initTable[string, seq[FieldDeltaCallback]]())
  if fieldPath notin sm.fieldDeltaSubs[key].callbacksByPath:
    sm.fieldDeltaSubs[key].callbacksByPath[fieldPath] = @[]
  sm.fieldDeltaSubs[key].callbacksByPath[fieldPath].add(cb)
  let idx = sm.fieldDeltaSubs[key].callbacksByPath[fieldPath].len - 1
  release(sm.lock)
  result = FieldDeltaSubscriptionHandle(key: key, path: fieldPath, index: idx)

proc unsubscribeFieldDelta*(sm: SubscriptionManager; h: FieldDeltaSubscriptionHandle) =
  acquire(sm.lock)
  if h.key in sm.fieldDeltaSubs and h.path in sm.fieldDeltaSubs[h.key].callbacksByPath:
    var seqRef = sm.fieldDeltaSubs[h.key].callbacksByPath[h.path]
    if h.index >= 0 and h.index < seqRef.len:
      seqRef[h.index] = nil
    var newCbs: seq[FieldDeltaCallback] = @[]
    for cb in seqRef:
      if cb != nil: newCbs.add(cb)
    if newCbs.len == 0:
      sm.fieldDeltaSubs[h.key].callbacksByPath.del(h.path)
      if sm.fieldDeltaSubs[h.key].callbacksByPath.len == 0:
        sm.fieldDeltaSubs.del(h.key)
        if h.key in sm.pathPartsCache: sm.pathPartsCache.del(h.key)
    else:
      sm.fieldDeltaSubs[h.key].callbacksByPath[h.path] = newCbs
  release(sm.lock)

proc subscribeFieldDeltaStream*(sm: SubscriptionManager; collection, docId: string; fieldPath: string; s: Stream): FieldDeltaSubscriptionHandle =
  let cb = proc (id: Id; path: string; deltaEvent: Value) {.closure.} =
    var ev = VObject()
    ev["collection"] = VString(id.collection)
    ev["docId"] = VString(id.docId)
    ev["version"] = VInt(int64(id.version))
    ev["fieldPath"] = VString(path)
    ev["delta"] = deltaEvent
    encodeTo(s, ev)
  result = sm.subscribeFieldDelta(collection, docId, fieldPath, cb)
