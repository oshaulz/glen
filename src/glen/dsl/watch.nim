## watch — declare reactive subscriptions in a single block and get
## back a `WatchScope` you can close all at once.
##
##   let scope = watch(db):
##     doc "users", "u1":
##       echo $id, " -> ", $newValue
##
##     field "users", "u1", "email":
##       echo $oldValue, " -> ", $newValue
##
##     collection "users":
##       echo "any user changed: ", $id
##
##   # later
##   scope.close()
##
## Inside each handler `db` is captured from the outer scope and the
## following names are injected:
##   * `doc`        — `id: Id`, `newValue: Value`
##   * `field`      — `id: Id`, `path: string`, `oldValue: Value`, `newValue: Value`
##   * `collection` — `id: Id`, `newValue: Value`
##
## Collection-wide subscriptions require the core support added in
## `subscription.nim` (see `subscribeCollection`).

import std/[macros, strutils]
import glen/types
import glen/db as glendb
import glen/subscription

type
  WatchHandleKind = enum whDoc, whField, whFieldDelta, whCollection
  WatchHandle = object
    case kind: WatchHandleKind
    of whDoc: doc: SubscriptionHandle
    of whField: field: FieldSubscriptionHandle
    of whFieldDelta: fieldDelta: FieldDeltaSubscriptionHandle
    of whCollection: coll: CollectionSubscriptionHandle

  WatchScope* = ref object
    db*: glendb.GlenDB
    handles: seq[WatchHandle]

proc newWatchScope*(db: glendb.GlenDB): WatchScope =
  WatchScope(db: db, handles: @[])

proc onDoc*(scope: WatchScope; collection, docId: string;
            cb: proc (id: Id; newValue: Value) {.closure.}) =
  let h = scope.db.subscribe(collection, docId, cb)
  scope.handles.add(WatchHandle(kind: whDoc, doc: h))

proc onField*(scope: WatchScope; collection, docId, fieldPath: string;
              cb: FieldCallback) =
  let h = scope.db.subscribeField(collection, docId, fieldPath, cb)
  scope.handles.add(WatchHandle(kind: whField, field: h))

proc onFieldDelta*(scope: WatchScope; collection, docId, fieldPath: string;
                   cb: FieldDeltaCallback) =
  let h = scope.db.subscribeFieldDelta(collection, docId, fieldPath, cb)
  scope.handles.add(WatchHandle(kind: whFieldDelta, fieldDelta: h))

proc onCollection*(scope: WatchScope; collection: string;
                   cb: proc (id: Id; newValue: Value) {.closure.}) =
  let h = scope.db.subscribeCollection(collection, cb)
  scope.handles.add(WatchHandle(kind: whCollection, coll: h))

proc close*(scope: WatchScope) =
  ## Unsubscribe every handle this scope holds. Idempotent: handles are
  ## cleared after the first call.
  for h in scope.handles:
    case h.kind
    of whDoc:        scope.db.unsubscribe(h.doc)
    of whField:      scope.db.unsubscribeField(h.field)
    of whFieldDelta: scope.db.unsubscribeFieldDelta(h.fieldDelta)
    of whCollection: scope.db.unsubscribeCollection(h.coll)
  scope.handles.setLen(0)

# ---- Macro ----

proc handlerCall(kind: string; scopeSym: NimNode; args: seq[NimNode];
                 body: NimNode): NimNode =
  ## Build the right onXxx call, with a closure over `body` that injects
  ## the right callback parameter names.
  case kind
  of "doc":
    if args.len != 2:
      error("watch: `doc` expects (collection, docId)", body)
    let cb = quote do:
      proc (id {.inject.}: Id; newValue {.inject.}: Value) {.closure.} =
        `body`
    result = newCall(bindSym"onDoc", scopeSym, args[0], args[1], cb)
  of "field":
    if args.len != 3:
      error("watch: `field` expects (collection, docId, fieldPath)", body)
    let cb = quote do:
      proc (id {.inject.}: Id; path {.inject.}: string;
            oldValue {.inject.}: Value; newValue {.inject.}: Value) {.closure.} =
        `body`
    result = newCall(bindSym"onField", scopeSym, args[0], args[1], args[2], cb)
  of "fielddelta":
    if args.len != 3:
      error("watch: `fieldDelta` expects (collection, docId, fieldPath)", body)
    let cb = quote do:
      proc (id {.inject.}: Id; path {.inject.}: string;
            deltaEvent {.inject.}: Value) {.closure.} =
        `body`
    result = newCall(bindSym"onFieldDelta", scopeSym, args[0], args[1], args[2], cb)
  of "collection":
    if args.len != 1:
      error("watch: `collection` expects (collection)", body)
    let cb = quote do:
      proc (id {.inject.}: Id; newValue {.inject.}: Value) {.closure.} =
        `body`
    result = newCall(bindSym"onCollection", scopeSym, args[0], cb)
  else:
    error("watch: unknown handler `" & kind & "`. Expected one of: doc, field, fieldDelta, collection", body)

macro watch*(db: glendb.GlenDB; body: untyped): WatchScope =
  ## Declare a batch of subscriptions over `db`. Each top-level entry is
  ## `<kind> arg1, arg2, ...: <handler-body>` where `<kind>` is one of
  ## `doc`, `field`, `fieldDelta`, `collection`.
  let scopeSym = genSym(nskLet, "watchScope")
  var stmts = newStmtList()
  stmts.add(newLetStmt(scopeSym, newCall(bindSym"newWatchScope", db)))

  if body.kind != nnkStmtList:
    error("watch: expected a block body", body)

  for entry in body:
    if entry.kind == nnkCommentStmt: continue
    if entry.kind notin {nnkCall, nnkCommand}:
      error("watch: each entry must be `<kind> <args>: <body>`", entry)
    # Shape: nnkCall(kindIdent, arg1, ..., bodyStmtList)
    if entry.len < 2 or entry[0].kind notin {nnkIdent, nnkSym}:
      error("watch: malformed handler entry", entry)
    let kind = ($entry[0]).toLowerAscii
    var args: seq[NimNode] = @[]
    var handlerBody: NimNode = nil
    for i in 1 ..< entry.len:
      let child = entry[i]
      if child.kind == nnkStmtList:
        handlerBody = child
      else:
        args.add(child)
    if handlerBody.isNil:
      error("watch: handler must end with a `:` block body", entry)
    stmts.add(handlerCall(kind, scopeSym, args, handlerBody))

  stmts.add(scopeSym)
  result = newBlockStmt(stmts)
