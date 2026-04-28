## sync — declarative replication topology built on top of
## `exportChanges` / `applyChanges` / `setPeerCursor`.
##
##   let sync = sync(db):
##     peer "node-b":
##       transport: myTransport          # SyncTransport ref
##       intervalMs: 5000
##       collections: ["users", "posts"]
##
##   while running:
##     sync.tick()                       # call from your event loop or timer
##
## A `SyncTransport` is anything implementing the two-way exchange — local
## changes go out, remote changes come back — so users can plug in HTTP,
## a mailbox dir, an IPC pipe, in-memory for tests, etc. The supervisor
## tracks per-peer cursors and calls `setPeerCursor` so the replication log
## can be GC'd.

import std/[macros, strutils]
import glen/db as glendb
import glen/util

# ---- Transport interface ----

type
  SyncDirection* = enum sdBidirectional, sdPushOnly, sdPullOnly

  SyncTransport* = ref object of RootObj
    ## Subclass and override `exchange` for your real transport.

  InMemoryTransport* = ref object of SyncTransport
    ## Test/demo transport: just buffers batches in two queues so two
    ## InMemoryTransports can be paired (`a.peerOf = b; b.peerOf = a`).
    inbox*: seq[ReplChange]
    peerOf*: InMemoryTransport

method exchange*(t: SyncTransport;
                 outgoing: seq[ReplChange]): seq[ReplChange] {.base.} =
  ## Send `outgoing` to the remote and return whatever it has for us.
  ## Default implementation is a no-op (drops outgoing, returns nothing) —
  ## override in your subclass.
  @[]

method exchange*(t: InMemoryTransport;
                 outgoing: seq[ReplChange]): seq[ReplChange] =
  if not t.peerOf.isNil:
    for ch in outgoing: t.peerOf.inbox.add(ch)
  result = t.inbox
  t.inbox.setLen(0)

# ---- Peer + sync supervisor ----

type
  SyncPeer* = ref object
    name*: string
    transport*: SyncTransport
    intervalMs*: int
    collections*: seq[string]
    direction*: SyncDirection
    lastTickMs*: int64
    cursor*: ReplExportCursor

  Sync* = ref object
    db*: glendb.GlenDB
    peers*: seq[SyncPeer]

proc newSync*(db: glendb.GlenDB): Sync =
  Sync(db: db, peers: @[])

proc addPeer*(s: Sync; name: string; transport: SyncTransport;
              intervalMs = 5000; collections: seq[string] = @[];
              direction = sdBidirectional): SyncPeer =
  result = SyncPeer(
    name: name, transport: transport, intervalMs: intervalMs,
    collections: collections, direction: direction,
    lastTickMs: 0, cursor: ReplExportCursor(s.db.getPeerCursor(name)))
  s.peers.add(result)

proc tickPeer*(s: Sync; peer: SyncPeer) =
  ## Run one push-pull exchange against the peer regardless of interval.
  var outgoing: seq[ReplChange] = @[]
  if peer.direction != sdPullOnly:
    let (newCursor, changes) = s.db.exportChanges(
      peer.cursor, includeCollections = peer.collections)
    outgoing = changes
    peer.cursor = newCursor
  let incoming = peer.transport.exchange(outgoing)
  if peer.direction != sdPushOnly and incoming.len > 0:
    s.db.applyChanges(incoming)
  s.db.setPeerCursor(peer.name, uint64(peer.cursor))
  peer.lastTickMs = nowMillis()

proc tick*(s: Sync) =
  ## Visit every peer whose `intervalMs` has elapsed since its last tick.
  let now = nowMillis()
  for p in s.peers:
    if p.lastTickMs == 0 or (now - p.lastTickMs) >= p.intervalMs:
      tickPeer(s, p)

proc tickAll*(s: Sync) =
  ## Force every peer to tick now (useful for tests / shutdown drain).
  for p in s.peers:
    tickPeer(s, p)

# ---- Macro ----

proc parsePeerOptions(nameLit: NimNode; body: NimNode): NimNode =
  ## Translates `peer "name": <opt-block>` into an `addPeer(...)` call.
  if body.kind != nnkStmtList:
    error("sync: `peer` block must contain options", body)

  var transport: NimNode = nil
  var intervalMs: NimNode = newLit(5000)
  var collections: NimNode = newTree(nnkPrefix, ident"@", newTree(nnkBracket))
  var direction: NimNode = bindSym"sdBidirectional"

  for opt in body:
    if opt.kind == nnkCommentStmt: continue
    if opt.kind notin {nnkCall, nnkCommand}:
      error("sync: peer option must be `key: value`", opt)
    if opt.len < 2 or opt[0].kind notin {nnkIdent, nnkSym}:
      error("sync: malformed peer option", opt)
    let key = ($opt[0]).toLowerAscii
    var rhs = opt[1]
    if rhs.kind == nnkStmtList and rhs.len == 1:
      rhs = rhs[0]
    case key
    of "transport":  transport = rhs
    of "intervalms": intervalMs = rhs
    of "collections":collections = rhs
    of "direction":  direction = rhs
    else:
      error("sync: unknown peer option `" & key & "`. Expected one of: transport, intervalMs, collections, direction", opt[0])

  if transport.isNil:
    error("sync: peer must specify a `transport`", body)

  result = newCall(bindSym"addPeer", ident"sync", nameLit, transport,
                  newTree(nnkExprEqExpr, ident"intervalMs", intervalMs),
                  newTree(nnkExprEqExpr, ident"collections", collections),
                  newTree(nnkExprEqExpr, ident"direction", direction))

macro sync*(db: glendb.GlenDB; body: untyped): Sync =
  ## Build a `Sync` for `db` from a peers block. Returns the supervisor;
  ## call `.tick()` from your event loop.
  if body.kind != nnkStmtList:
    error("sync: expected a block body", body)

  var stmts = newStmtList()
  stmts.add(newLetStmt(ident"sync", newCall(bindSym"newSync", db)))

  for entry in body:
    if entry.kind == nnkCommentStmt: continue
    if entry.kind notin {nnkCall, nnkCommand}:
      error("sync: top-level entries must be `peer \"name\": <opts>`", entry)
    if entry.len < 2 or entry[0].kind notin {nnkIdent, nnkSym} or ($entry[0]).toLowerAscii != "peer":
      error("sync: only `peer` entries are supported at the top level", entry)
    let nameArg = entry[1]
    let optsBody = if entry.len >= 3: entry[2] else: newStmtList()
    stmts.add(newTree(nnkDiscardStmt,
      parsePeerOptions(nameArg, optsBody)))

  stmts.add(ident"sync")
  result = newBlockStmt(stmts)
