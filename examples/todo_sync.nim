## Local-first todo CLI demonstrating Glen's multi-master replication.
##
## Each invocation owns one local database; two (or more) nodes sync
## through a shared "mailbox" directory by exchanging encoded change
## batches. changeId-based idempotency means re-pulling the same batch
## is a no-op, and HLC last-write-wins resolves concurrent edits.
##
## Build:
##   nim c -d:release --path:src examples/todo_sync.nim
##
## Demo run (two terminals, same machine):
##   # terminal 1
##   ./examples/todo_sync --db ./node-a add "buy milk"
##   ./examples/todo_sync --db ./node-a add "ship glen 0.3"
##   ./examples/todo_sync --db ./node-a push ./mailbox
##
##   # terminal 2
##   ./examples/todo_sync --db ./node-b pull ./mailbox
##   ./examples/todo_sync --db ./node-b list
##   ./examples/todo_sync --db ./node-b complete <id>
##   ./examples/todo_sync --db ./node-b push ./mailbox
##
##   # back in terminal 1
##   ./examples/todo_sync --db ./node-a pull ./mailbox
##   ./examples/todo_sync --db ./node-a list   # sees the completion

import std/[os, parseopt, strutils, strformat, random]
import glen/types, glen/db as glendb, glen/codec, glen/util

const TodoCollection = "todos"

proc usage() =
  echo """
todo_sync — local-first todo demo for Glen

USAGE:
  todo_sync --db <dir> add <text...>
  todo_sync --db <dir> complete <id>
  todo_sync --db <dir> list
  todo_sync --db <dir> push <mailbox-dir>
  todo_sync --db <dir> pull <mailbox-dir>

Each node has its own --db directory. Two nodes sync via a shared
<mailbox-dir>: one runs `push`, the other runs `pull`. Idempotency
makes repeated pulls safe; HLC LWW resolves concurrent edits.
"""

proc randId(): string =
  result = newString(10)
  for i in 0 ..< result.len:
    result[i] = chr(ord('a') + rand(25))

proc cmdAdd(db: glendb.GlenDB; text: string) =
  let id = randId()
  var v = VObject()
  v["text"] = VString(text)
  v["done"] = VBool(false)
  v["created"] = VInt(nowMillis())
  db.put(TodoCollection, id, v)
  echo &"added {id}: {text}"

proc cmdComplete(db: glendb.GlenDB; id: string) =
  let v = db.get(TodoCollection, id)
  if v.isNil:
    echo &"no todo with id {id}"
    return
  v["done"] = VBool(true)
  db.put(TodoCollection, id, v)
  echo &"completed {id}"

proc cmdList(db: glendb.GlenDB) =
  let items = db.getAll(TodoCollection)
  if items.len == 0:
    echo "(no todos)"
    return
  for (id, v) in items:
    let mark = if v["done"].b: "[x]" else: "[ ]"
    echo &"{mark} {id}  {v[\"text\"].s}"

# --- replication wire format ----------------------------------------------
# A batch is a Glen Value; we use the binary codec for transport. The schema
# is documented inline so a future version can evolve it without breaking
# old mailboxes.

proc replChangeToValue(ch: ReplChange): Value =
  result = VObject()
  result["collection"] = VString(ch.collection)
  result["docId"]      = VString(ch.docId)
  result["op"]         = VString(if ch.op == roPut: "put" else: "delete")
  result["version"]    = VInt(int64(ch.version))
  if not ch.value.isNil:
    result["value"] = ch.value
  result["changeId"]   = VString(ch.changeId)
  result["originNode"] = VString(ch.originNode)
  result["hlcWall"]    = VInt(ch.hlc.wallMillis)
  result["hlcCounter"] = VInt(int64(ch.hlc.counter))
  result["hlcNode"]    = VString(ch.hlc.nodeId)

proc valueToReplChange(v: Value): ReplChange =
  result.collection = v["collection"].s
  result.docId      = v["docId"].s
  result.op         = (if v["op"].s == "put": roPut else: roDelete)
  result.version    = uint64(v["version"].i)
  result.value      = v.getOrNil("value")
  result.changeId   = v["changeId"].s
  result.originNode = v["originNode"].s
  result.hlc.wallMillis = v["hlcWall"].i
  result.hlc.counter    = uint32(v["hlcCounter"].i)
  result.hlc.nodeId     = v["hlcNode"].s

proc cmdPush(db: glendb.GlenDB; mailbox: string) =
  createDir(mailbox)
  # Export every change we have. In production you'd track an outbound cursor
  # per peer to send only deltas; for a demo, idempotency on the receiver
  # makes the full-replay approach simple and correct.
  let (cursor, changes) = db.exportChanges(0'u64)
  var arr: seq[Value] = @[]
  for ch in changes:
    arr.add(replChangeToValue(ch))
  var batch = VObject()
  batch["from"]      = VString(db.nodeId)
  batch["endCursor"] = VInt(int64(cursor))
  batch["changes"]   = VArray(arr)
  # Atomic write: temp + rename so a concurrent puller never sees a partial.
  let path = mailbox / (db.nodeId & ".batch")
  let tmp  = path & ".tmp"
  writeFile(tmp, encode(batch))
  moveFile(tmp, path)
  echo &"pushed {changes.len} changes ({db.nodeId} -> {path})"

proc cmdPull(db: glendb.GlenDB; mailbox: string) =
  if not dirExists(mailbox):
    echo &"no mailbox at {mailbox}"
    return
  var totalApplied = 0
  for kind, path in walkDir(mailbox):
    if kind != pcFile: continue
    if not path.endsWith(".batch"): continue
    let raw = readFile(path)
    if raw.len == 0: continue
    let v = decode(raw)
    let fromNode = v["from"].s
    if fromNode == db.nodeId:
      continue                              # don't ingest our own batch
    var changes: seq[ReplChange] = @[]
    for cv in v["changes"].arr:
      changes.add(valueToReplChange(cv))
    db.applyChanges(changes)
    totalApplied += changes.len
    echo &"  applied {changes.len} from {fromNode}"
  echo &"pulled {totalApplied} changes total"

# --- entry point ----------------------------------------------------------

when isMainModule:
  randomize()
  var dbDir = ""
  var args: seq[string] = @[]
  let raw = commandLineParams()
  var i = 0
  while i < raw.len:
    let a = raw[i]
    if a == "--db":
      if i + 1 >= raw.len: usage(); quit 1
      dbDir = raw[i + 1]
      i += 2
    elif a.startsWith("--db="):
      dbDir = a["--db=".len .. ^1]
      inc i
    elif a in ["-h", "--help"]:
      usage(); quit 0
    else:
      args.add(a)
      inc i

  if dbDir.len == 0 or args.len == 0:
    usage(); quit 1

  let db = newGlenDB(dbDir)

  try:
    case args[0]
    of "add":
      if args.len < 2: usage(); quit 1
      cmdAdd(db, args[1 ..^ 1].join(" "))
    of "complete":
      if args.len != 2: usage(); quit 1
      cmdComplete(db, args[1])
    of "list":
      cmdList(db)
    of "push":
      if args.len != 2: usage(); quit 1
      cmdPush(db, args[1])
    of "pull":
      if args.len != 2: usage(); quit 1
      cmdPull(db, args[1])
    else:
      usage(); quit 1
  finally:
    db.close()
