## query — block DSL over the Glen query builder.
##
## Rewrites natural Nim operator expressions into `whereEq` / `whereGte` /
## `whereIn` / etc. calls, and dispatches geo / vector helpers when their
## section names appear.
##
##   let active = query(db, "users"):
##     where:
##       status == "active"
##       age >= 30
##       role in ["admin", "member"]
##       name.contains("li")
##     orderBy: name asc
##     limit: 10
##
## Geo (point index already created on the collection):
##
##   let nearby = query(db, "stores"):
##     near: ("byLoc", -73.98, 40.75, 2000.0)   # (indexName, lon, lat, radiusMeters)
##     limit: 20
##
## Vector (HNSW index already created):
##
##   let similar = query(db, "docs"):
##     nearestVector: ("byEmbedding", queryVec, 10)
##
## All bare values inside `where:` are wrapped through `toValue` (see
## `glen/dsl/literal`) so you can write `status == "active"` instead of
## `status == VString("active")`. Field names are dotted identifiers
## (`addr.city`) and turn into the dotted-path strings the planner expects.

import std/[macros, strutils]
import glen/types
import glen/db as glendb
import glen/geo
import glen/query as glenquery
import glen/dsl/literal
import glen/dsl/live

export live

# ---- Path helpers ----

proc fieldExprToString(node: NimNode): string =
  ## `name`              -> "name"
  ## `addr.city`         -> "addr.city"
  ## `profile.contact.email` -> "profile.contact.email"
  case node.kind
  of nnkIdent, nnkSym:
    result = $node
  of nnkDotExpr:
    result = fieldExprToString(node[0]) & "." & fieldExprToString(node[1])
  of nnkStrLit, nnkRStrLit, nnkTripleStrLit:
    result = node.strVal
  else:
    error("query: expected field path (ident or dotted ident), got " & $node.kind, node)

# ---- Predicate rewriting ----

proc rewritePredicate(qSym: NimNode; stmt: NimNode): NimNode =
  ## Map a single predicate statement to a `q.whereXxx(...)` call.
  case stmt.kind
  of nnkInfix:
    let op  = $stmt[0]
    let lhs = stmt[1]
    let rhs = stmt[2]
    case op
    of "==":
      result = newCall(bindSym"whereEq", qSym,
                       newLit(fieldExprToString(lhs)),
                       newCall(bindSym"toValue", rhs))
    of "!=":
      result = newCall(bindSym"whereNe", qSym,
                       newLit(fieldExprToString(lhs)),
                       newCall(bindSym"toValue", rhs))
    of "<":
      result = newCall(bindSym"whereLt", qSym,
                       newLit(fieldExprToString(lhs)),
                       newCall(bindSym"toValue", rhs))
    of "<=":
      result = newCall(bindSym"whereLte", qSym,
                       newLit(fieldExprToString(lhs)),
                       newCall(bindSym"toValue", rhs))
    of ">":
      result = newCall(bindSym"whereGt", qSym,
                       newLit(fieldExprToString(lhs)),
                       newCall(bindSym"toValue", rhs))
    of ">=":
      result = newCall(bindSym"whereGte", qSym,
                       newLit(fieldExprToString(lhs)),
                       newCall(bindSym"toValue", rhs))
    of "in":
      # `field in [a, b, c]` — wrap each rhs element through toValue.
      if rhs.kind != nnkBracket:
        error("query: `in` expects an array literal on the right", rhs)
      var arr = newTree(nnkBracket)
      for el in rhs:
        arr.add(newCall(bindSym"toValue", el))
      let valuesExpr = newTree(nnkPrefix, ident"@", arr)
      result = newCall(bindSym"whereIn", qSym,
                       newLit(fieldExprToString(lhs)),
                       valuesExpr)
    else:
      error("query: unsupported predicate operator `" & op & "`", stmt)
  of nnkCall:
    # `field.contains("xyz")`
    if stmt.len == 2 and stmt[0].kind == nnkDotExpr and $stmt[0][1] == "contains":
      let fieldNode = stmt[0][0]
      let needle = stmt[1]
      result = newCall(bindSym"whereContains", qSym,
                       newLit(fieldExprToString(fieldNode)),
                       needle)
    else:
      error("query: unsupported predicate call (only .contains supported here)", stmt)
  of nnkDotExpr:
    error("query: bare field reference is not a predicate. Did you mean `field == value`?", stmt)
  else:
    error("query: unsupported predicate shape: " & $stmt.kind, stmt)

# ---- Section dispatch ----

proc sectionLabel(node: NimNode): string =
  ## A section is `label: body` or `label body`. Returns the lowercased
  ## label or "" if the node isn't a section header.
  case node.kind
  of nnkCall, nnkCommand:
    if node.len >= 2 and node[0].kind in {nnkIdent, nnkSym}:
      return ($node[0]).toLowerAscii
  else: discard
  ""

proc sectionBody(node: NimNode): NimNode =
  ## For `label: body` (nnkCall with stmt list as the second child) returns
  ## the body. For `label arg` returns the arg. Caller must check kind.
  if node.len >= 2: node[1] else: newEmptyNode()

proc rewriteWhereBlock(qSym: NimNode; body: NimNode): NimNode =
  result = newStmtList()
  let stmts =
    if body.kind == nnkStmtList: body
    else: newStmtList(body)
  for s in stmts:
    if s.kind == nnkDiscardStmt or s.kind == nnkCommentStmt:
      continue
    let call = rewritePredicate(qSym, s)
    result.add(newTree(nnkDiscardStmt, call))

proc rewriteOrderBy(qSym: NimNode; body: NimNode): NimNode =
  ## `orderBy: name asc` / `orderBy: name desc` / `orderBy: name`
  ## Multiple orderings allowed via stmt list.
  result = newStmtList()
  let stmts =
    if body.kind == nnkStmtList: body
    else: newStmtList(body)
  for s in stmts:
    var fieldNode: NimNode = nil
    var asc = true
    case s.kind
    of nnkIdent, nnkSym, nnkDotExpr:
      fieldNode = s
    of nnkCommand, nnkCall:
      if s.len == 2:
        fieldNode = s[0]
        let dir = ($s[1]).toLowerAscii
        if dir == "asc": asc = true
        elif dir == "desc": asc = false
        else: error("query: orderBy direction must be `asc` or `desc`", s[1])
      else:
        error("query: orderBy entry must be `field [asc|desc]`", s)
    else:
      error("query: orderBy entry must be `field [asc|desc]`", s)
    result.add(newTree(nnkDiscardStmt,
      newCall(bindSym"orderByField", qSym,
              newLit(fieldExprToString(fieldNode)),
              newLit(asc))))

# ---- Top-level macro ----

macro query*(db: glendb.GlenDB; collection: static[string]; body: untyped): seq[(string, Value)] =
  ## Block DSL over the Glen query builder. Supported sections:
  ##   * `where:`        — predicates (==, !=, <, <=, >, >=, in, .contains)
  ##   * `orderBy:`      — `field asc|desc`, multiple lines OK
  ##   * `limit: N`      — page size
  ##   * `after: cursor` — opaque cursor from `nextCursor(rows)`
  ##
  ## Returns the materialised result of `q.run()`. For streaming use
  ## `queryBuilder` (returns a `var GlenQuery` you can pass to
  ## `runStream`).
  let qSym = genSym(nskVar, "q")
  var pre = newStmtList()
  pre.add(newVarStmt(qSym, newCall(bindSym"query", db, newLit(collection))))

  let stmts =
    if body.kind == nnkStmtList: body
    else: newStmtList(body)

  for s in stmts:
    case s.kind
    of nnkCommentStmt: continue
    of nnkCall, nnkCommand:
      let label = sectionLabel(s)
      let bodyArg = sectionBody(s)
      case label
      of "where":
        pre.add(rewriteWhereBlock(qSym, bodyArg))
      of "orderby":
        pre.add(rewriteOrderBy(qSym, bodyArg))
      of "limit":
        pre.add(newTree(nnkDiscardStmt,
          newCall(bindSym"limitN", qSym, bodyArg)))
      of "after":
        pre.add(newTree(nnkDiscardStmt,
          newCall(bindSym"afterCursor", qSym, bodyArg)))
      else:
        error("query: unknown section `" & label & "`. Expected one of: where, orderBy, limit, after", s)
    else:
      error("query: top-level entries must be sections (e.g. `where:` block)", s)

  pre.add(newCall(bindSym"run", qSym))
  result = newBlockStmt(pre)

macro queryBuilder*(db: glendb.GlenDB; collection: static[string]; body: untyped): glendb.GlenQuery =
  ## Same DSL as `query`, but returns the unrun builder so callers can
  ## `runStream` it or extend it further.
  let qSym = genSym(nskVar, "q")
  var pre = newStmtList()
  pre.add(newVarStmt(qSym, newCall(bindSym"query", db, newLit(collection))))

  let stmts =
    if body.kind == nnkStmtList: body
    else: newStmtList(body)

  for s in stmts:
    case s.kind
    of nnkCommentStmt: continue
    of nnkCall, nnkCommand:
      let label = sectionLabel(s)
      let bodyArg = sectionBody(s)
      case label
      of "where":   pre.add(rewriteWhereBlock(qSym, bodyArg))
      of "orderby": pre.add(rewriteOrderBy(qSym, bodyArg))
      of "limit":   pre.add(newTree(nnkDiscardStmt, newCall(bindSym"limitN", qSym, bodyArg)))
      of "after":   pre.add(newTree(nnkDiscardStmt, newCall(bindSym"afterCursor", qSym, bodyArg)))
      else: error("queryBuilder: unknown section `" & label & "`", s)
    else:
      error("queryBuilder: top-level entries must be sections", s)
  pre.add(qSym)
  result = newBlockStmt(pre)

# ---- Geo / vector helpers ----
#
# Index-aware searches don't fit neatly inside the `where:` algebra (they
# depend on a named index, not a field comparison), so they're exposed as
# direct procs the user calls instead. Keeps the DSL small and the dispatch
# unambiguous.

proc near*(db: glendb.GlenDB; collection, indexName: string;
           lon, lat, radiusMeters: float64;
           limit = 0): seq[(string, float64, Value)] {.inline.} =
  ## Documents within `radiusMeters` of (lon, lat) using haversine.
  ## Wraps `findWithinRadius`.
  db.findWithinRadius(collection, indexName, lon, lat, radiusMeters, limit)

proc nearest*(db: glendb.GlenDB; collection, indexName: string;
              lon, lat: float64; k: int;
              metric = gmGeographic): seq[(string, float64, Value)] {.inline.} =
  ## K-nearest neighbours by haversine (default) or planar Euclidean.
  ## Wraps `findNearest`.
  db.findNearest(collection, indexName, lon, lat, k, metric)

proc inBBox*(db: glendb.GlenDB; collection, indexName: string;
             minLon, minLat, maxLon, maxLat: float64;
             limit = 0): seq[(string, Value)] {.inline.} =
  ## Documents whose indexed point falls inside the bbox. Wraps `findInBBox`.
  db.findInBBox(collection, indexName, minLon, minLat, maxLon, maxLat, limit)

proc nearestVector*(db: glendb.GlenDB; collection, indexName: string;
                    query: openArray[float32]; k: int): seq[(string, float32)] {.inline.} =
  ## Approximate k-NN against an HNSW vector index. Wraps `findNearestVector`.
  db.findNearestVector(collection, indexName, query, k)

# ---- liveQuery — reactive query block --------------------------------------

proc rewritePredicateToConstr(stmt: NimNode): NimNode =
  ## Map a single predicate statement to a `QueryPredicate(...)` literal,
  ## suitable for accumulating into a `seq[QueryPredicate]` for a live
  ## query. Mirrors `rewritePredicate` but emits a constructor instead of
  ## a builder method call.
  let predSym = bindSym"QueryPredicate"
  template mkPred(opSym: NimNode; fieldStr: string; valExpr: NimNode): NimNode =
    newTree(nnkObjConstr, predSym,
      newTree(nnkExprColonExpr, ident"op", opSym),
      newTree(nnkExprColonExpr, ident"field", newLit(fieldStr)),
      newTree(nnkExprColonExpr, ident"value", valExpr))
  case stmt.kind
  of nnkInfix:
    let op  = $stmt[0]
    let lhs = stmt[1]
    let rhs = stmt[2]
    let field = fieldExprToString(lhs)
    let toV = newCall(bindSym"toValue", rhs)
    case op
    of "==": result = mkPred(bindSym"qoEq",  field, toV)
    of "!=": result = mkPred(bindSym"qoNe",  field, toV)
    of "<":  result = mkPred(bindSym"qoLt",  field, toV)
    of "<=": result = mkPred(bindSym"qoLte", field, toV)
    of ">":  result = mkPred(bindSym"qoGt",  field, toV)
    of ">=": result = mkPred(bindSym"qoGte", field, toV)
    of "in":
      if rhs.kind != nnkBracket:
        error("liveQuery: `in` expects an array literal on the right", rhs)
      var arr = newTree(nnkBracket)
      for el in rhs:
        arr.add(newCall(bindSym"toValue", el))
      let valuesExpr = newTree(nnkPrefix, ident"@", arr)
      result = newTree(nnkObjConstr, predSym,
        newTree(nnkExprColonExpr, ident"op", bindSym"qoIn"),
        newTree(nnkExprColonExpr, ident"field", newLit(field)),
        newTree(nnkExprColonExpr, ident"values", valuesExpr))
    else:
      error("liveQuery: unsupported predicate operator `" & op & "`", stmt)
  of nnkCall:
    if stmt.len == 2 and stmt[0].kind == nnkDotExpr and $stmt[0][1] == "contains":
      let field = fieldExprToString(stmt[0][0])
      let needle = stmt[1]
      result = mkPred(bindSym"qoContains", field,
                      newCall(bindSym"VString", needle))
    else:
      error("liveQuery: unsupported predicate call (only .contains supported)", stmt)
  else:
    error("liveQuery: unsupported predicate shape: " & $stmt.kind, stmt)

macro liveQuery*(db: glendb.GlenDB; collection: static[string]; body: untyped): LiveQuery =
  ## Block DSL for a reactive query. Same predicate algebra as `query:`
  ## (`==`, `!=`, `<`, `<=`, `>`, `>=`, `in`, `.contains`) under a
  ## `where:` section. `orderBy` / `limit` are not supported here — see
  ## the `LiveQuery` doc.
  let predsSym = genSym(nskVar, "preds")
  let predSeqType = newTree(nnkBracketExpr, bindSym"seq", bindSym"QueryPredicate")
  var pre = newStmtList()
  pre.add(newVarStmt(predsSym,
    newCall(newTree(nnkBracketExpr, bindSym"newSeqOfCap", bindSym"QueryPredicate"),
            newLit(4))))

  if body.kind != nnkStmtList:
    error("liveQuery: expected a block body", body)

  for s in body:
    case s.kind
    of nnkCommentStmt: continue
    of nnkCall, nnkCommand:
      let label = sectionLabel(s)
      let bodyArg = sectionBody(s)
      case label
      of "where":
        let stmts =
          if bodyArg.kind == nnkStmtList: bodyArg
          else: newStmtList(bodyArg)
        for p in stmts:
          if p.kind == nnkCommentStmt: continue
          pre.add(newCall(newDotExpr(predsSym, ident"add"),
                          rewritePredicateToConstr(p)))
      of "orderby", "limit", "after":
        error("liveQuery: `" & label & ":` is not supported on live queries — see the LiveQuery docs", s)
      else:
        error("liveQuery: unknown section `" & label & "`. Only `where:` is supported.", s)
    else:
      error("liveQuery: top-level entries must be sections", s)

  discard predSeqType
  pre.add(newCall(bindSym"newLiveQuery", db, newLit(collection), predsSym))
  result = newBlockStmt(pre)
