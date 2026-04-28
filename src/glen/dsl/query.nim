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
import glen/index
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

# ---- Projection helpers (used by `select:`) -------------------------------

proc projectFields*(rows: seq[(string, Value)];
                    fields: openArray[string]): seq[(string, Value)] =
  ## For each (id, doc) pair, build a new VObject containing only the
  ## listed dotted-field paths. Missing fields are skipped (not stored as
  ## VNull); preserve them by listing their parent path or none at all.
  result = newSeqOfCap[(string, Value)](rows.len)
  for (id, doc) in rows:
    var projected = VObject()
    for f in fields:
      let parts = fieldPath(f)
      let v = extractField(doc, parts)
      if not v.isNil:
        # For nested paths we flatten the leaf onto the projection using
        # the deepest segment as the key (so `addr.city` ends up under
        # `city`). To preserve the full nested shape, just project the
        # parent (`addr`).
        projected[parts[^1]] = v
    result.add((id, projected))

# ---- Reduction helpers (used by `count` / `first` / `exists`) -------------

proc countQuery*(q: GlenQuery): int {.inline.} =
  q.run().len

proc firstQuery*(q: GlenQuery): tuple[ok: bool; id: string; value: Value] =
  let rows = q.run()
  if rows.len == 0:
    result = (false, "", nil)
  else:
    result = (true, rows[0][0], rows[0][1])

proc existsQuery*(q: GlenQuery): bool {.inline.} =
  q.run().len > 0

# ---- Page pagination helper ----------------------------------------------

type
  Page*[T] = object
    ## Bundle of `rows`, the opaque cursor that points after the last row,
    ## and a `hasMore` hint. `hasMore` is true when the underlying query
    ## had `limit: N` set and exactly N rows came back — meaning there
    ## might be more on the next page. With no limit, `hasMore` is false.
    rows*: seq[T]
    cursor*: string
    hasMore*: bool

proc pageOf*(rows: seq[(string, Value)];
             limitN: int): Page[(string, Value)] =
  ## Wrap a result set as a `Page`. Returns the same rows, the cursor
  ## from `nextCursor(rows)`, and `hasMore = (limitN > 0 and rows.len >= limitN)`.
  result.rows = rows
  result.cursor = nextCursor(rows)
  result.hasMore = limitN > 0 and rows.len >= limitN

# ---- Geo / vector prefilter -----------------------------------------------
#
# `near:` / `similar:` declared at the query top level run an index lookup
# first, then post-filter the candidate set with `where:` predicates. This
# avoids wedging spatial syntax into the predicate algebra (where it
# doesn't belong: it depends on a named index, not a field comparison).

proc prefilterNear*(db: glendb.GlenDB; collection, indexName: string;
                    lon, lat, radiusMeters: float64;
                    preds: seq[QueryPredicate];
                    limit = 0): seq[(string, Value)] =
  ## Documents within `radiusMeters` (haversine) of (lon, lat) that also
  ## satisfy every predicate in `preds`. Result is sorted by distance
  ## ascending — same order the underlying R-tree returns.
  result = @[]
  for (id, _, doc) in db.findWithinRadius(collection, indexName,
                                          lon, lat, radiusMeters, 0):
    if doc.isNil: continue
    if evalAll(preds, doc):
      result.add((id, doc))
      if limit > 0 and result.len >= limit: break

proc prefilterSimilar*(db: glendb.GlenDB; collection, indexName: string;
                       query: openArray[float32]; k: int;
                       preds: seq[QueryPredicate]): seq[(string, Value)] =
  ## Documents whose embedding is among the k-nearest for `query` AND
  ## satisfy every predicate in `preds`. Order: by approximate distance
  ## (closest first) as the HNSW index returns it.
  result = @[]
  let pairs = db.findNearestVector(collection, indexName, query, k)
  for (id, _) in pairs:
    let doc = db.get(collection, id)
    if doc.isNil: continue
    if evalAll(preds, doc):
      result.add((id, doc))

proc prefilterSimilarWithin*(db: glendb.GlenDB; collection, indexName: string;
                             query: openArray[float32];
                             maxDistance: float32;
                             preds: seq[QueryPredicate];
                             maxResults = 0): seq[(string, Value)] =
  ## Threshold variant of `prefilterSimilar`: every doc whose embedding
  ## is within `maxDistance` of `query` AND satisfies the predicates.
  result = @[]
  let pairs = db.findNearestVectorWithin(collection, indexName, query,
                                         maxDistance, maxResults)
  for (id, _) in pairs:
    let doc = db.get(collection, id)
    if doc.isNil: continue
    if evalAll(preds, doc):
      result.add((id, doc))

# ---- query macro -----------------------------------------------------------

proc parseSelectFields(body: NimNode): seq[string] =
  ## `select: name, age, addr.city` arrives as a stmtList containing
  ## either a single comma-tuple or one ident per line. Accept both shapes.
  result = @[]
  let stmts =
    if body.kind == nnkStmtList: body
    else: newStmtList(body)
  for s in stmts:
    case s.kind
    of nnkCommentStmt: continue
    of nnkIdent, nnkSym, nnkDotExpr:
      result.add(fieldExprToString(s))
    of nnkTupleConstr, nnkPar, nnkCommand, nnkCall:
      # `select: a, b` parses as nnkCommand or nnkPar of idents in some
      # Nim versions. Iterate any direct children.
      for child in s:
        if child.kind in {nnkIdent, nnkSym, nnkDotExpr}:
          result.add(fieldExprToString(child))
    of nnkInfix:
      # `a, b` may show up as nnkInfix(`,`, a, b) — flatten recursively.
      proc walk(n: NimNode; acc: var seq[string]) =
        if n.kind == nnkInfix and $n[0] == ",":
          walk(n[1], acc); walk(n[2], acc)
        else:
          acc.add(fieldExprToString(n))
      walk(s, result)
    else:
      error("query: select: expected field name(s), got " & $s.kind, s)
  if result.len == 0:
    error("query: select: needs at least one field name", body)

proc parseNearArgs(node: NimNode): tuple[idx, lon, lat, radius: NimNode] =
  ## `near: "byLoc", lon, lat, radius` arrives as nnkStmtList of one
  ## tuple-shaped command, or as a flat call with positional args.
  var args: seq[NimNode] = @[]
  proc collect(n: NimNode) =
    case n.kind
    of nnkStmtList:
      for c in n: collect(c)
    of nnkPar, nnkTupleConstr, nnkCommand, nnkCall:
      for c in n: args.add(c)
    of nnkInfix:
      if $n[0] == ",":
        collect(n[1]); collect(n[2])
      else:
        args.add(n)
    else:
      args.add(n)
  collect(node)
  if args.len != 4:
    error("query: near: expected (indexName, lon, lat, radiusMeters)", node)
  result = (args[0], args[1], args[2], args[3])

proc parseSimilarArgs(node: NimNode): tuple[idx, vec, k, threshold: NimNode] =
  ## `similar: "byEmbed", queryVec, 10`           — top-k
  ## `similar: ("byEmbed", queryVec, 10)`         — same, paren tuple
  ## `similar: ("byEmbed", queryVec, 10, 0.2)`    — top-k AND threshold
  ## `similar: ("byEmbed", queryVec, threshold = 0.2)` — threshold-only
  ##
  ## Returns (idx, vec, k, threshold) where unused slots are nil.
  var args: seq[NimNode] = @[]
  var threshold: NimNode = nil
  proc collect(n: NimNode) =
    case n.kind
    of nnkStmtList:
      for c in n: collect(c)
    of nnkPar, nnkTupleConstr, nnkCommand, nnkCall:
      for c in n: collect(c)
    of nnkInfix:
      if $n[0] == ",":
        collect(n[1]); collect(n[2])
      else:
        args.add(n)
    of nnkExprEqExpr:
      if n[0].kind in {nnkIdent, nnkSym} and $n[0] == "threshold":
        threshold = n[1]
      else:
        error("query: similar: only `threshold = X` is supported as a named arg", n)
    else:
      args.add(n)
  collect(node)
  case args.len
  of 2:
    if threshold.isNil:
      error("query: similar: needs at least (indexName, queryVec, k) — got 2 positional args without `threshold`", node)
    result = (args[0], args[1], newLit(0), threshold)   # threshold-only
  of 3:
    result = (args[0], args[1], args[2], threshold)     # top-k (+ optional threshold)
  of 4:
    if not threshold.isNil:
      error("query: similar: cannot mix positional threshold with `threshold = X`", node)
    result = (args[0], args[1], args[2], args[3])        # top-k AND threshold
  else:
    error("query: similar: expected (indexName, queryVec, k [, threshold])", node)

proc rewritePredicateToConstr(stmt: NimNode): NimNode =
  ## Map a single predicate statement to a `QueryPredicate(...)` literal,
  ## suitable for accumulating into a `seq[QueryPredicate]` for a live or
  ## prefiltered query. Mirrors `rewritePredicate` but emits a constructor
  ## instead of a builder method call.
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
        error("query: `in` expects an array literal on the right", rhs)
      var arr = newTree(nnkBracket)
      for el in rhs:
        arr.add(newCall(bindSym"toValue", el))
      let valuesExpr = newTree(nnkPrefix, ident"@", arr)
      result = newTree(nnkObjConstr, predSym,
        newTree(nnkExprColonExpr, ident"op", bindSym"qoIn"),
        newTree(nnkExprColonExpr, ident"field", newLit(field)),
        newTree(nnkExprColonExpr, ident"values", valuesExpr))
    else:
      error("query: unsupported predicate operator `" & op & "`", stmt)
  of nnkCall:
    if stmt.len == 2 and stmt[0].kind == nnkDotExpr and $stmt[0][1] == "contains":
      let field = fieldExprToString(stmt[0][0])
      let needle = stmt[1]
      result = mkPred(bindSym"qoContains", field,
                      newCall(bindSym"VString", needle))
    else:
      error("query: unsupported predicate call (only .contains supported)", stmt)
  else:
    error("query: unsupported predicate shape: " & $stmt.kind, stmt)

# Build a `seq[QueryPredicate]` literal from a `where:` block — used by
# the geo/vector prefilter path, which post-filters on predicates.
proc buildPredicateSeq(body: NimNode): NimNode =
  let predsSym = genSym(nskVar, "preds")
  result = newStmtList()
  result.add(newVarStmt(predsSym,
    newCall(newTree(nnkBracketExpr, bindSym"newSeqOfCap",
                    bindSym"QueryPredicate"), newLit(4))))
  let stmts =
    if body.kind == nnkStmtList: body
    else: newStmtList(body)
  for s in stmts:
    if s.kind == nnkCommentStmt: continue
    result.add(newCall(newDotExpr(predsSym, ident"add"),
                       rewritePredicateToConstr(s)))
  result.add(predsSym)
  result = newBlockStmt(result)

macro query*(db: glendb.GlenDB; collection: static[string]; body: untyped): untyped =
  ## Block DSL over the Glen query builder.
  ##
  ## Sections (any may appear at most once):
  ##   * `where:`        — predicates (==, !=, <, <=, >, >=, in, .contains)
  ##   * `orderBy:`      — `field asc|desc`, multiple lines OK
  ##   * `limit: N`      — page size
  ##   * `after: cursor` — opaque cursor from `nextCursor(rows)`
  ##   * `select: a, b`  — projection: each result row keeps only listed fields
  ##   * `near: "idx", lon, lat, radius` — geo prefilter (haversine metres)
  ##   * `similar: "idx", queryVec, k`   — vector kNN prefilter
  ##   * `count: ()` / `first: ()` / `exists: ()` — terminal reduction
  ##
  ## Default return is `seq[(string, Value)]`. `count:` returns `int`,
  ## `first:` returns `(ok: bool, id: string, value: Value)`, `exists:`
  ## returns `bool`.
  ##
  ## `near:` / `similar:` and `select:` are mutually compatible. Reductions
  ## (`count`/`first`/`exists`) are exclusive — at most one per query.

  if body.kind != nnkStmtList:
    error("query: expected a block body", body)

  # Collect sections by name so we can reorder properly.
  var whereBody, orderByBody, limitArg, afterArg, selectBody,
      nearBody, similarBody: NimNode
  whereBody = nil; orderByBody = nil; limitArg = nil; afterArg = nil
  selectBody = nil; nearBody = nil; similarBody = nil
  var reduction = ""    # "" / "count" / "first" / "exists"
  for s in body:
    if s.kind == nnkCommentStmt: continue
    if s.kind notin {nnkCall, nnkCommand}:
      error("query: top-level entries must be sections (e.g. `where:` block)", s)
    let label = sectionLabel(s)
    let bodyArg = sectionBody(s)
    case label
    of "where":   whereBody = bodyArg
    of "orderby": orderByBody = bodyArg
    of "limit":   limitArg = bodyArg
    of "after":   afterArg = bodyArg
    of "select":  selectBody = bodyArg
    of "near":    nearBody = bodyArg
    of "similar": similarBody = bodyArg
    of "count", "first", "exists", "page":
      if reduction.len > 0:
        error("query: only one reduction (count/first/exists/page) per query", s)
      reduction = label
    else:
      error("query: unknown section `" & label & "`", s)

  if not nearBody.isNil and not similarBody.isNil:
    error("query: `near:` and `similar:` are mutually exclusive", nearBody)

  # ---- Geo / vector prefilter path -------------------------------------
  if not nearBody.isNil or not similarBody.isNil:
    if not orderByBody.isNil:
      error("query: orderBy: is incompatible with near:/similar: (results are sorted by distance)", orderByBody)
    if not afterArg.isNil:
      error("query: after: cursors aren't supported with near:/similar:", afterArg)
    let predsExpr =
      if whereBody.isNil:
        newCall(newTree(nnkBracketExpr, bindSym"newSeqOfCap",
                        bindSym"QueryPredicate"), newLit(0))
      else:
        buildPredicateSeq(whereBody)
    var rowsExpr: NimNode
    if not nearBody.isNil:
      let (idx, lon, lat, rad) = parseNearArgs(nearBody)
      let limitExpr = if limitArg.isNil: newLit(0) else: limitArg
      rowsExpr = newCall(bindSym"prefilterNear",
        db, newLit(collection), idx, lon, lat, rad, predsExpr, limitExpr)
    else:
      let (idx, vec, k, threshold) = parseSimilarArgs(similarBody)
      let maxResultsExpr = if limitArg.isNil: newLit(0) else: limitArg
      if threshold.isNil:
        # Pure top-k.
        rowsExpr = newCall(bindSym"prefilterSimilar",
          db, newLit(collection), idx, vec, k, predsExpr)
        if not limitArg.isNil:
          # Trim k post-hoc (rare; user usually expresses bound via `k`).
          rowsExpr = quote do:
            (
              let r = `rowsExpr`
              if `limitArg` > 0 and r.len > `limitArg`: r[0 ..< `limitArg`] else: r
            )
      else:
        # Threshold path: every match within `threshold`. If a positional
        # k > 0 was also provided, treat it as the cap on results. The
        # `threshold` literal is converted to float32 here so callers can
        # pass an int / float / float32 interchangeably.
        let thresholdExpr = newCall(ident"float32", threshold)
        let cap =
          if k.kind in nnkIntLit..nnkUInt64Lit and k.intVal == 0:
            maxResultsExpr
          else: k
        rowsExpr = newCall(bindSym"prefilterSimilarWithin",
          db, newLit(collection), idx, vec, thresholdExpr, predsExpr, cap)

    var pre = newStmtList()
    let rowsSym = genSym(nskLet, "rows")
    pre.add(newLetStmt(rowsSym, rowsExpr))

    var resultExpr: NimNode = rowsSym
    if not selectBody.isNil:
      let fieldsLit = parseSelectFields(selectBody)
      var arr = newTree(nnkBracket)
      for f in fieldsLit: arr.add(newLit(f))
      resultExpr = newCall(bindSym"projectFields", rowsSym,
                           newTree(nnkPrefix, ident"@", arr))
    case reduction
    of "count":  pre.add(newDotExpr(resultExpr, ident"len"))
    of "first":
      let firstSym = genSym(nskLet, "rows")
      pre.add(quote do:
        block:
          let `firstSym` = `resultExpr`
          if `firstSym`.len == 0: (ok: false, id: "", value: Value(nil))
          else: (ok: true, id: `firstSym`[0][0], value: `firstSym`[0][1])
      )
    of "exists": pre.add(infix(newDotExpr(resultExpr, ident"len"), ">", newLit(0)))
    of "page":
      let limitForPage = if limitArg.isNil: newLit(0) else: limitArg
      pre.add(newCall(bindSym"pageOf", resultExpr, limitForPage))
    else:        pre.add(resultExpr)

    result = newBlockStmt(pre)
    return

  # ---- Standard builder path ------------------------------------------
  let qSym = genSym(nskVar, "q")
  var pre = newStmtList()
  pre.add(newVarStmt(qSym, newCall(bindSym"query", db, newLit(collection))))
  if not whereBody.isNil:
    pre.add(rewriteWhereBlock(qSym, whereBody))
  if not orderByBody.isNil:
    pre.add(rewriteOrderBy(qSym, orderByBody))
  if not limitArg.isNil:
    pre.add(newTree(nnkDiscardStmt, newCall(bindSym"limitN", qSym, limitArg)))
  if not afterArg.isNil:
    pre.add(newTree(nnkDiscardStmt, newCall(bindSym"afterCursor", qSym, afterArg)))

  case reduction
  of "count":
    pre.add(newCall(bindSym"countQuery", qSym))
  of "first":
    pre.add(newCall(bindSym"firstQuery", qSym))
  of "exists":
    pre.add(newCall(bindSym"existsQuery", qSym))
  of "page":
    let runCall = newCall(bindSym"run", qSym)
    let projected =
      if selectBody.isNil: runCall
      else:
        let fieldsLit = parseSelectFields(selectBody)
        var arr = newTree(nnkBracket)
        for f in fieldsLit: arr.add(newLit(f))
        newCall(bindSym"projectFields", runCall,
                newTree(nnkPrefix, ident"@", arr))
    let limitForPage = if limitArg.isNil: newLit(0) else: limitArg
    pre.add(newCall(bindSym"pageOf", projected, limitForPage))
  else:
    let runCall = newCall(bindSym"run", qSym)
    if not selectBody.isNil:
      let fieldsLit = parseSelectFields(selectBody)
      var arr = newTree(nnkBracket)
      for f in fieldsLit: arr.add(newLit(f))
      pre.add(newCall(bindSym"projectFields", runCall,
                      newTree(nnkPrefix, ident"@", arr)))
    else:
      pre.add(runCall)

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

# ---- liveCount / liveExists — reactive aggregations -----------------------

proc buildLivePredsAndCall(db, collection, body: NimNode;
                           target: string;
                           ctorSym: NimNode): NimNode =
  ## Shared predicate-collection logic for liveCount / liveExists. Both
  ## take the same `where:`-only block as `liveQuery` and just call a
  ## different constructor.
  let predsSym = genSym(nskVar, "preds")
  var pre = newStmtList()
  pre.add(newVarStmt(predsSym,
    newCall(newTree(nnkBracketExpr, bindSym"newSeqOfCap", bindSym"QueryPredicate"),
            newLit(4))))
  if body.kind != nnkStmtList:
    error(target & ": expected a block body", body)
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
      else:
        error(target & ": only `where:` is supported", s)
    else:
      error(target & ": top-level entries must be sections", s)
  pre.add(newCall(ctorSym, db, collection, predsSym))
  result = newBlockStmt(pre)

macro liveCount*(db: glendb.GlenDB; collection: static[string]; body: untyped): LiveCount =
  ## Reactive count: `lc.value` returns the current count, `lc.onChange`
  ## fires whenever the count changes. Same predicate grammar as `liveQuery`.
  buildLivePredsAndCall(db, newLit(collection), body, "liveCount", bindSym"newLiveCount")

macro liveExists*(db: glendb.GlenDB; collection: static[string]; body: untyped): LiveExists =
  ## Reactive bool: `le.value` returns whether any matching row exists,
  ## `le.onChange` fires only on transition between true ↔ false.
  buildLivePredsAndCall(db, newLit(collection), body, "liveExists", bindSym"newLiveExists")
