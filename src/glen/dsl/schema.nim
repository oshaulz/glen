## schema — combines a Zod-style validator with index declarations so
## every collection is described in one place.
##
##   schema users:
##     fields:
##       name:  zString().trim().minLen(2).maxLen(64)
##       age:   zInt().gte(0).lte(150)
##       email: zString().trim().minLen(3)
##       role:  zEnum(["admin", "member"]).default("member")
##     indexes:
##       byEmail:  equality "email"
##       byAge:    range    "age"
##       byLoc:    geo      "addr.lon", "addr.lat"
##
## Generates:
##   * `let usersSchema*: Schema[...] = zobject: ...`
##   * `const usersCollection* = "users"`
##   * `proc registerUsersSchema*(db: GlenDB)` — creates indexes (idempotent
##     re-creation is the existing semantics; the manifest dedupes on disk)
##   * `proc validateUsers*(v: Value): ValidationResult[...]` — a thin alias
##     over `parse`
##
## The `equality` / `range` index kinds use the same underlying engine and
## differ only in whether the planner treats them as range-scannable. Glen's
## current index engine is rangeable for any single-field index, so the
## distinction is currently informational; we keep both keywords so callers
## can document intent and stay forward-compatible.

import std/[macros, strutils]
import glen/types
import glen/db as glendb
import glen/validators
import glen/vectorindex

# Re-exporting so users importing this DSL don't need to also import
# glen/validators just to write `zString()` etc. inside the fields block.
export validators

# ---- Index spec — captured at compile time, executed at register time ----

type
  GlenIndexKind* = enum
    gikEquality, gikRange, gikGeo, gikPolygon, gikVector

  GlenIndexSpec* = object
    name*: string
    case kind*: GlenIndexKind
    of gikEquality, gikRange:
      fieldPath*: string
    of gikGeo:
      lonField*, latField*: string
    of gikPolygon:
      polygonField*: string
    of gikVector:
      embeddingField*: string
      dim*: int
      metric*: VectorMetric

proc applyIndex*(db: glendb.GlenDB; collection: string; spec: GlenIndexSpec) =
  case spec.kind
  of gikEquality, gikRange:
    db.createIndex(collection, spec.name, spec.fieldPath)
  of gikGeo:
    db.createGeoIndex(collection, spec.name, spec.lonField, spec.latField)
  of gikPolygon:
    db.createPolygonIndex(collection, spec.name, spec.polygonField)
  of gikVector:
    db.createVectorIndex(collection, spec.name, spec.embeddingField,
                         spec.dim, spec.metric)

# ---- Macro implementation ----

proc parseIndexSpec(name: string; rhs: NimNode): NimNode =
  ## Translates a single `name: <kind> <args...>` line into a
  ## `GlenIndexSpec(...)` constructor expression.
  var kindIdent: NimNode
  var args: seq[NimNode]
  case rhs.kind
  of nnkCommand, nnkCall:
    if rhs.len < 2 or rhs[0].kind notin {nnkIdent, nnkSym}:
      error("schema: index entry must be `name: <kind> <args>`", rhs)
    kindIdent = rhs[0]
    for i in 1 ..< rhs.len: args.add(rhs[i])
  of nnkIdent, nnkSym:
    kindIdent = rhs
  else:
    error("schema: index entry must be `name: <kind> <args>`", rhs)

  let kind = ($kindIdent).toLowerAscii
  let nameLit = newLit(name)
  let specSym = bindSym"GlenIndexSpec"
  case kind
  of "equality", "eq":
    if args.len != 1:
      error("schema: `equality` index needs one field path string", rhs)
    result = newTree(nnkObjConstr, specSym,
      newTree(nnkExprColonExpr, ident"kind", bindSym"gikEquality"),
      newTree(nnkExprColonExpr, ident"name", nameLit),
      newTree(nnkExprColonExpr, ident"fieldPath", args[0]))
  of "range":
    if args.len != 1:
      error("schema: `range` index needs one field path string", rhs)
    result = newTree(nnkObjConstr, specSym,
      newTree(nnkExprColonExpr, ident"kind", bindSym"gikRange"),
      newTree(nnkExprColonExpr, ident"name", nameLit),
      newTree(nnkExprColonExpr, ident"fieldPath", args[0]))
  of "geo":
    if args.len != 2:
      error("schema: `geo` index needs (lonField, latField) — two strings", rhs)
    result = newTree(nnkObjConstr, specSym,
      newTree(nnkExprColonExpr, ident"kind", bindSym"gikGeo"),
      newTree(nnkExprColonExpr, ident"name", nameLit),
      newTree(nnkExprColonExpr, ident"lonField", args[0]),
      newTree(nnkExprColonExpr, ident"latField", args[1]))
  of "polygon", "poly":
    if args.len != 1:
      error("schema: `polygon` index needs one field name string", rhs)
    result = newTree(nnkObjConstr, specSym,
      newTree(nnkExprColonExpr, ident"kind", bindSym"gikPolygon"),
      newTree(nnkExprColonExpr, ident"name", nameLit),
      newTree(nnkExprColonExpr, ident"polygonField", args[0]))
  of "vector":
    if args.len < 2 or args.len > 3:
      error("schema: `vector` index needs (embeddingField, dim [, metricExpr])", rhs)
    let metricExpr =
      if args.len == 3: args[2]
      else: bindSym"vmCosine"
    result = newTree(nnkObjConstr, specSym,
      newTree(nnkExprColonExpr, ident"kind", bindSym"gikVector"),
      newTree(nnkExprColonExpr, ident"name", nameLit),
      newTree(nnkExprColonExpr, ident"embeddingField", args[0]),
      newTree(nnkExprColonExpr, ident"dim", args[1]),
      newTree(nnkExprColonExpr, ident"metric", metricExpr))
  else:
    error("schema: unknown index kind `" & kind & "`. Expected one of: equality, range, geo, polygon, vector", kindIdent)

proc cap(s: string): string =
  if s.len == 0: s
  else: toUpperAscii(s[0]) & s[1 ..^ 1]

macro schema*(name: untyped; body: untyped): untyped =
  ## Declare a Glen collection's schema and indexes in one block.
  ##
  ## `name` is an identifier or string literal; the macro generates symbols
  ## prefixed with that name (e.g. `usersSchema`, `registerUsersSchema`,
  ## `validateUsers`).
  if body.kind != nnkStmtList:
    error("schema: expected a block body", body)

  var nameStr = ""
  case name.kind
  of nnkIdent, nnkSym: nameStr = $name
  of nnkStrLit, nnkRStrLit, nnkTripleStrLit: nameStr = name.strVal
  else: error("schema: name must be ident or string literal", name)
  let baseCap = cap(nameStr)

  var fieldsBlock: NimNode = nil
  var indexLines: seq[(string, NimNode)] = @[]

  for s in body:
    if s.kind == nnkCommentStmt: continue
    if s.kind notin {nnkCall, nnkCommand}:
      error("schema: expected `fields:` / `indexes:` sections", s)
    let label = ($s[0]).toLowerAscii
    let sectionBody = s[1]
    case label
    of "fields":
      if sectionBody.kind != nnkStmtList:
        error("schema: `fields:` must be a block", sectionBody)
      fieldsBlock = sectionBody
    of "indexes":
      if sectionBody.kind != nnkStmtList:
        error("schema: `indexes:` must be a block", sectionBody)
      for entry in sectionBody:
        if entry.kind == nnkCommentStmt: continue
        case entry.kind
        of nnkCall:
          # `byEmail: equality "email"` — nnkCall(name, sectionBody[stmtList[expr]])
          if entry.len == 2 and entry[0].kind in {nnkIdent, nnkSym}:
            let idxName = $entry[0]
            var rhs = entry[1]
            # `name: kind args` parses as nnkCall(name, stmtList(<inner>))
            if rhs.kind == nnkStmtList:
              if rhs.len != 1:
                error("schema: index entry must have a single rhs", rhs)
              rhs = rhs[0]
            indexLines.add((idxName, rhs))
          else:
            error("schema: malformed index entry", entry)
        of nnkExprColonExpr:
          if entry.len == 2 and entry[0].kind in {nnkIdent, nnkSym}:
            indexLines.add(($entry[0], entry[1]))
          else:
            error("schema: malformed index entry", entry)
        else:
          error("schema: index entries must be `name: <kind> <args>`", entry)
    else:
      error("schema: unknown section `" & label & "`. Expected `fields:` and/or `indexes:`", s)

  if fieldsBlock.isNil:
    error("schema: `fields:` section is required", body)

  let schemaSym  = ident(nameStr & "Schema")
  let collConst  = ident(nameStr & "Collection")
  let registerFn = ident("register" & baseCap & "Schema")
  let validateFn = ident("validate" & baseCap)

  # let usersSchema* = zobject: ...fields...
  let schemaLet = newTree(nnkLetSection,
    newTree(nnkIdentDefs,
      postfix(schemaSym, "*"),
      newEmptyNode(),
      newCall(bindSym"zobject", fieldsBlock)))

  # const usersCollection* = "users"
  let constDecl = newTree(nnkConstSection,
    newTree(nnkConstDef,
      postfix(collConst, "*"),
      newEmptyNode(),
      newLit(nameStr)))

  # proc registerUsersSchema*(db: GlenDB) =
  #   applyIndex(db, "users", GlenIndexSpec(...))
  let dbParam = ident"db"
  var registerBody = newStmtList()
  for (idxName, rhs) in indexLines:
    let specExpr = parseIndexSpec(idxName, rhs)
    registerBody.add(newCall(bindSym"applyIndex",
      dbParam, newLit(nameStr), specExpr))
  if registerBody.len == 0:
    registerBody.add(newTree(nnkDiscardStmt, newEmptyNode()))

  let glenDbType = bindSym"GlenDB"
  let registerProc = newProc(
    name = postfix(registerFn, "*"),
    params = [newEmptyNode(),
              newIdentDefs(dbParam, glenDbType)],
    body = registerBody)

  # proc validateUsers*(v: Value): auto = usersSchema.parse(v)
  let vParam = ident"v"
  let validateBody = newCall(bindSym"parse", schemaSym, vParam)
  let validateProc = newProc(
    name = postfix(validateFn, "*"),
    params = [ident"auto",
              newIdentDefs(vParam, bindSym"Value")],
    body = validateBody)

  result = newStmtList(constDecl, schemaLet, registerProc, validateProc)
