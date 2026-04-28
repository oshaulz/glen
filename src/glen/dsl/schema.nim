## schema — typed-record schema, validator, indexes, and migrations in one
## block.
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
## For the example above the macro generates:
##
##   * `type Users* = object`
##       `name*: string`, `age*: int64`, `email*: string`, `role*: string`
##     (each field's static type is extracted from its schema expression
##     via `schemaValueType` — `zString()` → `string`, `zInt()` → `int64`,
##     `zArray(zInt())` → `seq[int64]`, etc.)
##   * `const usersCollection* = "users"`
##   * `let usersSchema*: Schema[Users]` — a parser closure that fills in
##     a `var Users` field by field
##   * `proc toValue*(u: Users): Value` — Glen Value codec
##   * `proc parseUsers*(v: Value): ValidationResult[Users]`
##   * `proc validateUsers*(v: Value): ValidationResult[Users]` (alias)
##   * `proc registerUsersSchema*(db: GlenDB)` — creates declared indexes
##   * `proc getUsers*(db: GlenDB; id: string): (bool, Users)` — typed read
##   * `proc putUsers*(db: GlenDB; id: string; u: Users)` — typed write
##
## Untyped CRUD via `db.put` / `db.get` / `Collection` proxy still works;
## the typed accessors are convenience wrappers.
##
## See `migrations:` and `version:` sections below for schema evolution.

import std/[macros, strutils, options, tables]
import glen/types
import glen/db as glendb
import glen/util
import glen/validators
import glen/vectorindex

export util.nowMillis

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

proc checkStrictKeys*(input: Value; allowed: openArray[string];
                      path: seq[string];
                      issues: var seq[ValidationIssue]): bool =
  ## Strict-mode helper: append an issue for every object key that isn't in
  ## `allowed`. Returns `true` if any unknown key was found. No-ops on
  ## non-object inputs (the regular parser will already reject those).
  if input.isNil or input.kind != vkObject: return false
  result = false
  for key in input.obj.keys:
    var ok = false
    for name in allowed:
      if name == key: ok = true; break
    if not ok:
      issues.add(ValidationIssue(
        path: pushPath(path, key),
        expected: "no field `" & key & "` (schema is strict)",
        actual: "extra field",
        message: "Unknown field `" & key & "` (schema is strict)"))
      result = true

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

# ---- Field encode helpers used by generated `toValue` ----

proc encodeField*(s: string): Value {.inline.} = VString(s)
proc encodeField*(b: bool): Value {.inline.} = VBool(b)
proc encodeField*[T: SomeInteger](i: T): Value {.inline.} = VInt(int64(i))
proc encodeField*[T: SomeFloat](f: T): Value {.inline.} = VFloat(float64(f))
proc encodeField*(v: Value): Value {.inline.} =
  if v.isNil: VNull() else: v

proc encodeField*[T](items: seq[T]): Value =
  var arr: seq[Value] = newSeqOfCap[Value](items.len)
  for it in items: arr.add(encodeField(it))
  VArray(arr)

proc encodeField*[T](opt: Option[T]): Value =
  if opt.isNone: VNull() else: encodeField(opt.get)

# ---- Macro implementation ----

proc dottedName(n: NimNode): string =
  case n.kind
  of nnkIdent, nnkSym: $n
  of nnkDotExpr: dottedName(n[0]) & "." & dottedName(n[1])
  else:
    error("schema: expected ident or dotted ident, got " & $n.kind, n); ""

proc validateFieldPrefix(path: string; declared: openArray[string];
                         arg: NimNode) =
  ## Validate that the top-level segment of a dotted path matches one of
  ## the declared field names. Catches typos in ident / dotted-ident form
  ## at compile time. (String literals stay opaque — same lax behaviour
  ## the underlying engine has always had.)
  if declared.len == 0: return
  let head =
    block:
      let dot = path.find('.')
      if dot < 0: path else: path[0 ..< dot]
  for d in declared:
    if d == head: return
  error("schema: index references undeclared field `" & head &
    "` (declared fields: " & declared.join(", ") & ")", arg)

proc fieldArgToString(arg: NimNode; declared: openArray[string]): NimNode =
  ## Convert an index `equality`/`range` field-arg into a string literal
  ## NimNode. Accepts:
  ##   * `"email"`              — string literal (current syntax, lax)
  ##   * `email`                — bare ident → "email" (validated)
  ##   * `addr.city`            — dotted ident → "addr.city" (prefix validated)
  ##   * `(name, age)`          — tuple → "name,age" (each prefix validated)
  ##   * `(addr.city, status)`  — tuple of dotted idents
  case arg.kind
  of nnkStrLit, nnkRStrLit, nnkTripleStrLit:
    arg
  of nnkIdent, nnkSym:
    let s = $arg
    validateFieldPrefix(s, declared, arg)
    newLit(s)
  of nnkDotExpr:
    let s = dottedName(arg)
    validateFieldPrefix(s, declared, arg)
    newLit(s)
  of nnkPar, nnkTupleConstr:
    var parts: seq[string] = @[]
    for child in arg:
      let s = fieldArgToString(child, declared)
      if s.kind notin {nnkStrLit, nnkRStrLit, nnkTripleStrLit}:
        error("schema: composite index field must be ident / dotted ident / string", child)
      parts.add(s.strVal)
    newLit(parts.join(","))
  else:
    error("schema: index field must be ident, dotted ident, string, or tuple of those — got " & $arg.kind, arg)
    newLit("")

proc parseIndexSpec(name: string; rhs: NimNode;
                    declared: openArray[string] = []): NimNode =
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
      error("schema: `equality` index needs one field path", rhs)
    result = newTree(nnkObjConstr, specSym,
      newTree(nnkExprColonExpr, ident"kind", bindSym"gikEquality"),
      newTree(nnkExprColonExpr, ident"name", nameLit),
      newTree(nnkExprColonExpr, ident"fieldPath", fieldArgToString(args[0], declared)))
  of "range":
    if args.len != 1:
      error("schema: `range` index needs one field path", rhs)
    result = newTree(nnkObjConstr, specSym,
      newTree(nnkExprColonExpr, ident"kind", bindSym"gikRange"),
      newTree(nnkExprColonExpr, ident"name", nameLit),
      newTree(nnkExprColonExpr, ident"fieldPath", fieldArgToString(args[0], declared)))
  of "geo":
    if args.len != 2:
      error("schema: `geo` index needs (lonField, latField)", rhs)
    result = newTree(nnkObjConstr, specSym,
      newTree(nnkExprColonExpr, ident"kind", bindSym"gikGeo"),
      newTree(nnkExprColonExpr, ident"name", nameLit),
      newTree(nnkExprColonExpr, ident"lonField", fieldArgToString(args[0], declared)),
      newTree(nnkExprColonExpr, ident"latField", fieldArgToString(args[1], declared)))
  of "polygon", "poly":
    if args.len != 1:
      error("schema: `polygon` index needs one field name", rhs)
    result = newTree(nnkObjConstr, specSym,
      newTree(nnkExprColonExpr, ident"kind", bindSym"gikPolygon"),
      newTree(nnkExprColonExpr, ident"name", nameLit),
      newTree(nnkExprColonExpr, ident"polygonField", fieldArgToString(args[0], declared)))
  of "vector":
    if args.len < 2 or args.len > 3:
      error("schema: `vector` index needs (embeddingField, dim [, metricExpr])", rhs)
    let metricExpr =
      if args.len == 3: args[2]
      else: bindSym"vmCosine"
    result = newTree(nnkObjConstr, specSym,
      newTree(nnkExprColonExpr, ident"kind", bindSym"gikVector"),
      newTree(nnkExprColonExpr, ident"name", nameLit),
      newTree(nnkExprColonExpr, ident"embeddingField", fieldArgToString(args[0], declared)),
      newTree(nnkExprColonExpr, ident"dim", args[1]),
      newTree(nnkExprColonExpr, ident"metric", metricExpr))
  else:
    error("schema: unknown index kind `" & kind & "`. Expected one of: equality, range, geo, polygon, vector", kindIdent)

proc cap(s: string): string =
  if s.len == 0: s
  else: toUpperAscii(s[0]) & s[1 ..^ 1]

proc parseFieldEntry(stmt: NimNode): (NimNode, NimNode) =
  ## Field entries take the same shape as `zobject`: `name: <expr>`. For
  ## indented chains like `name: zString()\n  .minLen(2)` Nim wraps the
  ## RHS in a stmt list; we unwrap a single-statement list to get the
  ## inner expression.
  case stmt.kind
  of nnkExprColonExpr:
    if stmt[0].kind notin {nnkIdent, nnkSym}:
      error("schema: field name must be identifier", stmt[0])
    var rhs = stmt[1]
    if rhs.kind == nnkStmtList and rhs.len == 1:
      rhs = rhs[0]
    return (stmt[0], rhs)
  of nnkCall:
    if stmt.len == 2 and stmt[0].kind in {nnkIdent, nnkSym}:
      var rhs = stmt[1]
      if rhs.kind == nnkStmtList and rhs.len == 1:
        rhs = rhs[0]
      return (stmt[0], rhs)
    error("schema: malformed field entry", stmt)
  else:
    error("schema: field entry must be `name: <schemaExpr>`", stmt)

macro schema*(name: untyped; body: untyped): untyped =
  ## Declare a Glen collection's typed schema, validator, indexes, and
  ## migrations in one block. See module doc for the generated surface.
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
  var versionLit: NimNode = newLit(0)
  var migrationLines: seq[(int, int, NimNode)] = @[]   # (fromVer, toVer, body)
  var keyField: NimNode = nil   # `key: fieldName` — derives docId from field
  var strictMode = false        # `strict: true` — error on unknown fields
  var softDeleteField: NimNode = nil  # `softDelete: fieldName` — tombstone field

  for s in body:
    if s.kind == nnkCommentStmt: continue
    if s.kind notin {nnkCall, nnkCommand, nnkExprColonExpr}:
      error("schema: expected `fields:` / `indexes:` / `version:` / `migrations:` / `key:` / `strict:` sections", s)
    let label = ($s[0]).toLowerAscii
    let sectionBody = s[1]
    case label
    of "fields":
      if sectionBody.kind != nnkStmtList:
        error("schema: `fields:` must be a block", sectionBody)
      fieldsBlock = sectionBody
    of "key":
      var v = sectionBody
      if v.kind == nnkStmtList and v.len == 1: v = v[0]
      if v.kind notin {nnkIdent, nnkSym}:
        error("schema: `key:` must name a field declared in this schema (e.g. `key: email`)", v)
      keyField = v
    of "strict":
      var v = sectionBody
      if v.kind == nnkStmtList and v.len == 1: v = v[0]
      if v.kind == nnkIdent and $v == "true": strictMode = true
      elif v.kind == nnkIdent and $v == "false": strictMode = false
      else:
        error("schema: `strict:` must be `true` or `false`", v)
    of "softdelete":
      var v = sectionBody
      if v.kind == nnkStmtList and v.len == 1: v = v[0]
      if v.kind notin {nnkIdent, nnkSym}:
        error("schema: `softDelete:` must name a field declared in this schema (e.g. `softDelete: deletedAt`)", v)
      softDeleteField = v
    of "version":
      var v = sectionBody
      if v.kind == nnkStmtList and v.len == 1: v = v[0]
      if v.kind notin nnkIntLit..nnkUInt64Lit:
        error("schema: `version:` must be an integer literal", v)
      versionLit = v
    of "migrations":
      if sectionBody.kind != nnkStmtList:
        error("schema: `migrations:` must be a block", sectionBody)
      for entry in sectionBody:
        if entry.kind == nnkCommentStmt: continue
        # `fromVer -> toVer: body` parses to nnkInfix with 4 children:
        # [op, fromVer, toVer, bodyStmtList]. The 4th child is the colon
        # block attached during command-style parsing.
        var fromN, toN, bodyN: NimNode = nil
        if entry.kind == nnkInfix and entry.len >= 4 and $entry[0] == "->":
          fromN = entry[1]
          toN   = entry[2]
          bodyN = entry[3]
        elif entry.kind == nnkCall and entry.len == 2 and
             entry[0].kind == nnkInfix and $entry[0][0] == "->":
          fromN = entry[0][1]
          toN   = entry[0][2]
          bodyN = entry[1]
        else:
          error("schema: migration must be `fromVer -> toVer: <body>`", entry)
        if fromN.kind notin nnkIntLit..nnkUInt64Lit:
          error("schema: migration fromVer must be int literal", fromN)
        if toN.kind notin nnkIntLit..nnkUInt64Lit:
          error("schema: migration toVer must be int literal", toN)
        if bodyN.isNil:
          error("schema: migration body missing", entry)
        migrationLines.add((int(fromN.intVal), int(toN.intVal), bodyN))
    of "indexes":
      if sectionBody.kind != nnkStmtList:
        error("schema: `indexes:` must be a block", sectionBody)
      for entry in sectionBody:
        if entry.kind == nnkCommentStmt: continue
        case entry.kind
        of nnkCall:
          if entry.len == 2 and entry[0].kind in {nnkIdent, nnkSym}:
            let idxName = $entry[0]
            var rhs = entry[1]
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
      error("schema: unknown section `" & label &
        "`. Expected `fields:`, `indexes:`, `version:`, or `migrations:`", s)

  if fieldsBlock.isNil:
    error("schema: `fields:` section is required", body)

  # Generated symbol names
  let typeIdent  = ident(baseCap)                       # `Users`
  let schemaSym  = ident(nameStr & "Schema")
  let collConst  = ident(nameStr & "Collection")
  let versionConst = ident(nameStr & "SchemaVersion")
  let registerFn = ident("register" & baseCap & "Schema")
  let validateFn = ident("validate" & baseCap)
  let parseFn    = ident("parse" & baseCap)
  let getFn         = ident("get" & baseCap)
  let putFn         = ident("put" & baseCap)
  let addFn         = ident("add" & baseCap)
  let putManyFn     = ident("put" & baseCap & "Many")
  let getManyFn     = ident("get" & baseCap & "Many")
  let migrateFn     = ident("migrate" & baseCap)
  let softDeleteFn  = ident("softDelete" & baseCap)
  let restoreFn     = ident("restore" & baseCap)

  # ---- Walk the fields ---------------------------------------------------
  # For each `field: <expr>` we emit a `let usersFooSchema = <expr>` so the
  # schema is evaluated once. `schemaValueType` extracts the static type
  # from that schema, which we then use in:
  #   * the `type Users` declaration, for the field type
  #   * the parser closure, where each parsed value is assigned into the
  #     accumulator
  #   * the `toValue` proc, where each field is encoded back into a Value.

  var fieldNames: seq[NimNode] = @[]
  var fieldSchemaSyms: seq[NimNode] = @[]
  var schemaLets = newStmtList()

  for raw in fieldsBlock:
    if raw.kind == nnkCommentStmt: continue
    let (fname, fexpr) = parseFieldEntry(raw)
    let fSchemaSym = ident(nameStr & cap($fname) & "Schema")
    schemaLets.add(newLetStmt(fSchemaSym, fexpr))
    fieldNames.add(fname)
    fieldSchemaSyms.add(fSchemaSym)

  if fieldNames.len == 0:
    error("schema: `fields:` block must declare at least one field", fieldsBlock)

  # Validate `key:` names a declared field. If absent, the schema falls
  # back to the explicit-id-on-put behaviour (callers supply docIds).
  if not keyField.isNil:
    var found = false
    for fn in fieldNames:
      if $fn == $keyField: found = true; break
    if not found:
      error("schema: `key:` field `" & $keyField &
        "` is not declared in `fields:`", keyField)

  # Same for `softDelete:`.
  if not softDeleteField.isNil:
    var found = false
    for fn in fieldNames:
      if $fn == $softDeleteField: found = true; break
    if not found:
      error("schema: `softDelete:` field `" & $softDeleteField &
        "` is not declared in `fields:` (declare it as `optional(zInt())`)", softDeleteField)

  # ---- type Users* = object -------------------------------------
  var recList = newTree(nnkRecList)
  for i in 0 ..< fieldNames.len:
    let typeExpr = newCall(bindSym"schemaValueType", fieldSchemaSyms[i])
    recList.add(newIdentDefs(postfix(fieldNames[i], "*"),
                             typeExpr, newEmptyNode()))
  let typeSection = newTree(nnkTypeSection,
    newTree(nnkTypeDef,
      postfix(typeIdent, "*"),
      newEmptyNode(),
      newTree(nnkObjectTy, newEmptyNode(), newEmptyNode(), recList)))

  # ---- let usersSchema*: Schema[Users] = newSchema("users", ...) ---------
  # Parser closure:
  #   if not ensureObject(input, path, issues): return none(Users)
  #   var acc: Users
  #   let before = issues.len
  #   for each field:
  #     let fv = getObjectField(input, "field")
  #     let parsed = runSchema(fooSchema, fv, pushPath(path, "field"), issues)
  #     if parsed.isSome: acc.field = parsed.get
  #   if issues.len > before: return none(Users)
  #   some(acc)
  let inputSym  = genSym(nskParam, "input")
  let pathSym   = genSym(nskParam, "path")
  let issuesSym = genSym(nskParam, "issues")
  let accSym    = genSym(nskVar,   "acc")
  let beforeSym = genSym(nskLet,   "before")
  var parserBody = newStmtList()
  let optTypeForResult = newTree(nnkBracketExpr, bindSym"Option", typeIdent)
  let returnNone = newTree(nnkReturnStmt, newCall(bindSym"none", typeIdent))
  parserBody.add(newIfStmt(
    (cond: prefix(newCall(bindSym"ensureObject", inputSym, pathSym, issuesSym), "not"),
     body: newStmtList(returnNone))))
  parserBody.add(newVarStmt(accSym, newCall(typeIdent)))
  parserBody.add(newLetStmt(beforeSym, newDotExpr(issuesSym, ident"len")))

  # Strict mode: reject undeclared fields. We emit an explicit array of
  # allowed names rather than building a HashSet at runtime — n is small
  # (typical schemas have <20 fields), and the linear scan keeps the
  # generated code readable.
  if strictMode:
    var allowedArr = newTree(nnkBracket)
    for fn in fieldNames:
      allowedArr.add(newLit($fn))
    let allowedSeq = newTree(nnkPrefix, ident"@", allowedArr)
    parserBody.add(newTree(nnkDiscardStmt,
      newCall(bindSym"checkStrictKeys",
        inputSym, allowedSeq, pathSym, issuesSym)))
  for i in 0 ..< fieldNames.len:
    let fname = fieldNames[i]
    let fSchema = fieldSchemaSyms[i]
    let keyLit = newLit($fname)
    let parsedSym = genSym(nskLet, "parsed")
    let fvSym = genSym(nskLet, "fv")
    parserBody.add(quote do:
      let `fvSym` = getObjectField(`inputSym`, `keyLit`)
      let `parsedSym` = runSchema(`fSchema`, `fvSym`,
                                  pushPath(`pathSym`, `keyLit`),
                                  `issuesSym`)
      if `parsedSym`.isSome:
        `accSym`.`fname` = `parsedSym`.get
    )
  parserBody.add(newIfStmt(
    (cond: infix(newDotExpr(issuesSym, ident"len"), ">", beforeSym),
     body: newStmtList(returnNone))))
  parserBody.add(newCall(bindSym"some", accSym))

  let parserLambda = newTree(nnkLambda,
    newEmptyNode(), newEmptyNode(), newEmptyNode(),
    newTree(nnkFormalParams, optTypeForResult,
      newIdentDefs(inputSym, bindSym"Value"),
      newIdentDefs(pathSym, newTree(nnkBracketExpr, bindSym"seq", bindSym"string")),
      newIdentDefs(issuesSym,
                   newTree(nnkVarTy,
                           newTree(nnkBracketExpr, bindSym"seq", bindSym"ValidationIssue")))),
    newEmptyNode(), newEmptyNode(),
    parserBody)

  let schemaCall = newCall(
    newTree(nnkBracketExpr, bindSym"newSchema", typeIdent),
    newLit(nameStr),
    parserLambda)
  let schemaTypeAnnot = newTree(nnkBracketExpr, bindSym"Schema", typeIdent)
  let schemaLet = newTree(nnkLetSection,
    newTree(nnkIdentDefs,
      postfix(schemaSym, "*"),
      schemaTypeAnnot,
      schemaCall))

  # ---- proc toValue*(u: Users): Value -----------------------------------
  let uParam = ident"u"
  let resultSym = ident"result"
  var encodeBody = newStmtList()
  encodeBody.add(newAssignment(resultSym, newCall(bindSym"VObject")))
  for fname in fieldNames:
    let keyLit = newLit($fname)
    let fieldVal = newDotExpr(uParam, fname)
    encodeBody.add(newAssignment(
      newTree(nnkBracketExpr, resultSym, keyLit),
      newCall(bindSym"encodeField", fieldVal)))
  let toValueProc = newProc(
    name = postfix(ident"toValue", "*"),
    params = [bindSym"Value", newIdentDefs(uParam, typeIdent)],
    body = encodeBody)

  # ---- proc parseUsers* / validateUsers* --------------------------------
  let vParam = ident"v"
  let parseProc = newProc(
    name = postfix(parseFn, "*"),
    params = [newTree(nnkBracketExpr, bindSym"ValidationResult", typeIdent),
              newIdentDefs(vParam, bindSym"Value")],
    body = newCall(bindSym"parse", schemaSym, vParam))
  let validateProc = newProc(
    name = postfix(validateFn, "*"),
    params = [newTree(nnkBracketExpr, bindSym"ValidationResult", typeIdent),
              newIdentDefs(vParam, bindSym"Value")],
    body = newCall(bindSym"parse", schemaSym, vParam))

  # ---- const usersCollection*, usersSchemaVersion* ----------------------
  let collDecl = newTree(nnkConstSection,
    newTree(nnkConstDef,
      postfix(collConst, "*"), newEmptyNode(), newLit(nameStr)))
  let versionDecl = newTree(nnkConstSection,
    newTree(nnkConstDef,
      postfix(versionConst, "*"), newEmptyNode(), versionLit))

  # ---- proc registerUsersSchema*(db: GlenDB) ----------------------------
  let dbParam = ident"db"
  var declaredNames: seq[string] = @[]
  for fn in fieldNames: declaredNames.add($fn)
  var registerBody = newStmtList()
  for (idxName, rhs) in indexLines:
    let specExpr = parseIndexSpec(idxName, rhs, declaredNames)
    registerBody.add(newCall(bindSym"applyIndex",
      dbParam, newLit(nameStr), specExpr))
  if registerBody.len == 0:
    registerBody.add(newTree(nnkDiscardStmt, newEmptyNode()))
  let glenDbType = bindSym"GlenDB"
  let registerProc = newProc(
    name = postfix(registerFn, "*"),
    params = [newEmptyNode(), newIdentDefs(dbParam, glenDbType)],
    body = registerBody)

  # ---- proc getUsers / putUsers ----------------------------------------
  # When `key:` is declared, putUsers takes only (db, u) and derives the
  # docId from the key field via stringification (`$u.<key>`). Otherwise
  # the caller supplies the id explicitly, matching the rest of CRUD.
  let idParam = ident"id"
  var putProc, addProc: NimNode
  if keyField.isNil:
    # No key: classic two-arg form.
    let putBody = newCall(newDotExpr(dbParam, ident"put"),
                          newLit(nameStr), idParam,
                          newCall(ident"toValue", uParam))
    putProc = newProc(
      name = postfix(putFn, "*"),
      params = [newEmptyNode(),
                newIdentDefs(dbParam, glenDbType),
                newIdentDefs(idParam, bindSym"string"),
                newIdentDefs(uParam, typeIdent)],
      body = putBody)
    # addUsers returns a fresh ULID.
    let addBody = newCall(newDotExpr(dbParam, ident"add"),
                          newLit(nameStr),
                          newCall(ident"toValue", uParam))
    addProc = newProc(
      name = postfix(addFn, "*"),
      params = [bindSym"string",
                newIdentDefs(dbParam, glenDbType),
                newIdentDefs(uParam, typeIdent)],
      body = addBody)
  else:
    # `key: <field>` — derive docId from the field. `$` so non-string
    # field types still work (int, char, etc.); strings pass through.
    let keyAccess = newDotExpr(uParam, keyField)
    let keyExpr   = newTree(nnkPrefix, ident"$", keyAccess)
    # putUsers(db, u: Users) — no id arg.
    let putBody = quote do:
      `dbParam`.put(`nameStr`, `keyExpr`, toValue(`uParam`))
    putProc = newProc(
      name = postfix(putFn, "*"),
      params = [newEmptyNode(),
                newIdentDefs(dbParam, glenDbType),
                newIdentDefs(uParam, typeIdent)],
      body = putBody)
    # addUsers(db, u): string — same as put, but returns the key value
    # so callers don't have to fish it back out of the record.
    let addBody = quote do:
      let derivedId = `keyExpr`
      `dbParam`.put(`nameStr`, derivedId, toValue(`uParam`))
      derivedId
    addProc = newProc(
      name = postfix(addFn, "*"),
      params = [bindSym"string",
                newIdentDefs(dbParam, glenDbType),
                newIdentDefs(uParam, typeIdent)],
      body = addBody)

  # Return type is `(bool, Users)` — using ParTy/TupleTy node so it's valid
  # in a proc signature.
  let resTupleType = newTree(nnkTupleConstr, bindSym"bool", typeIdent)
  let getBody = quote do:
    let doc = `dbParam`.get(`nameStr`, `idParam`)
    if doc.isNil:
      result = (false, `typeIdent`())
    else:
      let parsed = `parseFn`(doc)
      if parsed.ok:
        result = (true, parsed.value)
      else:
        result = (false, `typeIdent`())
  let getProc = newProc(
    name = postfix(getFn, "*"),
    params = [resTupleType,
              newIdentDefs(dbParam, glenDbType),
              newIdentDefs(idParam, bindSym"string")],
    body = getBody)

  # ---- proc putUsersMany / getUsersMany --------------------------------
  # Bulk variants. Without `key:`, the put form takes (id, value) pairs
  # like `db.putMany`. With `key:`, the id is derived from each record's
  # key field — fewer args, less to get wrong.
  #
  # `getUsersMany` takes a list of ids and returns only the rows that
  # exist AND validate against the schema. Bad / missing rows are silently
  # skipped — matches `db.getMany`'s lenient semantics. If you need
  # per-id status, call `getUsers` in a loop.
  let itemsParam = ident"items"
  let idsParam = ident"ids"
  var putManyProc: NimNode
  let putManyResultType = newTree(nnkBracketExpr, bindSym"seq", bindSym"string")
  if keyField.isNil:
    # Without key: openArray[(string, Users)] — explicit ids
    let pairType = newTree(nnkTupleConstr, bindSym"string", typeIdent)
    let putManyBody = quote do:
      var encoded: seq[(string, Value)] = newSeqOfCap[(string, Value)](`itemsParam`.len)
      for it in `itemsParam`:
        encoded.add((it[0], toValue(it[1])))
      `dbParam`.putMany(`nameStr`, encoded)
      result = newSeqOfCap[string](`itemsParam`.len)
      for it in `itemsParam`: result.add(it[0])
    putManyProc = newProc(
      name = postfix(putManyFn, "*"),
      params = [putManyResultType,
                newIdentDefs(dbParam, glenDbType),
                newIdentDefs(itemsParam,
                  newTree(nnkBracketExpr, bindSym"openArray", pairType))],
      body = putManyBody)
  else:
    # With key: openArray[Users] — ids derived from each record
    let putManyBody = quote do:
      var encoded: seq[(string, Value)] = newSeqOfCap[(string, Value)](`itemsParam`.len)
      result = newSeqOfCap[string](`itemsParam`.len)
      for it in `itemsParam`:
        let derived = $it.`keyField`
        encoded.add((derived, toValue(it)))
        result.add(derived)
      `dbParam`.putMany(`nameStr`, encoded)
    putManyProc = newProc(
      name = postfix(putManyFn, "*"),
      params = [putManyResultType,
                newIdentDefs(dbParam, glenDbType),
                newIdentDefs(itemsParam,
                  newTree(nnkBracketExpr, bindSym"openArray", typeIdent))],
      body = putManyBody)

  let getManyResType = newTree(nnkBracketExpr, bindSym"seq",
    newTree(nnkTupleConstr, bindSym"string", typeIdent))
  let getManyBody = quote do:
    let raw = `dbParam`.getMany(`nameStr`, `idsParam`)
    result = newSeqOfCap[(string, `typeIdent`)](raw.len)
    for (id, doc) in raw:
      if doc.isNil: continue
      let parsed = `parseFn`(doc)
      if parsed.ok:
        result.add((id, parsed.value))
  let getManyProc = newProc(
    name = postfix(getManyFn, "*"),
    params = [getManyResType,
              newIdentDefs(dbParam, glenDbType),
              newIdentDefs(idsParam,
                newTree(nnkBracketExpr, bindSym"openArray", bindSym"string"))],
    body = getManyBody)

  # ---- soft-delete procs (generated only when `softDelete:` declared) ---
  # `softDeleteUsers(db, id)` stamps the configured field with the current
  # millis. `restoreUsers(db, id)` removes the field entirely so the
  # `optional()` validator sees it as absent (none).
  #
  # The procs operate on the raw Value, not the typed record, so they
  # don't trip the strict-mode check or run the full validator pipeline
  # — soft-deletion shouldn't fail just because the schema later picked
  # up new mandatory fields.
  var softDeleteProc, restoreProc: NimNode = nil
  if not softDeleteField.isNil:
    let fieldNameLit = newLit($softDeleteField)
    softDeleteProc = quote do:
      proc `softDeleteFn`*(`dbParam`: glendb.GlenDB; `idParam`: string) =
        ## Mark the named row as soft-deleted by setting the schema's
        ## tombstone field to `nowMillis()`. No-op if the row is missing.
        let doc = `dbParam`.get(`nameStr`, `idParam`)
        if doc.isNil: return
        doc[`fieldNameLit`] = VInt(nowMillis())
        `dbParam`.put(`nameStr`, `idParam`, doc)
    restoreProc = quote do:
      proc `restoreFn`*(`dbParam`: glendb.GlenDB; `idParam`: string) =
        ## Clear the tombstone field, restoring a previously soft-deleted row.
        let doc = `dbParam`.get(`nameStr`, `idParam`)
        if doc.isNil: return
        if doc.hasKey(`fieldNameLit`):
          doc.obj.del(`fieldNameLit`)
          `dbParam`.put(`nameStr`, `idParam`, doc)

  # ---- proc migrateUsers*(db: GlenDB) -----------------------------------
  # Each doc carries a `_v` int field (default 0 if missing). For every
  # declared `from -> to: <body>` pair (sorted), if the doc's current
  # version equals `from`, run the body to mutate the doc in place, then
  # bump the local version to `to`. After all steps, stamp `_v` to the
  # target schema version and write the doc back.
  var sorted = migrationLines
  for i in 0 ..< sorted.len:
    for j in 0 ..< sorted.len - i - 1:
      if sorted[j][0] > sorted[j+1][0] or
         (sorted[j][0] == sorted[j+1][0] and sorted[j][1] > sorted[j+1][1]):
        let tmp = sorted[j]
        sorted[j] = sorted[j+1]
        sorted[j+1] = tmp

  var migrationSteps = newStmtList()
  for (fromVer, toVer, mbody) in sorted:
    let fromLit = newLit(fromVer)
    let toLit = newLit(toVer)
    migrationSteps.add(quote do:
      if curVer == `fromLit`:
        `mbody`
        curVer = `toLit`
    )

  let targetVerLit = versionLit
  let migrateBody = quote do:
    for entry in `dbParam`.getAll(`nameStr`):
      let id {.inject.} = entry[0]
      let doc {.inject.} = entry[1]
      if doc.isNil: continue
      let rawVer = doc["_v"]
      let origVer = (if rawVer.isNil or rawVer.kind != vkInt: 0
                     else: int(rawVer.i))
      var curVer {.inject.} = origVer
      `migrationSteps`
      # Write back whenever any migration ran (curVer advanced) or the
      # stamped version is missing/stale. This is idempotent: re-running
      # migrate on already-current docs is a no-op.
      if curVer != origVer or origVer != `targetVerLit`:
        doc["_v"] = VInt(int64(`targetVerLit`))
        `dbParam`.put(`nameStr`, id, doc)

  let migrateProc = newProc(
    name = postfix(migrateFn, "*"),
    params = [newEmptyNode(), newIdentDefs(dbParam, glenDbType)],
    body = migrateBody)

  result = newStmtList(
    schemaLets,
    typeSection,
    schemaLet,
    toValueProc,
    parseProc,
    validateProc,
    collDecl,
    versionDecl,
    registerProc,
    putProc,
    addProc,
    getProc,
    putManyProc,
    getManyProc,
    migrateProc)
  if not softDeleteProc.isNil:
    result.add(softDeleteProc)
    result.add(restoreProc)
