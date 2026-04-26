## Zod-like validation system for Glen
##
## Provides composable schemas with macro-powered object builders that can
## validate `Value` trees (Convex-style) and materialize statically typed
## Nim tuples.

import std/[options, re, sets, strutils, tables]
import std/macros

import glen/types

type
  ValidationIssue* = object
    path*: seq[string]
    message*: string
    expected*: string
    actual*: string

  ValidationResult*[T] = object
    ok*: bool
    value*: T
    issues*: seq[ValidationIssue]

  SchemaParser[T] = proc (
    input: Value;
    path: seq[string];
    issues: var seq[ValidationIssue]
  ): Option[T] {.closure.}

  Schema*[T] = ref object
    name*: string
    parser*: SchemaParser[T]

template schemaValueType*[T](schema: Schema[T]): typedesc =
  T

proc newSchema*[T](name: string; parser: SchemaParser[T]): Schema[T] =
  Schema[T](name: name, parser: parser)

proc pushPath*(path: seq[string]; segment: string): seq[string] =
  result = newSeq[string](path.len + 1)
  if path.len > 0:
    for i, part in path:
      result[i] = part
  result[^1] = segment

proc describePath*(path: seq[string]): string =
  if path.len == 0:
    return "$"
  "$." & path.join(".")

proc kindName*(value: Value): string =
  if value.isNil:
    return "missing"
  case value.kind
  of vkNull: "null"
  of vkBool: "bool"
  of vkInt: "int"
  of vkFloat: "float"
  of vkString: "string"
  of vkBytes: "bytes"
  of vkArray: "array"
  of vkObject: "object"
  of vkId: "id"

proc mkIssue(path: seq[string]; expected, actual, message: string): ValidationIssue =
  ValidationIssue(
    path: path,
    expected: expected,
    actual: actual,
    message: if message.len == 0:
      "Expected " & expected & " but received " & actual
    else:
      message
  )

proc runSchema*[T](schema: Schema[T]; input: Value; path: seq[string]; issues: var seq[ValidationIssue]): Option[T] =
  if schema.isNil or schema.parser.isNil:
    raise newException(ValueError, "Schema parser missing for " & (if schema.isNil: "nil" else: schema.name))
  schema.parser(input, path, issues)

proc parse*[T](schema: Schema[T]; input: Value): ValidationResult[T] =
  var issues: seq[ValidationIssue] = @[]
  let parsed = runSchema(schema, input, @[], issues)
  result.issues = issues
  if parsed.isSome and issues.len == 0:
    result.ok = true
    result.value = parsed.get
  else:
    result.ok = false

proc parseOrRaise*[T](schema: Schema[T]; input: Value): T =
  let res = parse(schema, input)
  if not res.ok:
    var parts: seq[string] = @[]
    for issue in res.issues:
      parts.add(describePath(issue.path) & ": " & issue.message)
    raise newException(ValueError, "Validation failed:\n" & parts.join("\n"))
  res.value

proc getObjectField*(value: Value; key: string): Value =
  if value.isNil or value.kind != vkObject:
    return nil
  if key in value.obj:
    return value.obj[key]
  nil

proc ensureObject*(value: Value; path: seq[string]; issues: var seq[ValidationIssue]): bool =
  if value.isNil or value.kind != vkObject:
    issues.add(mkIssue(path, "object", kindName(value), "Expected object at " & describePath(path)))
    return false
  true

# -----------------------------
# Primitive schema constructors
# -----------------------------

proc zAny*(name = "any"): Schema[Value] =
  newSchema[Value](name, proc (input: Value; path: seq[string]; issues: var seq[ValidationIssue]): Option[Value] =
    if input.isNil:
      return some(VNull())
    some(input)
  )

proc zString*(name = "string"): Schema[string] =
  newSchema[string](name, proc (input: Value; path: seq[string]; issues: var seq[ValidationIssue]): Option[string] =
    if input.isNil or input.kind != vkString:
      issues.add(mkIssue(path, "string", kindName(input), "Value must be a string"))
      return none(string)
    some(input.s)
  )

proc zBool*(name = "bool"): Schema[bool] =
  newSchema[bool](name, proc (input: Value; path: seq[string]; issues: var seq[ValidationIssue]): Option[bool] =
    if input.isNil or input.kind != vkBool:
      issues.add(mkIssue(path, "bool", kindName(input), "Value must be a bool"))
      return none(bool)
    some(input.b)
  )

proc zInt*(name = "int"): Schema[int64] =
  newSchema[int64](name, proc (input: Value; path: seq[string]; issues: var seq[ValidationIssue]): Option[int64] =
    if input.isNil or input.kind != vkInt:
      issues.add(mkIssue(path, "int", kindName(input), "Value must be an int"))
      return none(int64)
    some(input.i)
  )

proc zFloat*(name = "float"): Schema[float64] =
  newSchema[float64](name, proc (input: Value; path: seq[string]; issues: var seq[ValidationIssue]): Option[float64] =
    if input.isNil or (input.kind notin {vkFloat, vkInt}):
      issues.add(mkIssue(path, "float", kindName(input), "Value must be a float"))
      return none(float64)
    if input.kind == vkFloat:
      some(input.f)
    else:
      some(float64(input.i))
  )

proc withCheck[T](schema: Schema[T]; label: string; check: proc (
    value: T;
    path: seq[string];
    issues: var seq[ValidationIssue]
  ): bool {.closure.}): Schema[T] =
  let base = schema
  newSchema[T](base.name & label, proc (input: Value; path: seq[string]; issues: var seq[ValidationIssue]): Option[T] =
    let parsed = runSchema(base, input, path, issues)
    if parsed.isSome:
      let value = parsed.get
      if check(value, path, issues):
        return some(value)
      return none(T)
    else:
      return none(T)
  )

proc minLen*(schema: Schema[string]; minValue: Natural; message = ""): Schema[string] =
  withCheck(schema, ".minLen", proc (value: string; path: seq[string]; issues: var seq[ValidationIssue]): bool =
    if value.len < minValue:
      issues.add(mkIssue(path, "string(len >= " & $minValue & ")", "len=" & $value.len, if message.len == 0: "String shorter than " & $minValue else: message))
      return false
    true
  )

proc maxLen*(schema: Schema[string]; maxValue: Natural; message = ""): Schema[string] =
  withCheck(schema, ".maxLen", proc (value: string; path: seq[string]; issues: var seq[ValidationIssue]): bool =
    if value.len > maxValue:
      issues.add(mkIssue(path, "string(len <= " & $maxValue & ")", "len=" & $value.len, if message.len == 0: "String longer than " & $maxValue else: message))
      return false
    true
  )

proc matches*(schema: Schema[string]; pattern: Regex; message = ""): Schema[string] =
  withCheck(schema, ".matches", proc (value: string; path: seq[string]; issues: var seq[ValidationIssue]): bool =
    if not match(value, pattern):
      issues.add(mkIssue(path, "pattern", value, if message.len == 0: "String did not match required pattern" else: message))
      return false
    true
  )

proc map*[A, B](schema: Schema[A]; name = "map"; fn: proc (value: A): B {.closure.}): Schema[B] =
  let base = schema
  newSchema[B](base.name & "." & name, proc (input: Value; path: seq[string]; issues: var seq[ValidationIssue]): Option[B] =
    let parsed = runSchema(base, input, path, issues)
    if parsed.isSome:
      return some(fn(parsed.get))
    none(B)
  )

proc trim*(schema: Schema[string]): Schema[string] =
  schema.map("trim", proc (value: string): string = value.strip())

proc gte*(schema: Schema[int64]; floor: int64; message = ""): Schema[int64] =
  withCheck(schema, ".gte", proc (value: int64; path: seq[string]; issues: var seq[ValidationIssue]): bool =
    if value < floor:
      issues.add(mkIssue(path, "int >= " & $floor, $value, if message.len == 0: "Value below minimum" else: message))
      return false
    true
  )

proc lte*(schema: Schema[int64]; ceiling: int64; message = ""): Schema[int64] =
  withCheck(schema, ".lte", proc (value: int64; path: seq[string]; issues: var seq[ValidationIssue]): bool =
    if value > ceiling:
      issues.add(mkIssue(path, "int <= " & $ceiling, $value, if message.len == 0: "Value above maximum" else: message))
      return false
    true
  )

proc gte*(schema: Schema[float64]; floor: float64; message = ""): Schema[float64] =
  withCheck(schema, ".gte", proc (value: float64; path: seq[string]; issues: var seq[ValidationIssue]): bool =
    if value < floor:
      issues.add(mkIssue(path, "float >= " & $floor, $value, if message.len == 0: "Value below minimum" else: message))
      return false
    true
  )

proc lte*(schema: Schema[float64]; ceiling: float64; message = ""): Schema[float64] =
  withCheck(schema, ".lte", proc (value: float64; path: seq[string]; issues: var seq[ValidationIssue]): bool =
    if value > ceiling:
      issues.add(mkIssue(path, "float <= " & $ceiling, $value, if message.len == 0: "Value above maximum" else: message))
      return false
    true
  )

proc refine*[T](schema: Schema[T]; message: string; predicate: proc (value: T): bool {.closure.}): Schema[T] =
  withCheck(schema, ".refine", proc (value: T; path: seq[string]; issues: var seq[ValidationIssue]): bool =
    if not predicate(value):
      issues.add(mkIssue(path, schema.name & ".refine", $value, message))
      return false
    true
  )

proc optional*[T](schema: Schema[T]): Schema[Option[T]] =
  let base = schema
  newSchema[Option[T]](base.name & "?", proc (input: Value; path: seq[string]; issues: var seq[ValidationIssue]): Option[Option[T]] =
    if input.isNil or (not input.isNil and input.kind == vkNull):
      return some(none(T))
    let parsed = runSchema(base, input, path, issues)
    if parsed.isSome:
      return some(some(parsed.get))
    none(Option[T])
  )

proc nullable*[T](schema: Schema[T]): Schema[Option[T]] =
  let base = schema
  newSchema[Option[T]](base.name & ".nullable", proc (input: Value; path: seq[string]; issues: var seq[ValidationIssue]): Option[Option[T]] =
    if not input.isNil and input.kind == vkNull:
      return some(none(T))
    let parsed = runSchema(base, input, path, issues)
    if parsed.isSome:
      return some(some(parsed.get))
    none(Option[T])
  )

proc default*[T](schema: Schema[T]; fallback: T): Schema[T] =
  let base = schema
  newSchema[T](base.name & ".default", proc (input: Value; path: seq[string]; issues: var seq[ValidationIssue]): Option[T] =
    if input.isNil or (not input.isNil and input.kind == vkNull):
      return some(fallback)
    runSchema(base, input, path, issues)
  )

proc describe*[T](schema: Schema[T]; name: string): Schema[T] =
  let base = schema
  newSchema[T](name, base.parser)

proc zLiteral*(literal: string): Schema[string] =
  zString("literal").refine("Expected literal \"" & literal & "\"", proc (value: string): bool = value == literal)

proc zLiteral*(literal: int64): Schema[int64] =
  zInt("literal").refine("Expected literal " & $literal, proc (value: int64): bool = value == literal)

proc zLiteral*(literal: bool): Schema[bool] =
  zBool("literal").refine("Expected literal " & $(literal), proc (value: bool): bool = value == literal)

proc zEnum*(values: openArray[string]): Schema[string] =
  var allowed = initHashSet[string]()
  for v in values:
    allowed.incl(v)
  zString("enum").refine("Value must be one of " & $values, proc (value: string): bool = value in allowed)

proc zArray*[T](element: Schema[T]): Schema[seq[T]] =
  let inner = element
  newSchema[seq[T]]("array", proc (input: Value; path: seq[string]; issues: var seq[ValidationIssue]): Option[seq[T]] =
    if input.isNil or input.kind != vkArray:
      issues.add(mkIssue(path, "array", kindName(input), "Value must be an array"))
      return none(seq[T])
    var items: seq[T] = @[]
    items.setLen(input.arr.len)
    var ok = true
    for idx, item in input.arr:
      let childPath = pushPath(path, $idx)
      let parsed = runSchema(inner, item, childPath, issues)
      if parsed.isSome:
        items[idx] = parsed.get
      else:
        ok = false
    if ok:
      some(items)
    else:
      none(seq[T])
  )

proc zRecord*[T](valueSchema: Schema[T]): Schema[Table[string, T]] =
  let inner = valueSchema
  newSchema[Table[string, T]]("record", proc (input: Value; path: seq[string]; issues: var seq[ValidationIssue]): Option[Table[string, T]] =
    if input.isNil or input.kind != vkObject:
      issues.add(mkIssue(path, "object", kindName(input), "Value must be an object"))
      return none(Table[string, T])
    var tableResult = initTable[string, T]()
    var ok = true
    for key, val in input.obj:
      let parsed = runSchema(inner, val, pushPath(path, key), issues)
      if parsed.isSome:
        tableResult[key] = parsed.get
      else:
        ok = false
    if ok:
      some(tableResult)
    else:
      none(Table[string, T])
  )

proc zUnion*[T](schemas: varargs[Schema[T]]): Schema[T] =
  var branches: seq[Schema[T]] = @[]
  for schema in schemas:
    branches.add(schema)
  newSchema[T]("union", proc (input: Value; path: seq[string]; issues: var seq[ValidationIssue]): Option[T] =
    if branches.len == 0:
      issues.add(mkIssue(path, "union", kindName(input), "Union schema requires at least one branch"))
      return none(T)
    var bestIssues: seq[ValidationIssue] = @[]
    var hasBest = false
    for schema in branches:
      var branchIssues: seq[ValidationIssue] = @[]
      let parsed = runSchema(schema, input, path, branchIssues)
      if parsed.isSome and branchIssues.len == 0:
        return parsed
      if not hasBest or branchIssues.len < bestIssues.len:
        bestIssues = branchIssues
        hasBest = true
    for issue in bestIssues:
      issues.add(issue)
    none(T)
  )

# -----------------------------
# Macro-powered object builder
# -----------------------------

proc asExpr(node: NimNode): NimNode =
  case node.kind
  of nnkStmtList:
    if node.len == 0:
      return newEmptyNode()
    if node.len == 1:
      return node[0]
    result = newTree(nnkStmtListExpr)
    for child in node:
      result.add(child)
  of nnkStmtListExpr:
    result = node
  else:
    result = node

proc normalizeFieldNode(stmt: NimNode): NimNode =
  result = newEmptyNode()
  template makeField(nameNode, exprNode: NimNode): NimNode =
    newTree(nnkExprColonExpr, nameNode, asExpr(exprNode))

  case stmt.kind
  of nnkExprColonExpr:
    result = makeField(stmt[0], stmt[1])
  of nnkCommand:
    if stmt.len == 3 and stmt[0].kind in {nnkIdent, nnkSym} and stmt[1].kind == nnkIdent and stmt[1].strVal == "do":
      result = makeField(stmt[0], stmt[2])
  of nnkCall:
    if stmt.len == 2 and stmt[0].kind in {nnkIdent, nnkSym}:
      result = makeField(stmt[0], stmt[1])
  else:
    discard

macro zobject*(body: untyped): untyped =
  let stmts =
    if body.kind == nnkStmtList:
      body
    else:
      newStmtList(body)

  var tupleType = newTree(nnkTupleTy)
  var schemaLets = newStmtList()
  var fieldParsers = newStmtList()
  let accSym = genSym(nskVar, "acc")
  let inputSym = genSym(nskParam, "input")
  let pathSym = genSym(nskParam, "path")
  let issuesSym = genSym(nskParam, "issues")
  let beforeSym = genSym(nskLet, "before")

  var fieldCount = 0
  for rawStmt in stmts:
    let stmt = normalizeFieldNode(rawStmt)
    if stmt.kind != nnkExprColonExpr:
      error("zobject expects `name: schema` entries, got " & rawStmt.repr, rawStmt)
    else:
      let fieldNameNode = stmt[0]
      if fieldNameNode.kind != nnkIdent:
        error("Field name must be identifier", fieldNameNode)
      let schemaExpr = stmt[1]
      let schemaSym = genSym(nskLet, $fieldNameNode & "Schema")
      schemaLets.add(newLetStmt(schemaSym, schemaExpr))

      let fieldType = newCall(bindSym"schemaValueType", schemaSym)
      tupleType.add(newIdentDefs(fieldNameNode, fieldType, newEmptyNode()))

      let keyLit = newLit($fieldNameNode)
      let parsedSym = genSym(nskLet, $fieldNameNode & "Parsed")
      let fieldValSym = genSym(nskLet, $fieldNameNode & "Value")

      fieldParsers.add(quote do:
        let `fieldValSym` = getObjectField(`inputSym`, `keyLit`)
        let `parsedSym` = runSchema(`schemaSym`, `fieldValSym`, pushPath(`pathSym`, `keyLit`), `issuesSym`)
        if `parsedSym`.isSome:
          `accSym`.`fieldNameNode` = `parsedSym`.get
      )
      inc fieldCount

  if fieldCount == 0:
    error("zobject requires at least one field", body)

  let builderSym = genSym(nskProc, "buildZObject")
  result = quote do:
    proc `builderSym`(): auto =
      `schemaLets`
      newSchema[`tupleType`]("object", proc (`inputSym`: Value; `pathSym`: seq[string]; `issuesSym`: var seq[ValidationIssue]): Option[`tupleType`] =
        if not ensureObject(`inputSym`, `pathSym`, `issuesSym`):
          return none(`tupleType`)
        var `accSym`: `tupleType`
        let `beforeSym` = `issuesSym`.len
        `fieldParsers`
        if `issuesSym`.len > `beforeSym`:
          return none(`tupleType`)
        some(`accSym`)
      )
    `builderSym`()

# Convenience alias for readability
template schema*(body: typed): untyped =
  zobject(body)

