## %* — Value literal macro for Glen.
##
## Mirrors the shape of `std/json`'s `%*`, but produces a Glen `Value`. Lets
## you write
##
##   let u = %*{
##     "name": "Alice",
##     "age": 30,
##     "tags": ["admin", "ops"],
##     "addr": { "city": "NYC", "lon": -73.98, "lat": 40.75 }
##   }
##
## instead of the `VObject() / v["k"] = VString(..)` ladder. Identifiers and
## arbitrary expressions are wrapped via `toValue` overloads at runtime, so
## you can interpolate variables — `%*{ "name": userName }` works because
## `toValue(string): Value` is in scope.

import std/[macros, tables]
import glen/types

# ---- Runtime overloads used by the macro for non-literal expressions ----

proc toValue*(v: Value): Value {.inline.} =
  if v.isNil: VNull() else: v

proc toValue*(s: string): Value {.inline.} = VString(s)
proc toValue*(b: bool): Value {.inline.} = VBool(b)
proc toValue*[T: SomeInteger](i: T): Value {.inline.} = VInt(int64(i))
proc toValue*[T: SomeFloat](f: T): Value {.inline.} = VFloat(float64(f))
proc toValue*(bs: seq[byte]): Value {.inline.} = VBytes(bs)

proc toValue*[T](items: openArray[T]): Value =
  var arr: seq[Value] = newSeqOfCap[Value](items.len)
  for it in items: arr.add(toValue(it))
  VArray(arr)

proc toValue*[T](items: seq[T]): Value =
  var arr: seq[Value] = newSeqOfCap[Value](items.len)
  for it in items: arr.add(toValue(it))
  VArray(arr)

proc toValue*(t: Table[string, Value]): Value =
  result = VObject()
  for k, v in t.pairs: result[k] = v

# ---- Macro implementation ----

proc buildValueExpr(node: NimNode): NimNode =
  ## Walks a literal-shaped AST and emits Glen Value construction calls.
  ## Falls back to `toValue(expr)` for anything we don't recognise as a
  ## structural literal.
  case node.kind
  of nnkTableConstr:
    # {"k": v, ...} → VObject() with assignments
    let objSym = genSym(nskVar, "vobj")
    var stmts = newStmtList()
    stmts.add(newVarStmt(objSym, newCall(bindSym"VObject")))
    for child in node:
      if child.kind != nnkExprColonExpr:
        error("%*: object entries must be `key: value`", child)
      let keyNode = child[0]
      var keyExpr: NimNode
      case keyNode.kind
      of nnkStrLit, nnkRStrLit, nnkTripleStrLit: keyExpr = keyNode
      of nnkIdent, nnkSym: keyExpr = newLit($keyNode)
      else:
        error("%*: object keys must be string literals or identifiers, got " & $keyNode.kind, keyNode)
      let valExpr = buildValueExpr(child[1])
      stmts.add(newAssignment(
        newTree(nnkBracketExpr, objSym, keyExpr),
        valExpr
      ))
    stmts.add(objSym)
    result = newBlockStmt(stmts)
  of nnkBracket:
    # [a, b, c] → VArray(@[a', b', c'])
    var elems = newTree(nnkPrefix, ident"@", newTree(nnkBracket))
    for child in node:
      elems[1].add(buildValueExpr(child))
    result = newCall(bindSym"VArray", elems)
  of nnkStrLit, nnkRStrLit, nnkTripleStrLit:
    result = newCall(bindSym"VString", node)
  of nnkIntLit..nnkUInt64Lit:
    result = newCall(bindSym"VInt", newCall(ident"int64", node))
  of nnkFloatLit..nnkFloat64Lit:
    result = newCall(bindSym"VFloat", node)
  of nnkNilLit:
    result = newCall(bindSym"VNull")
  of nnkIdent, nnkSym:
    let s = $node
    case s
    of "true":  result = newCall(bindSym"VBool", ident"true")
    of "false": result = newCall(bindSym"VBool", ident"false")
    of "nil":   result = newCall(bindSym"VNull")
    else:
      result = newCall(bindSym"toValue", node)
  else:
    # Arbitrary expression — defer to runtime toValue overloads.
    result = newCall(bindSym"toValue", node)

macro `%*`*(body: untyped): Value =
  ## Build a Glen `Value` from a JSON-like literal.
  ##
  ## Recognised forms:
  ##   * `{}` → empty VObject
  ##   * `{"k": v, ...}` → VObject (keys may be string literals or identifiers)
  ##   * `[a, b, c]` → VArray
  ##   * string / int / float / bool / nil literals → VString / VInt / VFloat / VBool / VNull
  ##   * any other expression → `toValue(expr)` at runtime
  result = buildValueExpr(body)
