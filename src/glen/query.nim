## Query layer types + helpers, free of any GlenDB dependency. The actual
## bound Query (.run / .runStream) lives in glen/db so it can dispatch to
## indexes; this module is the predicate AST + evaluator shared by both.

import std/[strutils, algorithm, base64]
import glen/types
import glen/index

type
  QueryOp* = enum
    qoEq, qoNe, qoLt, qoLte, qoGt, qoGte, qoIn, qoContains

  QueryPredicate* = object
    op*:     QueryOp
    field*:  string                # dotted path
    value*:  Value                 # for eq/ne/lt/lte/gt/gte/contains
    values*: seq[Value]            # for `in`

  QueryOrder* = object
    field*:  string
    asc*:    bool

# ---- Predicate evaluation ----

proc fieldPath*(s: string): seq[string] =
  result = @[]
  for p in s.split('.'):
    if p.len > 0: result.add(p)

proc compareValues*(a, b: Value): int =
  ## Numeric and string compare; returns -1/0/1. Mismatched types compare via
  ## kind ordinal so the sort is total.
  if a.isNil and b.isNil: return 0
  if a.isNil: return -1
  if b.isNil: return 1
  if a.kind != b.kind: return cmp(ord(a.kind), ord(b.kind))
  case a.kind
  of vkInt:    cmp(a.i, b.i)
  of vkFloat:  cmp(a.f, b.f)
  of vkString: cmp(a.s, b.s)
  of vkBool:   cmp(int(a.b), int(b.b))
  of vkNull:   0
  else:        cmp($a, $b)

proc valuesEqual*(a, b: Value): bool =
  ## Equality with "nullish" coalescing: a missing field (nil ref) and
  ## an explicit `VNull()` are considered equal, so `where: x == nil`
  ## matches both "field absent" and "field set to null". This is the
  ## intuitive behaviour for soft-delete tombstones, optional fields,
  ## and other "is this empty?" predicates.
  let aNullish = a.isNil or a.kind == vkNull
  let bNullish = b.isNil or b.kind == vkNull
  if aNullish or bNullish: return aNullish and bNullish
  a == b

proc evalPredicate*(p: QueryPredicate; doc: Value): bool =
  let v = extractField(doc, fieldPath(p.field))
  case p.op
  of qoEq: valuesEqual(v, p.value)
  of qoNe: not valuesEqual(v, p.value)
  of qoLt:  compareValues(v, p.value) <  0
  of qoLte: compareValues(v, p.value) <= 0
  of qoGt:  compareValues(v, p.value) >  0
  of qoGte: compareValues(v, p.value) >= 0
  of qoIn:
    var hit = false
    for x in p.values:
      if valuesEqual(v, x): hit = true; break
    hit
  of qoContains:
    if v.isNil or v.kind != vkString or p.value.isNil or p.value.kind != vkString:
      false
    else:
      v.s.contains(p.value.s)

proc evalAll*(preds: seq[QueryPredicate]; doc: Value): bool =
  for p in preds:
    if not evalPredicate(p, doc): return false
  true

# ---- Cursor encode / decode (opaque) ----

proc encodeCursor*(lastDocId: string): string =
  encode(lastDocId)

proc decodeCursor*(c: string): string =
  if c.len == 0: return ""
  try: result = decode(c)
  except CatchableError: result = ""

# ---- Sort + slice helpers ----

proc sortDocs*(rows: var seq[(string, Value)]; orders: seq[QueryOrder]) =
  if orders.len == 0: return
  rows.sort(proc (a, b: (string, Value)): int =
    for o in orders:
      let av = extractField(a[1], fieldPath(o.field))
      let bv = extractField(b[1], fieldPath(o.field))
      let c = compareValues(av, bv)
      if c != 0:
        return if o.asc: c else: -c
    cmp(a[0], b[0]))   # stable secondary by docId

proc applyCursorAndLimit*(rows: seq[(string, Value)]; cursor: string;
                          limitN: int): seq[(string, Value)] =
  result = @[]
  let lastSeen = decodeCursor(cursor)
  var skipping = lastSeen.len > 0
  for (id, doc) in rows:
    if skipping:
      if id == lastSeen: skipping = false
      continue
    result.add((id, doc))
    if limitN > 0 and result.len >= limitN: break
