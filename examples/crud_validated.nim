## CRUD example with schema validation.
##
## Demonstrates create / read / update / delete against a Glen collection,
## with every write gated by a Zod-style validator schema.
##
## Run with:
##   nim c -r --path:src examples/crud_validated.nim

import std/[options, os, strformat]

import glen/glen
import glen/types, glen/db as glendb
import glen/validators

let dir = getCurrentDir() / "crud_example_db"
removeDir(dir)
let database = newGlenDB(dir)

let UserSchema = zobject:
  name: zString().trim().minLen(2).maxLen(64)
  age: zInt().gte(0).lte(150)
  email: zString().trim().minLen(3)
  role: zEnum(["admin", "member", "guest"]).default("member")
  active: zBool().default(true)

proc mkUser(name: string; age: int64; email: string; role = ""): Value =
  result = VObject()
  result["name"] = VString(name)
  result["age"] = VInt(age)
  result["email"] = VString(email)
  if role.len > 0:
    result["role"] = VString(role)

proc createUser(id: string; doc: Value): bool =
  ## Validate then persist. Returns false (and prints issues) on failure.
  let res = UserSchema.parse(doc)
  if not res.ok:
    echo &"create users/{id} rejected:"
    for issue in res.issues:
      echo &"  {describePath(issue.path)}: {issue.message}"
    return false
  database.put("users", id, doc)
  echo &"create users/{id} ok"
  true

proc readUser(id: string): Value =
  result = database.get("users", id)
  if result.isNil:
    echo &"read users/{id} -> not found"
  else:
    echo &"read users/{id} -> {result}"

proc updateUser(id: string; mutate: proc (v: Value)): bool =
  let current = database.get("users", id)
  if current.isNil:
    echo &"update users/{id} rejected: not found"
    return false
  mutate(current)
  let res = UserSchema.parse(current)
  if not res.ok:
    echo &"update users/{id} rejected:"
    for issue in res.issues:
      echo &"  {describePath(issue.path)}: {issue.message}"
    return false
  database.put("users", id, current)
  echo &"update users/{id} ok"
  true

proc deleteUser(id: string) =
  database.delete("users", id)
  echo &"delete users/{id} ok"

# --- Create ----------------------------------------------------------------
discard createUser("u1", mkUser("  Alice  ", 30, "alice@example.com", "admin"))
discard createUser("u2", mkUser("Bob", 25, "bob@example.com"))

# Validation failure: name too short, age out of range, bad role.
let bad = mkUser("X", 999, "carol@example.com", "wizard")
discard createUser("u3", bad)

# --- Read ------------------------------------------------------------------
discard readUser("u1")
discard readUser("u2")
discard readUser("missing")

# --- Update ----------------------------------------------------------------
discard updateUser("u2", proc (v: Value) =
  v["age"] = VInt(26)
  v["role"] = VString("admin")
)

# Update that fails validation: blank email.
discard updateUser("u1", proc (v: Value) =
  v["email"] = VString("")
)

# --- Delete ----------------------------------------------------------------
deleteUser("u2")
discard readUser("u2")

database.snapshotAll()
