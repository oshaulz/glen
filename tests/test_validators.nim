import std/[options, unittest]

import glen/types
import glen/validators

let UserSchema = zobject:
  name: zString().trim().minLen(2)
  age: zInt().gte(0).lte(120)
  tags: zArray(zString().minLen(1)).optional()
  admin: zBool().default(false)
  status: zEnum(["active", "disabled"]).default("active")

let SearchSchema = zUnion(
  zString().trim(),
  zInt().map("stringify", proc (value: int64): string = $value)
)

suite "validator schemas":
  test "object success with defaults":
    var doc = VObject()
    doc["name"] = VString("  Terra ")
    doc["age"] = VInt(32)
    doc["tags"] = VArray(@[VString("nim"), VString("db")])

    let result = UserSchema.parse(doc)
    check result.ok
    check result.value.name == "Terra"
    check result.value.admin == false
    check result.value.status == "active"
    check result.value.tags.isSome
    check result.value.tags.get().len == 2

  test "missing optional fields stay none":
    var doc = VObject()
    doc["name"] = VString("Delta")
    doc["age"] = VInt(9)

    let result = UserSchema.parse(doc)
    check result.ok
    check result.value.tags.isNone
    check result.value.admin == false

  test "detailed issues bubble up":
    var doc = VObject()
    doc["name"] = VString("Z")
    doc["age"] = VInt(-1)
    doc["tags"] = VArray(@[VString(""), VString("ok")])

    let result = UserSchema.parse(doc)
    check not result.ok
    var nameIssue = false
    var ageIssue = false
    var tagIssue = false
    for issue in result.issues:
      let pathStr = describePath(issue.path)
      if pathStr == "$.name": nameIssue = true
      if pathStr == "$.age": ageIssue = true
      if pathStr == "$.tags.0": tagIssue = true
    check nameIssue
    check ageIssue
    check tagIssue

  test "union coercion to string":
    let fromInt = SearchSchema.parse(VInt(42))
    check fromInt.ok
    check fromInt.value == "42"

    let fromString = SearchSchema.parse(VString("abc"))
    check fromString.ok
    check fromString.value == "abc"

