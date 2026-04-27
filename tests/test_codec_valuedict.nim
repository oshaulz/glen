import std/[unittest, tables]
import glen/codec, glen/types, glen/errors

proc mkUser(name: string; status: string; region: string): Value =
  result = VObject()
  result["name"]   = VString(name)
  result["status"] = VString(status)
  result["region"] = VString(region)

suite "codec value-dictionary":
  test "encodeWithDict round-trips with value dict":
    let dict = newKeyDict(@["name", "status", "region"])
    dict.valueDictByField["status"] = newValueDict(@["active", "inactive"])
    dict.valueDictByField["region"] = newValueDict(@["us-east", "eu-west"])
    let v = mkUser("alice", "active", "us-east")
    let enc = encodeWithDict(v, dict)
    let dec = decodeWithDict(enc, dict)
    check dec["name"].s == "alice"
    check dec["status"].s == "active"
    check dec["region"].s == "us-east"

  test "value-dict-encoded body is smaller than key-dict alone":
    let keysOnly = newKeyDict(@["name", "status", "region"])
    let withVals = newKeyDict(@["name", "status", "region"])
    withVals.valueDictByField["status"] = newValueDict(@["active", "inactive", "pending"])
    withVals.valueDictByField["region"] = newValueDict(@["us-east", "eu-west"])
    # Pick a doc where every value field is dictionarised.
    let v = mkUser("alice", "active", "us-east")
    let withK = encodeWithDict(v, keysOnly)
    let withV = encodeWithDict(v, withVals)
    check withV.len < withK.len

  test "values not in the dict fall back to inline string":
    let dict = newKeyDict(@["name", "status"])
    dict.valueDictByField["status"] = newValueDict(@["active", "inactive"])
    # 'name' is not in valueDict; "alice" doesn't get dictionarised.
    let v = mkUser("alice", "active", "")  # region empty, ignored
    let enc = encodeWithDict(v, dict)
    let dec = decodeWithDict(enc, dict)
    check dec["name"].s == "alice"

  test "rare values fall back to inline (status not in vd entries)":
    let dict = newKeyDict(@["status"])
    dict.valueDictByField["status"] = newValueDict(@["active", "inactive"])
    var v = VObject()
    v["status"] = VString("oddball-not-in-dict")
    let enc = encodeWithDict(v, dict)
    let dec = decodeWithDict(enc, dict)
    check dec["status"].s == "oddball-not-in-dict"

  test "populateValueDicts respects threshold":
    var docs = initTable[string, Value]()
    for i in 0 ..< 10:
      docs["d" & $i] = mkUser("u" & $i, "active", "us-east")
    docs["odd"] = mkUser("z", "rare-status", "eu-west")
    let dict = newKeyDict(@["name", "status", "region"])
    populateValueDicts(dict, docs, threshold = 5)
    # 'active' appeared 10× → in dict; 'rare-status' once → not in dict.
    check "status" in dict.valueDictByField
    check "active" in dict.valueDictByField["status"].valToId
    check "rare-status" notin dict.valueDictByField["status"].valToId
    # 'us-east' appeared 10× → in dict; 'eu-west' once → not in dict.
    check "us-east" in dict.valueDictByField["region"].valToId
    check "eu-west" notin dict.valueDictByField["region"].valToId

  test "field with no high-cardinality values gets no value dict":
    var docs = initTable[string, Value]()
    for i in 0 ..< 5:
      docs["d" & $i] = mkUser("u" & $i, "uniq-" & $i, "us-east")
    let dict = newKeyDict(@["name", "status", "region"])
    populateValueDicts(dict, docs, threshold = 3)
    # Every status value appears once → no entries crossed threshold.
    check "status" notin dict.valueDictByField
    # region appears 5× → it is dictionarised.
    check "region" in dict.valueDictByField

  test "TAG_STRING_DICT outside TAG_OBJECT_DICT raises":
    # Construct a payload that starts with TAG_STRING_DICT directly.
    let dict = newKeyDict(@[])
    dict.valueDictByField["x"] = newValueDict(@["a"])
    # Hand-craft a malformed body: just TAG_STRING_DICT byte + varuint(0).
    let bad = "\x0B\x00"
    expect CodecError:
      discard decodeWithDict(bad, dict)

  test "nested object's child uses its own field's value dict":
    var inner = VObject()
    inner["status"] = VString("active")
    var outer = VObject()
    outer["info"] = inner
    let dict = newKeyDict(@["info", "status"])
    dict.valueDictByField["status"] = newValueDict(@["active"])
    let enc = encodeWithDict(outer, dict)
    let dec = decodeWithDict(enc, dict)
    check dec["info"]["status"].s == "active"
