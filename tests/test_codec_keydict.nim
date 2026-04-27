import std/[unittest, tables]
import glen/codec, glen/types, glen/errors

proc mkUser(name: string; age: int; status: string): Value =
  result = VObject()
  result["name"] = VString(name)
  result["age"] = VInt(int64(age))
  result["status"] = VString(status)

suite "codec key-dictionary":
  test "encodeWithDict round-trips through decodeWithDict":
    let dict = newKeyDict(@["name", "age", "status"])
    let v = mkUser("alice", 30, "active")
    let enc = encodeWithDict(v, dict)
    let dec = decodeWithDict(enc, dict)
    check dec["name"].s == "alice"
    check dec["age"].i == 30
    check dec["status"].s == "active"

  test "dict-encoded body is smaller than plain":
    let dict = newKeyDict(@["name", "age", "status"])
    let v = mkUser("alice", 30, "active")
    let plain = encode(v)
    let withDict = encodeWithDict(v, dict)
    check withDict.len < plain.len

  test "plain decode raises on dict-encoded body":
    let dict = newKeyDict(@["name", "age", "status"])
    let v = mkUser("alice", 30, "active")
    let enc = encodeWithDict(v, dict)
    expect CodecError:
      discard decode(enc)

  test "decodeWithDict reads plain bodies fine":
    # Mixed snapshot: most bodies use the dict, but a few from before the
    # dict existed could be plain TAG_OBJECT — caller must still decode them.
    let dict = newKeyDict(@["name", "age", "status"])
    let plain = encode(mkUser("bob", 25, "inactive"))
    let dec = decodeWithDict(plain, dict)
    check dec["name"].s == "bob"

  test "inline fallback for keys not in dict":
    let dict = newKeyDict(@["name", "status"])  # 'age' is NOT in dict
    let v = mkUser("alice", 30, "active")
    let enc = encodeWithDict(v, dict)
    let dec = decodeWithDict(enc, dict)
    check dec["age"].i == 30

  test "nested objects use dict recursively":
    let dict = newKeyDict(@["profile", "address", "city"])
    var v = VObject()
    v["profile"] = VObject()
    v["profile"]["address"] = VObject()
    v["profile"]["address"]["city"] = VString("SF")
    let enc = encodeWithDict(v, dict)
    let plain = encode(v)
    check enc.len < plain.len
    let dec = decodeWithDict(enc, dict)
    check dec["profile"]["address"]["city"].s == "SF"

  test "buildKeyDict respects threshold":
    var docs = initTable[string, Value]()
    for i in 0 ..< 10:
      docs["d" & $i] = mkUser("u" & $i, i, "active")
    # Threshold 5: name, age, status all appear 10× → all in dict.
    let dict = buildKeyDict(docs, threshold = 5)
    check "name" in dict.keyToId
    check "age" in dict.keyToId
    check "status" in dict.keyToId
    # Threshold 11: nothing reaches it.
    let dict2 = buildKeyDict(docs, threshold = 11)
    check dict2.idToKey.len == 0

  test "buildKeyDict orders by descending frequency":
    var docs = initTable[string, Value]()
    # 'name' appears 10×; 'rare' appears 4×; 'extras' appears 3×.
    for i in 0 ..< 10:
      var v = VObject()
      v["name"] = VString("n" & $i)
      if i < 4: v["rare"] = VString("r")
      if i < 3: v["extras"] = VString("e")
      docs["d" & $i] = v
    let dict = buildKeyDict(docs, threshold = 3)
    check dict.idToKey.len == 3
    # 'name' is most frequent → id 0.
    check dict.idToKey[0] == "name"

  test "round-trip survives mixed encoding within one snapshot":
    let dict = newKeyDict(@["name", "status"])
    var v = VObject()
    v["name"] = VString("alice")
    v["status"] = VString("active")
    v["random_one_off"] = VInt(7)
    let enc = encodeWithDict(v, dict)
    let dec = decodeWithDict(enc, dict)
    check dec["name"].s == "alice"
    check dec["status"].s == "active"
    check dec["random_one_off"].i == 7
