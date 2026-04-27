import std/unittest
import glen/cache
import glen/types

suite "cache behavior":
  test "eviction under capacity pressure":
    var c = newLruCache(64)
    c.put(("c", "k1"), VString("aaaaaaaaaa"))
    c.put(("c", "k2"), VString("bbbbbbbbbb"))
    c.put(("c", "k3"), VString("cccccccccc"))
    discard c.get(("c", "k3"))
    c.put(("c", "k4"), VString("dddddddddd"))
    let g1 = c.get(("c", "k1"))
    let g3 = c.get(("c", "k3"))
    check g1.isNil
    check not g3.isNil
