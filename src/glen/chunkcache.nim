# Generic decoded-chunk LRU.
#
# Used by `glen/timeseries` and `glen/tilestack` to avoid re-decoding the same
# bit-packed chunk on repeated reads. Both engines have query patterns where a
# chunk gets touched many times in a row (e.g. `latest()` repeatedly reads the
# tail chunk; `readPointHistory` walks every chunk for one cell).
#
# Implementation: OrderedTable preserves insertion order, so we get FIFO-style
# eviction. On hit we delete + reinsert to bump the entry to the back; on
# insert-when-full we pop the front. O(1) amortised; small constants.

import std/[tables, locks]
export tables   # so generic instantiations in caller modules can find pairs/keys

type
  ChunkCache*[K, V] = ref object
    data: OrderedTable[K, V]
    maxEntries: int
    lock: Lock

proc newChunkCache*[K, V](maxEntries: int): ChunkCache[K, V] =
  doAssert maxEntries >= 1
  result = ChunkCache[K, V](
    data: initOrderedTable[K, V](),
    maxEntries: maxEntries)
  initLock(result.lock)

proc get*[K, V](c: ChunkCache[K, V]; key: K): (bool, V) =
  acquire(c.lock)
  defer: release(c.lock)
  if key notin c.data:
    var empty: V
    return (false, empty)
  let v = c.data[key]
  # Bump to back: del then reinsert
  c.data.del(key)
  c.data[key] = v
  (true, v)

proc put*[K, V](c: ChunkCache[K, V]; key: K; value: V) =
  acquire(c.lock)
  defer: release(c.lock)
  if key in c.data:
    c.data.del(key)
  c.data[key] = value
  # Evict oldest if over capacity. OrderedTable iterates in insertion order.
  while c.data.len > c.maxEntries:
    var firstKey: K
    var found = false
    for k, _ in c.data:
      firstKey = k
      found = true
      break
    if not found: break
    c.data.del(firstKey)

proc invalidate*[K, V](c: ChunkCache[K, V]; key: K) =
  acquire(c.lock)
  defer: release(c.lock)
  if key in c.data: c.data.del(key)

proc clear*[K, V](c: ChunkCache[K, V]) =
  acquire(c.lock)
  defer: release(c.lock)
  c.data.clear()

proc len*[K, V](c: ChunkCache[K, V]): int =
  acquire(c.lock)
  defer: release(c.lock)
  c.data.len
