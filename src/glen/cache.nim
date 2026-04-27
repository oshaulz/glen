# Glen adaptive LRU cache for documents

import std/[tables, locks, hashes]
import glen/types

type
  CacheKey* = tuple[collection, docId: string]

  CacheEntry = ref object
    key: CacheKey
    value: Value
    prev, next: CacheEntry
    size: int

  CacheShard = ref object
    map: Table[CacheKey, CacheEntry]
    head, tail: CacheEntry
    capacity: int
    current: int
    lock: Lock
    hits: int
    misses: int
    puts: int
    evictions: int

  LruCache* = ref object
    shards: seq[CacheShard]
    capacity*: int
    hits*: int
    misses*: int
    puts*: int
    evictions*: int

proc makeShard(capacity: int): CacheShard =
  result = CacheShard(map: initTable[CacheKey, CacheEntry](), capacity: capacity)
  initLock(result.lock)

proc newLruCache*(capacity: int; numShards: int = 1): LruCache =
  var shardsCount = if numShards <= 0: 1 else: numShards
  result = LruCache(capacity: capacity)
  result.shards.setLen(shardsCount)
  let baseCap = capacity div shardsCount
  var rem = capacity - baseCap * shardsCount
  for i in 0 ..< shardsCount:
    let cap = baseCap + (if rem > 0: 1 else: 0)
    if rem > 0: dec rem
    result.shards[i] = makeShard(cap)

proc removeNode(c: CacheShard; e: CacheEntry) =
  if e.prev != nil: e.prev.next = e.next
  if e.next != nil: e.next.prev = e.prev
  if c.head == e: c.head = e.next
  if c.tail == e: c.tail = e.prev
  e.prev = nil; e.next = nil

proc addFront(c: CacheShard; e: CacheEntry) =
  e.next = c.head
  if c.head != nil: c.head.prev = e
  c.head = e
  if c.tail == nil: c.tail = e

proc touch(c: CacheShard; e: CacheEntry) =
  removeNode(c, e); addFront(c, e)

proc estimateSize(v: Value): int =
  case v.kind
  of vkNull: 1
  of vkBool: 1
  of vkInt: 9
  of vkFloat: 9
  of vkString: 8 + v.s.len
  of vkBytes: 8 + v.bytes.len
  of vkArray:
    var s = 8
    for it in v.arr: s += estimateSize(it)
    s
  of vkObject:
    var s = 8
    for k, vv in v.obj: s += 8 + k.len + estimateSize(vv)
    s
  of vkId: 32 + v.id.collection.len + v.id.docId.len

proc chooseShard(c: LruCache; key: CacheKey): CacheShard =
  # Combine the two hashes without joining the strings — saves the string
  # alloc and memcpy that `collection & ":" & docId` would otherwise pay
  # on every cache get/put.
  var h: Hash = 0
  h = h !& hash(key.collection)
  h = h !& hash(key.docId)
  h = !$h
  c.shards[abs(h.int) mod c.shards.len]

proc get*(c: LruCache; key: CacheKey): Value =
  let s = c.chooseShard(key)
  acquire(s.lock)
  defer: release(s.lock)
  let e = s.map.getOrDefault(key, nil)
  if e != nil:
    if e != s.head: s.touch(e)
    inc s.hits; inc c.hits
    return e.value
  inc s.misses; inc c.misses

proc put*(c: LruCache; key: CacheKey; value: Value) =
  let s = c.chooseShard(key)
  acquire(s.lock)
  defer: release(s.lock)
  let size = estimateSize(value)
  let existing = s.map.getOrDefault(key, nil)
  if existing != nil:
    s.current -= existing.size
    existing.value = value; existing.size = size
    s.current += size
    if existing != s.head: s.touch(existing)
  else:
    let e = CacheEntry(key: key, value: value, size: size)
    s.map[key] = e
    s.addFront(e)
    s.current += size
    inc s.puts; inc c.puts
  while s.current > s.capacity and s.tail != nil:
    let victim = s.tail
    s.removeNode(victim)
    s.map.del(victim.key)
    s.current -= victim.size
    inc s.evictions; inc c.evictions

proc adjustCapacity*(c: LruCache; newCap: int) =
  c.capacity = newCap
  let shardsCount = c.shards.len
  let baseCap = newCap div shardsCount
  var rem = newCap - baseCap * shardsCount
  for s in c.shards.mitems:
    acquire(s.lock)
    s.capacity = baseCap + (if rem > 0: 1 else: 0)
    if rem > 0: dec rem
    while s.current > s.capacity and s.tail != nil:
      let victim = s.tail
      s.removeNode(victim)
      s.map.del(victim.key)
      s.current -= victim.size
    release(s.lock)

proc del*(c: LruCache; key: CacheKey) =
  let s = c.chooseShard(key)
  acquire(s.lock)
  defer: release(s.lock)
  let e = s.map.getOrDefault(key, nil)
  if e != nil:
    s.removeNode(e)
    s.current -= e.size
    s.map.del(key)

type CacheShardStats* = object
  entries*: int
  current*: int
  capacity*: int
  hits*: int
  misses*: int
  puts*: int
  evictions*: int

type CacheStats* = object
  shards*: seq[CacheShardStats]
  totalCapacity*: int
  totalCurrent*: int
  hits*: int
  misses*: int
  puts*: int
  evictions*: int

proc stats*(c: LruCache): CacheStats =
  result.totalCapacity = c.capacity
  result.hits = c.hits
  result.misses = c.misses
  result.puts = c.puts
  result.evictions = c.evictions
  result.shards = @[]
  var totalCur = 0
  for s in c.shards:
    acquire(s.lock)
    let st = CacheShardStats(entries: s.map.len, current: s.current, capacity: s.capacity, hits: s.hits, misses: s.misses, puts: s.puts, evictions: s.evictions)
    release(s.lock)
    totalCur += st.current
    result.shards.add(st)
  result.totalCurrent = totalCur
