## Simple-8b integer codec.
##
## Packs a stream of unsigned 60-bit integers into 64-bit words. Each word has
## a 4-bit selector at the top, choosing one of 16 patterns:
##
##   sel | n   | bits per value | notes
##   ----+-----+----------------+----------------------------
##    0  | 240 |  0             | RLE: 240 zeros
##    1  | 120 |  0             | RLE: 120 zeros
##    2  |  60 |  1
##    3  |  30 |  2
##    4  |  20 |  3
##    5  |  15 |  4
##    6  |  12 |  5
##    7  |  10 |  6
##    8  |   8 |  7
##    9  |   7 |  8             | unused 4 bits in payload
##   10  |   6 | 10
##   11  |   5 | 12
##   12  |   4 | 15
##   13  |   3 | 20
##   14  |   2 | 30
##   15  |   1 | 60
##
## Encoder strategy: greedy "pack as many as possible into each word", with the
## RLE selectors used when there's a run of zeros at the front of what's left.
## This is the standard greedy S8b that papers refer to; it's a tiny bit
## suboptimal vs LP but ~one-pass and very fast.
##
## All values must fit in 60 bits (i.e. value < 1 shl 60). Caller is
## responsible for using zigzag for signed inputs.

type
  S8bError* = object of CatchableError

const
  Selector0Run = 240
  Selector1Run = 120
  # Greedy table: (n, bits) for selectors 2..15.
  packTable: array[14, (int, int)] = [
    (60,  1), (30,  2), (20,  3), (15,  4), (12,  5),
    (10,  6), ( 8,  7), ( 7,  8), ( 6, 10), ( 5, 12),
    ( 4, 15), ( 3, 20), ( 2, 30), ( 1, 60)
  ]
  # Lookup by selector index (0..15).
  selectorN:    array[16, int] = [240, 120, 60, 30, 20, 15, 12, 10, 8, 7, 6, 5, 4, 3, 2, 1]
  selectorBits: array[16, int] = [  0,   0,  1,  2,  3,  4,  5,  6, 7, 8,10,12,15,20,30,60]

proc bitsRequired*(v: uint64): int {.inline.} =
  ## Position of the highest set bit, +1. 0 returns 0.
  if v == 0: return 0
  var x = v
  result = 0
  while x != 0:
    inc result
    x = x shr 1

proc canPack(values: openArray[uint64]; start, n, bits: int): bool {.inline.} =
  if start + n > values.len: return false
  let limit = if bits >= 64: high(uint64) else: (1'u64 shl bits) - 1
  for i in start ..< start + n:
    if values[i] > limit: return false
  true

proc encodeWord(values: openArray[uint64]; start: int): (uint64, int) =
  ## Pack the next chunk of `values` starting at `start`. Returns
  ## (encodedWord, valuesConsumed).
  ##
  ## RLE selectors (0, 1) are used when the prefix is exactly Selector{0,1}Run
  ## consecutive zeros — this is the standard greedy variant. Otherwise pick
  ## the (n, bits) row that consumes the most values without overflow.
  # RLE-240
  if start + Selector0Run <= values.len:
    var allZero = true
    for i in start ..< start + Selector0Run:
      if values[i] != 0'u64: allZero = false; break
    if allZero:
      return (0'u64 shl 60, Selector0Run)
  # RLE-120
  if start + Selector1Run <= values.len:
    var allZero = true
    for i in start ..< start + Selector1Run:
      if values[i] != 0'u64: allZero = false; break
    if allZero:
      return (1'u64 shl 60, Selector1Run)
  # Standard 14 packings, prefer the one that consumes the most values.
  for sel in 2 .. 15:
    let n = selectorN[sel]
    let bits = selectorBits[sel]
    if canPack(values, start, n, bits):
      var word = uint64(sel) shl 60
      var shift = 0
      for i in 0 ..< n:
        word = word or (values[start + i] shl shift)
        shift += bits
      return (word, n)
  raise newException(S8bError, "Simple-8b: value > 2^60 cannot be packed")

proc encodeSimple8b*(values: openArray[uint64]): seq[uint64] =
  ## Encode values into a sequence of 64-bit words. Caller writes them as
  ## little-endian bytes (or any agreed format).
  result = @[]
  var i = 0
  while i < values.len:
    let (word, consumed) = encodeWord(values, i)
    result.add(word)
    i += consumed

proc decodeSimple8b*(words: openArray[uint64]; n: int): seq[uint64] =
  ## Decode `n` values out of `words`. Stops when n values have been produced;
  ## extra trailing words are ignored. Raises if the stream runs out early.
  result = newSeqOfCap[uint64](n)
  for w in words:
    let sel = int((w shr 60) and 0xF'u64)
    let count = selectorN[sel]
    let bits = selectorBits[sel]
    if bits == 0:
      # RLE: emit `count` zeros, capped at remaining required.
      var k = count
      if result.len + k > n: k = n - result.len
      for _ in 0 ..< k: result.add(0'u64)
    else:
      let mask = (1'u64 shl bits) - 1
      for j in 0 ..< count:
        if result.len >= n: return
        result.add((w shr (j * bits)) and mask)
    if result.len >= n: return
  if result.len < n:
    raise newException(S8bError, "Simple-8b: stream ended before producing " & $n & " values")

# -------- Helpers for typical use: zigzag-encoded signed deltas --------

proc zigzag64*(x: int64): uint64 {.inline.} =
  ## Standard zigzag mapping: 0 → 0, -1 → 1, 1 → 2, -2 → 3, ...
  cast[uint64]((x shl 1) xor (x shr 63))

proc unzigzag64*(u: uint64): int64 {.inline.} =
  cast[int64]((u shr 1) xor (0'u64 - (u and 1)))

# -------- Bytes wrapper (for embedding in arbitrary streams) --------
#
# We write words as little-endian uint64s. Decoding requires knowing how many
# values were encoded (separately stored).

proc encodeSimple8bToBytes*(values: openArray[uint64]): seq[byte] =
  let words = encodeSimple8b(values)
  result = newSeq[byte](words.len * 8)
  for i, w in words:
    var v = w
    for j in 0 ..< 8:
      result[i * 8 + j] = byte(v and 0xFF)
      v = v shr 8

proc decodeSimple8bFromBytes*(buf: openArray[byte]; n: int): seq[uint64] =
  let nWords = buf.len div 8
  var words = newSeq[uint64](nWords)
  for i in 0 ..< nWords:
    var w: uint64 = 0
    for j in 0 ..< 8:
      w = w or (uint64(buf[i * 8 + j]) shl (j * 8))
    words[i] = w
  decodeSimple8b(words, n)

