import std/[unittest, random]
import glen/simple8b

suite "simple8b: round-trip across selector classes":
  test "all-zero input uses RLE selectors":
    let n = 1000
    let zeros = newSeq[uint64](n)
    let words = encodeSimple8b(zeros)
    # 1000 / 240 = 4 full RLE words + 1 word of (240*4=960; remainder 40 zeros)
    # ~5–6 words
    check words.len <= 8
    let back = decodeSimple8b(words, n)
    check back == zeros

  test "single-bit values":
    var v: seq[uint64] = @[]
    for i in 0 ..< 1024:
      v.add(if (i and 1) == 1: 1'u64 else: 0'u64)
    let words = encodeSimple8b(v)
    let back = decodeSimple8b(words, v.len)
    check back == v

  test "wide values use 60-bit selector":
    var v: seq[uint64] = @[]
    for i in 0'u64 ..< 100'u64:
      v.add((1'u64 shl 60) - 1 - i)  # near 2^60-1
    let words = encodeSimple8b(v)
    # selector 15 packs 1 value per word, so we expect ~100 words
    check words.len == 100
    let back = decodeSimple8b(words, v.len)
    check back == v

  test "value over 2^60 raises":
    var v: seq[uint64] = @[1'u64 shl 60]
    expect S8bError:
      discard encodeSimple8b(v)

  test "mixed widths pack greedily":
    var v: seq[uint64] = @[]
    var rng = initRand(42)
    for i in 0 ..< 5000:
      v.add(uint64(rng.rand(0 .. 100)))
    let words = encodeSimple8b(v)
    let back = decodeSimple8b(words, v.len)
    check back == v
    # Sanity: 5000 small values should not need 5000 words.
    check words.len < 1000

  test "byte wrapper round-trips":
    let v = @[0'u64, 1'u64, 2'u64, 3'u64, 4'u64, 5'u64, 6'u64, 7'u64]
    let bytes = encodeSimple8bToBytes(v)
    check bytes.len mod 8 == 0
    let back = decodeSimple8bFromBytes(bytes, v.len)
    check back == v

  test "zigzag round-trip for signed deltas":
    let xs: seq[int64] = @[0'i64, -1'i64, 1'i64, -42'i64, 100_000'i64, -100_000'i64]
    var encoded: seq[uint64] = @[]
    for x in xs: encoded.add(zigzag64(x))
    let back = decodeSimple8b(encodeSimple8b(encoded), encoded.len)
    var decoded: seq[int64] = @[]
    for u in back: decoded.add(unzigzag64(u))
    check decoded == xs

  test "decode stops at n even if extra trailing words exist":
    # Force the encoder to emit a word that holds more values than we want.
    # Encoding 60 ones (selector 2) gives one word; ask for only 30 back.
    var v: seq[uint64] = @[]
    for _ in 0 ..< 60: v.add(1'u64)
    let words = encodeSimple8b(v)
    check words.len == 1
    let back = decodeSimple8b(words, 30)
    check back.len == 30
    for x in back: check x == 1'u64

  test "regular timestamp deltas (smoke: monotonic counter)":
    # 1024 timestamps spaced by exactly 60_000 ms — DoD = 0 after the first.
    var ts: seq[int64] = @[]
    for i in 0 ..< 1024:
      ts.add(int64(i) * 60_000)
    # We actually encode the deltas, then DoDs, then S8b them.
    var dods: seq[uint64] = @[]
    if ts.len >= 2:
      var prevDelta = ts[1] - ts[0]
      dods.add(zigzag64(prevDelta))   # first delta itself, encoded as if a DoD
      for i in 2 ..< ts.len:
        let delta = ts[i] - ts[i-1]
        let dod = delta - prevDelta
        dods.add(zigzag64(dod))
        prevDelta = delta
    let words = encodeSimple8b(dods)
    # 1023 zigzag values: first nonzero (the first delta), rest zero.
    # The trailing 1022 zeros should mostly RLE: 4 × 240 + leftover = ~5 words +
    # 1 word for the leading nonzero. Bound generously.
    check words.len <= 8
    let back = decodeSimple8b(words, dods.len)
    check back == dods
