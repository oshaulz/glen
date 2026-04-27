## Microbenchmark for the bit-decode hot path.
##
## Times pure decoder calls on pre-encoded payloads. Output is intended for
## A/B comparison before/after SIMD or scalar-batching optimisations.

import std/[times, monotimes, math, random, strutils]
import glen/bitpack
import glen/simple8b

template timed(label: string; iters: int; body: untyped): float64 =
  let t0 = getMonoTime()
  for _ in 0 ..< iters:
    body
  let dur = (getMonoTime() - t0).inNanoseconds.float64 / 1e9
  let nsPerIter = 1e9 * dur / float64(iters)
  echo label.alignLeft(36), " ", $iters, " iters, ", formatFloat(dur, ffDecimal, 4), " s, ", formatFloat(nsPerIter, ffDecimal, 1), " ns/iter"
  dur

template timed_s8b(label: string; iters: int; words: seq[uint64]; n: int): float64 =
  let t0 = getMonoTime()
  for _ in 0 ..< iters:
    let r = decodeSimple8b(words, n)
    doAssert r.len == n
  let dur = (getMonoTime() - t0).inNanoseconds.float64 / 1e9
  let nsPerIter = 1e9 * dur / float64(iters)
  echo label.alignLeft(36), " ", $iters, " iters, ", formatFloat(dur, ffDecimal, 4), " s, ", formatFloat(nsPerIter, ffDecimal, 1), " ns/iter"
  dur

# ----- benchmark fixtures -----

proc encodeXorStream(values: seq[float64]): (seq[byte], int) =
  ## Encode a sequence of float64s as a v0 + (n-1) Gorilla XOR stream.
  var w = newBitWriter()
  if values.len == 0: return (w.bytes, w.bitLen)
  w.writeBitsU64(cast[uint64](values[0]), 64)
  var xstate: XorState
  var prev = cast[uint64](values[0])
  for i in 1 ..< values.len:
    let bits = cast[uint64](values[i])
    encodeXor(w, xstate, prev xor bits)
    prev = bits
  (w.bytes, w.bitLen)

proc decodeXorStream(payload: seq[byte]; bitLen, n: int): seq[float64] =
  result = newSeq[float64](n)
  var r = newBitReader(payload, bitLen)
  if n <= 0: return
  let bits0 = r.readBitsU64(64)
  result[0] = cast[float64](bits0)
  var xstate: XorState
  var prev = bits0
  for i in 1 ..< n:
    let xord = decodeXor(r, xstate)
    prev = prev xor xord
    result[i] = cast[float64](prev)

proc encodeDoDStream(values: seq[int64]): (seq[byte], int) =
  var w = newBitWriter()
  if values.len < 2: return (w.bytes, w.bitLen)
  w.writeBitsU64(zigzag(values[1] - values[0]) and 0xFFFFFFFF'u64, 32)
  var prevDelta = values[1] - values[0]
  for i in 2 ..< values.len:
    let delta = values[i] - values[i - 1]
    let dod = delta - prevDelta
    encodeDoD(w, dod)
    prevDelta = delta
  (w.bytes, w.bitLen)

proc decodeDoDStream(payload: seq[byte]; bitLen, n: int; startTs: int64): seq[int64] =
  result = newSeq[int64](n)
  if n <= 0: return
  result[0] = startTs
  if n < 2: return
  var r = newBitReader(payload, bitLen)
  let firstDelta = unzigzag(r.readBitsU64(32))
  result[1] = startTs + firstDelta
  var prevDelta = firstDelta
  for i in 2 ..< n:
    let dod = decodeDoD(r)
    let delta = prevDelta + dod
    result[i] = result[i - 1] + delta
    prevDelta = delta

# ----- workloads -----

proc smoothFloats(n: int; seed: int = 1): seq[float64] =
  var rng = initRand(seed)
  result = newSeq[float64](n)
  var v = 273.15
  for i in 0 ..< n:
    v += rng.gauss(0.0, 0.1)
    result[i] = v

proc constFloats(n: int; v = 0.0): seq[float64] =
  result = newSeq[float64](n)
  for i in 0 ..< n: result[i] = v

proc regularTimestamps(n: int; cadenceMs: int64 = 60_000): seq[int64] =
  result = newSeq[int64](n)
  for i in 0 ..< n: result[i] = int64(i) * cadenceMs

proc irregularTimestamps(n: int; seed = 7): seq[int64] =
  var rng = initRand(seed)
  result = newSeq[int64](n)
  var t: int64 = 0
  for i in 0 ..< n:
    t += int64(rng.rand(10..120_000))
    result[i] = t

# ----- entry point -----

proc main() =
  echo "------- decodeXor (Gorilla, float64 streams) -------"
  block:
    let n = 4096
    let xs = smoothFloats(n)
    let (payload, bitLen) = encodeXorStream(xs)
    echo "smooth(4096):              encoded ", payload.len, " B / ", bitLen, " bits = ",
      formatFloat(bitLen.float64 / float64(n), ffDecimal, 2), " bits/value"
    discard timed("decodeXor smooth(4096)", 5_000, (let _ = decodeXorStream(payload, bitLen, n); discard))
  block:
    let n = 4096
    let xs = constFloats(n)
    let (payload, bitLen) = encodeXorStream(xs)
    echo "constant(4096):            encoded ", payload.len, " B / ", bitLen, " bits = ",
      formatFloat(bitLen.float64 / float64(n), ffDecimal, 2), " bits/value"
    discard timed("decodeXor constant(4096)", 50_000, (let _ = decodeXorStream(payload, bitLen, n); discard))
  block:
    var rng = initRand(13)
    let n = 4096
    var xs = newSeq[float64](n)
    for i in 0 ..< n: xs[i] = rng.gauss(0.0, 1.0) # high-entropy
    let (payload, bitLen) = encodeXorStream(xs)
    echo "noisy(4096):               encoded ", payload.len, " B / ", bitLen, " bits = ",
      formatFloat(bitLen.float64 / float64(n), ffDecimal, 2), " bits/value"
    discard timed("decodeXor noisy(4096)", 5_000, (let _ = decodeXorStream(payload, bitLen, n); discard))

  echo ""
  echo "------- decodeDoD (timestamps) -------"
  block:
    let ts = regularTimestamps(4096)
    let (payload, bitLen) = encodeDoDStream(ts)
    echo "regular(4096):             encoded ", payload.len, " B / ", bitLen, " bits = ",
      formatFloat(bitLen.float64 / float64(ts.len - 1), ffDecimal, 2), " bits/delta"
    discard timed("decodeDoD regular(4096)", 50_000, (let _ = decodeDoDStream(payload, bitLen, ts.len, 0); discard))
  block:
    let ts = irregularTimestamps(4096)
    let (payload, bitLen) = encodeDoDStream(ts)
    echo "irregular(4096):           encoded ", payload.len, " B / ", bitLen, " bits = ",
      formatFloat(bitLen.float64 / float64(ts.len - 1), ffDecimal, 2), " bits/delta"
    discard timed("decodeDoD irregular(4096)", 5_000, (let _ = decodeDoDStream(payload, bitLen, ts.len, 0); discard))

  echo ""
  echo "------- decodeSimple8b -------"
  block:
    let n = 4096
    var dods: seq[uint64] = newSeq[uint64](n)
    # All zeros — best case for RLE selectors.
    let words = encodeSimple8b(dods)
    echo "all-zero(4096):            encoded ", words.len, " words = ",
      formatFloat(float64(words.len * 64) / float64(n), ffDecimal, 4), " bits/value"
    discard timed_s8b("decodeSimple8b all-zero(4096)", 100_000, words, n)
  block:
    var rng = initRand(17)
    let n = 4096
    var dods = newSeq[uint64](n)
    for i in 0 ..< n: dods[i] = uint64(rng.rand(0..255))
    let words = encodeSimple8b(dods)
    echo "small(4096):               encoded ", words.len, " words = ",
      formatFloat(float64(words.len * 64) / float64(n), ffDecimal, 4), " bits/value"
    discard timed_s8b("decodeSimple8b small(4096)", 50_000, words, n)
  block:
    var rng = initRand(19)
    let n = 4096
    var dods = newSeq[uint64](n)
    for i in 0 ..< n: dods[i] = uint64(rng.rand(0..int(uint32.high)))
    let words = encodeSimple8b(dods)
    echo "wide(4096):                encoded ", words.len, " words = ",
      formatFloat(float64(words.len * 64) / float64(n), ffDecimal, 4), " bits/value"
    discard timed_s8b("decodeSimple8b wide(4096)", 20_000, words, n)

  echo ""
  echo "------- decodeXorRun (clz zero-run skip) vs per-call -------"
  block:
    # Constant stream (every XOR is 0): per-call does N readBits (1 each).
    # decodeXorRun bulk-skips runs via clz. Should be a real win.
    let n = 4096
    let xs = constFloats(n)
    let (payload, bitLen) = encodeXorStream(xs)
    proc decodePerCall: seq[float64] =
      result = newSeq[float64](n)
      var r = newBitReader(payload, bitLen)
      let bits0 = r.readBitsU64(64)
      result[0] = cast[float64](bits0)
      var xstate: XorState
      var prev = bits0
      for i in 1 ..< n:
        let xord = decodeXor(r, xstate)
        prev = prev xor xord
        result[i] = cast[float64](prev)
    proc decodeRun: seq[float64] =
      result = newSeq[float64](n)
      var r = newBitReader(payload, bitLen)
      let bits0 = r.readBitsU64(64)
      result[0] = cast[float64](bits0)
      var xstate: XorState
      let xors = decodeXorRun(r, xstate, n - 1)
      var prev = bits0
      for i in 0 ..< (n - 1):
        prev = prev xor xors[i]
        result[i + 1] = cast[float64](prev)
    discard timed("constant: per-call decodeXor",  50_000, (let _ = decodePerCall(); discard))
    discard timed("constant: decodeXorRun (clz)",  50_000, (let _ = decodeRun(); discard))
  block:
    # Smooth stream (mostly non-zero XORs): no zero-run benefit; should be
    # roughly equal or slightly slower for the run variant due to alloc.
    let n = 4096
    let xs = smoothFloats(n)
    let (payload, bitLen) = encodeXorStream(xs)
    proc decodePerCall: seq[float64] =
      result = newSeq[float64](n)
      var r = newBitReader(payload, bitLen)
      let bits0 = r.readBitsU64(64)
      result[0] = cast[float64](bits0)
      var xstate: XorState
      var prev = bits0
      for i in 1 ..< n:
        let xord = decodeXor(r, xstate)
        prev = prev xor xord
        result[i] = cast[float64](prev)
    proc decodeRun: seq[float64] =
      result = newSeq[float64](n)
      var r = newBitReader(payload, bitLen)
      let bits0 = r.readBitsU64(64)
      result[0] = cast[float64](bits0)
      var xstate: XorState
      let xors = decodeXorRun(r, xstate, n - 1)
      var prev = bits0
      for i in 0 ..< (n - 1):
        prev = prev xor xors[i]
        result[i + 1] = cast[float64](prev)
    discard timed("smooth: per-call decodeXor",    5_000, (let _ = decodePerCall(); discard))
    discard timed("smooth: decodeXorRun (clz)",    5_000, (let _ = decodeRun(); discard))

  echo ""
  echo "------- bit primitives (clz/ctz hot path) -------"
  block:
    var rng = initRand(23)
    var inputs = newSeq[uint64](100_000)
    for i in 0 ..< inputs.len: inputs[i] = uint64(rng.rand(int.high))
    let n = inputs.len
    discard timed("countLeadingZeros64", 100, (block:
      var s = 0
      for x in inputs: s += countLeadingZeros64(x)
      doAssert s >= 0))
    discard timed("countTrailingZeros64", 100, (block:
      var s = 0
      for x in inputs: s += countTrailingZeros64(x)
      doAssert s >= 0))

main()
