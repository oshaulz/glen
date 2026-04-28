# Utility helpers for Glen
import std/[os, sysrand, times]

proc nowMillis*(): int64 =
  (epochTime() * 1000).int64

proc ensureDir*(path: string) =
  if not dirExists(path): createDir(path)

# ---- ULID-style id generator ---------------------------------------------
#
# 26 characters of Crockford base32. Layout (matches the ULID spec):
#   * 10 chars: milliseconds since UNIX epoch, big-endian
#   * 16 chars: 80 bits of randomness, drawn from the OS RNG
#
# Properties we care about:
#   * Lexicographically sortable — string-compare ≈ time-of-insert order.
#   * URL-safe (no ambiguous chars: I / L / O / U are excluded).
#   * Multi-master safe: 80 bits of cryptographic entropy per id makes
#     collisions vanishingly unlikely even across nodes / threads /
#     forked processes inserting in the same ms.
#
# The randomness comes from `std/sysrand`'s `urandom`, which reads
# `/dev/urandom` on POSIX and `BCryptGenRandom` on Windows. This avoids
# a real failure mode of PRNG-based ULIDs: two processes seeded from the
# same nanosecond timestamp (same-ns fork, container snapshot+clone)
# producing identical id streams. The OS RNG state is never visible to
# user space, so clones can't reproduce it.

const Crockford = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"

var ulidLastMs {.threadvar.}: int64
var ulidLastSuffix {.threadvar.}: array[16, char]

proc encodeBase32(value: uint64; nChars: int): string =
  result = newString(nChars)
  var v = value
  for i in countdown(nChars - 1, 0):
    result[i] = Crockford[int(v and 0x1F'u64)]
    v = v shr 5

proc bumpSuffix(suffix: var array[16, char]) =
  ## Treat the 16-char suffix as a base-32 number and add 1, with carry.
  ## Wraps to all-zero on overflow (~2^80 ids in the same ms — never).
  for i in countdown(15, 0):
    let idx = Crockford.find(suffix[i])
    if idx < 31:
      suffix[i] = Crockford[idx + 1]
      return
    suffix[i] = Crockford[0]   # carry

proc randomSuffix(buf: var array[16, char]) =
  ## Fill `buf` with 16 Crockford-base32 chars from the OS RNG. Each
  ## char is uniformly distributed over 32 values → 80 bits of entropy
  ## total. Reads 16 bytes and masks the low 5 bits of each (uniform
  ## even from a non-uniform byte source, since 32 is a power of 2).
  var bytes: array[16, byte]
  if not urandom(bytes):
    raise newException(IOError, "newId: OS RNG (sysrand.urandom) failed")
  for i in 0 ..< 16:
    buf[i] = Crockford[int(bytes[i]) and 0x1F]

proc newId*(): string =
  ## Fresh ULID-style identifier: 26 chars, sortable, URL-safe.
  ##
  ## Monotonicity: within a single millisecond on the same thread, the
  ## random suffix is incremented (not re-randomised), so successive
  ## `newId()` calls always lexicographically increase. Across threads /
  ## processes / nodes, the OS-provided 80 bits of entropy keep
  ## collisions vanishingly rare (P ≈ 2^-80 per same-ms pair).
  let ms = nowMillis()
  if ms == ulidLastMs:
    bumpSuffix(ulidLastSuffix)
  else:
    ulidLastMs = ms
    randomSuffix(ulidLastSuffix)
  result = newStringOfCap(26)
  result.add(encodeBase32(uint64(ms), 10))
  for c in ulidLastSuffix: result.add(c)
