# nim c --mm:orc -d:useMalloc -r test_async_taskpools.nim
#
# ASan:
# nim c --mm:orc -d:useMalloc --cc:clang --passc:-fsanitize=address --passl:-fsanitize=address --debugger:native -r test_async_taskpools.nim
# TSan:
# nim c --mm:orc -d:taskpoolsTsan -d:useMalloc --cc:clang --passc:-fsanitize=thread --passl:-fsanitize=thread --debugger:native -r test_async_taskpools.nim

import std/[atomics, os]
import pkg/chronos
import ../async_taskpools

var checks = 0
template check(cond: untyped) =
  doAssert cond
  inc checks

proc isOneTwoThree(x: int): bool =
  x == 123

proc addUp(a, b: int, c: int): int =
  a + b + c

proc slow(ms: int): int =
  sleep(ms)
  ms

proc greet(name: string): string =
  "hello " & name

proc noResult(x: ptr int) =
  x[] = 42

proc typedProc(x: float): float =
  x * 2.0

proc generic[T](x: T): T =
  x + x

proc answer(): int =
  42

proc makeSeq(n: int): seq[int] =
  for i in 0 ..< n:
    result.add i * i

proc strLen(s: string): int =
  s.len

proc repeated(n: int, c: string): string =
  for i in 0 ..< n:
    result.add c

proc sumSeq(xs: seq[int]): int =
  for x in xs:
    result += x

proc doubled(xs: seq[int]): seq[int] =
  for x in xs:
    result.add x * 2

proc joinAll(xs: seq[string], sep: string): string =
  for i, x in xs:
    if i > 0:
      result.add sep
    result.add x

proc splitFirst(s: string): seq[string] =
  for c in s:
    result.add $c

proc minMax(a, b: int): tuple[lo, hi: int] =
  if a < b: (a, b) else: (b, a)

proc counted(ran: ptr Atomic[int], ms: int): int =
  discard ran[].fetchAdd(1)
  if ms > 0:
    sleep(ms)
  ms

const NumThreads = 4

proc main() {.async.} =
  var atp = AsyncTaskpool.new(NumThreads)

  check await atp.spawn isOneTwoThree(123)
  check not (await atp.spawn isOneTwoThree(456))

  let n = 10
  check (await atp.spawn addUp(n, 20, 30)) == 60

  check (await atp.spawn greet("world")) == "hello world"
  check (await atp.spawn strLen("hello")) == 5
  check (await atp.spawn repeated(3, "ab")) == "ababab"
  check (await atp.spawn sumSeq(@[1, 2, 3, 4])) == 10
  check (await atp.spawn doubled(@[1, 2, 3])) == @[2, 4, 6]
  check (await atp.spawn joinAll(@["a", "b", "c"], "-")) == "a-b-c"
  check (await atp.spawn splitFirst("xyz")) == @["x", "y", "z"]

  block:
    var owned = "moved in"
    check (await atp.spawn strLen(move owned)) == 8
    var xs = @[5, 6, 7]
    check (await atp.spawn sumSeq(move xs)) == 18

  var sink = 0
  await atp.spawn noResult(addr sink)
  check sink == 42

  check (await atp.spawn typedProc(21)) == 42.0

  check (await atp.spawn generic(21)) == 42

  check (await atp.spawn answer()) == 42

  check (await atp.spawn makeSeq(5)) == @[0, 1, 4, 9, 16]

  check (await atp.spawn minMax(9, 4)) == (4, 9)

  block:
    var ticks = 0
    proc ticker() {.async.} =
      while true:
        await sleepAsync(1.milliseconds)
        inc ticks
    let t = ticker()
    let futs = @[
      atp.spawn slow(60),
      atp.spawn slow(60),
      atp.spawn slow(60),
      atp.spawn slow(60),
    ]
    check (await allFutures(futs).withTimeout(5.seconds))
    for f in futs:
      check (await f) == 60
    await t.cancelAndWait()
    check ticks > 5

  block:
    let f = atp.spawn slow(50)
    await f.cancelAndWait()
    check f.cancelled()
    await atp.syncAll()

  block:
    var ran: Atomic[int]
    var blockers: seq[TaskFuture[int]]
    for i in 0 ..< NumThreads:
      blockers.add atp.spawn counted(addr ran, 100)
    var queued: seq[TaskFuture[int]]
    for i in 0 ..< 8:
      queued.add atp.spawn counted(addr ran, 0)
    for f in queued:
      f.cancelSoon()
      check f.cancelled()
    for f in blockers:
      check (await f) == 100
    await atp.syncAll()
    check ran.load() == NumThreads
    for f in queued:
      check f.cancelled()

  block:
    for i in 0 ..< 8:
      discard atp.spawn addUp(i, 1, 1)
    await atp.syncAll()
    check true

  block:
    const N = 2000
    var futs: seq[Future[int]]
    for i in 0 ..< N:
      futs.add atp.spawn addUp(i, 1, 1)
    var total = 0
    for i in 0 ..< N:
      total += await futs[i]
    check total == (N * (N - 1)) div 2 + 2 * N

  block:
    for i in 0 ..< 16:
      discard atp.spawn slow(30)
    check atp.pending > 0

  await atp.syncAll()
  check atp.pending == 0
  await atp.shutdown()

waitFor main()
echo "ok (", checks, " checks)"
