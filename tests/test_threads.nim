# nim c --mm:orc -d:useMalloc -r test_threads.nim
#
# ASan:
# nim c --mm:orc -d:useMalloc --cc:clang --passc:-fsanitize=address --passl:-fsanitize=address --debugger:native -r test_threads.nim
# TSan:
# nim c --mm:orc -d:taskpoolsTsan -d:useMalloc --cc:clang --passc:-fsanitize=thread --passl:-fsanitize=thread --debugger:native -r test_threads.nim

import std/[atomics, os]
import pkg/chronos
import ../async_taskpools

const
  NumLoops = 4
  NumTasks = 50

var checks: Atomic[int]
template check(cond: untyped) =
  doAssert cond
  discard checks.fetchAdd(1, moRelease)

var ran: Atomic[int]

proc work(x: int): int =
  sleep(1)
  discard ran.fetchAdd(1, moRelease)
  x * 2

proc slow(ms: int): int =
  sleep(ms)
  ms

proc nothing() =
  discard

type Loop = object
  atp: ptr AsyncTaskpoolObj
  id: int

proc run(atp: ptr AsyncTaskpoolObj, id: int) {.async: (raises: []).} =
  check atp.numThreads >= 2

  var futs: seq[TaskFuture[int]]
  for i in 0 ..< NumTasks:
    futs.add atp.spawn work(id * NumTasks + i)

  let v = atp.spawn nothing()
  await noCancel v
  check v.finished()

  for i in 0 ..< NumTasks:
    check (await noCancel futs[i]) == (id * NumTasks + i) * 2
  for f in futs:
    check f.finished()

  let running = atp.spawn slow(200)
  await noCancel sleepAsync(20.milliseconds)
  let t0 = Moment.now()
  await running.cancelAndWait()
  check (Moment.now() - t0).milliseconds < 100
  check running.cancelled()

  await AsyncTaskpool.drain()

proc loopThread(loop: Loop) {.thread, nimcall.} =
  waitFor run(loop.atp, loop.id)

proc main() {.async: (raises: []).} =
  let atp = AsyncTaskpool.newOrDie(4)
  var threads: array[NumLoops, Thread[Loop]]

  for i in 0 ..< NumLoops:
    try:
      createThread(threads[i], loopThread, Loop(atp: atp.handle, id: i + 1))
    except CatchableError as exc:
      raiseAssert exc.msg

  await run(atp.handle, 0)
  joinThreads(threads)

  check atp.pending == 0
  check ran.load(moAcquire) == (NumLoops + 1) * NumTasks

  for i in 0 ..< NumTasks:
    discard atp.spawn work(i)
  check atp.pending == NumTasks
  await atp.syncAll()
  check atp.pending == 0
  check ran.load(moAcquire) == (NumLoops + 2) * NumTasks

  await atp.shutdown()

waitFor main()
echo "ok (", checks.load(moAcquire), " checks)"
