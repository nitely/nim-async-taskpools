# Copyright (c) 2026 Esteban C Borsani - @nitely
# License: MIT

{.push raises: [], gcsafe.}

import std/[atomics, cpuinfo, macros]
import pkg/chronos
import pkg/taskpools

when not compileOption("threads"):
  {.error: "async_taskpools requires --threads:on".}

type
  WaiterFuture = Future[void].Raising([CancelledError])

  AsyncTaskpoolObj* = object
    tp: Taskpool
    count: Atomic[int]
    closing: Atomic[bool]

  AsyncTaskpool* = ref AsyncTaskpoolObj

  TaskFuture*[T] = Future[T].Raising([CancelledError])

  AsyncTask[T] = object
    atp: ptr AsyncTaskpoolObj
    fut: TaskFuture[T]
    cancelled: Atomic[bool]
    when T isnot void:
      fv: Flowvar[T]

var
  loopCount {.threadvar.}: int
  loopWaiter {.threadvar.}: WaiterFuture

proc handle*(atp: AsyncTaskpool): ptr AsyncTaskpoolObj =
  addr atp[]

proc new*(
    T: type AsyncTaskpool, numThreads = countProcessors()
): AsyncTaskpool {.raises: [CatchableError].} =
  AsyncTaskpool(tp: Taskpool.new(max(2, numThreads)))

proc newOrDie*(
    T: type AsyncTaskpool, numThreads = countProcessors()
): AsyncTaskpool =
  try:
    T.new(numThreads)
  except CatchableError as exc:
    raiseAssert "AsyncTaskpool.new: " & exc.msg

proc taskpool(atp: ptr AsyncTaskpoolObj): Taskpool =
  atp.tp

proc pending*(atp: ptr AsyncTaskpoolObj): int =
  atp.count.load(moAcquire)

proc pending*(atp: AsyncTaskpool): int =
  atp.handle().pending()

proc syncAll*(atp: ptr AsyncTaskpoolObj) {.async: (raises: []).} =
  if loopCount == 0:
    return
  if loopWaiter.isNil:
    loopWaiter = WaiterFuture.init("AsyncTaskpool.syncAll")
  await noCancel loopWaiter

proc syncAll*(atp: AsyncTaskpool): Future[void] {.async: (raw: true, raises: []).} =
  atp.handle.syncAll()

proc shutdown*(atp: AsyncTaskpool) {.async: (raises: []).} =
  if atp.closing.exchange(true, moAcquireRelease):
    raiseAssert "AsyncTaskpool.shutdown called more than once"
  await atp.syncAll()
  if atp.count.load(moAcquire) != 0:
    raiseAssert "AsyncTaskpool.shutdown: event loops exited with pending tasks"
  atp.tp.shutdown()

proc taskDone[T](udata: pointer) {.nimcall, gcsafe, raises: [].} =
  let task = cast[ref AsyncTask[T]](udata)
  let atp = task.atp
  GC_unref(task)
  discard atp.count.fetchSub(1, moAcquireRelease)
  dec loopCount
  if loopCount == 0 and not loopWaiter.isNil:
    loopWaiter.complete()
    loopWaiter = nil

proc completeTask[T](udata: pointer) {.nimcall, gcsafe, raises: [].} =
  let task = cast[ref AsyncTask[T]](udata)
  when T is void:
    if not task.fut.finished():
      task.fut.complete()
  else:
    while not task.fv.isReady():
      cpuRelax()
    let res = sync(move task.fv)
    if not task.fut.finished():
      task.fut.complete(res)
  task.fut.cancelCallback = nil
  callSoon(taskDone[T], udata)

proc newAsyncTask[T](atp: ptr AsyncTaskpoolObj): ref AsyncTask[T] =
  if atp.closing.load(moAcquire):
    raiseAssert "AsyncTaskpool.spawn on a pool that is shutting down"
  let task = new(AsyncTask[T])
  task.atp = atp
  task.fut = TaskFuture[T].init("AsyncTaskpool.spawn")
  let raw = addr task[]
  task.fut.cancelCallback = proc(_: pointer) {.gcsafe, raises: [].} =
    raw.cancelled.store(true, moRelease)
  discard atp.count.fetchAdd(1, moRelease)
  inc loopCount
  GC_ref(task)
  task

proc taskFuture[T](task: ref AsyncTask[T]): TaskFuture[T] =
  task.fut

proc attach[T](task: ref AsyncTask[T], fv: sink Flowvar[T]) =
  task.fv = fv

proc taskPtr[T](task: ref AsyncTask[T]): pointer =
  cast[pointer](task)

proc isCancelled[T](task: pointer): bool =
  cast[ptr AsyncTask[T]](task).cancelled.load(moAcquire)

proc notify[T](disp: DispatcherHandle, task: pointer) =
  callSoon(disp, completeTask[T], task)

{.pop.}

proc spawnImpl(atp, fnCall: NimNode): NimNode =
  fnCall.expectKind(nnkCall)

  let
    fn = fnCall[0]
    retTy = fnCall.getTypeInst()
    hasRet = retTy.typeKind != ntyVoid

  if fn.kind != nnkSym or
      fn.symKind notin {nskProc, nskFunc}:
    error("spawn expects a call to a named proc", fnCall)

  let
    dispParam = genSym(nskParam, "disp")
    taskParam = genSym(nskParam, "taskp")
    taskFn = genSym(nskProc, $fn & "_asyncTask")

  var
    params = @[
      if hasRet: retTy else: newEmptyNode(),
      newIdentDefs(dispParam, bindSym"DispatcherHandle"),
      newIdentDefs(taskParam, bindSym"pointer"),
    ]
    fwdCall = nnkCall.newTree(fn)
    spawnCall = nnkCall.newTree(taskFn)

  let formals = fn.getImpl().params()
  for i in 1 ..< formals.len:
    let ty = formals[i][^2]
    if ty.kind == nnkBracketExpr and ty[0].eqIdent("varargs"):
      error("varargs calls cannot be spawned", fnCall)

  for i in 1 ..< fnCall.len:
    let
      arg = fnCall[i]
      p = genSym(nskParam, "a" & $i)
    params.add newIdentDefs(p, arg.getTypeInst())
    fwdCall.add p
    spawnCall.add arg

  let
    cancelledCall =
      newCall(nnkBracketExpr.newTree(bindSym"isCancelled", retTy), taskParam)
    notifyCall =
      newCall(
        nnkBracketExpr.newTree(bindSym"notify", retTy), dispParam, taskParam
      )
    taskBody =
      if hasRet:
        quote do:
          if not `cancelledCall`:
            result = `fwdCall`
          `notifyCall`
      else:
        quote do:
          if not `cancelledCall`:
            `fwdCall`
          `notifyCall`

  let taskProc = newProc(
    name = taskFn,
    params = params,
    body = taskBody,
    pragmas = nnkPragma.newTree(
      ident"nimcall",
      ident"gcsafe",
      nnkExprColonExpr.newTree(ident"raises", nnkBracket.newTree()),
    ),
  )

  let
    atpVar = genSym(nskLet, "atp")
    dispVar = genSym(nskLet, "disp")
    task = genSym(nskLet, "task")
    tpVar = genSym(nskLet, "tp")
  spawnCall.insert(1, dispVar)
  spawnCall.insert(2, newCall(bindSym"taskPtr", task))

  let
    newTaskCall =
      newCall(nnkBracketExpr.newTree(bindSym"newAsyncTask", retTy), atpVar)
    poolCall = newCall(bindSym"taskpool", atpVar)
    dispCall = newCall(bindSym"handle", newCall(bindSym"getThreadDispatcher"))
    tpSpawnCall = newCall(bindSym"spawn", tpVar, spawnCall)
    runCall =
      if hasRet: newCall(bindSym"attach", task, tpSpawnCall) else: tpSpawnCall
    futCall = newCall(bindSym"taskFuture", task)

  result = quote do:
    block:
      `taskProc`
      let
        `atpVar` = `atp`
        `tpVar` = `poolCall`
        `dispVar` = `dispCall`
        `task` = `newTaskCall`
      `runCall`
      `futCall`
  # echo result.toStrLit()

macro spawn*(
    atp: ptr AsyncTaskpoolObj, fnCall: typed
): untyped =
  spawnImpl(atp, fnCall)

macro spawn*(
    atp: AsyncTaskpool, fnCall: typed
): untyped =
  spawnImpl(newCall(bindSym"handle", atp), fnCall)
