{.push raises: [], gcsafe.}

import std/[atomics, cpuinfo, macros]
import pkg/chronos
import pkg/taskpools

when not compileOption("threads"):
  {.error: "async_taskpool requires --threads:on".}

type
  WaiterFuture = Future[void].Raising([CancelledError])

  InProgress = object
    count: Atomic[int]
    closing: Atomic[bool]
    waiter: WaiterFuture

  AsyncTaskpool* = object
    tp: Taskpool
    inProgress: ptr InProgress

  TaskFuture*[T] = Future[T].Raising([CancelledError])

  AsyncTask[T] = object
    fut: TaskFuture[T]
    inProgress: ptr InProgress
    cancelled: Atomic[bool]
    when T isnot void:
      fv: Flowvar[T]

proc new*(
    T: type AsyncTaskpool, numThreads = countProcessors()
): AsyncTaskpool {.raises: [CatchableError].} =
  AsyncTaskpool(
    tp: Taskpool.new(max(2, numThreads)),
    inProgress: createShared(InProgress),
  )

proc newOrDie*(
    T: type AsyncTaskpool, numThreads = countProcessors()
): AsyncTaskpool =
  try:
    T.new(numThreads)
  except CatchableError as exc:
    raiseAssert "AsyncTaskpool.new: " & exc.msg

proc taskpool(atp: AsyncTaskpool): Taskpool =
  atp.tp

proc inProgress(atp: AsyncTaskpool): ptr InProgress =
  atp.inProgress

proc pending*(atp: AsyncTaskpool): int =
  atp.inProgress.count.load(moAcquire)

proc numThreads*(atp: AsyncTaskpool): int =
  atp.tp.numThreads

proc syncAll*(atp: AsyncTaskpool) {.async: (raises: []).} =
  if atp.inProgress.count.load(moAcquire) == 0:
    return
  var fut = atp.inProgress.waiter
  if fut.isNil:
    fut = WaiterFuture.init("AsyncTaskpool.syncAll")
    atp.inProgress.waiter = fut
  await noCancel fut

proc shutdown*(atp: AsyncTaskpool) {.async: (raises: []).} =
  if atp.inProgress.closing.exchange(true, moAcquireRelease):
    raiseAssert "AsyncTaskpool.shutdown called more than once"
  await atp.syncAll()
  var tp = atp.tp
  tp.shutdown()
  reset(atp.inProgress[])
  deallocShared(atp.inProgress)

proc taskDone(udata: pointer) {.nimcall, gcsafe, raises: [].} =
  let inProgress = cast[ptr InProgress](udata)
  if inProgress.count.fetchSub(1, moAcquireRelease) == 1 and
      not inProgress.waiter.isNil:
    let fut = inProgress.waiter
    inProgress.waiter = nil
    fut.complete()

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
  let inProgress = task.inProgress
  task.fut.cancelCallback = nil
  GC_unref(task)
  callSoon(taskDone, cast[pointer](inProgress))

proc newTaskFuture[T](): TaskFuture[T] =
  TaskFuture[T].init("AsyncTaskpool.spawn")

proc newAsyncTask[T](
    fut: TaskFuture[T], inProgress: ptr InProgress
): ref AsyncTask[T] =
  if inProgress.closing.load(moAcquire):
    raiseAssert "AsyncTaskpool.spawn on a pool that is shutting down"
  let task = new(AsyncTask[T])
  task.fut = fut
  task.inProgress = inProgress
  let raw = addr task[]
  fut.cancelCallback = proc(_: pointer) {.gcsafe, raises: [].} =
    raw.cancelled.store(true, moRelease)
  discard inProgress.count.fetchAdd(1, moRelease)
  GC_ref(task)
  task

proc attach[T](task: ref AsyncTask[T], fv: sink Flowvar[T]) =
  task.fv = fv

proc taskPtr[T](task: ref AsyncTask[T]): pointer =
  cast[pointer](task)

proc isCancelled[T](task: pointer): bool =
  cast[ptr AsyncTask[T]](task).cancelled.load(moAcquire)

proc notify[T](disp: DispatcherHandle, task: pointer) =
  callSoon(disp, completeTask[T], task)

{.pop.}

macro spawn*(atp: AsyncTaskpool, fnCall: typed): untyped =
  fnCall.expectKind(nnkCall)

  let
    fn = fnCall[0]
    retTy = fnCall.getTypeInst()
    hasRet = retTy.typeKind != ntyVoid

  if fn.kind != nnkSym or
      fn.symKind notin {nskProc, nskFunc, nskMethod, nskConverter}:
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
    fut = genSym(nskLet, "fut")
    task = genSym(nskLet, "task")
    tpVar = genSym(nskLet, "tp")
  spawnCall.insert(1, dispVar)
  spawnCall.insert(2, newCall(bindSym"taskPtr", task))

  let
    newFut = newCall(nnkBracketExpr.newTree(bindSym"newTaskFuture", retTy))
    newTaskCall = newCall(
      nnkBracketExpr.newTree(bindSym"newAsyncTask", retTy),
      fut,
      newCall(bindSym"inProgress", atpVar),
    )
    poolCall = newCall(bindSym"taskpool", atpVar)
    dispCall = newCall(bindSym"handle", newCall(bindSym"getThreadDispatcher"))
    tpSpawnCall = newCall(bindSym"spawn", tpVar, spawnCall)
    runCall =
      if hasRet: newCall(bindSym"attach", task, tpSpawnCall) else: tpSpawnCall

  result = quote do:
    block:
      `taskProc`
      let
        `atpVar` = `atp`
        `tpVar` = `poolCall`
        `dispVar` = `dispCall`
        `fut` = `newFut`
        `task` = `newTaskCall`
      `runCall`
      `fut`
  # echo result.toStrLit()
