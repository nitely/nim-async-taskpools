{.push raises: [], gcsafe.}

import std/[atomics, cpuinfo, isolation, macros, sysatomics]
import pkg/chronos
import pkg/taskpools

when not compileOption("threads"):
  {.error: "async_taskpool requires --threads:on".}

type
  AsyncTaskpool* = object
    tp: Taskpool

  TaskFuture*[T] = Future[T].Raising([CancelledError])

  AsyncTask[T] = object
    fut: TaskFuture[T]
    cancelled: Atomic[bool]
    when T isnot void:
      fv: Flowvar[T]

proc new*(
    T: type AsyncTaskpool, numThreads = countProcessors()
): AsyncTaskpool {.raises: [CatchableError].} =
  AsyncTaskpool(tp: Taskpool.new(max(2, numThreads)))

proc taskpool(atp: AsyncTaskpool): Taskpool =
  atp.tp

proc numThreads*(atp: AsyncTaskpool): int =
  atp.tp.numThreads

proc syncAll*(atp: AsyncTaskpool) =
  atp.tp.syncAll()

proc shutdown*(atp: var AsyncTaskpool) =
  atp.tp.shutdown()

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
  GC_unref(task)

proc newTaskFuture[T](): TaskFuture[T] =
  TaskFuture[T].init("AsyncTaskpool.spawn")

proc newAsyncTask[T](fut: TaskFuture[T]): ref AsyncTask[T] =
  let task = new(AsyncTask[T])
  task.fut = fut
  let raw = addr task[]
  fut.cancelCallback = proc(_: pointer) {.gcsafe, raises: [].} =
    raw.cancelled.store(true, moRelease)
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

  if fn.kind != nnkSym:
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
    dispVar = genSym(nskLet, "disp")
    fut = genSym(nskLet, "fut")
    task = genSym(nskLet, "task")
    tpVar = genSym(nskLet, "tp")
  spawnCall.insert(1, dispVar)
  spawnCall.insert(2, newCall(bindSym"taskPtr", task))

  let
    newFut = newCall(nnkBracketExpr.newTree(bindSym"newTaskFuture", retTy))
    newTaskCall =
      newCall(nnkBracketExpr.newTree(bindSym"newAsyncTask", retTy), fut)
    poolCall = newCall(bindSym"taskpool", atp)
    dispCall = newCall(bindSym"handle", newCall(bindSym"getThreadDispatcher"))
    tpSpawnCall = newCall(bindSym"spawn", tpVar, spawnCall)
    runCall =
      if hasRet: newCall(bindSym"attach", task, tpSpawnCall) else: tpSpawnCall

  result = quote do:
    block:
      `taskProc`
      let
        `tpVar` = `poolCall`
        `dispVar` = `dispCall`
        `fut` = `newFut`
        `task` = `newTaskCall`
      `runCall`
      `fut`
  # echo result.toStrLit()
