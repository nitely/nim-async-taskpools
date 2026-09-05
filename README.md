# nim-async-taskpools

Async taskpools for Chronos - PoC

Note it requires Chronos dev or a version that contains [#694](https://github.com/status-im/nim-chronos/pull/694).

## Usage

```nim
import chronos
import async_taskpools

proc greet(name: string): string {.raises: [], gcsafe.} =
  "hello " & name

proc main() {.async: (raises: [CancelledError]).} =
  var tp = AsyncTaskpool.newOrDie(numThreads = 4)
  defer: await tp.shutdown()
  let s = await tp.spawn greet("world")
  echo s

waitFor main()

```

## License

MIT
