# nim-async-taskpools

Async taskpools for Chronos - PoC


## Usage

```nim
import chronos
import async_taskpool

proc greet(name: string): string =
  "hello " & name

proc main() {.async.} =
  var tp = AsyncTaskpool.new(numThreads = 4)
  let s = await tp.spawn greet("world")
  echo s

waitFor main()

```

## License

MIT
