# @watchmanlab/stream

**A Declarative TypeScript library that gives you lazy, reusable, composable, traceable, scalable, and controllable code execution.**

One unified model across **sync & async**, **hot & cold**, **push & pull**, and **lazy & eager** execution—built to work standalone or mix seamlessly with any paradigm and library you already use.

## Install

```sh
npm install @watchmanlab/stream
```

### Quick start

```ts
import {
  filter,
  map,
  iterableSource,
  listen,
  take,
  delay,
} from "@watchmanlab/stream";

// `source` is lazy and replayable
// so you can consume it multiple times
const source = iterableSource([1, 2, 3, 4, 5, 6])
  .pipe(filter((value) => value > 2))
  .pipe(map((value, index) => `${index}:${value}`))
  .pipe(take(3))
  .pipe(delay(100));

//consume it using:
for await (const value of source) {
  console.log(value);
}

//OR
source
  .consume((consumer, value) => {
    console.log(value);
    consumer.next();
  })
  .next();

//OR
source.pipe(listen((value) => console.log(value)));
```

### Core concepts

The library is built around four primitives: `Consumable`, `Consumer`, `Source`, and `Stream`. Everything composes through `.pipe()` and everything is lazy by default — nothing runs until a consumer requests values.

At first glance this looks like any other reactive/streaming library — `.pipe()`, operators, sources, hot vs cold. But there is a fundamental design decision that sets it apart: **the primitives are intentionally dumb and unbounded**. No built-in backpressure, no buffer strategies, no async orchestration, no error handling baked in. This is not a gap — it's what makes the library fast, predictable, and extensible. Everything beyond raw value delivery is a transformer you compose in, and you only pay for what you pipe.

This means backpressure, async workflows, error handling, flow control — none of it is a configuration option on the consumer or the source. Each is a layer you insert into the pipeline exactly where you need it, with exactly the strategy that fits your use case. And because transformers are just composable functions, you can build your own and it will work identically to anything in the library.

## Consumable

The base contract of the library. Any object with a `consume(handler, options)` method is a `Consumable`. Sources, streams, and transformers all implement it — so they are interchangeable anywhere a `Consumable` is expected.

```ts
import type { Consumable } from "@watchmanlab/stream";

function logAll<T>(source: Consumable<T>) {

 const consumer = source.consume((c, value) => {
      console.log(value);
      c.next();// c = consumer
    })x

    consumer.next();
}
```


## Consumer

The fundamental unit of consumption. A `Consumer` receives values and controls its own processing rate through a **credit system**: a value is only processed when the consumer has credit, granted by calling `.next()`. Values pushed without available credit are buffered in an internal FIFO queue and drained in order as credit is granted.

```ts
import { Consumer } from "@watchmanlab/stream";

const consumer = new Consumer<number>((self, value) => {
  console.log(value);
  self.next(); // grant credit for the next value
});

consumer.next(); // grant initial credit
consumer.push(1); // processed immediately
consumer.push(2); // queued, processed after handler calls next()
```

**Lifecycle states:** `active` → `drain` → `complete` or `abort`.

- `terminate("complete")` — drains the remaining queue at the consumer's own pace before stopping.
- `terminate("abort")` — immediately stops and clears the queue.

Beyond receiving and processing values, a `Consumer` is fully introspectable. Every internal moment is observable as a first-class `Source` you can pipe and compose:

| Event              | Emits                                              |
| ------------------ | -------------------------------------------------- |
| `$push`            | Every value delivered to the consumer              |
| `$beforeConsuming` | Every value just before the handler runs           |
| `$afterConsuming`  | Every value just after the handler calls `.next()` |
| `$next`            | When the queue is exhausted and credit is granted  |
| `$drain`           | When the consumer enters drain state               |
| `$terminate`       | The termination reason                             |

`inProcessValue` holds the value currently being handled, accessible at any point during handler execution.

This makes a `Consumer` a self-contained execution unit you can monitor, trace, and build flow control around — without touching the pipeline itself.

## Source

Abstract base class for all sources, streams and transformers. Provides `.pipe()` for chaining, `.collect()` for terminal collection, and both `for await...of` and `for...of` iteration.

```ts
import { iterableSource, map, filter } from "@watchmanlab/stream";

const source = iterableSource([1, 2, 3, 4, 5]);

// pipe transformers
const result = source.pipe(filter((v) => v > 2)).pipe(map((v) => v * 10));

// async iteration
for await (const value of result) {
  console.log(value); // 30, 40, 50
}

//if the source is synchronous then you can consumer it synchronously
for (const value of result) {
  console.log(value); // 30, 40, 50
}
for await (const value of result) {
  console.log(value); // 30, 40, 50
}
// collect n values
const values = await result.collect(3); // [30, 40, 50]

// sync collect
const sync = result.collect(3, { sync: true }); // [30, 40, 50]
```

`Source.from(consumable)` wraps any `Consumable` in a `Source` proxy, giving it `.pipe()` and iteration without changing its behaviour. It is also used to expose a read-only view of a `Stream`.

## Stream

A multicast push source that broadcasts values to all its consumers. The primary building block for hot event sources.

```ts
import { Stream, listen } from "@watchmanlab/stream";

const stream = new Stream<number>();

stream.pipe(listen((v) => console.log("A:", v)));
stream.pipe(listen((v) => console.log("B:", v)));

stream.pushMany(1, 2, 3);
stream.terminate("complete");
```

**Lifecycle events** are exposed as first-class `Source`s:

| Event                | Emits                                         |
| -------------------- | --------------------------------------------- |
| `$push`              | Every pushed value                            |
| `$next`              | The fastest consumer that called `next()`     |
| `$consumerNext`      | Any consumer that called `next()`             |
| `$consumerJoin`      | Every new consumer                            |
| `$consumerLeft`      | Every consumer that left                      |
| `$firstConsumerJoin` | When the first consumer joins an empty stream |
| `$lastConsumerLeft`  | When the last consumer leaves                 |
| `$drain`             | When the stream enters drain state            |
| `$terminate`         | The termination reason                        |

**Resource management** via `using` / `await using`:

```ts
{
  using stream = new Stream<number>(); // terminates with "abort" on scope exit
}
{
  await using stream = new Stream<number>(); // terminates with "complete" on scope exit
}
```

`Stream.from(consumable)` bridges any unicast `Consumable` into a multicast `Stream`. Upstream pulls are driven by the fastest consumer; slower consumers buffer independently.

```ts
const shared = Stream.from(iterableSource([1, 2, 3]));
shared.pipe(listen((v) => console.log("A:", v)));
shared.pipe(listen((v) => console.log("B:", v)));
```

### Sources

Some of the sources are replayable — you can consume them multiple times and each consumer gets its own independent execution.

---

#### `valuesSource(...values)`

Emits a fixed list of inline values then completes. Shorthand for `iterableSource(values)`.

```ts
valuesSource(1, 2, 3).pipe(listen(console.log)); // 1, 2, 3
```

#### `iterableSource(iterable)`

Wraps any `Iterable` as a pull-based source. A new iterator is created per consumer via `Symbol.iterator`, so each consumer gets a fresh traversal. Also accepts a factory function returning an `Iterable`.

```ts
iterableSource([1, 2, 3]).pipe(listen(console.log));
iterableSource(() => new Set([1, 2, 3])).pipe(listen(console.log)); // factory form
```

#### `asyncIterableSource(iterable)`

Wraps any `AsyncIterable` as a pull-based source. A new async iterator is created per consumer. Also accepts a factory function returning an `AsyncIterable`.

```ts
async function* gen() {
  yield 1;
  yield 2;
}
asyncIterableSource(gen()).pipe(listen(console.log));
```

#### `generatorSource(fn)`

Wraps a generator function as a pull-based source. The function is called once per consumer, so each consumer gets its own independent generator instance.

```ts
generatorSource(function* () {
  yield 1;
  yield 2;
  yield 3;
}).pipe(listen(console.log)); // 1, 2, 3
```

#### `asyncGeneratorSource(fn)`

Wraps an async generator function as a pull-based source. The function is called once per consumer, so each consumer gets its own independent async generator instance.

```ts
asyncGeneratorSource(async function* () {
  yield await fetch("/a");
  yield await fetch("/b");
}).pipe(listen(console.log));
```

#### `iteratorSource(iterator)`

Wraps a synchronous `Iterator` directly. Each `next()` call pulls one value. Terminates with `"complete"` when the iterator is done. Also accepts a factory function returning an `Iterator`. Note: if a raw iterator (not a factory) is passed, it is shared across all consumers.

```ts
iteratorSource([1, 2, 3].values()).pipe(listen(console.log)); // 1, 2, 3
iteratorSource(() => [1, 2, 3].values()).pipe(listen(console.log)); // factory form, replayable
```

#### `asyncIteratorSource(iterator)`

Wraps an `AsyncIterator` directly. Each `next()` call pulls one value asynchronously. Also accepts a factory function returning an `AsyncIterator`. If a raw iterator is passed, it is shared across all consumers.

```ts
asyncIteratorSource(() => someAsyncIterator()).pipe(listen(console.log));
```

#### `functionSource(fn)`

Calls `fn` once on the first `next()`, emits the result, then completes. Useful for wrapping a single lazy computation as a source.

```ts
functionSource(() => Math.random()).pipe(listen(console.log)); // one random number
```

#### `rangeSource(start, end)`

Emits integers from `start` (inclusive) to `end` (exclusive), then completes.

```ts
rangeSource(1, 5).pipe(listen(console.log)); // 1, 2, 3, 4
```

#### `intervalSource(ms)`

Emits an incrementing counter (starting at `0`) every `ms` milliseconds via `setInterval`. Never completes on its own — must be terminated externally (e.g. with `take`, `takeUntil`, or `scope`).

```ts
intervalSource(500).pipe(take(3)).pipe(listen(console.log)); // 0, 1, 2
```

#### `timeoutSource(ms, value?)`

Emits a single value after `ms` milliseconds via `setTimeout`, then completes. The emitted value defaults to `undefined` if not provided.

```ts
timeoutSource(1000).pipe(listen(() => console.log("done")));
timeoutSource(1000, "tick").pipe(listen(console.log)); // 'tick'
```

#### `eventTargetSource(target, eventType)`

Emits DOM or Node.js events of the given type from an `EventTarget`. A new event listener is registered per consumer and removed on termination. Fully typed for known DOM event types.

```ts
eventTargetSource(window, "click").pipe(listen((e) => console.log(e.type)));
eventTargetSource(window, "resize")
  .pipe(debounce(200))
  .pipe(listen(console.log));
```

#### `abortSignalSource(signal)`

Emits once when the given `AbortSignal` is aborted, then completes. If the signal is already aborted at subscription time, emits immediately.

```ts
const controller = new AbortController();
abortSignalSource(controller.signal).pipe(listen(() => console.log("aborted")));
controller.abort();
```

#### `abortControllerSource(controller)`

Convenience wrapper around `abortSignalSource` that accepts an `AbortController` directly instead of its signal.

```ts
const controller = new AbortController();
abortControllerSource(controller).pipe(listen(() => console.log("aborted")));
controller.abort();
```

#### `gcTokenSource(token)`

Emits once when the given object is garbage collected, using `FinalizationRegistry`. If the object is already collected at subscription time, emits immediately. Useful for tying stream lifetimes to object lifetimes.

```ts
let obj: object | null = {};
gcTokenSource(obj).pipe(listen(() => console.log("collected")));
obj = null; // allow GC
```

#### `mergeSource(...sources)`

Merges multiple sources into one, emitting values from all inputs as they arrive. Unlike the `merge` transformer, this is a standalone source that takes all inputs upfront. Terminates when the **last** input terminates.

```ts
const s1 = intervalSource(500).pipe(map(() => "A"));
const s2 = intervalSource(700).pipe(map(() => "B"));
mergeSource(s1, s2).pipe(listen(console.log)); // A, B, A, A, B, ...
```

#### `combineSource(...sources)`

Emits a tuple of the latest value from each input whenever any input emits. Uses `EMPTY` for inputs that have not yet emitted. Terminates when any input terminates. For `combineLatest` semantics, pipe `latest(1)` on each input before passing to `combineSource`.

```ts
const s1 = valuesSource(1, 2, 3).pipe(delay(100));
const s2 = valuesSource("a", "b").pipe(delay(200));
combineSource(s1, s2).pipe(listen(console.log));
// [1, EMPTY], [2, EMPTY], [2, 'a'], [3, 'a'], [3, 'b']
```

#### `zipSource(...sources)`

Combines values from multiple sources pairwise into tuples. Emits a tuple only when all inputs have provided a new value. Terminates when any input terminates. Unmatched values remaining at termination are available on `.$rest`.

```ts
const s1 = valuesSource(1, 2, 3);
const s2 = valuesSource("a", "b", "c");
zipSource(s1, s2).pipe(listen(console.log)); // [1,'a'], [2,'b'], [3,'c']
```

### Streams

| Stream      | Description                                                                   |
| ----------- | ----------------------------------------------------------------------------- |
| `Stream<T>` | Multicast push source. The base hot stream                                    |
| `Signal<T>` | A `Stream` that terminates after the first push. For one-shot events          |
| `State<T>`  | A `Stream` with a `.value` property. Setting `.value` pushes to all consumers |

### Transformers

Transformers are functions that accept a source and return a new source. They compose with `.pipe()`.

---

#### Filtering

##### `filter(predicate, complement?)`

Passes only values that satisfy the predicate. Rejected values are forwarded to the lazy `.$complements` stream at zero cost if unused. The `.$complements` stream is passive relative to `filter` — consuming it never drives the upstream.

```ts
const f = valuesSource(1, 2, 3, 4).pipe(filter((v) => v % 2 === 0));
f.$complements.pipe(listen(console.log)); // 1, 3
f.pipe(listen(console.log)); // 2, 4
```

An optional inline `complement` callback can be passed as a second argument instead of subscribing to `.$complements`.

##### `take(n)`

Takes the first `n` values then terminates. Works with both cold and hot sources.

```ts
intervalSource(100).pipe(take(3)).pipe(listen(console.log)); // 0, 1, 2
```

##### `takeWhile(predicate)`

Takes values while the predicate returns `true`. Terminates on the first `false`.

```ts
valuesSource(1, 2, 3, 4)
  .pipe(takeWhile((v) => v < 3))
  .pipe(listen(console.log)); // 1, 2
```

##### `takeUntil($notifier)`

Takes values until the notifier emits, then terminates. Useful for tying a stream's lifetime to an external event.

```ts
const stop = timeoutSource(1000);
intervalSource(200).pipe(takeUntil(stop)).pipe(listen(console.log));
```

##### `skip(n)`

Skips the first `n` values, then passes all subsequent values through.

```ts
valuesSource(1, 2, 3, 4).pipe(skip(2)).pipe(listen(console.log)); // 3, 4
```

##### `skipWhile(predicate)`

Skips values while the predicate returns `true`. Once the predicate returns `false`, all subsequent values pass through without re-checking.

```ts
valuesSource(1, 2, 3, 4)
  .pipe(skipWhile((v) => v < 3))
  .pipe(listen(console.log)); // 3, 4
```

##### `skipUntil($notifier)`

Skips all values until the notifier emits, then passes all subsequent values through.

```ts
const start = timeoutSource(600);
stream.pipe(skipUntil(start)).pipe(listen(console.log));
```

##### `distinct(keySelector?, $flushes?)`

Filters out duplicate values using a `Set`. Optionally accepts a `keySelector` to compare by a derived key. A `$flushes` notifier resets the seen-set when it emits.

```ts
valuesSource(1, 1, 2, 2, 3).pipe(distinct()).pipe(listen(console.log)); // 1, 2, 3

// by key
valuesSource({ id: 1 }, { id: 1 }, { id: 2 })
  .pipe(distinct((v) => v.id))
  .pipe(listen(console.log)); // {id:1}, {id:2}

// with flush
const reset = new Signal();
stream.pipe(distinct(undefined, reset)).pipe(listen(console.log));
reset.push(); // clears the seen-set
```

##### `first(predicate?)`

Emits the first value then terminates. Optionally accepts a predicate — emits the first value for which it returns `true`. Emits `EMPTY` if the source completes without a match.

```ts
valuesSource(1, 2, 3).pipe(first()).pipe(listen(console.log)); // 1
valuesSource(1, 2, 3)
  .pipe(first((v) => v > 1))
  .pipe(listen(console.log)); // 2
```

##### `last()`

Emits the last value when the source completes. Emits `EMPTY` if the source completes without emitting. Combine with aggregation transformers like `count`, `sum`, or `scan` to get their final result.

```ts
valuesSource(1, 2, 3).pipe(last()).pipe(listen(console.log)); // 3
```

##### `range(start, offset)`

Passes only values at indices `[start, start + offset]` (0-based). Values before `start` are skipped; the stream terminates after `offset` values.

```ts
valuesSource("a", "b", "c", "d", "e")
  .pipe(range(1, 3))
  .pipe(listen(console.log)); // b, c, d
```

---

#### Transformation

##### `map(fn)`

Transforms each value using `(value, index) => result`. The index is per-consumer .

```ts
valuesSource(1, 2, 3)
  .pipe(map((v, i) => `${i}:${v}`))
  .pipe(listen(console.log)); // 0:1, 1:2, 2:3
```

##### `flat(depth?)`

Flattens a stream of arrays into individual values. `depth` controls how deep to flatten (default `0` = one level, equivalent to `Array.flat(1)`).

```ts
valuesSource([1, 2], [3, 4]).pipe(flat()).pipe(listen(console.log)); // 1, 2, 3, 4
valuesSource([[1, 2], [3]])
  .pipe(flat(1))
  .pipe(listen(console.log)); // 1, 2, 3
```

##### `flat$(depth?)`

Flattens a stream of `Consumable`s sequentially — consume each inner `Consumable` and waits for it to complete before moving to the next. `depth` controls how many levels of nesting to flatten (default `1`).

```ts
valuesSource(valuesSource(1, 2), valuesSource(3, 4))
  .pipe(flat$())
  .pipe(listen(console.log)); // 1, 2, 3, 4
```

##### `scan(seed, reducer)`

Accumulates values with a reducer and emits the running accumulator after each value — like `Array.reduce` but streaming. Pipe `last()` after it to get only the final result.

```ts
valuesSource(1, 2, 3)
  .pipe(scan(0, (acc, v) => acc + v))
  .pipe(listen(console.log)); // 1, 3, 6

// final value only
valuesSource(1, 2, 3)
  .pipe(scan(0, (acc, v) => acc + v))
  .pipe(last())
  .pipe(listen(console.log)); // 6
```

##### `scanArray()`

Accumulates all values into a growing array and emits the same array reference after each push. The array is mutated in place for performance. Use `last()` to get the final collected array.

```ts
valuesSource(1, 2, 3).pipe(scanArray()).pipe(last()).pipe(listen(console.log)); // [1, 2, 3]
```

##### `resolve(concurrency?)`

Resolves a stream of `Promise`s. Emits resolved values or `Error<reason>` for rejections. The `concurrency` parameter (default `1`) controls how many promises can be in-flight simultaneously. Upstream credit is withheld until a slot is free, meaning the upstream producer is not even called until a slot opens — so there will be at most exactly `concurrency` promises in flight at any time, never more.

```ts
generatorSource(function* () {
  yield fetch("/a");
  yield fetch("/b");
  yield fetch("/c");
})
  .pipe(resolve(2))
  .pipe(listen(console.log));
```

##### `safe(fn)`

Wraps a transformer pipeline in a try/catch. On success, values pass through unchanged. On a thrown error, emits `Error<thrown>` instead of crashing the pipeline.

```ts
valuesSource(1, 2, 3)
  .pipe(
    safe(
      map((v) => {
        if (v === 2) throw "bad";
        return v;
      }),
    ),
  )
  .pipe(listen(console.log)); // 1, Error('bad'), 3
```

##### `context(ctx)`

Wraps each value with a shared mutable context object, emitting `{ value, context }`. The same context reference is shared across all values in the stream, making it useful for accumulating state without a separate `scan`.

```ts
valuesSource(1, 2, 3)
  .pipe(context({ total: 0 }))
  .pipe(tap(({ value, context }) => (context.total += value)))
  .pipe(listen(({ value, context }) => console.log(value, context.total)));
// 1 1, 2 3, 3 6
```

---

#### Aggregation

All aggregation transformers emit a running value after each input — not just at the end so you can track the proggress. Pipe `last()` after any of them to get only the final result.

##### `count()`

Emits the running count after each value.

```ts
valuesSource("a", "b", "c").pipe(count()).pipe(listen(console.log)); // 1, 2, 3
valuesSource("a", "b", "c")
  .pipe(count())
  .pipe(last())
  .pipe(listen(console.log)); // 3
```

##### `sum()`

Emits the running sum after each numeric value.

```ts
valuesSource(1, 2, 3).pipe(sum()).pipe(last()).pipe(listen(console.log)); // 6
```

##### `min()`

Emits the running minimum after each numeric value.

```ts
valuesSource(3, 1, 4, 1, 5).pipe(min()).pipe(last()).pipe(listen(console.log)); // 1
```

##### `max()`

Emits the running maximum after each numeric value.

```ts
valuesSource(3, 1, 4, 1, 5).pipe(max()).pipe(last()).pipe(listen(console.log)); // 5
```

##### `every(predicate)`

Emits `true` while all values satisfy the predicate. Emits `false` and terminates immediately on the first failure.

```ts
valuesSource(1, 2, 3)
  .pipe(every((v) => v > 0))
  .pipe(last())
  .pipe(listen(console.log)); // true
valuesSource(1, -1, 3)
  .pipe(every((v) => v > 0))
  .pipe(last())
  .pipe(listen(console.log)); // false
```

##### `batch(size)`

Collects values into arrays of up to `size`. Emits a partial batch with any remaining values on stream completion.

```ts
valuesSource(1, 2, 3, 4, 5).pipe(batch(2)).pipe(listen(console.log)); // [1,2], [3,4], [5]
```

##### `buffer(size, startBufferEvery?)`

Collects values into fixed-size sliding-window arrays. A new buffer starts every `startBufferEvery` values (defaults to `size`, giving non-overlapping windows). Incomplete buffers are emitted on stream completion.

```ts
// non-overlapping
valuesSource(1, 2, 3, 4, 5).pipe(buffer(3)).pipe(listen(console.log)); // [1,2,3], [4,5]

// sliding window
valuesSource(1, 2, 3, 4).pipe(buffer(3, 1)).pipe(listen(console.log)); // [1,2,3], [2,3,4], [3,4], [4]
```

##### `zip(...others)`

Combines values from multiple sources pairwise into tuples. Emits a tuple only when all inputs have provided a new value. Terminates when any input terminates. Unmatched values remaining at termination are available on `.$rest`.

```ts
const s1 = new Stream<number>();
const s2 = new Stream<string>();
s1.pipe(zip(s2)).pipe(listen(console.log));
s1.push(1);
s2.push("a"); // [1, 'a']
s1.push(2);
s2.push("b"); // [2, 'b']
```

##### `combine(...others)`

Emits a tuple of the latest value from each input whenever any input emits. Uses `EMPTY` for inputs that have not yet emitted. Terminates when any input terminates. For `combineLatest` semantics, pipe `latest(1)` on each input before combining.

```ts
const s1 = valuesSource(1, 2, 3).pipe(delay(100));
const s2 = valuesSource("a", "b").pipe(delay(200));
s1.pipe(combine(s2)).pipe(listen(console.log));
// [1, EMPTY], [2, EMPTY], [2, 'a'], [3, 'a'], [3, 'b']
```

##### `merge(...others)`

Merges multiple sources into one, emitting values from all inputs as they arrive. Terminates when the primary input terminates.

```ts
const s1 = intervalSource(500).pipe(map(() => "A"));
const s2 = intervalSource(700).pipe(map(() => "B"));
s1.pipe(merge(s2)).pipe(listen(console.log)); // A, B, A, A, B, ...
```

---

#### Flow Control

##### `debounce(ms)`

Emits the latest value only after `ms` milliseconds of silence. Each new incoming value resets the timer. Useful for search inputs or resize events.

```ts
stream.pipe(debounce(300)).pipe(listen(console.log));
```

##### `delay(ms)`

Delays each value by `ms` milliseconds using `setTimeout`. If `ms <= 0`, uses `queueMicrotask` instead.

```ts
valuesSource(1, 2, 3).pipe(delay(500)).pipe(listen(console.log));
```

##### `delayOnce(ms)`

Delays only the first value by `ms` milliseconds. All subsequent values pass through immediately.

```ts
valuesSource(1, 2, 3).pipe(delayOnce(500)).pipe(listen(console.log)); // 1 delayed, 2 and 3 immediate
```

##### `pace(ms)`

Rate-limits output to at most one value per `ms` milliseconds. The timer is decoupled from emission: if a value arrives after the window has already expired, it is emitted directly with no delay. If a value arrives before the window expires, it is scheduled for the next available slot. Credit flows back to slow the upstream producer.

```ts
// fast upstream — values are spaced out to 500ms apart
intervalSource(50).pipe(pace(500)).pipe(listen(console.log));

// slow upstream — values that arrive after the window pass through immediately
intervalSource(1000).pipe(pace(500)).pipe(listen(console.log));
```

##### `gate($control)`

Opens or closes the stream based on a boolean control source. `true` opens the gate and starts consuming the input; `false` closes it and terminates the inner consumer. The gate re-opens on the next `true`.

```ts
const toggle = intervalSource(3000).pipe(map((_, i) => i % 2 === 0));
intervalSource(500).pipe(gate(toggle)).pipe(listen(console.log));
// values flow for 3s, stop for 3s, flow for 3s, ...
```

##### `passive()`

Subscribes to the upstream without calling `next()`, so it never drives upstream pulls. Values are only received when the upstream pushes them independently.

To understand why this matters: on a shared stream, the fastest consumer is the one that drives the source — when it calls `next()`, the shared source pulls the next value and broadcasts it to all consumers. Faster consumers process it immediately; slower ones enqueue it and drain at their own pace. If an observer consumer also calls `next()`, it competes to become the driver and pulls values faster than the intended driver would. When the intended driver is slow and cannot afford to miss values (so throttling transformers like `debounce` or `pace` are not an option), this extra pressure causes values to pile up in the slow consumer's queue, which can grow without bound and eventually exhaust memory. A passive consumer opts out entirely: it registers to receive values but never calls `next()`, so it never becomes the driver. It only receives values that the fastest active consumer already triggered, at exactly that consumer's pace.

This is useful when you want to observe a shared stream without affecting its throughput.

```ts
const shared = intervalSource(500).pipe(share());

// this consumer drives the source — calls next() after each value
shared.pipe(listen((v) => console.log("driver:", v)));

// this consumer only observes — never calls next(), never pulls
shared.pipe(passive()).pipe(listen((v) => console.log("observer:", v)));
```

##### `pump()`

Eagerly drains the upstream by pre-granting credit, pushing all values into an internal `Stream`. Useful for activating a dormant pipeline that has no terminal consumer yet.

```ts
valuesSource(1, 2, 3)
  .pipe(map((v) => v * 2))
  .pipe(tap(console.log))
  .pipe(pump());
```

##### `share()`

Converts a pull-based replayable source into a single shared execution via `Stream.from`. Without `share()`, each consumer of a replayable source gets its own independent run — its own iterator, its own timer, its own generator instance. `share()` collapses that into one execution that broadcasts to all consumers.

Push-based sources like `eventTargetSource` or `abortSignalSource` don't need `share()` — each consumer already subscribes independently to the same live event, so there's no shared execution to coordinate. `share()` is specifically meaningful with pull-based sources like `iterableSource`, `generatorSource`, `rangeSource`, `intervalSource`, and similar.

Note: with synchronous replayable sources, the first eager consumer will exhaust the source before others can subscribe — register all consumers before triggering consumption.

```ts
const shared = intervalSource(500).pipe(share());
shared.pipe(listen((v) => console.log("A", v)));
shared.pipe(listen((v) => console.log("B", v)));
```

##### `latest(n)`

Buffers the last `n` values. Late consumers receive the buffered values immediately on subscription, then continue with live values. Requires `share()` before it when used with replayable sources, otherwise the source replays from the beginning for each consumer.

```ts
const buffered = intervalSource(300).pipe(share()).pipe(latest(2));
// subscribe 2 seconds later — immediately receives the 2 most recent values
setTimeout(() => buffered.pipe(listen(console.log)), 2000);
```

##### `switch$()`

Subscribes to the latest inner `Consumable` emitted by the upstream, cancelling the previous one when a new one arrives. Non-`Consumable` values are passed through directly. Best used with hot sources — replayable sources may be exhausted before the inner consumer attaches.

```ts
const stream = new Stream<Consumable<number> | number>();
stream.pipe(switch$()).pipe(listen(console.log));

stream.push(valuesSource(1, 2)); // starts consuming inner
stream.push(valuesSource(3, 4)); // cancels previous, starts new
stream.push(99); // plain value, passed through
```

---

#### Side Effects & Termination

##### `tap(fn)`

Runs a side-effect callback `(value, index) => void` for each value without modifying it. The value passes through unchanged.

```ts
valuesSource(1, 2, 3)
  .pipe(tap((v, i) => console.log("side effect", i, v)))
  .pipe(listen());
```

##### `tapInput(fn)`

Runs a side-effect with access to the raw input source before it enters the pipeline. Returns the input unchanged. Useful for tapping into side-channel streams like `filter.$complements` without breaking the chain.

```ts
source
  .pipe(filter((v) => v > 2))
  .pipe(tapInput((f) => f.$complements.pipe(listen(console.log))))
  .pipe(listen(console.log));
```

##### `tapTerminate(fn)`

Runs a callback when the stream terminates, receiving the termination reason (`"complete"` or `"abort"`). Values pass through unchanged.

```ts
valuesSource(1, 2, 3)
  .pipe(tapTerminate((reason) => console.log("done:", reason)))
  .pipe(listen());
// done: complete
```

##### `listen(fn?)`

Hot driver — consume the source, calls `fn` for each value, and automatically grants credit after each call. Returns the input source for further chaining. The callback is optional if you only want to drive consumption.

```ts
valuesSource(1, 2, 3).pipe(listen(console.log)); // 1, 2, 3
valuesSource(1, 2, 3).pipe(listen()); // drives consumption, no callback
```

##### `terminate()`

Ignores all values and emits the termination reason (`"complete"` or `"abort"`) as a single value when the stream ends.

```ts
valuesSource(1, 2, 3).pipe(terminate()).pipe(listen(console.log)); // 'complete'
```

##### `scope(...others)`

Terminates the pipeline when any of the provided notifier sources terminates (either `complete` or `abort`). The first notifier to terminate wins.

```ts
const lifetime = timeoutSource(5000);
intervalSource(500).pipe(scope(lifetime)).pipe(listen(console.log)); // stops after 5s
```

##### `scopeStrict(...others)`

Like `scope`, but requires all notifiers to terminate before the pipeline terminates.

```ts
const s1 = timeoutSource(1000);
const s2 = timeoutSource(2000);
intervalSource(300).pipe(scopeStrict(s1, s2)).pipe(listen(console.log)); // stops after 2s
```

##### `print(label?)`

Logs each value to the console with an optional label prefix, then passes it through unchanged. Values continue downstream.

```ts
valuesSource(1, 2, 3).pipe(print("val:")).pipe(listen()); // val: 1, val: 2, val: 3
```

##### `toConsole(label?)`

Prints each value to the console and eagerly drains the stream via `pump()`.

```ts
valuesSource(1, 2, 3).pipe(toConsole("result:")); // result: 1, result: 2, result: 3
```

## Protocol

The backpressure model is formally specified in [PROTOCOL.md](./PROTOCOL.md).

## License

MIT
