import { EMPTY } from "./consts";
import { Consumable } from "./consumable";
import { Consumer } from "./consumer";
import { Empty, TerminateReason } from "./types";

/**
 * Abstract base class for all streams, source adapters and transformers.
 * it's not a mandatory but very useful primitive to extends from ,
 * the library care only about {@link Consumable} contract,
 *
 * Provides:
 * - {@link pipe} for composing transformers in a readable chain.
 * - `Symbol.asyncIterator` for consuming values with `for await...of`.
 * - {@link Source.from} static factory to wrap any {@link Consumable}.
 *
 * @template VALUE The type of values emitted by this source.
 *
 * @example
 * const source = iterableSource([1, 2, 3]);
 * source.pipe(map(v => v * 2)).pipe(listen(console.log));
 *
 * @example
 * for await (const value of iterableSource([1, 2, 3])) {
 *   console.log(value);
 * }
 */
export abstract class Source<VALUE>
  implements Consumable<VALUE>, AsyncIterable<VALUE>, Iterable<VALUE>
{
  private static _refCount = -1;
  private _name: string;
  async *[Symbol.asyncIterator](): AsyncIterator<VALUE> {
    let resolve: (value: VALUE) => void;
    let reject: (reason: TerminateReason) => void;

    const consumer = this.consume((_, value) => resolve(value), {
      terminate: (_, reason) => reject(reason),
    });

    try {
      while (consumer.status === "active" || consumer.status === "drain") {
        yield new Promise<VALUE>((res, rej) => {
          resolve = res;
          reject = rej;
          consumer.next();
        });
      }
    } catch (e) {
      if (e !== "complete") consumer.terminate("abort");
    } finally {
      consumer.terminate("complete");
    }
  }

  *[Symbol.iterator](): Iterator<VALUE> {
    let current: VALUE | Empty = EMPTY;
    const consumer = this.consume((_, value) => (current = value));

    try {
      while (consumer.status !== "abort" && consumer.status !== "complete") {
        consumer.next();
        if (current === EMPTY) break;
        yield current as VALUE;
        current = EMPTY;
      }
    } catch (error) {
      consumer.terminate("abort");
    } finally {
      consumer.terminate("complete");
    }
  }

  constructor(name?: string) {
    Source._refCount++;
    if (Source._refCount === Number.MAX_SAFE_INTEGER) Source._refCount = 1;

    this._name = `${name ?? "source"}_${Source._refCount}`;
  }
  get name(): string {
    return this._name;
  }
  abstract consume(
    handler: Consumer.Handler<VALUE>,
    options?: Consumer.Options<VALUE>,
  ): Consumer<VALUE>;
  /**
   * Composes this source with a transformer function.
   * The transformer receives this source and returns a new output (usually another `Source`).
   *
   * @param transform A function that takes this source and returns a transformed output.
   * @returns The result of applying the transformer.
   */
  pipe<OUTPUT>(transform: ($input: this) => OUTPUT): OUTPUT {
    return transform(this);
  }
  /**
   * A terminal operation that collect "n" values or one value sync/async
   * @param n the number of requested values
   * @param options if options.sync is true then it return values synchronously
   * @return values or promise of values or value or promise of value
   */
  collect<N extends number = 1, SYNC extends boolean = false>(
    n = 1 as N,
    options?: Source.CollectOptions<SYNC>,
  ): N extends 1
    ? SYNC extends false
      ? Promise<VALUE | Empty>
      : VALUE | Empty
    : SYNC extends false
      ? Promise<VALUE[]>
      : VALUE[] {
    let count = n;
    const values: VALUE[] = [];

    if (options?.sync) {
      for (const value of this) {
        values.push(value);
        if (--count === 0) break;
      }
      return (n > 1 ? values : (values[0] ?? EMPTY)) as never;
    } else {
      return new Promise(async (resolve) => {
        for await (const value of this) {
          values.push(value);
          if (--count === 0) break;
        }
        resolve(n > 1 ? values : (values[0] ?? EMPTY));
      }) as never;
    }
  }
  /**
   * Wraps any {@link Consumable} in a `Source` proxy, giving it access to `pipe`
   * and async iteration without changing its consumption behaviour.
   * and it's used for wrapping a complex sources like {@link Stream} to encapsulate behaviors and get a readonly source.
   *
   * @param consumable Any consumable to wrap and/or hide behaviors .
   */
  static from<VALUE>($consumable: Consumable<VALUE>): Source<VALUE> {
    return new SourceProxy($consumable);
  }
}

export namespace Source {
  export type AnySource = Source<any>;
  export type CollectOptions<SYNC extends boolean> = { sync?: SYNC };
}

class SourceProxy<VALUE> extends Source<VALUE> {
  constructor(private $consumable: Consumable<VALUE>) {
    super();
  }
  consume(
    handler: Consumer.Handler<VALUE>,
    options?: Consumer.Options<VALUE>,
  ): Consumer<VALUE> {
    return this.$consumable.consume(handler, options);
  }
}
