import { EMPTY } from "../core/consts";
import { Consumable } from "../core/consumable";
import { Consumer } from "../core/consumer";
import { Source } from "../core/source";
import { Stream } from "../core/stream";
import { Empty, ValueOfConsumable, ZipedArray } from "../core/types";

/**
 * Combines values from multiple {@link Consumable}s pairwise into tuples.
 * Emits a tuple only when all inputs have provided a new value.
 * Unmatched values remaining at termination are available on `$rest`.
 * Terminate when any of the $inputs terminate
 *
 * @example
 * const s1 = new Stream<number>();
 * const s2 = new Stream<string>();
 *
 * new ZipSource(s1,s2).pipe(listen(console.log));
 * s1.push(1); s2.push('a'); // [1, 'a']
 */
export class ZipSource<
  INPUTS extends Consumable.AnyConsumable[],
> extends Source<ZipedArray<INPUTS>> {
  private _$rest?: Stream<
    [...{ [K in keyof INPUTS]: ValueOfConsumable<INPUTS[K]> | Empty }]
  >;
  constructor(readonly $inputs: INPUTS) {
    super();
  }
  get $rest() {
    return Source.from(
      (this._$rest ??= new Stream({
        lastConsumerLeft: (stream, consumer) => {
          this._$rest = undefined;
        },
      })),
    );
  }
  override consume(
    handler: Consumer.Handler<ZipedArray<INPUTS>>,
    options?: Consumer.Options<ZipedArray<INPUTS>>,
  ): Consumer<ZipedArray<INPUTS>> {
    const { next, terminate, ...rest } = options ?? {};

    const buffer = new Array(this.$inputs.length).fill(EMPTY);
    let count = buffer.length;

    const output$ = new Consumer<ZipedArray<INPUTS>>(handler, {
      ...rest,
      next(consumer) {
        consumers$.forEach((c) => c.next());
        next?.(consumer);
      },
      terminate: (consumer, reason) => {
        if (count < buffer.length) this._$rest?.push([...buffer] as any);
        consumers$.forEach((c) => c.terminate(reason));
        consumers$.length = 0;
        buffer.length = 0;
        terminate?.(consumer, reason);
      },
    });

    const consumers$ = this.$inputs.map(($input, index) =>
      $input.consume(
        (_, value) => {
          buffer[index] = value;

          if (!--count) {
            count = buffer.length;
            const values = [...buffer];
            buffer.fill(EMPTY);
            output$.push(values as any);
          }
        },
        {
          terminate(_, r) {
            output$.terminate(r);
          },
        },
      ),
    );

    return output$;
  }
}
/**
 * Combines values from multiple {@link Consumable}s pairwise into tuples.
 * Emits a tuple only when all inputs have provided a new value.
 * Unmatched values remaining at termination are available on `$rest`.
 * Terminate when any of the $inputs terminate
 *
 * @param $inputs a set of {@link Consumable}s to zip.
 *
 * @example
 * const s1 = new Stream<number>();
 * const s2 = new Stream<string>();
 * zipSource(s1,s2).pipe(listen(console.log));
 * s1.push(1); s2.push('a'); // [1, 'a']
 */
export function zipSource<INPUTS extends Consumable.AnyConsumable[]>(
  ...$inputs: INPUTS
) {
  return new ZipSource($inputs);
}
