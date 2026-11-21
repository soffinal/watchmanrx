import { EMPTY } from "../core/consts";
import { Consumable } from "../core/consumable";
import { Consumer } from "../core/consumer";
import { Source } from "../core/source";
import { Empty, ValueOfConsumable } from "../core/types";

/**
 * Emits the value or {@link EMPTY} from each input whenever any input emits.
 * To get the CombineLatest behvior,  pipe `latest` to each input before passing it to this transformer .
 * Unlike `zip`, does not wait for all inputs to have a value — uses the last known value or {@link EMPTY} .
 * Terminate when any of the inputs terminate.
 *
 * @example
 * const s1 = valuesSource(1,2,3).pipe(delay(100));
 * const s2 = valuesSource('a','b').pipe(delay(200));
 * new CombineSource(s1,s2).pipe(listen(console.log));
 */
export class CombineSource<
  INPUTS extends Consumable.AnyConsumable[],
  VALUE extends [
    ...{ [K in keyof INPUTS]: ValueOfConsumable<INPUTS[K]> | Empty },
  ] = [...{ [K in keyof INPUTS]: ValueOfConsumable<INPUTS[K]> | Empty }],
> extends Source<VALUE> {
  constructor(readonly $inputs: INPUTS) {
    super();
  }

  override consume(
    handler: Consumer.Handler<VALUE>,
    options?: Consumer.Options<VALUE>,
  ): Consumer<VALUE> {
    const { next, terminate, ...rest } = options ?? {};

    const buffer = new Array(this.$inputs.length).fill(EMPTY);

    const output$ = new Consumer<VALUE>(handler, {
      ...rest,
      next(r) {
        consumers.forEach((entry) => {
          if (!entry.pending) {
            entry.pending = true;
            entry.consumer$.next();
          }
        });
        next?.(r);
      },
      terminate: (c, r) => {
        consumers.forEach((entry) => entry.consumer$.terminate(r));
        consumers.length = 0;
        buffer.length = 0;
        terminate?.(c, r);
      },
    });

    const consumers = this.$inputs.map(($input, index) => {
      const entry = {
        consumer$: $input.consume(
          (_, value) => {
            buffer[index] = value;
            entry.pending = false;
            output$.push([...buffer] as any);
          },
          {
            terminate(c, r) {
              output$.terminate(r);
            },
          },
        ),
        pending: false,
      };
      return entry;
    });
    return output$;
  }
}

/**
 * Emits the value or {@link EMPTY} from each input whenever any input emits.
 * To get the CombineLatest behvior,  pipe `latest` to each input before passing it to this transformer .
 * Unlike `zip`, does not wait for all inputs to have a value — uses the last known value or {@link EMPTY} .
 * Terminate when any of the inputs terminate.

 * @param $inputs a set of {@link Consumable}s to combine.
 *
 * @example
 * const s1 = valuesSource(1,2,3).pipe(delay(100));
 * const s2 = valuesSource('a','b').pipe(delay(200));
 * combineSource(s1,s2).pipe(listen(console.log));
 */
export function combineSource<INPUTS extends Consumable.AnyConsumable[]>(
  ...$inputs: INPUTS
) {
  return new CombineSource($inputs);
}
