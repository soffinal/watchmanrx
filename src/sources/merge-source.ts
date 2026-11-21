import { Consumable } from "../core/consumable";
import { Consumer } from "../core/consumer";
import { Source } from "../core/source";

import { ValueOfConsumable } from "../core/types";

/**
 * Merges multiple consumables into one, emitting values from all inputs as they arrive.
 * Terminates when the last `$input` terminates.
 *
 * @example
 * const s1 = intervalSource(500).pipe(map(() => 'A'));
 * const s2 = intervalSource(700).pipe(map(() => 'B'));
 * new MergeSource(s1,s2).pipe(listen(console.log));
 */
export class MergeSource<
  INPUTS extends Consumable.AnyConsumable[],
  VALUE extends ValueOfConsumable<INPUTS[number]> = ValueOfConsumable<
    INPUTS[number]
  >,
> extends Source<VALUE> {
  constructor(readonly $inputs: INPUTS) {
    super();
  }
  consume(
    handler: Consumer.Handler<VALUE>,
    options?: Consumer.Options<VALUE>,
  ): Consumer<VALUE> {
    const { next, terminate, ...rest } = options ?? {};

    const output$ = new Consumer(handler, {
      ...rest,
      next(c) {
        consumers.forEach((entry) => {
          if (!entry.pending) {
            entry.pending = true;
            entry.consumer.next();
          }
        });

        next?.(c);
      },
      terminate(c, r) {
        consumers.forEach((entry) => entry.consumer.terminate(r));
        consumers.length = 0;
        terminate?.(c, r);
      },
    });

    let terminatedCount = this.$inputs.length;

    const consumers = this.$inputs.map(($input) => {
      const entry = {
        consumer: $input.consume(
          (_, v) => {
            entry.pending = false;
            output$.push(v);
          },
          {
            terminate(_, r) {
              terminatedCount--;
              if (!terminatedCount) output$.terminate(r);
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
 * Merges multiple consumables into one, emitting values from all inputs as they arrive.
 * Terminates when the last `$input` terminates.
 *
 * @param $inputs a set of {@link Consumable}s to merge.
 *
 * @example
 * const s1 = intervalSource(500).pipe(map(() => 'A'));
 * const s2 = intervalSource(700).pipe(map(() => 'B'));
 * mergeSource(s1,s2).pipe(listen(console.log));
 */
export function mergeSource<INPUTS extends Consumable.AnyConsumable[]>(
  ...$inputs: INPUTS
) {
  return new MergeSource($inputs);
}
