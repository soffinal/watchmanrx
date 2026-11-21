import { Consumable } from "../core/consumable";
import { Consumer } from "../core/consumer";
import { Source } from "../core/source";
import { ValueOfConsumable } from "../core/types";
import { passive } from "./passive";

/**
 * Terminates the stream when any of the `others` consumables `abort` or `complete`.
 * The first consumable to terminate will terminates the output.
 *
 * @example
 * const lifetime = timeoutSource(5000);
 * intervalSource(500).pipe(scope(lifetime)).pipe(listen(console.log));
 */
export class Scope<
  INPUT extends Consumable.AnyConsumable,
  OTHERS extends [
    other: Consumable.AnyConsumable,
    ...others: Consumable.AnyConsumable[],
  ],
  VALUE extends ValueOfConsumable<INPUT> = ValueOfConsumable<INPUT>,
> extends Source<VALUE> {
  constructor(
    private $input: INPUT,
    private others: OTHERS,
  ) {
    super();
  }
  override consume(
    handler: Consumer.Handler<VALUE>,
    options?: Consumer.Options<VALUE> | undefined,
  ): Consumer<VALUE> {
    const { terminate, ...rest } = options ?? {};

    const output$ = this.$input.consume(handler, {
      ...rest,
      terminate(c, r) {
        others$.forEach((c) => c.terminate(r));
        terminate?.(c, r);
      },
    });
    const others$ = this.others.map((other) =>
      Source.from(other)
        .pipe(passive())
        .consume((c) => c.next(), {
          terminate: (_, r) => output$.terminate(r),
        })
        .next(),
    );

    return output$;
  }
}
/**
 * Terminates the stream when any of the `others` consumables `abort` or `complete`.
 * The first consumable to terminate will terminates the output.
 *
 * @param others One or more notifier streams. The first to emit terminates the output.
 *
 * @example
 * const lifetime = timeoutSource(5000);
 * intervalSource(500).pipe(scope(lifetime)).pipe(listen(console.log));
 */
export function scope<
  INPUT extends Consumable.AnyConsumable,
  OTHERS extends [
    other: Consumable.AnyConsumable,
    ...others: Consumable.AnyConsumable[],
  ],
>(...others: OTHERS) {
  return ($input: INPUT) => new Scope($input, others);
}
