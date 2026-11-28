import { Consumable } from "../core/consumable";
import { Stream } from "../core/stream";
import { ValueOfConsumable } from "../core/types";

/**
 * Converts a `Consumable` into a multicast `Stream` via `Stream.from`.
 * Multiple consumers will receive the same values.
 * `NOTE`: using share with replayable synchronous sources like `iterableSource`
 * will consumer the source by the first synchronous eager consumer so other consumers
 * will not get any value until you trigger consumption after the registration
 * of all consumers
 *
 * @param options Optional Stream.from options.
 *
 * @example
 * const shared = intervalSource(500).pipe(share());
 * shared.pipe(listen(v => console.log('A', v)));
 * shared.pipe(listen(v => console.log('B', v)));
 */
export function share<INPUT extends Consumable.AnyConsumable>(
  options?: Stream.FromOptions<ValueOfConsumable<INPUT>>,
) {
  return ($input: INPUT) =>
    Stream.from<ValueOfConsumable<INPUT>>($input, options);
}
