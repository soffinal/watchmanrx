import { ValueOfArray } from "../core/types";
import { iterableSource, type IterableSource } from "./iterable-source";

/**
 * `Replayable`
 *
 * Emits a fixed list of values then completes. Shorthand for `iterableSource(values)`.
 *
 * @example
 * valuesSource(1, 2, 3).pipe(listen(console.log)); // 1, 2, 3
 */
export function valuesSource<VALUES extends [value: any, ...values: any[]]>(
  ...values: VALUES
): IterableSource<ValueOfArray<VALUES>> {
  return iterableSource(values);
}
