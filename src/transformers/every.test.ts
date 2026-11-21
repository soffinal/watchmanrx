import { describe, it, expect } from "bun:test";
import { valuesSource } from "../sources/values-source";
import { every } from "./every";
import { listen } from "./listen";
import { Stream } from "../core/stream";

describe("every", () => {
  it("emits true when all values satisfy predicate", () => {
    let result: any;

    valuesSource(1, 2, 3)
      .pipe(
        every((v) => {
          return v > 0;
        }),
      )
      .pipe(
        listen((v) => {
          result = v;
        }),
      );

    expect(result).toBe(true);
  });

  it("emits false and terminates early on first failure", () => {
    let result: any;
    const seen: number[] = [];
    valuesSource(1, -1, 3, 4)
      .pipe(
        every((v) => {
          seen.push(v);
          return v > 0;
        }),
      )
      .pipe(listen((v) => (result = v)));

    expect(result).toBe(false);
    expect(seen).toEqual([1, -1]);
  });

  it("emits true for empty stream", () => {
    let result: any;
    const stream = new Stream<number>();
    stream.pipe(every((v) => v > 0)).pipe(listen((v) => (result = v)));
    stream.terminate("complete");

    expect(result).toBe(true);
  });
});
