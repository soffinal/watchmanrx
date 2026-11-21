import { describe, it, expect } from "bun:test";
import { valuesSource } from "../sources/values-source";
import { take } from "./take";
import { listen } from "./listen";
import { rangeSource } from "../sources/range-source";

describe("take", () => {
  it("takes the first N values then terminates", () => {
    const results: number[] = [];
    valuesSource(1, 2, 3, 4, 5)
      .pipe(take(3))
      .pipe(listen((v) => results.push(v)));
    expect(results).toEqual([1, 2, 3]);
  });

  it("take(0) emits nothing", () => {
    const results: number[] = [];
    valuesSource(1, 2, 3)
      .pipe(take(0))
      .pipe(listen((v) => results.push(v)));
    expect(results).toEqual([]);
  });

  it("take more than available emits all", () => {
    const results: number[] = [];
    valuesSource(1, 2)
      .pipe(take(10))
      .pipe(listen((v) => results.push(v)));
    expect(results).toEqual([1, 2]);
  });

  it("terminates upstream after N values", () => {
    const results: number[] = [];
    rangeSource(1, 1000)
      .pipe(take(3))
      .pipe(listen((v) => results.push(v)));
    expect(results).toEqual([1, 2, 3]);
  });
});
