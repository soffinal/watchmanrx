import { describe, it, expect } from "bun:test";
import { iterableSource } from "./iterable-source";
import { listen } from "../transformers/listen";

describe("iterableSource", () => {
  it("emits values from an array", () => {
    const results: number[] = [];
    iterableSource([1, 2, 3]).pipe(listen((v) => results.push(v)));
    expect(results).toEqual([1, 2, 3]);
  });

  it("emits values from a Set", () => {
    const results: number[] = [];
    iterableSource(new Set([1, 2, 3])).pipe(listen((v) => results.push(v)));
    expect(results).toEqual([1, 2, 3]);
  });

  it("accepts a factory function", () => {
    const results: number[] = [];
    iterableSource(() => [10, 20, 30]).pipe(listen((v) => results.push(v)));
    expect(results).toEqual([10, 20, 30]);
  });

  it("creates a new iterator per consumer", () => {
    const r1: number[] = [];
    const r2: number[] = [];
    const source = iterableSource([1, 2, 3]);
    source.pipe(listen((v) => r1.push(v)));
    source.pipe(listen((v) => r2.push(v)));
    expect(r1).toEqual([1, 2, 3]);
    expect(r2).toEqual([1, 2, 3]);
  });

  it("completes after all values are emitted", () => {
    let status: string | undefined;
    iterableSource([1])
      .consume((self, v) => self.next(), {
        terminate: (_, r) => (status = r),
      })
      .next();
    expect(status).toBe("complete");
  });

  it("emits nothing for empty iterable", () => {
    const results: number[] = [];
    iterableSource<number>([]).pipe(listen((v) => results.push(v)));
    expect(results).toEqual([]);
  });
});
