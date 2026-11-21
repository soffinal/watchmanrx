import {} from "bun";
import { describe, it, expect } from "bun:test";
import { iteratorSource } from "./iterator-source";
import { listen } from "../transformers/listen";

describe("iteratorSource", () => {
  it("emits values from an iterator", () => {
    const results: number[] = [];
    iteratorSource([1, 2, 3].values()).pipe(listen((v) => results.push(v)));
    expect(results).toEqual([1, 2, 3]);
  });

  it("accepts a factory function returning an iterator", () => {
    const results: number[] = [];
    iteratorSource(() => [10, 20, 30].values()).pipe(
      listen((v) => results.push(v)),
    );
    expect(results).toEqual([10, 20, 30]);
  });

  it("completes when iterator is done", () => {
    let status: string | undefined;
    iteratorSource([1].values())
      .consume((self, v) => self.next(), {
        terminate: (_, r) => (status = r),
      })
      .next();
    expect(status).toBe("complete");
  });

  it("emits nothing for empty iterator", () => {
    const results: number[] = [];
    iteratorSource<number>([].values()).pipe(listen((v) => results.push(v)));
    expect(results).toEqual([]);
  });
});
