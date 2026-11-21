import {} from "bun";
import { describe, it, expect } from "bun:test";
import { mergeSource } from "./merge-source";
import { listen } from "../transformers/listen";
import { Stream } from "../core/stream";

describe("merge", () => {
  it("emits values from all inputs", () => {
    const s1 = new Stream<number>();
    const s2 = new Stream<number>();
    const results: number[] = [];

    mergeSource(s1, s2).pipe(listen((v) => results.push(v)));

    s1.push(1);
    s2.push(2);
    s1.push(3);
    s2.push(4);

    expect(results).toEqual([1, 2, 3, 4]);
  });

  it("terminates when all inputs terminates", () => {
    const s1 = new Stream<number>();
    const s2 = new Stream<number>();
    const results: number[] = [];

    mergeSource(s1, s2).pipe(listen((v) => results.push(v)));

    s1.push(1);
    s1.terminate("complete");
    s2.terminate("complete");
    s2.push(99); // should not arrive

    expect(results).toEqual([1]);
  });

  it("merges three streams of different types", () => {
    const s1 = new Stream<string>();
    const s2 = new Stream<number>();
    const s3 = new Stream<boolean>();
    const results: (string | number | boolean)[] = [];

    mergeSource(s1, s2, s3).pipe(listen((v) => results.push(v)));

    s1.push("a");
    s2.push(1);
    s3.push(true);

    expect(results).toEqual(["a", 1, true]);
  });
});
