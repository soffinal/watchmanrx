import { describe, it, expect } from "bun:test";
import { combineSource } from "./combine-source";
import { listen } from "../transformers/listen";
import { Stream } from "../core/stream";
import { EMPTY } from "../core/consts";

describe("combine", () => {
  it("emits  values or EMPTY from all inputs on any emission", () => {
    const s1 = new Stream<number>();
    const s2 = new Stream<string>();
    const results: any[] = [];

    combineSource(s1, s2).pipe(listen((v) => results.push([...v])));

    s1.push(1);
    s2.push("a");
    s1.push(2);

    expect(results[0]).toEqual([1, EMPTY]);
    expect(results[1]).toEqual([1, "a"]);
    expect(results[2]).toEqual([2, "a"]);
  });
  it("terminate  when any input terminate", () => {
    const s1 = new Stream<number>();
    const s2 = new Stream<string>();
    const results: any[] = [];

    combineSource(s1, s2).pipe(listen((v) => results.push([...v])));

    s1.push(1);
    s2.push("a");
    s2.terminate("complete");
    s1.push(2);

    expect(results[0]).toEqual([1, EMPTY]);
    expect(results[1]).toEqual([1, "a"]);
    expect(results[2]).toBe(undefined);
  });
});
