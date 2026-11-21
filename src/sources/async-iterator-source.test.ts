import { describe, it, expect } from "bun:test";
import { asyncIteratorSource } from "./async-iterator-source";

describe("asyncIteratorSource", () => {
  it("emits values from an async iterator", async () => {
    const results: number[] = [];
    let counter = 0;
    const iter: AsyncIterator<number> = {
      next: () =>
        Promise.resolve(
          ++counter > 3
            ? { value: undefined as any, done: true }
            : { value: counter },
        ),
    };

    await new Promise<void>((done) => {
      asyncIteratorSource(iter)
        .consume(
          (self, v) => {
            results.push(v);
            if (results.length === 3) done();
            else self.next();
          },
          { terminate: () => done() },
        )
        .next();
    });

    expect(results).toEqual([1, 2, 3]);
  });

  it("accepts a factory function", async () => {
    const results: number[] = [];
    let counter = 0;
    const factory = () => ({
      next: () =>
        Promise.resolve(
          ++counter > 2
            ? { value: undefined as any, done: true }
            : { value: counter },
        ),
    });

    await new Promise<void>((done) => {
      asyncIteratorSource(factory)
        .consume(
          (self, v) => {
            results.push(v);
            self.next();
          },
          { terminate: () => done() },
        )
        .next();
    });

    expect(results).toEqual([1, 2]);
  });

  it("completes when iterator is done", async () => {
    let status: string | undefined;
    const iter: AsyncIterator<number> = {
      next: () => Promise.resolve({ value: undefined as any, done: true }),
    };

    await new Promise<void>((done) => {
      asyncIteratorSource(iter)
        .consume((self, v) => self.next(), {
          terminate: (_, r) => {
            status = r;
            done();
          },
        })
        .next();
    });

    expect(status).toBe("complete");
  });
});
