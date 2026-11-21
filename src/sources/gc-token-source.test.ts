import { describe, it, expect } from "bun:test";
import { gcTokenSource } from "./gc-token-source";

describe("gcTokenSource", () => {
  it("should emit after a token is garbage collected", async () => {
    let result = "";
    const { promise, resolve } = Promise.withResolvers();

    {
      const object = {} as any;

      gcTokenSource(object)
        .consume(() => {
          result = "kechmahaja";
          resolve();
        })
        .next();
    }
    Bun.gc(true);

    await promise;

    expect(result).toBe("kechmahaja");
  });
});
