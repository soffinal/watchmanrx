import {} from "bun";
import { describe, it, expect, mock } from "bun:test";
import { abortControllerSource } from "./abort-controller-source";
import { listen } from "../transformers/listen";
describe("abortControllerSource", () => {
  it("emit when abort controller abort", () => {
    const controller = new AbortController();
    const fn = mock(() => {});
    abortControllerSource(controller).pipe(listen(fn));
    controller.abort();
    expect(fn).toHaveBeenCalled();
  });

  it("emit immediately when abort signal is already abort", () => {
    const controller = new AbortController();
    const fn = mock(() => {});
    controller.abort();
    abortControllerSource(controller).pipe(listen(fn));
    expect(fn).toHaveBeenCalled();
  });
});
