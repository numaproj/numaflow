import {
  clearObservabilitySearch,
  parseBooleanParam,
  updateObservabilitySearch,
} from "./observabilityURLState";

describe("observability URL state", () => {
  it("preserves navigation parameters while updating view state", () => {
    expect(
      updateObservabilitySearch(
        "?namespace=default&pipeline=demo&type=monoVertex&vertex=input",
        { vertexTab: "pods", logsPaused: true }
      )
    ).toBe(
      "?namespace=default&pipeline=demo&type=monoVertex&vertex=input&vertexTab=pods&logsPaused=1"
    );
  });

  it("removes empty values and clears only observability state", () => {
    const search = clearObservabilitySearch(
      "?namespace=default&pipeline=demo&vertex=input&pod=demo-input-0&logsSearch=error"
    );

    expect(search).toBe("?namespace=default&pipeline=demo");
  });

  it("parses booleans with a caller supplied default", () => {
    expect(parseBooleanParam(new URLSearchParams("logsFocus=1"), "logsFocus")).toBe(
      true
    );
    expect(parseBooleanParam(new URLSearchParams(), "logsWrap", true)).toBe(
      true
    );
  });
});
