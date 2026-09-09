import {
  buildObservabilityViewUrl,
  clearObservabilitySearch,
  parseBooleanParam,
  updateObservabilitySearch,
} from "./observabilityURLState";

describe("observability URL state", () => {
  it("preserves navigation parameters while updating view state", () => {
    expect(
      updateObservabilitySearch(
        "?namespace=default&pipeline=demo&type=monoVertex&vertex=input",
        { vertexTab: "spec", specLine: 12, logsPaused: true }
      )
    ).toBe(
      "?namespace=default&pipeline=demo&type=monoVertex&vertex=input&vertexTab=spec&specLine=12&logsPaused=1"
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

  it("builds a durable metrics URL without changing the current URL", () => {
    const location = {
      pathname: "/",
      search:
        "?namespace=default&pipeline=demo&type=monoVertex&vertex=demo&vertexTab=processingRates",
    } as any;

    expect(
      buildObservabilityViewUrl(location, {
        vertexTab: "metrics",
        metric: "monovtx_processing_rate",
        metricPanels: "monovtx_processing_rate-panel",
        metricDuration: "5m",
      })
    ).toContain(
      "?namespace=default&pipeline=demo&type=monoVertex&vertex=demo&vertexTab=metrics&metric=monovtx_processing_rate&metricPanels=monovtx_processing_rate-panel&metricDuration=5m"
    );
    expect(location.search).toContain("vertexTab=processingRates");
  });
});
