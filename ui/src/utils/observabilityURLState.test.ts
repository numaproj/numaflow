import {
  buildObservabilityViewUrl,
  clearObservabilitySearch,
  parseBooleanParam,
  parseMetricFilters,
  readMetricRequestFromSearch,
  serializeMetricFilters,
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

  it("round-trips search values that contain reserved characters", () => {
    const search = updateObservabilitySearch(
      "?namespace=default&vertex=input",
      { logsSearch: "error = foo&bar%baz" }
    );
    const params = new URLSearchParams(search);
    expect(params.get("logsSearch")).toBe("error = foo&bar%baz");
  });

  it("clears specLine when patched to null", () => {
    expect(
      updateObservabilitySearch(
        "?namespace=default&vertex=input&specLine=12-15",
        { specLine: null }
      )
    ).toBe("?namespace=default&vertex=input");
  });

  it("reads only metric fields that are present in the URL", () => {
    expect(
      readMetricRequestFromSearch(
        "?namespace=default&vertex=demo&vertexTab=metrics&metric=monovtx_read_total"
      )
    ).toEqual({ req: {} });
    expect(
      readMetricRequestFromSearch(
        "?metric=monovtx_read_total&metricDimension=mono-vertex&metricDuration=1m&metricFilter=pod:demo-0"
      )
    ).toEqual({
      req: { dimension: "mono-vertex", duration: "1m" },
      filters: { pod: "demo-0" },
    });
  });

  it("parses metric filters at the first colon only", () => {
    expect(parseMetricFilters("pod:foo:bar,container:udf")).toEqual({
      pod: "foo:bar",
      container: "udf",
    });
    expect(serializeMetricFilters({ pod: "foo:bar", container: "udf" })).toBe(
      "pod:foo:bar,container:udf"
    );
  });
});
