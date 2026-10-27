import {
  bufferPercentFull,
  buildVertexHealth,
  drainTime,
  formatAgo,
  formatCompact,
  formatDrain,
  formatMicros,
  appendDaemonSample,
  daemonSeries,
  readDaemonRates,
  seriesValues,
  splitDropSeries,
  signalQueries,
  slowestStage,
  toSparkline,
  trendVsAverage,
  VertexHealthInput,
  watermarkLag,
  worstTone,
} from "./goldenSignalsModel";
import { HealthCard, PodFleetItem } from "../../../../../../../types/declarations/pods";

const NOW = Date.parse("2026-10-12T12:00:00Z");
const monoIds = { namespaceId: "ns", pipelineId: "mv", vertexId: "mv", type: "monoVertex" };
const pipelineIds = { namespaceId: "ns", pipelineId: "p", vertexId: "cat", type: "udf" };

function fleetItem(overrides: Partial<PodFleetItem> = {}): PodFleetItem {
  return {
    pod: { name: "pod-0", containers: ["numa"], containerSpecMap: new Map() },
    runtime: { name: "pod-0", status: "Running", containerDetailsMap: {} },
    severity: "healthy",
    containerResources: [],
    restartCount: 0,
    ...overrides,
  };
}

function input(overrides: Partial<VertexHealthInput> = {}): VertexHealthInput {
  return {
    type: "monoVertex",
    vertexId: "mv",
    metricsEnabled: true,
    now: NOW,
    daemon: { rate1m: 15, rate5m: 15, rate15m: 15, pending: 0 },
    series: {
      p99: [1000, 1000, 1000],
      drops: [],
      duplicateDrops: [],
      nacks: [],
      fallback: [],
      critical: [],
    },
    queries: signalQueries(monoIds),
    fleetItems: [fleetItem()],
    ...overrides,
  };
}

function card(health: ReturnType<typeof buildVertexHealth>, id: HealthCard["id"]): HealthCard {
  return health.cards.find((entry) => entry.id === id) as HealthCard;
}

describe("signalQueries", () => {
  it("uses MonoVertex patterns and filters, including fallback writes", () => {
    const queries = signalQueries(monoIds);
    expect(queries.p99).toEqual(
      expect.objectContaining({
        pattern_name: "mono_vertex_histogram",
        metric_name: "monovtx_processing_time_bucket",
        dimension: "mono-vertex",
        quantile: "0.99",
        filters: { namespace: "ns", mvtx_name: "mv" },
      })
    );
    expect(queries.drops?.metric_name).toBe("monovtx_dropped_total");
    expect(queries.fallback?.metric_name).toBe("monovtx_fallback_sink_write_total");
    expect(queries.critical).toEqual(
      expect.objectContaining({
        pattern_name: "mono_vertex_critical_errors",
        metric_name: "monovtx_critical_error_total",
        duration: "5m",
      })
    );
    expect(queries.stageWrite).toBeUndefined();
  });

  it("uses pipeline vertex patterns without fallback or transformer stages", () => {
    const queries = signalQueries(pipelineIds);
    expect(queries.p99?.pattern_name).toBe("vertex_histogram");
    expect(queries.drops?.metric_name).toBe("forwarder_drop_total");
    expect(queries.drops?.filters).toEqual({ namespace: "ns", pipeline: "p", vertex: "cat" });
    expect(queries.critical).toEqual(
      expect.objectContaining({
        pattern_name: "vertex_critical_errors",
        metric_name: "forwarder_critical_error_total",
        duration: "5m",
      })
    );
    expect(queries.fallback).toBeUndefined();
    expect(queries.stageTransformer).toBeUndefined();
  });
});

describe("series math and formatting", () => {
  it("sums series per timestamp and drops negative samples", () => {
    expect(
      seriesValues([
        { values: [[2, "1"], [1, "2"]] },
        { values: [[1, "3"], [2, "-1"]] },
      ])
    ).toEqual([5, 1]);
    expect(seriesValues(null)).toBeNull();
    expect(seriesValues([])).toEqual([]);
  });

  it("keeps duplicate drops out of the loss series", () => {
    expect(
      splitDropSeries(
        [
          { metric: { reason: "duplicate" }, values: [[1, "1"], [2, "2"]] },
          { metric: { reason: "buffer full" }, values: [[1, "3"], [2, "4"]] },
          { metric: { reason: "duplicate" }, values: [[1, "1"]] },
        ],
        false
      )
    ).toEqual({ loss: [3, 4], duplicate: [2, 2] });
    expect(
      splitDropSeries([{ metric: { mvtx_name: "mv" }, values: [[1, "4"]] }], true)
    ).toEqual({ loss: [], duplicate: [4] });
  });

  it("compares the latest point with the earlier average", () => {
    expect(trendVsAverage([10, 10, 20])).toBeCloseTo(1);
    expect(trendVsAverage([5])).toBeUndefined();
    expect(trendVsAverage([0, 0, 4])).toBeUndefined();
  });

  it("resamples into scaled bars", () => {
    const bars = toSparkline([1, 1, 2, 2, 3, 3, 4, 4, 5, 5, 6, 6, 7, 7, 8, 8]);
    expect(bars).toHaveLength(8);
    expect(bars?.[7]).toBe(100);
    expect(bars?.[0]).toBe(13);
    expect(toSparkline([0, 0])).toEqual(Array(8).fill(8));
    expect(toSparkline([])).toBeUndefined();
  });

  it("formats microseconds, counts, ages, and drain times", () => {
    expect(formatMicros(450)).toBe("450 µs");
    expect(formatMicros(3_200)).toBe("3.2 ms");
    expect(formatMicros(4_290_000)).toBe("4.29 s");
    expect(formatCompact(2800)).toBe("2.8k");
    expect(formatCompact(14.83)).toBe("15");
    expect(formatCompact(0.24)).toBe("0.2");
    expect(formatAgo("2026-09-30T12:00:00Z", NOW)).toBe("12d ago");
    expect(formatAgo(undefined, NOW)).toBeUndefined();
    expect(formatDrain(30)).toBe("<1 min");
    expect(formatDrain(240)).toBe("~4 min");
  });
});

describe("derived signals", () => {
  it("computes drain time for empty, draining, stalled, and unreported backlogs", () => {
    expect(drainTime(0, 10)).toBe(0);
    expect(drainTime(300, 10)).toBe(30);
    expect(drainTime(5, 0)).toBe(Infinity);
    expect(drainTime(undefined, 10)).toBeUndefined();
  });

  it("uses outgoing edges, falls back to incoming for sinks, and ignores -1", () => {
    const edges = [
      { from: "in", to: "cat", watermarks: [NOW - 5000] },
      { from: "cat", to: "out", watermarks: [String(NOW - 2000), "-1"] },
    ];
    expect(watermarkLag(edges, "cat", NOW)).toBe(2000);
    expect(watermarkLag(edges, "out", NOW)).toBe(2000);
    expect(watermarkLag([{ from: "cat", to: "out", watermarks: [-1] }], "cat", NOW)).toBeUndefined();
  });

  it("finds the slowest stage and the worst tone", () => {
    expect(
      slowestStage([
        { name: "read", micros: 10 },
        { name: "sink", micros: 30 },
      ])?.name
    ).toBe("sink");
    expect(worstTone(["ok", "warn", "neutral"])).toBe("warn");
    expect(worstTone(["neutral", undefined])).toBe("neutral");
  });

  it("reads daemon rates for MonoVertex and sums pipeline partitions, ignoring -1", () => {
    expect(
      readDaemonRates(
        { processingRates: { "1m": "15", "5m": "14", "15m": "13" }, pendings: { default: "-1" } },
        "monoVertex",
        "mv"
      )
    ).toEqual({ rate1m: 15, rate5m: 14, rate15m: 13, pending: undefined });
    expect(
      readDaemonRates(
        {
          cat: [
            { processingRates: { "1m": 1 }, pendings: { default: 2 } },
            { processingRates: { "1m": 3 }, pendings: { default: 4 } },
          ],
        },
        "udf",
        "cat"
      )
    ).toEqual(expect.objectContaining({ rate1m: 4, pending: 6 }));
  });

  it("keeps a 30 minute daemon history and ignores an empty snapshot", () => {
    const now = 40 * 60_000;
    const kept = appendDaemonSample(
      [
        { at: now - 31 * 60_000, rate1m: 1 },
        { at: now - 60_000, rate1m: 4, pending: 2 },
      ],
      { at: now, rate1m: 5, pending: 3 },
      now
    );
    expect(kept.map((sample) => sample.rate1m)).toEqual([4, 5]);
    expect(daemonSeries(kept, "pending")).toEqual([2, 3]);
    expect(appendDaemonSample(kept, { at: now }, now)).toEqual([
      { at: now - 60_000, rate1m: 4, pending: 2 },
    ]);
    const stable = [{ at: now - 1000, rate1m: 1 }];
    expect(appendDaemonSample(stable, { at: now }, now)).toBe(stable);
    expect(appendDaemonSample([{ at: now, rate1m: 1 }], { at: now, rate1m: 9 }, now)).toEqual([
      { at: now, rate1m: 9 },
    ]);
  });
});

describe("buildVertexHealth", () => {
  it("summarizes a healthy MonoVertex", () => {
    const health = buildVertexHealth(
      input({
        health: { dataHealthStatus: "healthy", dataHealthCode: "D1" },
        monoStatus: { readyReplicas: 3, desiredReplicas: 3, lastScaledAt: "2026-09-30T12:00:00Z" },
        series: {
          p99: [1000, 1000, 1000],
          stageRead: [10],
          stageSink: [900],
          drops: [],
          duplicateDrops: [],
          nacks: [],
          fallback: [],
          critical: [],
        },
      })
    );
    expect(health.tone).toBe("ok");
    expect(health.verdict).toEqual({ label: "Data flow healthy (D1)", tone: "ok" });
    expect(health.replicas).toEqual({ label: "3/3 ready · scaled 12d ago", tone: "ok" });
    expect(card(health, "latency")).toEqual(
      expect.objectContaining({
        value: "P99 1.0 ms",
        detail: "slowest stage: sink P99 900 µs",
        stages: [
          { name: "read", value: "10 µs" },
          { name: "sink", value: "900 µs" },
        ],
        stagesNote: undefined,
      })
    );
    expect(card(health, "traffic")).toEqual(
      expect.objectContaining({ value: "15 msg/s", detail: "5m avg 15 · 15m avg 15" })
    );
    expect(card(health, "backlog").value).toBe("No backlog");
    expect(card(health, "loss")).toEqual(
      expect.objectContaining({
        title: "Message outcomes",
        value: "No drops",
        detail: "No active outcomes · hover for details",
      })
    );
    expect(card(health, "errors")).toEqual(
      expect.objectContaining({ value: "All 1 pod healthy", detail: "0 restarts · 0 errors (24h)", tone: "ok" })
    );
  });

  it("explains batch time that the stages do not account for", () => {
    const latency = card(
      buildVertexHealth(
        input({ series: { p99: [4_290_000], stageRead: [600], stageSink: [1900], stageAck: [100] } })
      ),
      "latency"
    );
    expect(latency.stagesNote).toBe(
      "Stages add up to 2.6 ms; the rest of the batch time is spent waiting for messages."
    );
  });

  it("applies latency trend thresholds", () => {
    expect(card(buildVertexHealth(input({ series: { p99: [100, 100, 130] } })), "latency").tone).toBe(
      "warn"
    );
    expect(card(buildVertexHealth(input({ series: { p99: [100, 100, 250] } })), "latency").tone).toBe(
      "critical"
    );
  });

  it("flags stalled, idle, and slowing traffic", () => {
    const stalled = card(buildVertexHealth(input({ daemon: { rate1m: 0, pending: 7 } })), "traffic");
    expect(stalled).toEqual(expect.objectContaining({ value: "Stalled", tone: "critical" }));
    const idle = card(buildVertexHealth(input({ daemon: { rate1m: 0, pending: 0 } })), "traffic");
    expect(idle).toEqual(expect.objectContaining({ value: "Idle", tone: "neutral" }));
    const slowing = card(buildVertexHealth(input({ daemon: { rate1m: 4, rate15m: 10 } })), "traffic");
    expect(slowing.tone).toBe("warn");
    const history = [
      { at: 1, rate1m: 10 },
      { at: 2, rate1m: 20 },
    ];
    const traced = card(
      buildVertexHealth(
        input({
          metricsEnabled: false,
          daemonHistory: history,
          series: { readRate: [1, 2, 3] } as VertexHealthInput["series"],
        })
      ),
      "traffic"
    );
    expect(traced.sparkline).toEqual(toSparkline(daemonSeries(history, "rate1m")));
    expect(traced.detail).toBe("5m avg 15 · 15m avg 15");
  });

  it("measures buffer fullness against the usage limit", () => {
    const buffer = {
      bufferName: "b",
      ackPendingCount: 0,
      pendingCount: 0,
      totalMessages: 0,
      bufferLength: 100,
      isFull: false,
      bufferUsage: 0.4,
      bufferUsageLimit: 0.8,
    };
    expect(bufferPercentFull(buffer)).toBe(50);
    expect(bufferPercentFull({ ...buffer, bufferUsage: 0.8 })).toBe(100);
    expect(bufferPercentFull({ ...buffer, bufferUsage: 0.95 })).toBe(100);
    expect(bufferPercentFull({ ...buffer, bufferUsage: 0.1, isFull: true })).toBe(100);
    expect(bufferPercentFull({ ...buffer, bufferUsageLimit: 0 })).toBe(40);
  });

  it("covers each backlog state and rule", () => {
    const backlog = (overrides: Partial<VertexHealthInput>) =>
      card(buildVertexHealth(input(overrides)), "backlog");
    expect(backlog({ daemon: { rate1m: 1, pending: 240 } })).toEqual(
      expect.objectContaining({ value: "~4 min to drain", detail: "240 pending", tone: "ok" })
    );
    expect(backlog({ daemon: { rate1m: 1, pending: 600 } }).tone).toBe("warn");
    expect(backlog({ daemon: { rate1m: 0, pending: 5 } })).toEqual(
      expect.objectContaining({ value: "Not draining", tone: "critical" })
    );
    expect(backlog({ daemon: { rate1m: 15 } })).toEqual(
      expect.objectContaining({ value: "Pending not reported", tone: "neutral" })
    );
    expect(
      backlog({
        daemon: { rate1m: 1, pending: 3 },
        daemonHistory: [
          { at: 1, pending: 1 },
          { at: 2, pending: 1 },
          { at: 3, pending: 3 },
        ],
        series: {},
      }).detail
    ).toBe("3 pending · ↑200% vs 30m avg");
    expect(
      backlog({
        daemon: { rate1m: 1, pending: 3 },
        daemonHistory: [
          { at: 1, pending: 1 },
          { at: 2, pending: 1 },
          { at: 3, pending: 3 },
        ],
        series: {},
      }).breakdown?.items.find((item) => item.name === "Pending change")?.description
    ).toContain("last 30 minutes");

    const buffer = {
      bufferName: "b",
      ackPendingCount: 0,
      pendingCount: 0,
      totalMessages: 0,
      bufferLength: 100,
      isFull: false,
      bufferUsage: 0.12,
      bufferUsageLimit: 0.8,
    };
    const pipeline = {
      type: "udf",
      vertexId: "cat",
      queries: signalQueries(pipelineIds),
      daemon: { rate1m: 10, pending: 3 },
    };
    const withBuffer = backlog({
      ...pipeline,
      buffers: [buffer],
      watermarkEdges: [{ from: "cat", to: "out", watermarks: [NOW - 2100] }],
    });
    expect(withBuffer).toEqual(
      expect.objectContaining({
        value: "<1 min to drain",
        detail: "3 pending · buffer 15% full · watermark lag 2.1 s",
        tone: "ok",
        inspect: { kind: "tab", tab: "buffers" },
      })
    );
    expect(withBuffer.breakdown?.items.map((item) => item.name)).toEqual([
      "Pending messages",
      "Buffer fullness",
      "Watermark lag",
    ]);
    expect(backlog({ ...pipeline, daemon: { rate1m: 10 }, buffers: [buffer] })).toEqual(
      expect.objectContaining({ value: "buffer 15% full", detail: "Source does not report pending messages" })
    );
    expect(
      backlog({ ...pipeline, buffers: [buffer, { ...buffer, bufferUsage: 0.4, bufferUsageLimit: 0.5 }] })
        .detail
    ).toContain("buffer 80% full");
    expect(backlog({ ...pipeline, buffers: [{ ...buffer, bufferUsage: 0.65 }] }).tone).toBe("warn");
    expect(backlog({ ...pipeline, buffers: [{ ...buffer, isFull: true }] }).tone).toBe("critical");
    expect(
      backlog({ ...pipeline, watermarkEdges: [{ from: "cat", to: "out", watermarks: [NOW - 90_000] }] })
        .tone
    ).toBe("warn");
    expect(
      backlog({ ...pipeline, watermarkEdges: [{ from: "cat", to: "out", watermarks: [NOW - 400_000] }] })
        .tone
    ).toBe("critical");
  });

  it("marks non-duplicate drops critical and duplicate, nack, or fallback writes warn", () => {
    const loss = (series: VertexHealthInput["series"]) =>
      card(
        buildVertexHealth(
          input({ type: "udf", queries: signalQueries(pipelineIds), series: { p99: [1], ...series } })
        ),
        "loss"
      );
    const mixed = loss({
      drops: [0, 2.5],
      duplicateDrops: [1],
      nacks: [],
      fallback: [],
      critical: [],
    });
    expect(mixed).toEqual(expect.objectContaining({ value: "2.5 drops/s", tone: "critical" }));
    expect(mixed.breakdown?.items).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ name: "Duplicate-message drops", value: "1/s" }),
        expect.objectContaining({ name: "Other runtime drops", value: "2.5/s" }),
      ])
    );
    expect(
      loss({ drops: [], duplicateDrops: [2], nacks: [], fallback: [], critical: [] })
    ).toEqual(expect.objectContaining({ value: "2 duplicate drops/s", tone: "warn" }));
    expect(loss({ drops: [], duplicateDrops: [], nacks: [], fallback: [], critical: [1] }).tone).toBe(
      "ok"
    );
    expect(loss({ drops: [], duplicateDrops: [], nacks: [0.2], fallback: [], critical: [] }).tone).toBe(
      "warn"
    );
    expect(loss({ drops: [], duplicateDrops: [], nacks: [], fallback: [1.4], critical: [] }).tone).toBe(
      "warn"
    );
    expect(loss({ drops: [], duplicateDrops: [], nacks: [], fallback: [], critical: [] }).sparkline).toEqual(
      Array(8).fill(8)
    );
    expect(loss({ drops: null, duplicateDrops: null, nacks: null, fallback: null }).value).toBe(
      "No data"
    );
    expect(loss({}).value).toBe("Loading…");
  });

  it("does not present partial outcome metrics as zero", () => {
    const loss = (series: VertexHealthInput["series"]) =>
      card(buildVertexHealth(input({ series: { p99: [1], ...series } })), "loss");

    expect(loss({ drops: [], duplicateDrops: [], nacks: undefined, fallback: [] })).toEqual(
      expect.objectContaining({ value: "Loading…", tone: "neutral" })
    );
    expect(loss({ drops: [], duplicateDrops: [], nacks: null, fallback: [] })).toEqual(
      expect.objectContaining({
        value: "Outcome metrics unavailable",
        detail: expect.stringContaining("1 outcome metrics unavailable"),
        tone: "neutral",
      })
    );
    expect(loss({ drops: [], duplicateDrops: [], nacks: [], fallback: null })).toEqual(
      expect.objectContaining({ value: "Outcome metrics unavailable", tone: "neutral" })
    );
    expect(loss({ drops: [2], duplicateDrops: [], nacks: null, fallback: [] })).toEqual(
      expect.objectContaining({ value: "2 drops/s", tone: "critical" })
    );
  });

  it("labels every message outcome with an explanation", () => {
    const health = buildVertexHealth(
      input({
        series: {
          drops: [],
          duplicateDrops: [1],
          nacks: [2],
          fallback: [3],
          critical: [4],
        },
      })
    );
    const outcomes = card(health, "loss").breakdown?.items;
    expect(outcomes).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ name: "Duplicate-message drops", value: "1/s", available: true }),
        expect.objectContaining({ name: "Nacks / redeliveries", value: "2/s" }),
        expect.objectContaining({ name: "Fallback deliveries", value: "3/s" }),
      ])
    );
    expect(outcomes).not.toEqual(
      expect.arrayContaining([expect.objectContaining({ name: "Critical runtime errors" })])
    );
  });

  it("marks unhealthy pods critical and restarts or recent errors warn", () => {
    const errors = (overrides: Partial<VertexHealthInput>) =>
      card(buildVertexHealth(input(overrides)), "errors");
    const crashing = fleetItem({
      severity: "critical",
      runtime: { name: "pod-1", status: "CrashLoopBackOff", containerDetailsMap: {} },
      restartCount: 39,
    });
    expect(errors({ fleetItems: [fleetItem(), crashing], recentErrorCount: 2 })).toEqual(
      expect.objectContaining({
        value: "1 unhealthy pod",
        detail: "39 restarts · 2 errors (24h)",
        tone: "critical",
        inspect: { kind: "critical-pods" },
      })
    );
    expect(errors({ fleetItems: [fleetItem({ severity: "unknown", runtime: undefined })] }).value).toBe(
      "All 1 pod healthy"
    );
    expect(errors({ recentErrorCount: 1 })).toEqual(
      expect.objectContaining({ tone: "warn", inspect: { kind: "tab", tab: "errors" } })
    );
    expect(errors({ series: { critical: [2] } })).toEqual(
      expect.objectContaining({
        value: "2 critical errors / 5m",
        tone: "critical",
        inspect: expect.objectContaining({
          kind: "metrics",
          metric: "monovtx_critical_error_total",
          presets: { duration: "5m" },
        }),
      })
    );
    expect(errors({ series: { critical: [1.2] } }).value).toBe("1 critical error / 5m");
    expect(
      errors({ series: { critical: [2] } }).breakdown?.items
    ).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ name: "Critical runtime errors", value: "2 / 5m" }),
      ])
    );
  });

  it("maps daemon verdicts and pipeline replicas", () => {
    const pipeline = buildVertexHealth(
      input({
        type: "udf",
        vertexId: "cat",
        queries: signalQueries(pipelineIds),
        health: { dataHealthStatus: "critical", dataHealthCode: "D3" },
        scale: { min: 1, max: 5 },
        fleetItems: [fleetItem(), fleetItem({ runtime: { name: "p", status: "Pending", containerDetailsMap: {} } })],
      })
    );
    expect(pipeline.verdict).toEqual({ label: "Pipeline data flow critical (D3)", tone: "critical" });
    expect(pipeline.replicas).toEqual({ label: "1 pod running · scale 1–5", tone: "warn" });
    expect(pipeline.tone).toBe("critical");
    expect(buildVertexHealth(input({ health: { dataHealthStatus: "unknown" } })).verdict?.tone).toBe(
      "neutral"
    );
    expect(
      buildVertexHealth(input({ monoStatus: { readyReplicas: 1, desiredReplicas: 3 } })).replicas
    ).toEqual({ label: "1/3 ready", tone: "warn" });
    expect(
      buildVertexHealth(
        input({ type: "udf", vertexId: "cat", queries: signalQueries(pipelineIds), scale: { min: 2, max: 2 } })
      ).replicas?.label
    ).toBe("1 pod running · scale 2");
  });

  it("falls back to live values when Prometheus is off", () => {
    const health = buildVertexHealth(input({ metricsEnabled: false, series: {} }));
    expect(health.meta).toBe("live values · Prometheus not configured");
    expect(card(health, "latency")).toEqual(
      expect.objectContaining({ value: "Metrics unavailable", tone: "neutral" })
    );
    expect(card(health, "latency").inspect).toBeUndefined();
    expect(card(health, "loss").value).toBe("Metrics unavailable");
    expect(card(health, "traffic").value).toBe("15 msg/s");
    expect(health.cards.every((entry) => entry.sparkline === undefined)).toBe(true);
  });
});
