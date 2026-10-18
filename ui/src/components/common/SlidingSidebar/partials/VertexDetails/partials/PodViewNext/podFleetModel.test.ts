import {
  buildPodFleetItems,
  chipLabel,
  classifyPod,
  filterAndSortPodFleet,
  IDLE_HINT,
  IDLE_WINDOW_MS,
  podStatusTone,
  unhealthyReason,
} from "./podFleetModel";
import {
  ContainerInfoProps,
  Pod,
  PodDetail,
  PodRuntimeInfo,
} from "../../../../../../../types/declarations/pods";

const NOW = Date.parse("2026-10-13T12:00:00Z");
const OLD = new Date(NOW - IDLE_WINDOW_MS - 1).toISOString();

const pod = (name: string, createdAt = OLD): Pod => ({
  name,
  containers: ["numa", "udf"],
  containerSpecMap: new Map([
    ["numa", { name: "numa", cpuParsed: 10, memoryParsed: 10 }],
    ["udf", { name: "udf", cpuParsed: 10, memoryParsed: 10 }],
  ]),
  createdAt,
});

const container = (overrides: Partial<ContainerInfoProps> = {}): ContainerInfoProps => ({
  state: "Running",
  restartCount: 0,
  lastStartedAt: "",
  ...overrides,
});

const running = (containerDetailsMap: Record<string, ContainerInfoProps> = {}): PodRuntimeInfo => ({
  name: "pod",
  status: "Running",
  containerDetailsMap,
});

const usage = (numa: number, udf: number): PodDetail => ({
  name: "pod",
  containerMap: new Map([
    ["numa", { name: "numa", cpuParsed: numa, memoryParsed: 1 }],
    ["udf", { name: "udf", cpuParsed: udf, memoryParsed: 1 }],
  ]),
});

describe("classifyPod", () => {
  const busy = { rate5m: 5, rate15m: 5 };
  const silent = { rate5m: 0, rate15m: 0 };

  it.each([
    ["crash loop beats overload and idle", running({ udf: container({ state: "Waiting", waitingReason: "CrashLoopBackOff" }) }), 200, silent, "critical"],
    ["a non-running phase is critical", { name: "p", status: "Pending", containerDetailsMap: {} }, 0, busy, "critical"],
    ["overload beats idle", running(), 81, silent, "warning"],
    ["exactly 80% is not overloaded", running(), 80, busy, "healthy"],
    ["no reads for 15 min is idle", running(), 50, silent, "idle"],
    ["reads in the window are healthy", running(), 50, busy, "healthy"],
    ["recent reads that stopped within 15 min stay healthy", running(), 50, { rate5m: 0, rate15m: 0.2 }, "healthy"],
  ] as const)("%s", (_name, runtime, cpu, readRates, expected) => {
    expect(
      classifyPod({ runtime, maxCPUPercent: cpu, readRates, createdAt: OLD, now: NOW }).severity
    ).toBe(expected);
  });

  it("never marks young pods or pods without rate data idle", () => {
    const young = new Date(NOW - 60_000).toISOString();
    expect(
      classifyPod({ runtime: running(), readRates: silent, createdAt: young, now: NOW }).severity
    ).toBe("healthy");
    expect(classifyPod({ runtime: running(), createdAt: OLD, now: NOW }).severity).toBe("healthy");
  });

  it("explains each severity and falls back to unknown without runtime", () => {
    expect(
      classifyPod({ runtime: running(), readRates: silent, createdAt: OLD, now: NOW }).reason
    ).toBe(IDLE_HINT);
    expect(classifyPod({ runtime: running(), maxMemoryPercent: 140, now: NOW }).reason).toBe(
      "memory 140% of request"
    );
    expect(classifyPod({}).severity).toBe("unknown");
    expect(
      unhealthyReason(running({ udsink: container({ state: "Terminated", lastTerminationReason: "OOMKilled" }) }))
    ).toBe("udsink: OOMKilled");
  });
});

describe("podFleetModel helpers", () => {
  it("formats chip labels and status tones", () => {
    expect(chipLabel("simple-mono-vertex-mv-0-a94xj")).toBe("0");
    expect(chipLabel("pipeline-vertex-alpha")).toBe("alpha");
    expect(podStatusTone("Running")).toBe("running");
    expect(podStatusTone("OOMKilled")).toBe("failed");
  });

  it("excludes numa from usage and treats pods missing from rate data as silent", () => {
    const hotNuma = pod("hot-numa");
    const hotUdf = pod("hot-udf");
    const quiet = pod("quiet");
    const details = new Map<string, PodDetail>([
      ["hot-numa", usage(50, 2)],
      ["hot-udf", usage(1, 15)],
      ["quiet", usage(1, 1)],
    ]);
    const runtime = new Map<string, PodRuntimeInfo>(
      ["hot-numa", "hot-udf", "quiet"].map((name) => [name, { ...running(), name }])
    );
    const rates = new Map([
      ["hot-numa", { rate5m: 3, rate15m: 3 }],
      ["hot-udf", { rate5m: 9, rate15m: 9 }],
    ]);

    const items = buildPodFleetItems([hotNuma, hotUdf, quiet], details, runtime, rates, NOW);

    expect(items.map((item) => [item.pod.name, item.severity])).toEqual([
      ["hot-numa", "healthy"],
      ["hot-udf", "warning"],
      ["quiet", "idle"],
    ]);
    expect(items[0].maxCPUPercent).toBe(20);
    expect(items[2].readRates).toEqual({ rate5m: 0, rate15m: 0 });
    expect(buildPodFleetItems([quiet], details, runtime, undefined, NOW)[0].severity).toBe("healthy");
  });

  it("filters by severity and sorts red, yellow, grey, green", () => {
    const items = buildPodFleetItems(
      [pod("a"), pod("b"), pod("c"), pod("d")],
      new Map([["b", usage(1, 20)]]),
      new Map<string, PodRuntimeInfo>([
        ["a", { ...running(), name: "a" }],
        ["b", { ...running(), name: "b" }],
        ["c", { ...running(), name: "c" }],
        ["d", { name: "d", status: "Failed", containerDetailsMap: {} }],
      ]),
      new Map([
        ["a", { rate5m: 1, rate15m: 1 }],
        ["b", { rate5m: 1, rate15m: 1 }],
      ]),
      NOW
    );

    expect(filterAndSortPodFleet(items, "all", "", "severity").map((item) => item.pod.name)).toEqual([
      "d",
      "b",
      "c",
      "a",
    ]);
    expect(filterAndSortPodFleet(items, "idle", "", "name").map((item) => item.pod.name)).toEqual(["c"]);
    expect(items.map((item) => item.pod.name)).toEqual(["a", "b", "c", "d"]);
  });
});
