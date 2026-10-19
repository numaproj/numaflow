import {
  buildPodFleetItems,
  chipLabel,
  classifyPod,
  filterAndSortPodFleet,
  podStatusTone,
  unhealthyReason,
} from "./podFleetModel";
import {
  ContainerInfoProps,
  Pod,
  PodDetail,
  PodRuntimeInfo,
} from "../../../../../../../types/declarations/pods";

const pod = (name: string): Pod => ({
  name,
  containers: ["numa", "udf"],
  containerSpecMap: new Map([
    ["numa", { name: "numa", cpuParsed: 10, memoryParsed: 10 }],
    ["udf", { name: "udf", cpuParsed: 10, memoryParsed: 10 }],
  ]),
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
  it.each([
    ["crash loop beats overload", running({ udf: container({ state: "Waiting", waitingReason: "CrashLoopBackOff" }) }), 200, "critical"],
    ["a non-running phase is critical", { name: "p", status: "Pending", containerDetailsMap: {} }, 0, "critical"],
    ["overload is warning", running(), 81, "warning"],
    ["exactly 80% is not overloaded", running(), 80, "healthy"],
    ["a running pod under the limit is healthy", running(), 50, "healthy"],
  ] as const)("%s", (_name, runtime, cpu, expected) => {
    expect(classifyPod({ runtime, maxCPUPercent: cpu }).severity).toBe(expected);
  });

  it("explains warning and falls back to unknown without runtime", () => {
    expect(classifyPod({ runtime: running(), maxMemoryPercent: 140 }).reason).toBe(
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

  it("excludes numa from usage", () => {
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

    const items = buildPodFleetItems([hotNuma, hotUdf, quiet], details, runtime);

    expect(items.map((item) => [item.pod.name, item.severity])).toEqual([
      ["hot-numa", "healthy"],
      ["hot-udf", "warning"],
      ["quiet", "healthy"],
    ]);
    expect(items[0].maxCPUPercent).toBe(20);
  });

  it("filters by severity and sorts red, yellow, green", () => {
    const items = buildPodFleetItems(
      [pod("a"), pod("b"), pod("d")],
      new Map([["b", usage(1, 20)]]),
      new Map<string, PodRuntimeInfo>([
        ["a", { ...running(), name: "a" }],
        ["b", { ...running(), name: "b" }],
        ["d", { name: "d", status: "Failed", containerDetailsMap: {} }],
      ])
    );

    expect(filterAndSortPodFleet(items, "all", "", "severity").map((item) => item.pod.name)).toEqual([
      "d",
      "b",
      "a",
    ]);
    expect(items.map((item) => item.pod.name)).toEqual(["a", "b", "d"]);
  });
});
