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
    expect(chipLabel("simple-mono-vertex-mv-0-a94xj", "simple-mono-vertex", true)).toBe("mv-0");
    expect(chipLabel("simple-mono-vertex-mv-1-bbbbb", "simple-mono-vertex", true)).toBe("mv-1");
    expect(chipLabel("orders-payment-authorization-12-k2m9x", "payment-authorization", false)).toBe(
      "pay-12"
    );
    expect(chipLabel("simple-pipeline-out-0-w04df", "out", false)).toBe("out-0");
    expect(chipLabel("pipeline-vertex-alpha", "vertex", false)).toBe("alpha");
    expect(podStatusTone("Running")).toBe("running");
    expect(podStatusTone("OOMKilled")).toBe("failed");
  });

  it("attributes usage to every container, including numa", () => {
    const hotNuma = pod("hot-numa");
    const hotUdf = pod("hot-udf");
    const quiet = pod("quiet");
    const withUsage = (
      name: string,
      numaCPU: string,
      udfCPU: string
    ): PodRuntimeInfo => ({
      ...running({
        numa: container({
          totalCPU: numaCPU,
          requestedCPU: "10m",
          totalMemory: "1Mi",
          requestedMemory: "10Mi",
        }),
        udf: container({
          totalCPU: udfCPU,
          requestedCPU: "10m",
          totalMemory: "1Mi",
          requestedMemory: "10Mi",
        }),
      }),
      name,
    });
    const runtime = new Map<string, PodRuntimeInfo>([
      ["hot-numa", withUsage("hot-numa", "50m", "2m")],
      ["hot-udf", withUsage("hot-udf", "1m", "15m")],
      ["quiet", withUsage("quiet", "1m", "1m")],
    ]);

    const items = buildPodFleetItems([hotNuma, hotUdf, quiet], runtime);

    expect(items.map((item) => [item.pod.name, item.severity])).toEqual([
      ["hot-numa", "warning"],
      ["hot-udf", "warning"],
      ["quiet", "healthy"],
    ]);
    expect(items[0].maxCPUPercent).toBe(500);
    expect(items[0].reason).toBe("numa CPU 500% of request");
    expect(items[0].containerResources).toEqual([
      { name: "numa", cpu: "50m", memory: "1Mi", cpuPercent: 500, memoryPercent: 10 },
      { name: "udf", cpu: "2m", memory: "1Mi", cpuPercent: 20, memoryPercent: 10 },
    ]);
  });

  it("reads pods-info quantities and leaves missing requests unknown", () => {
    const runtime = new Map<string, PodRuntimeInfo>([
      [
        "runtime-only",
        {
          ...running({
            numa: container({
              totalCPU: "100m",
              requestedCPU: "10m",
              totalMemory: "100Mi",
              requestedMemory: "50Mi",
            }),
            udf: container({ totalCPU: "1", requestedCPU: "" }),
          }),
          name: "runtime-only",
        },
      ],
    ]);

    const [item] = buildPodFleetItems([pod("runtime-only")], runtime);
    expect(item.maxCPUPercent).toBeCloseTo(1000);
    expect(item.maxMemoryPercent).toBeCloseTo(200);
    expect(item.containerResources).toEqual([
      expect.objectContaining({ name: "numa", cpu: "100m", memory: "100Mi", cpuPercent: 1000, memoryPercent: 200 }),
      expect.objectContaining({ name: "udf", cpu: "1", memory: undefined, cpuPercent: undefined }),
    ]);
  });

  it("filters by severity and sorts red, yellow, green", () => {
    const items = buildPodFleetItems(
      [pod("a"), pod("b"), pod("d")],
      new Map<string, PodRuntimeInfo>([
        ["a", { ...running(), name: "a" }],
        [
          "b",
          {
            ...running({
              udf: container({ totalCPU: "20m", requestedCPU: "10m" }),
            }),
            name: "b",
          },
        ],
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
