import {
  buildPodFleetItems,
  chipLabel,
  classifyPod,
  filterAndSortPodFleet,
  podStatusTone,
} from "./podFleetModel";
import { Pod, PodDetail, PodRuntimeInfo } from "../../../../../../../types/declarations/pods";

const pod = (name: string): Pod => ({
  name,
  containers: ["main"],
  containerSpecMap: new Map([
    ["main", { name: "main", cpuParsed: 10, memoryParsed: 10 }],
  ]),
});

describe("podFleetModel", () => {
  it.each([
    ["CrashLoopBackOff", 1, 1, "unknown"],
    ["Running", 76, 10, "critical"],
    ["Running", 10, 86, "critical"],
    ["Pending", 1, 1, "warning"],
    ["Running", 31, 10, "warning"],
    ["Running", 10, 51, "warning"],
    ["Running", 10, 10, "healthy"],
  ] as const)(
    "classifies %s with CPU %s and memory %s as %s",
    (status, cpu, memory, expected) => {
      expect(
        classifyPod(cpu, memory, {
          name: "pod",
          status,
          containerDetailsMap: {},
        })
      ).toBe(expected);
    }
  );

  it("classifies absent runtime and resource evidence as unknown", () => {
    expect(classifyPod(undefined, undefined)).toBe("unknown");
  });

  it("keeps a running pod green when usage is missing and ignores restarts", () => {
    const running = {
      name: "pod",
      status: "Running",
      containerDetailsMap: {},
    };
    expect(classifyPod(undefined, undefined, running)).toBe("healthy");
    expect(classifyPod(10, 10, running, 4)).toBe("healthy");
    expect(chipLabel("simple-mono-vertex-mv-0-a94xj")).toBe("0");
    expect(chipLabel("pipeline-vertex-alpha")).toBe("alpha");
    expect(podStatusTone("Running")).toBe("running");
    expect(podStatusTone("OOMKilled")).toBe("failed");
  });

  it("derives fleet items and filters/sorts a copy of their input", () => {
    const healthy = pod("alpha");
    const critical = pod("bravo");
    const details = new Map<string, PodDetail>([
      [
        "alpha",
        {
          name: "alpha",
          containerMap: new Map([
            ["main", { name: "main", cpuParsed: 1, memoryParsed: 1 }],
          ]),
        },
      ],
      [
        "bravo",
        {
          name: "bravo",
          containerMap: new Map([
            ["main", { name: "main", cpuParsed: 8, memoryParsed: 1 }],
          ]),
        },
      ],
    ]);
    const runtime = new Map<string, PodRuntimeInfo>([
      ["alpha", { name: "alpha", status: "Running", containerDetailsMap: {} }],
      ["bravo", { name: "bravo", status: "Running", containerDetailsMap: {} }],
    ]);

    const items = buildPodFleetItems([healthy, critical], details, runtime);
    const criticalOnly = filterAndSortPodFleet(items, "critical", "", "severity");
    const nameSorted = filterAndSortPodFleet(items, "all", "", "name");

    expect(criticalOnly.map((item) => item.pod.name)).toEqual(["bravo"]);
    expect(nameSorted.map((item) => item.pod.name)).toEqual(["alpha", "bravo"]);
    expect(items.map((item) => item.pod.name)).toEqual(["alpha", "bravo"]);
  });
});
