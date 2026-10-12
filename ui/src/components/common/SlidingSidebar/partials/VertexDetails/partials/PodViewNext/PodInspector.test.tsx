import { render, screen, within } from "@testing-library/react";
import "@testing-library/jest-dom";
import { AppContext } from "../../../../../../../App";
import { PodInspector } from "./PodInspector";
import { Pod, PodDetail, PodRuntimeInfo } from "../../../../../../../types/declarations/pods";

jest.mock("../../../../../MetricsModalWrapper", () => ({
  MetricsModalWrapper: ({ value }: { value: string }) => <span>{value}</span>,
}));

const pod: Pod = {
  name: "demo-input-0",
  containers: ["main", "sidecar"],
  containerSpecMap: new Map([
    ["main", { name: "main", cpu: "10m", memory: "64Mi", cpuParsed: 10, memoryParsed: 64 }],
    ["sidecar", { name: "sidecar", cpu: "100m", memory: "128Mi", cpuParsed: 100, memoryParsed: 128 }],
  ]),
};

const podDetails: PodDetail = {
  name: "demo-input-0",
  containerMap: new Map([
    ["main", { name: "main", cpu: "5m", memory: "32Mi", cpuParsed: 5, memoryParsed: 32 }],
  ]),
};

const runtime: PodRuntimeInfo = {
  name: "demo-input-0",
  status: "Running",
  totalCPU: "5m",
  totalMemory: "32Mi",
  containerDetailsMap: {
    main: {
      state: "Terminated",
      restartCount: 2,
      lastStartedAt: "",
      lastTerminationReason: "OOMKilled",
      lastTerminationMessage: "Memory limit exceeded",
      lastTerminationExitCode: 137,
    },
    sidecar: {
      state: "Waiting",
      restartCount: 3,
      lastStartedAt: "",
      waitingReason: "CrashLoopBackOff",
    },
  },
};

function renderInspector(containerName: string, details: PodDetail | undefined = podDetails) {
  render(
    <AppContext.Provider value={{ disableMetricsCharts: true } as any}>
      <PodInspector
        namespaceId="default"
        pipelineId="demo"
        vertexId="input"
        type="source"
        pod={pod}
        podDetails={details}
        containerName={containerName}
        runtime={runtime}
      />
    </AppContext.Provider>
  );
  return {
    podCard: within(screen.getByTestId("pod-overview")),
    containerCard: within(screen.getByTestId("container-overview")),
  };
}

const rowValue = (card: ReturnType<typeof within>, label: string) =>
  card.getByText(label).nextElementSibling;

describe("PodInspector", () => {
  it("separates pod-wide and container-scoped details into two cards", () => {
    const { podCard, containerCard } = renderInspector("main");

    expect(screen.queryByText("Resources")).not.toBeInTheDocument();
    expect(screen.queryByText("Quick Metrics")).not.toBeInTheDocument();
    expect(rowValue(podCard, "Name")).toHaveTextContent("demo-input-0");
    expect(rowValue(podCard, "Restarts")).toHaveTextContent("5");
    expect(rowValue(podCard, "CPU")).toHaveTextContent("5m");
    expect(rowValue(containerCard, "Name")).toHaveTextContent("main");
    expect(rowValue(containerCard, "Restarts")).toHaveTextContent("2");
    expect(rowValue(containerCard, "CPU")).toHaveTextContent("5m / 10m (50.00%)");
    expect(rowValue(containerCard, "Memory")).toHaveTextContent("32Mi / 64Mi (50.00%)");
    expect(rowValue(containerCard, "Last Termination")).toHaveTextContent("OOMKilled");
    expect(rowValue(containerCard, "Exit Code")).toHaveTextContent("137");
    expect(containerCard.queryByText("Waiting Reason")).not.toBeInTheDocument();
  });

  it("omits the percent when usage is unavailable and shows waiting details", () => {
    const { containerCard } = renderInspector("sidecar", undefined);

    expect(rowValue(containerCard, "CPU")).toHaveTextContent("— / 100m");
    expect(rowValue(containerCard, "CPU")).not.toHaveTextContent("%");
    expect(rowValue(containerCard, "Restarts")).toHaveTextContent("3");
    expect(rowValue(containerCard, "Waiting Reason")).toHaveTextContent("CrashLoopBackOff");
    expect(containerCard.queryByText("Last Termination")).not.toBeInTheDocument();
    expect(containerCard.queryByText("Exit Code")).not.toBeInTheDocument();
  });
});
