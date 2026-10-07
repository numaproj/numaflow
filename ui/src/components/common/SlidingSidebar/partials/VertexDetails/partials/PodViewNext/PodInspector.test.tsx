import { render, screen } from "@testing-library/react";
import "@testing-library/jest-dom";
import { AppContext } from "../../../../../../../App";
import { PodInspector } from "./PodInspector";

jest.mock("../../../../../MetricsModalWrapper", () => ({
  MetricsModalWrapper: ({ value }: { value: string }) => <span>{value}</span>,
}));

describe("PodInspector", () => {
  it("shows selected pod resources and conditional termination details", () => {
    render(
      <AppContext.Provider value={{ disableMetricsCharts: true } as any}>
        <PodInspector
          namespaceId="default"
          pipelineId="demo"
          vertexId="input"
          type="source"
          pod={{
            name: "demo-input-0",
            containers: ["main"],
            containerSpecMap: new Map([
              ["main", { name: "main", cpu: "10m", memory: "64Mi", cpuParsed: 10, memoryParsed: 64 }],
            ]),
          }}
          podDetails={{
            name: "demo-input-0",
            containerMap: new Map([
              ["main", { name: "main", cpu: "5m", memory: "32Mi", cpuParsed: 5, memoryParsed: 32 }],
            ]),
          }}
          containerName="main"
          runtime={{
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
            },
          }}
        />
      </AppContext.Provider>
    );

    expect(screen.getByText("Pod Overview")).toBeVisible();
    expect(screen.getByText("Last Termination")).toBeVisible();
    expect(screen.getByText("OOMKilled")).toBeVisible();
    expect(screen.getByText("Exit code")).toBeVisible();
    expect(screen.getByText("Quick Metrics")).toBeVisible();
  });
});
