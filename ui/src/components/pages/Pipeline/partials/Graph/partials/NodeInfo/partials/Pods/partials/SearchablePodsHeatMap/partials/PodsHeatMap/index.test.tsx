import React from "react";
import "@testing-library/jest-dom";
import { fireEvent, render, screen } from "@testing-library/react";
import { PodsHeatMap } from "./index";
import { fill } from "../../../../../../../../../../../../../utils/gradients";
import {
  Pod,
  PodContainerSpec,
  PodRuntimeInfo,
} from "../../../../../../../../../../../../../types/declarations/pods";

const containerSpecMap = new Map<string, PodContainerSpec>([
  ["numa", { name: "numa" }],
  ["udf", { name: "udf" }],
]);

const pod = {
  name: "simple-pipeline-infer-0-xah5w",
  containers: ["numa", "udf"],
  containerSpecMap: containerSpecMap,
};

const pods: Pod[] = [pod];

const podRuntimeByName = new Map<string, PodRuntimeInfo>([
  [
    pod.name,
    {
      name: pod.name,
      status: "Running",
      containerDetailsMap: {
        numa: {
          state: "Running",
          restartCount: 0,
          lastStartedAt: "",
          totalCPU: "50m",
          requestedCPU: "100m",
          totalMemory: "10Mi",
          requestedMemory: "100Mi",
        },
      },
    },
  ],
]);

const onPodClick = jest.fn();

jest.mock("../../../../../../../../../../../../../utils/gradients");
const mockedFill = fill as jest.MockedFunction<typeof fill>;

describe("PodsHeatMap", () => {
  it("loads screen", async () => {
    mockedFill.mockReturnValue("test");
    const { container } = render(
      <PodsHeatMap
        pods={pods}
        selectedPod={pod}
        podRuntimeByName={podRuntimeByName}
        onPodClick={onPodClick}
      />
    );
    expect(screen.getByText("CPU")).toBeVisible();
    expect(container.getElementsByClassName("visx-polygon")[0]).toBeVisible();
    fireEvent.mouseMove(container.getElementsByClassName("visx-polygon")[0]);
    expect(screen.getByText("50.00%")).toBeVisible();
    expect(screen.getByText("10.00%")).toBeVisible();
    fireEvent.mouseLeave(container.getElementsByClassName("visx-polygon")[0]);
  });
  it("shows n/a instead of a negative percent when usage is missing", () => {
    mockedFill.mockReturnValue("test");
    const missingUsage = new Map<string, PodRuntimeInfo>([
      [
        pod.name,
        {
          name: pod.name,
          status: "Running",
          containerDetailsMap: {
            numa: {
              state: "Running",
              restartCount: 0,
              lastStartedAt: "",
              requestedCPU: "100m",
              requestedMemory: "128Mi",
            },
          },
        },
      ],
    ]);
    const { container } = render(
      <PodsHeatMap
        pods={pods}
        selectedPod={pod}
        podRuntimeByName={missingUsage}
        onPodClick={onPodClick}
      />
    );
    fireEvent.mouseMove(container.getElementsByClassName("visx-polygon")[0]);
    expect(screen.getAllByText("n/a").length).toBeGreaterThan(0);
    expect(screen.queryByText("-1%")).not.toBeInTheDocument();
  });
  it("no heatmap returned", async () => {
    mockedFill.mockReturnValue("test");
    render(
      <PodsHeatMap
        pods={undefined}
        selectedPod={pod}
        podRuntimeByName={undefined}
        onPodClick={onPodClick}
      />
    );
    expect(screen.getByText("MEM")).toBeVisible();
  });
  it("returns heatmap without hexagon", async () => {
    mockedFill.mockReturnValue("test");
    render(
      <PodsHeatMap
        pods={pods}
        selectedPod={pod}
        podRuntimeByName={new Map()}
        onPodClick={onPodClick}
      />
    );
    expect(screen.getByText("CPU")).toBeVisible();
  });
});
