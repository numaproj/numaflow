import { SearchablePodsHeatMap } from "./index";
import { render, screen } from "@testing-library/react";
import { fill } from "../../../../../../../../../../../utils/gradients";
import {
  Pod,
  PodContainerSpec,
  PodRuntimeInfo,
} from "../../../../../../../../../../../types/declarations/pods";

import "@testing-library/jest-dom";

const podContainerSpec: PodContainerSpec = {
  name: "numa",
};
const containerSpecMap = new Map<string, PodContainerSpec>([
  ["simple-pipeline-infer-0-xah5w", podContainerSpec],
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
          totalCPU: "10m",
          requestedCPU: "100m",
        },
      },
    },
  ],
]);

const onPodClick = jest.fn();

jest.mock("../../../../../../../../../../../utils/gradients");
const mockedFill = fill as jest.MockedFunction<typeof fill>;

describe("SearchablePodsHeatMap", () => {
  it("loads screen with valid search", async () => {
    mockedFill.mockReturnValue("test");
    render(
      <SearchablePodsHeatMap
        pods={pods}
        selectedPod={pod}
        podRuntimeByName={podRuntimeByName}
        onPodClick={onPodClick}
      />
    );
    expect(screen.getByText("Select a pod by resource")).toBeVisible();
    expect(screen.getByText("CPU")).toBeVisible();
    expect(screen.getByText("MEM")).toBeVisible();
    expect(
      screen.getByTestId("hexagon_simple-pipeline-infer-0-xah5w-cpu")
    ).toBeVisible();
  });
});
