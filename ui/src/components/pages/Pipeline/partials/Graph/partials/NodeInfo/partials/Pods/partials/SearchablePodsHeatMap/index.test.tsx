import { SearchablePodsHeatMap } from "./index";
import { render, screen } from "@testing-library/react";
import { fill } from "../../../../../../../../../../../utils/gradients";
import {
  Pod,
  PodContainerSpec,
  PodDetail,
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
const podDetail = {
  name: "simple-pipeline-infer-0-xah5w",
  containerMap: containerSpecMap,
};

const pods: Pod[] = [pod];

const podDetailMap = new Map<string, PodDetail>([
  ["simple-pipeline-infer-0-xah5w", podDetail],
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
        podsDetailsMap={podDetailMap}
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
