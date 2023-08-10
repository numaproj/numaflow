import { fireEvent, render, screen } from "@testing-library/react";
import { PodsHeatMap } from "./index";
import { fill } from "../../../../../../../../../../../../../utils/gradients";
import {
  Pod,
  PodContainerSpec,
  PodDetail,
} from "../../../../../../../../../../../../../types/declarations/pods";

const podContainerSpec: PodContainerSpec = {
  name: "numa",
  cpuParsed: 100,
  memoryParsed: 100,
};
const containerSpecMap = new Map<string, PodContainerSpec>([
  ["numa", podContainerSpec],
  ["udf", podContainerSpec],
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

const podDetailsMap = new Map<string, PodDetail>([
  ["simple-pipeline-infer-0-xah5w", podDetail],
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
        podsDetailsMap={podDetailsMap}
        onPodClick={onPodClick}
      />
    );
    expect(screen.getByTestId("podHeatMap")).toBeVisible();
    expect(container.getElementsByClassName("visx-polygon")[0]).toBeVisible();
    fireEvent.mouseMove(container.getElementsByClassName("visx-polygon")[0]);
    fireEvent.mouseLeave(container.getElementsByClassName("visx-polygon")[0]);
  });
  it("loads screen", async () => {
    mockedFill.mockReturnValue("test");
    render(
      <PodsHeatMap
        pods={undefined}
        selectedPod={pod}
        podsDetailsMap={undefined}
        onPodClick={onPodClick}
      />
    );
    expect(screen.getByTestId("podHeatMap")).toBeVisible();
  });
});
