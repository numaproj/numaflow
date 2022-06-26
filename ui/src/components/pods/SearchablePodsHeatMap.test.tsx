import { SearchablePodsHeatMap } from "./SearchablePodsHeatMap";
import { render, screen } from "@testing-library/react";
import { Pod, PodContainerSpec, PodDetail } from "../../utils/models/pods";
import { fill } from "../../utils/gradients";

const podContainerSpec: PodContainerSpec = {
  name: "main",
};
const containerSpecMap = new Map<string, PodContainerSpec>([
  ["simple-pipeline-infer-0-xah5w", podContainerSpec],
]);

const pod = {
  name: "simple-pipeline-infer-0-xah5w",
  containers: ["main", "udf"],
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

jest.mock("../../utils/gradients");
const mockedFill = fill as jest.MockedFunction<typeof fill>;

describe("PodsHeatMap", () => {
  it("loads screen", async () => {
    mockedFill.mockReturnValue("test");
    render(
      <SearchablePodsHeatMap
        pods={pods}
        selectedPod={pod}
        podsDetailMap={podDetailMap}
        onPodClick={onPodClick}
        setSelectedPod={pod}
      />
    );
    expect(screen.getByTestId("searchable-pods")).toBeVisible();
  });
});
