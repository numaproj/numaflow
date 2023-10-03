import { SearchablePodsHeatMap } from "./index";
import { fireEvent, render, screen } from "@testing-library/react";
import { fill } from "../../../../../../../../../../../utils/gradients";
import {
  Pod,
  PodContainerSpec,
  PodDetail,
} from "../../../../../../../../../../../types/declarations/pods";
import { Dispatch, SetStateAction } from "react";

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

describe("PodsHeatMap", () => {
  it("loads screen with valid search", async () => {
    mockedFill.mockReturnValue("test");
    const { container } = render(
      <SearchablePodsHeatMap
        pods={pods}
        selectedPod={pod}
        podsDetailsMap={podDetailMap}
        onPodClick={onPodClick}
        setSelectedPod={jest.fn() as Dispatch<SetStateAction<Pod | undefined>>}
      />
    );
    expect(screen.getByTestId("searchable-pods")).toBeVisible();
    expect(screen.getByTestId("searchable-pods-input")).toBeVisible();
    expect(screen.getByTestId("ClearIcon")).toBeVisible();
    fireEvent.click(screen.getByTestId("ClearIcon"));
    fireEvent.change(
      container.getElementsByClassName(
        "MuiInputBase-input css-yz9k0d-MuiInputBase-input"
      )[0],
      { target: { value: "simple" } }
    );
  });
  it("loads screen with invalid search", async () => {
    mockedFill.mockReturnValue("test");
    const { container } = render(
      <SearchablePodsHeatMap
        pods={pods}
        selectedPod={pod}
        podsDetailsMap={podDetailMap}
        onPodClick={onPodClick}
        setSelectedPod={jest.fn() as Dispatch<SetStateAction<Pod | undefined>>}
      />
    );
    expect(screen.getByTestId("searchable-pods")).toBeVisible();
    expect(screen.getByTestId("searchable-pods-input")).toBeVisible();
    expect(screen.getByTestId("ClearIcon")).toBeVisible();
    fireEvent.click(screen.getByTestId("ClearIcon"));
    fireEvent.change(
      container.getElementsByClassName(
        "MuiInputBase-input css-yz9k0d-MuiInputBase-input"
      )[0],
      { target: { value: "xyz" } }
    );
  });
});
