import { fireEvent, render, screen } from "@testing-library/react";
import "@testing-library/jest-dom";
import { PodFleet } from "./PodFleet";
import { Pod, PodDetail, PodRuntimeInfo } from "../../../../../../../types/declarations/pods";

const alpha: Pod = {
  name: "pipeline-vertex-alpha",
  containers: ["main"],
  containerSpecMap: new Map([
    ["main", { name: "main", cpuParsed: 10, memoryParsed: 10 }],
  ]),
};
const bravo: Pod = {
  name: "pipeline-vertex-bravo",
  containers: ["main"],
  containerSpecMap: new Map([
    ["main", { name: "main", cpuParsed: 10, memoryParsed: 10 }],
  ]),
};

const details = new Map<string, PodDetail>([
  [
    alpha.name,
    {
      name: alpha.name,
      containerMap: new Map([
        ["main", { name: "main", cpuParsed: 1, memoryParsed: 1 }],
      ]),
    },
  ],
  [
    bravo.name,
    {
      name: bravo.name,
      containerMap: new Map([
        ["main", { name: "main", cpuParsed: 9, memoryParsed: 1 }],
      ]),
    },
  ],
]);

const runtime = new Map<string, PodRuntimeInfo>([
  [alpha.name, { name: alpha.name, status: "Running", containerDetailsMap: {} }],
  [bravo.name, { name: bravo.name, status: "Running", containerDetailsMap: {} }],
]);

describe("PodFleet", () => {
  it("filters, searches, and selects pods", () => {
    const onPodSelect = jest.fn();
    render(
      <PodFleet
        pods={[alpha, bravo]}
        detailsByName={details}
        runtimeByName={runtime}
        selectedPod={alpha}
        onPodSelect={onPodSelect}
      />
    );

    expect(screen.getByText("All 2")).toBeVisible();
    expect(screen.getByTestId(`pod-fleet-cell-${alpha.name}`)).toHaveTextContent("alpha");
    expect(screen.getByTestId("pod-fleet-selected-status")).toHaveClass(
      "pod-fleet-status--running"
    );
    fireEvent.click(screen.getByTestId("pods-filter-critical"));
    expect(screen.queryByTestId(`pod-fleet-cell-${alpha.name}`)).not.toBeInTheDocument();
    const critical = screen.getByTestId(`pod-fleet-cell-${bravo.name}`);
    fireEvent.click(critical);
    expect(onPodSelect).toHaveBeenCalledWith(bravo);

    fireEvent.click(screen.getByTestId("pods-filter-all"));
    fireEvent.change(screen.getByLabelText("Search pods"), {
      target: { value: "alpha" },
    });
    expect(screen.getByTestId(`pod-fleet-cell-${alpha.name}`)).toBeInTheDocument();
    expect(screen.queryByTestId(`pod-fleet-cell-${bravo.name}`)).not.toBeInTheDocument();
  });
});
