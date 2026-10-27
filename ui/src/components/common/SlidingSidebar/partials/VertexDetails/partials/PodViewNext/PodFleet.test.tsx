import { useState } from "react";
import { fireEvent, render, screen } from "@testing-library/react";
import "@testing-library/jest-dom";
import { PodFleet } from "./PodFleet";
import { buildPodFleetItems, PodFleetFilter } from "./podFleetModel";
import { Pod, PodRuntimeInfo } from "../../../../../../../types/declarations/pods";

const alpha: Pod = {
  name: "pipeline-vertex-alpha",
  containers: ["main", "numa"],
  containerSpecMap: new Map([
    ["main", { name: "main", cpuParsed: 10, memoryParsed: 10 }],
    ["numa", { name: "numa", cpuParsed: 1, memoryParsed: 10 }],
  ]),
};
const bravo: Pod = {
  name: "pipeline-vertex-bravo",
  containers: ["main"],
  containerSpecMap: new Map([
    ["main", { name: "main", cpuParsed: 10, memoryParsed: 10 }],
  ]),
};

const runtime = new Map<string, PodRuntimeInfo>([
  [
    alpha.name,
    {
      name: alpha.name,
      status: "Running",
      containerDetailsMap: {
        main: {
          state: "Running",
          restartCount: 0,
          lastStartedAt: "",
          totalCPU: "1m",
          requestedCPU: "10m",
          totalMemory: "1Mi",
          requestedMemory: "10Mi",
        },
        numa: {
          state: "Running",
          restartCount: 0,
          lastStartedAt: "",
          totalCPU: "2m",
          requestedCPU: "1m",
          totalMemory: "1Mi",
          requestedMemory: "10Mi",
        },
      },
    },
  ],
  [
    bravo.name,
    {
      name: bravo.name,
      status: "Running",
      containerDetailsMap: {
        main: { state: "Waiting", waitingReason: "CrashLoopBackOff", restartCount: 3, lastStartedAt: "" },
      },
    },
  ],
]);

const items = buildPodFleetItems([alpha, bravo], runtime);

function StatefulFleet({
  selectedPod,
  onPodSelect = jest.fn(),
  initialFilter = "all",
}: {
  selectedPod?: Pod;
  onPodSelect?: (pod: Pod) => void;
  initialFilter?: PodFleetFilter;
}) {
  const [filter, setFilter] = useState<PodFleetFilter>(initialFilter);
  return (
    <PodFleet
      items={items}
      filter={filter}
      onFilterChange={setFilter}
      runtimeByName={runtime}
      selectedPod={selectedPod}
      onPodSelect={onPodSelect}
    />
  );
}

describe("PodFleet", () => {
  it("outlines only the pod the parent has selected", () => {
    const { rerender } = render(<StatefulFleet selectedPod={undefined} />);

    expect(document.querySelectorAll(".pod-fleet-cell--selected")).toHaveLength(0);
    expect(screen.queryByTestId("pod-fleet-selected")).not.toBeInTheDocument();

    rerender(<StatefulFleet selectedPod={bravo} />);

    expect(screen.getByTestId(`pod-fleet-cell-${bravo.name}`)).toHaveClass(
      "pod-fleet-cell--selected"
    );
    expect(screen.getByTestId(`pod-fleet-cell-${alpha.name}`)).not.toHaveClass(
      "pod-fleet-cell--selected"
    );
    expect(screen.getByTestId("pod-fleet-selected")).toHaveTextContent(bravo.name);
  });

  it("filters, searches, and selects pods", () => {
    const onPodSelect = jest.fn();
    render(<StatefulFleet selectedPod={alpha} onPodSelect={onPodSelect} />);

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

  it("reports filter changes to the parent and honours a parent-set filter", () => {
    const onFilterChange = jest.fn();
    render(
      <PodFleet
        items={items}
        filter="critical"
        onFilterChange={onFilterChange}
        runtimeByName={runtime}
        onPodSelect={jest.fn()}
      />
    );

    expect(screen.queryByTestId(`pod-fleet-cell-${alpha.name}`)).not.toBeInTheDocument();
    fireEvent.click(screen.getByTestId("pods-filter-all"));
    expect(onFilterChange).toHaveBeenCalledWith("all");
  });

  it("shows every container and highlights elevated resource usage", async () => {
    render(<StatefulFleet />);

    fireEvent.mouseOver(screen.getByTestId(`pod-fleet-cell-${alpha.name}`));
    const tooltip = await screen.findByTestId(`pod-fleet-tooltip-${alpha.name}`);
    expect(tooltip).toHaveTextContent("Usage as % of request");
    expect(tooltip).toHaveTextContent("main");
    expect(tooltip).toHaveTextContent("numa");
    expect(tooltip).toHaveTextContent("numa CPU 200% of request");
    expect(screen.getByText("200%").closest("td")).toHaveClass("is-hot");
    expect(screen.getAllByText("10%").length).toBeGreaterThan(0);
  });
});
