import { fireEvent, render, screen } from "@testing-library/react";
import "@testing-library/jest-dom";
import { PodFleet } from "./PodFleet";
import { IDLE_HINT } from "./podFleetModel";
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

const charlie: Pod = { ...alpha, name: "pipeline-vertex-charlie" };

const runtime = new Map<string, PodRuntimeInfo>([
  [alpha.name, { name: alpha.name, status: "Running", containerDetailsMap: {} }],
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
  [charlie.name, { name: charlie.name, status: "Running", containerDetailsMap: {} }],
]);

const rates = new Map([
  [alpha.name, { rate5m: 12, rate15m: 12 }],
  [bravo.name, { rate5m: 1, rate15m: 1 }],
]);

describe("PodFleet", () => {
  it("outlines only the pod the parent has selected", () => {
    const { rerender } = render(
      <PodFleet
        pods={[alpha, bravo]}
        detailsByName={details}
        runtimeByName={runtime}
        selectedPod={undefined}
        onPodSelect={jest.fn()}
      />
    );

    expect(document.querySelectorAll(".pod-fleet-cell--selected")).toHaveLength(0);
    expect(screen.queryByTestId("pod-fleet-selected")).not.toBeInTheDocument();

    rerender(
      <PodFleet
        pods={[alpha, bravo]}
        detailsByName={details}
        runtimeByName={runtime}
        selectedPod={bravo}
        onPodSelect={jest.fn()}
      />
    );

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

  it("shows silent pods under Idle with a partition hint", async () => {
    render(
      <PodFleet
        pods={[alpha, bravo, charlie]}
        detailsByName={details}
        runtimeByName={runtime}
        readRates={rates}
        onPodSelect={jest.fn()}
      />
    );

    const idle = screen.getByTestId(`pod-fleet-cell-${charlie.name}`);
    expect(idle).toHaveClass("pod-fleet-cell--idle");
    expect(screen.getByTestId(`pod-fleet-cell-${bravo.name}`)).toHaveClass(
      "pod-fleet-cell--critical"
    );
    expect(screen.getByTestId(`pod-fleet-cell-${alpha.name}`)).toHaveClass(
      "pod-fleet-cell--healthy"
    );
    fireEvent.click(screen.getByTestId("pods-filter-idle"));
    expect(screen.queryByTestId(`pod-fleet-cell-${alpha.name}`)).not.toBeInTheDocument();

    fireEvent.mouseOver(idle);
    expect(await screen.findByText(IDLE_HINT)).toBeInTheDocument();
    expect(screen.getByText("Read rate (5m): 0.0 msg/s")).toBeInTheDocument();
  });
});
