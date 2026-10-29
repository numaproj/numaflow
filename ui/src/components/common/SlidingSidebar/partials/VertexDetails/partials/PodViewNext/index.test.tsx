import { fireEvent, render, screen } from "@testing-library/react";
import "@testing-library/jest-dom";
import { failingContainer, PodViewNext } from "./index";
import { usePodViewState } from "../../../../../../pages/Pipeline/partials/Graph/partials/NodeInfo/partials/Pods/usePodViewState";
import { VertexDetailsContext } from "../../index";
import { signalQueries } from "./goldenSignalsModel";
import { VERTEX_DISCOVERY } from "./signalDiscovery.fixture";
import { PodFleetItem } from "../../../../../../../types/declarations/pods";

const mockFleetProps = jest.fn();
const mockUseVertexHealthData = jest.fn(() => ({
  queries: jest
    .requireActual("./goldenSignalsModel")
    .signalQueries(
      {
        namespaceId: "default",
        pipelineId: "pipeline",
        vertexId: "vertex",
        type: "udf",
      },
      jest.requireActual("./signalDiscovery.fixture").VERTEX_DISCOVERY
    ),
  series: { p99: [1000, 2000] } as any,
  metricsEnabled: true,
  now: Date.now(),
}));
jest.mock("./useVertexHealthData", () => ({
  useVertexHealthData: () => mockUseVertexHealthData(),
}));
jest.mock(
  "../../../../../../pages/Pipeline/partials/Graph/partials/NodeInfo/partials/Pods/usePodViewState",
  () => ({
    ...jest.requireActual(
      "../../../../../../pages/Pipeline/partials/Graph/partials/NodeInfo/partials/Pods/usePodViewState"
    ),
    usePodViewState: jest.fn(),
  })
);
jest.mock("./PodFleet", () => ({
  PodFleet: (props: any) => {
    mockFleetProps(props);
    return <div data-testid="pod-fleet" />;
  },
}));
jest.mock("./PodInspector", () => ({
  PodInspector: () => <aside data-testid="pod-inspector" />,
}));
jest.mock(
  "../../../../../../pages/Pipeline/partials/Graph/partials/NodeInfo/partials/Pods/partials/Containers",
  () => ({ Containers: () => <div data-testid="pod-containers" /> })
);
jest.mock(
  "../../../../../../pages/Pipeline/partials/Graph/partials/NodeInfo/partials/Pods/partials/PodDetails",
  () => ({ PodDetail: () => <div data-testid="pod-detail" /> })
);
jest.mock(
  "../../../../../../pages/Pipeline/partials/Graph/partials/NodeInfo/partials/Pods/partials/PodFocusControls",
  () => ({ PodFocusControls: () => <div data-testid="pod-focus-controls" /> })
);

const mockedUsePodViewState = usePodViewState as jest.Mock;
const selectedPod = {
  name: "pipeline-vertex-0",
  containers: ["main"],
  containerSpecMap: new Map(),
};

function renderPodView() {
  return render(
    <PodViewNext
      namespaceId="default"
      pipelineId="pipeline"
      vertexId="vertex"
      type="source"
    />
  );
}

describe("PodViewNext", () => {
  beforeEach(() => {
    mockUseVertexHealthData.mockReturnValue({
      queries: signalQueries(
        {
          namespaceId: "default",
          pipelineId: "pipeline",
          vertexId: "vertex",
          type: "udf",
        },
        VERTEX_DISCOVERY
      ),
      series: { p99: [1000, 2000] },
      metricsEnabled: true,
      now: Date.now(),
    });
    mockedUsePodViewState.mockReturnValue({
      pods: [selectedPod],
      podsDetails: new Map(),
      podsErr: undefined,
      loading: false,
      podRuntimeByName: new Map(),
      podRuntimeError: undefined,
      selectedPod,
      selectedContainer: "main",
      selectedRuntime: undefined,
      selectedPodDetails: undefined,
      selectPodFromFleet: jest.fn(),
      selectPodFromSearch: jest.fn(),
      selectContainer: jest.fn(),
    });
  });

  it("mounts one bounded log workspace and inspector for the selected pod", () => {
    const { container } = renderPodView();

    expect(screen.getAllByTestId("pod-view-beta")).toHaveLength(1);
    expect(screen.getByTestId("pod-fleet")).toBeInTheDocument();
    expect(screen.getByTestId("pod-inspector")).toBeInTheDocument();
    expect(screen.getAllByTestId("pod-detail")).toHaveLength(1);
    expect(
      container.querySelector(".pod-view-next-workspace")
    ).toBeInTheDocument();
    expect(container.querySelector(".pod-view-next-logs")).toBeInTheDocument();
  });

  it("asks for explicit pod selection with numa as the default container", () => {
    renderPodView();

    expect(mockedUsePodViewState).toHaveBeenCalledWith(
      expect.objectContaining({ vertexId: "vertex" }),
      { autoSelectFirstPod: false, defaultContainer: "numa", active: true }
    );
  });

  it("does not mount logs while the Pods tab is hidden", () => {
    render(
      <PodViewNext
        namespaceId="default"
        pipelineId="pipeline"
        vertexId="vertex"
        type="source"
        active={false}
      />
    );

    expect(screen.queryByTestId("pod-detail")).not.toBeInTheDocument();
    expect(mockedUsePodViewState).toHaveBeenCalledWith(
      expect.objectContaining({ active: false }),
      expect.objectContaining({ active: false })
    );
  });

  it("shows the unselected prompt without mounting logs", () => {
    mockedUsePodViewState.mockReturnValueOnce({
      pods: [selectedPod],
      podsDetails: new Map(),
      podsErr: undefined,
      loading: false,
      podRuntimeByName: new Map(),
      selectedPod: undefined,
    });

    const { container } = renderPodView();

    expect(screen.getByText("Select a pod")).toBeInTheDocument();
    expect(
      screen.getByText("Click a chip above to inspect logs and metrics")
    ).toBeInTheDocument();
    expect(container.querySelector(".pod-view-next-workspace")).toBeNull();
    expect(screen.queryByTestId("pod-detail")).not.toBeInTheDocument();
  });

  it.each([
    ["loading", { loading: true, pods: undefined }, "pods-loading"],
    ["error", { podsErr: new Error("pods failed") }, "pods-error"],
    ["empty", { pods: [] }, "pods-empty"],
  ])("renders the %s state without mounting the workspace", (_, state, testId) => {
    mockedUsePodViewState.mockReturnValueOnce({
      pods: [selectedPod],
      podsDetails: new Map(),
      podsErr: undefined,
      loading: false,
      podRuntimeByName: new Map(),
      selectedPod,
      ...state,
    });

    const { container } = renderPodView();

    expect(screen.getByTestId(testId)).toBeInTheDocument();
    expect(container.querySelector(".pod-view-next-workspace")).toBeNull();
  });

  it("renders the vertex health strip above the fleet", () => {
    renderPodView();

    const strip = screen.getByTestId("vertex-health");
    expect(strip.compareDocumentPosition(screen.getByTestId("pod-fleet"))).toBe(
      Node.DOCUMENT_POSITION_FOLLOWING
    );
    expect(screen.getByTestId("vertex-health-card-latency")).toHaveTextContent("P99 2.0 ms");
  });

  it("opens the processing-time panel from the Latency card", () => {
    const openMetrics = jest.fn();
    render(
      <VertexDetailsContext.Provider value={{ openMetrics, openVertexTab: jest.fn() } as any}>
        <PodViewNext namespaceId="default" pipelineId="pipeline" vertexId="vertex" type="udf" />
      </VertexDetailsContext.Provider>
    );

    fireEvent.click(screen.getByTestId("vertex-health-card-latency"));
    expect(openMetrics).toHaveBeenCalledWith({
      panelId: `${signalQueries(
        {
          namespaceId: "default",
          pipelineId: "pipeline",
          vertexId: "vertex",
          type: "udf",
        },
        VERTEX_DISCOVERY
      ).p99?.metric_name}-panel`,
      presets: { quantile: "0.99", duration: "5m" },
    });
  });

  it("filters to critical pods and opens the failing container from the Errors card", () => {
    const failingPod = {
      name: "pipeline-vertex-1",
      containers: ["numa", "udf"],
      containerSpecMap: new Map(),
    };
    const selectPodWithContainer = jest.fn();
    mockedUsePodViewState.mockReturnValueOnce({
      pods: [selectedPod, failingPod],
      podsDetails: new Map(),
      loading: false,
      podRuntimeByName: new Map([
        [
          failingPod.name,
          {
            name: failingPod.name,
            status: "Error",
            containerDetailsMap: {
              numa: { state: "Running", restartCount: 0 },
              udf: { state: "Terminated", restartCount: 4 },
            },
          },
        ],
      ]),
      selectedPod: undefined,
      selectPodFromFleet: jest.fn(),
      selectPodWithContainer,
    });

    renderPodView();
    expect(screen.getByTestId("vertex-health-card-errors")).toHaveTextContent("1 unhealthy pod");
    fireEvent.click(screen.getByTestId("vertex-health-card-errors"));

    expect(selectPodWithContainer).toHaveBeenCalledWith(failingPod, "udf");
    expect(mockFleetProps).toHaveBeenLastCalledWith(
      expect.objectContaining({ filter: "critical" })
    );
  });

  it("opens critical runtime metrics from the Errors card", () => {
    const openMetrics = jest.fn();
    mockUseVertexHealthData.mockReturnValueOnce({
      queries: signalQueries(
        {
          namespaceId: "default",
          pipelineId: "pipeline",
          vertexId: "vertex",
          type: "udf",
        },
        VERTEX_DISCOVERY
      ),
      series: { p99: [1000], critical: [3] },
      metricsEnabled: true,
      now: Date.now(),
    });

    render(
      <VertexDetailsContext.Provider value={{ openMetrics, openVertexTab: jest.fn() } as any}>
        <PodViewNext namespaceId="default" pipelineId="pipeline" vertexId="vertex" type="udf" />
      </VertexDetailsContext.Provider>
    );

    fireEvent.click(screen.getByTestId("vertex-health-card-errors"));
    expect(openMetrics).toHaveBeenCalledWith({
      panelId: "forwarder_critical_error_total-panel",
      presets: { duration: "10m" },
    });
  });
});

describe("failingContainer", () => {
  const item = (containerDetailsMap: any): PodFleetItem =>
    ({ runtime: { name: "p", containerDetailsMap }, restartCount: 0 } as any);

  it("prefers a container that is not running, then the most restarts", () => {
    expect(
      failingContainer(item({ numa: { state: "Running" }, udf: { state: "Waiting" } }))
    ).toBe("udf");
    expect(
      failingContainer(
        item({
          numa: { state: "Running", restartCount: 1 },
          udf: { state: "Running", restartCount: 5 },
        })
      )
    ).toBe("udf");
    expect(failingContainer(item({ numa: { state: "Running", restartCount: 0 } }))).toBeUndefined();
  });
});
