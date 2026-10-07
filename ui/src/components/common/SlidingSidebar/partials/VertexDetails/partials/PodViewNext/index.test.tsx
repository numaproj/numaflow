import { render, screen } from "@testing-library/react";
import "@testing-library/jest-dom";
import { PodViewNext } from "./index";
import { usePodViewState } from "../../../../../../pages/Pipeline/partials/Graph/partials/NodeInfo/partials/Pods/usePodViewState";

jest.mock(
  "../../../../../../pages/Pipeline/partials/Graph/partials/NodeInfo/partials/Pods/usePodViewState",
  () => ({ usePodViewState: jest.fn() })
);
jest.mock("./PodFleet", () => ({
  PodFleet: () => <div data-testid="pod-fleet" />,
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
    ["loading", { loading: true }, "pods-loading"],
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
});
