import { Pipeline } from "./index";
import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { usePipelineViewFetch } from "../../../utils/fetcherHooks/pipelineViewFetch";
import { usePipelineSummaryFetch } from "../../../utils/fetchWrappers/pipelineFetch";

global.ResizeObserver = require("resize-observer-polyfill");

import "@testing-library/jest-dom";
jest.mock("react-router-dom", () => ({
  ...jest.requireActual("react-router-dom"),
  useParams: () => ({
    namespaceId: "numaflow-system",
    pipelineId: "simple-pipeline",
  }),
}));

jest.mock("../../../utils/fetcherHooks/pipelineViewFetch");
const mockedUsePipelineViewFetch = usePipelineViewFetch as jest.MockedFunction<
  typeof usePipelineViewFetch
>;

jest.mock("../../../utils/fetchWrappers/pipelineFetch");
const mockedUsePipelineSummaryFetch =
  usePipelineSummaryFetch as jest.MockedFunction<
    typeof usePipelineSummaryFetch
  >;

const pipelineObj = {
  metadata: {
    name: "simple-pipeline",
    namespace: "default",
    creationTimestamp: "2023-06-06T05:18:34Z",
  },
  spec: {
    vertices: [
      {
        name: "in",
        source: {
          generator: {
            rpu: 5,
            duration: "1s",
            msgSize: 8,
          },
        },
      },
      {
        name: "cat",
        udf: {
          container: null,
          builtin: {
            name: "cat",
          },
          groupBy: null,
        },
      },
      {
        name: "out",
        sink: {
          log: {},
        },
      },
    ],
    edges: [
      {
        from: "in",
        to: "cat",
        conditions: null,
      },
      {
        from: "cat",
        to: "out",
        conditions: null,
      },
    ],
    watermark: {
      maxDelay: "0s",
    },
  },
  status: {
    conditions: [
      {
        type: "Configured",
        status: "True",
        lastTransitionTime: "2023-06-06T05:18:34Z",
        reason: "Successful",
        message: "Successful",
      },
    ],
    phase: "Running",
    lastUpdated: "2023-06-06T05:18:34Z",
  },
};
const verticesArr = [
  {
    id: "in",
    data: {
      name: "in",
      podnum: 0,
      source: {
        name: "in",
        source: {
          generator: {
            rpu: 5,
            duration: "1s",
            msgSize: 8,
          },
        },
        scale: {},
      },
      vertexMetrics: null,
      buffers: null,
    },
    position: {
      x: 0,
      y: 0,
    },
    draggable: false,
    type: "source",
  },
  {
    id: "cat",
    data: {
      name: "cat",
      podnum: 0,
      udf: {
        name: "cat",
        udf: {
          container: null,
          builtin: {
            name: "cat",
          },
          groupBy: null,
        },
        scale: {},
      },
      vertexMetrics: null,
      buffers: [
        {
          pipeline: "simple-pipeline",
          bufferName: "default-simple-pipeline-cat-0",
          pendingCount: 0,
          ackPendingCount: 4,
          totalMessages: 4,
          bufferLength: 30000,
          bufferUsageLimit: 0.8,
          bufferUsage: 0.00013333333333333334,
          isFull: false,
        },
      ],
    },
    position: {
      x: 0,
      y: 0,
    },
    draggable: false,
    type: "udf",
  },
  {
    id: "out",
    data: {
      name: "out",
      podnum: 0,
      sink: {
        name: "out",
        sink: {
          log: {},
        },
        scale: {},
      },
      test: "out",
      vertexMetrics: null,
      buffers: [
        {
          pipeline: "simple-pipeline",
          bufferName: "default-simple-pipeline-out-0",
          pendingCount: 0,
          ackPendingCount: 0,
          totalMessages: 0,
          bufferLength: 30000,
          bufferUsageLimit: 0.8,
          bufferUsage: 0,
          isFull: false,
        },
      ],
    },
    position: {
      x: 0,
      y: 0,
    },
    draggable: false,
    type: "sink",
  },
];
const edgesArr = [
  {
    id: "in-cat",
    source: "in",
    target: "cat",
    data: {
      conditions: null,
      backpressureLabel: 4,
      edgeWatermark: null,
    },
    animated: true,
    type: "custom",
  },
  {
    id: "cat-out",
    source: "cat",
    target: "out",
    data: {
      conditions: null,
      backpressureLabel: 0,
      edgeWatermark: null,
    },
    animated: true,
    type: "custom",
  },
];

describe("Pipeline", () => {
  it("Renders Pipeline", async () => {
    mockedUsePipelineSummaryFetch.mockReturnValue({
      data: {
        pipelineData: {
          name: "simple-pipeline",
          status: "healthy",
          lag: 1999,
          pipeline: {
            metadata: {
              creationTimestamp: "2023-11-07T06:54:57Z",
            },
            spec: {
              vertices: [],
              edges: [],
              watermark: {},
            },
            status: {
              conditions: [],
              lastUpdated: "2023-11-07T06:54:57Z",
            },
          },
        },
        isbData: {
          name: "default",
          status: "healthy",
          isbService: {
            metadata: {},
            spec: {
              jetstream: {
                version: "latest",
                replicas: 3,
                persistence: {
                  volumeSize: "3Gi",
                },
              },
            },
            status: {
              phase: "Running",
              conditions: [],
            },
          },
        },
      },
      loading: false,
      error: undefined,
      refresh: () => {
        return;
      },
    });
    mockedUsePipelineViewFetch.mockReturnValue({
      pipeline: pipelineObj,
      vertices: verticesArr,
      edges: edgesArr,
      generatorToColorIdxMap: new Map(),
      pipelineErr: undefined,
      buffersErr: undefined,
      loading: false,
      refresh: () => {
        return;
      },
    });
    render(<Pipeline />);
    await waitFor(() => {
      expect(screen.getByTestId("pipeline")).toBeInTheDocument();
      expect(screen.getByTestId("graph")).toBeInTheDocument();
    });
    fireEvent.click(screen.getByTestId("pipeline-k8s-events"));
  });

  it("Renders Pipeline Without Summary", async () => {
    mockedUsePipelineSummaryFetch.mockReturnValue({
      data: undefined,
      loading: false,
      error: undefined,
      refresh: () => {
        return;
      },
    });
    mockedUsePipelineViewFetch.mockReturnValue({
      pipeline: pipelineObj,
      vertices: verticesArr,
      edges: edgesArr,
      generatorToColorIdxMap: new Map(),
      pipelineErr: undefined,
      buffersErr: undefined,
      loading: false,
      refresh: () => {
        return;
      },
    });
    render(<Pipeline />);
    await waitFor(() => {
      expect(screen.getByTestId("pipeline")).toBeInTheDocument();
      expect(screen.getByTestId("graph")).toBeInTheDocument();
    });
  });

  it("Loading in Pipeline", async () => {
    mockedUsePipelineSummaryFetch.mockReturnValue({
      data: undefined,
      loading: true,
      error: undefined,
      refresh: () => {
        return;
      },
    });
    mockedUsePipelineViewFetch.mockReturnValue({
      pipeline: undefined,
      vertices: undefined,
      edges: undefined,
      generatorToColorIdxMap: new Map(),
      pipelineErr: undefined,
      buffersErr: undefined,
      loading: true,
      refresh: () => {
        return;
      },
    });
    render(<Pipeline />);
    await waitFor(() => {
      expect(
        screen.getByTestId("pipeline-summary-loading")
      ).toBeInTheDocument();
      expect(screen.getByTestId("pipeline-loading")).toBeInTheDocument();
      expect(screen.queryByTestId("graph")).toBeNull();
    });
  });

  it("Error in Pipeline", async () => {
    mockedUsePipelineSummaryFetch.mockReturnValue({
      data: undefined,
      loading: false,
      error: "some error",
      refresh: () => {
        return;
      },
    });
    mockedUsePipelineViewFetch.mockReturnValue({
      pipeline: undefined,
      vertices: undefined,
      edges: undefined,
      generatorToColorIdxMap: new Map(),
      pipelineErr: "some error",
      buffersErr: "some error",
      loading: true,
      refresh: () => {
        return;
      },
    });
    render(<Pipeline />);
    await waitFor(() => {
      expect(
        screen.getByText("Error loading pipeline summary")
      ).toBeInTheDocument();
      expect(screen.getByText("Error loading pipeline")).toBeInTheDocument();
      expect(screen.queryByTestId("graph")).toBeNull();
    });
  });
});
