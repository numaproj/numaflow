import React from "react";
import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import "@testing-library/jest-dom";

import { Buffers } from "./index";
import { VertexDetailsContext } from "../../index";

const mockUsePipelineISBDebugFetch = jest.fn();
const mockISBDebugRefresh = jest.fn();

jest.mock("../../../../../MetricsModalWrapper", () => ({
  MetricsModalWrapper: ({ value }) => <div>{value}</div>,
}));

jest.mock(
  "../../../../../../../utils/fetchWrappers/pipelineISBDebugFetch",
  () => ({
    usePipelineISBDebugFetch: (props) => mockUsePipelineISBDebugFetch(props),
  })
);

const mockBuffers = [
  {
    pipeline: "simple-pipeline",
    bufferName: "default-simple-pipeline-cat-0",
    pendingCount: 7,
    ackPendingCount: 2,
    totalMessages: 9,
    bufferLength: 30000,
    bufferUsageLimit: 0.8,
    bufferUsage: 0.5,
    isFull: true,
  },
  {
    pipeline: "simple-pipeline",
    bufferName: "default-simple-pipeline-cat-1",
    pendingCount: 0,
    ackPendingCount: 1,
    totalMessages: 1,
    bufferLength: 30000,
    bufferUsageLimit: 0.8,
    bufferUsage: 0.1,
    isFull: false,
  },
];

const mockISBDebugData = {
  streams: {
    streams: [
      {
        namespace: "default",
        pipeline: "simple-pipeline",
        vertex: "cat",
        partition: 0,
        stream: "default-simple-pipeline-cat-0",
        messages: 9,
        bytes: 1024,
        consumerCount: 1,
        firstSeq: 1,
        lastSeq: 9,
        scope: "vertex",
        sharedByInboundEdges: false,
      },
    ],
  },
  consumers: { consumers: [] },
  kvStores: { kvStores: [] },
};

const renderBuffers = (buffers = mockBuffers) => {
  const Wrapper = () => {
    const [expanded, setExpanded] = React.useState<Set<string>>(new Set());
    return (
      <VertexDetailsContext.Provider
        value={{
          openMetrics: jest.fn(),
          expanded,
          setExpanded,
          presets: undefined,
          setPresets: jest.fn(),
        }}
      >
        <Buffers
          buffers={buffers}
          namespaceId="default"
          pipelineId="simple-pipeline"
          vertexId="cat"
          type="udf"
        />
      </VertexDetailsContext.Provider>
    );
  };
  return render(<Wrapper />);
};

describe("Buffers", () => {
  beforeEach(() => {
    let hasLoadedISBDebug = false;
    mockUsePipelineISBDebugFetch.mockImplementation((props) => {
      if (props.enabled) {
        hasLoadedISBDebug = true;
      }
      return {
        data: hasLoadedISBDebug ? mockISBDebugData : undefined,
        loading: false,
        error: undefined,
        refresh: mockISBDebugRefresh,
      };
    });
    mockISBDebugRefresh.mockClear();
  });

  it("renders with empty buffers", async () => {
    renderBuffers([]);
    await waitFor(() => {
      expect(screen.getByText("Partition")).toBeInTheDocument();
      expect(screen.getByText("Is Full")).toBeInTheDocument();
      expect(screen.getByText("Ack Pending")).toBeInTheDocument();
      expect(screen.getByText("Pending")).toBeInTheDocument();
      expect(screen.getByText("Total Pending Messages")).toBeInTheDocument();
      expect(
        screen.getByText("No buffer information found")
      ).toBeInTheDocument();
      expect(screen.getByText("Advanced ISB Diagnostics")).toBeInTheDocument();
    });
  });

  it("renders with buffers", async () => {
    renderBuffers();
    await waitFor(() => {
      expect(
        screen.getByText("default-simple-pipeline-cat-0")
      ).toBeInTheDocument();
      expect(screen.getAllByText("yes").length).toBeGreaterThan(0);
      expect(screen.getAllByText("no").length).toBeGreaterThan(0);
      expect(screen.getByText("50.00%")).toBeInTheDocument();
      expect(screen.getAllByText("30000").length).toBeGreaterThan(0);
      expect(screen.getByText("9")).toBeInTheDocument();
      expect(screen.queryByText("Details")).not.toBeInTheDocument();
      expect(screen.queryByText("Stream Information")).not.toBeInTheDocument();
      expect(screen.queryByText("Refresh")).not.toBeInTheDocument();
    });
  });

  it("expands advanced details and lazy loads vertex ISB details", async () => {
    renderBuffers();

    fireEvent.click(screen.getByText("Advanced ISB Diagnostics"));

    await waitFor(() => {
      expect(
        screen.queryByText("Hide Inter-Step Buffer details")
      ).not.toBeInTheDocument();
      expect(
        screen.queryByText("Show Inter-Step Buffer details")
      ).not.toBeInTheDocument();
      expect(screen.getByText("Stream Information")).toBeInTheDocument();
    });

    const lastFetchProps =
      mockUsePipelineISBDebugFetch.mock.calls[
        mockUsePipelineISBDebugFetch.mock.calls.length - 1
      ][0];
    expect(lastFetchProps).toEqual(
      expect.objectContaining({
        namespaceId: "default",
        pipelineId: "simple-pipeline",
        vertexId: "cat",
        enabled: true,
      })
    );
    expect(lastFetchProps.partition).toBeUndefined();

    await waitFor(() => {
      expect(mockISBDebugRefresh).toHaveBeenCalledTimes(1);
    });

    fireEvent.click(screen.getByText("Advanced ISB Diagnostics"));

    await waitFor(() => {
      const lastFetchProps =
        mockUsePipelineISBDebugFetch.mock.calls[
          mockUsePipelineISBDebugFetch.mock.calls.length - 1
        ][0];
      expect(lastFetchProps).toEqual(
        expect.objectContaining({
          enabled: false,
        })
      );
      expect(
        screen.queryByText("ISB information is not available yet.")
      ).not.toBeInTheDocument();
    });
  });
});
