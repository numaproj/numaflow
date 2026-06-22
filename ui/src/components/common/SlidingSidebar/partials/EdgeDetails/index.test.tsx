import React from "react";
import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { EdgeDetails } from "./index";

import "@testing-library/jest-dom";

const mockISBDebugRefresh = jest.fn();

jest.mock("../../../../../utils/fetchWrappers/pipelineISBDebugFetch", () => ({
  usePipelineISBDebugFetch: () => ({
    data: {
      streams: { streams: [] },
      consumers: { consumers: [] },
      kvStores: {
        kvStores: [
          {
            namespace: "test-namespace",
            pipeline: "test-pipeline",
            scope: "edge",
            from: "in",
            to: "cat",
            bucket: "test-namespace-test-pipeline-in-cat_OT",
            stream: "KV_test-namespace-test-pipeline-in-cat_OT",
            values: 2,
            bytes: 1024,
            storage: "file",
          },
        ],
      },
    },
    loading: false,
    error: undefined,
    refresh: mockISBDebugRefresh,
  }),
}));

describe("EdgeDetails", () => {
  beforeEach(() => {
    jest.clearAllMocks();
  });

  it("no watermarks", async () => {
    render(<EdgeDetails edgeId="test-edge" />);
    await waitFor(() => {
      expect(screen.getByText("test-edge Edge")).toBeInTheDocument();
      expect(screen.getByText("No watermarks found")).toBeInTheDocument();
    });
  });

  it("with positive watermark", async () => {
    const rawWatermark = Date.now();
    render(<EdgeDetails edgeId="test-edge" watermarks={[rawWatermark]} />);
    await waitFor(() => {
      expect(screen.getByText("test-edge Edge")).toBeInTheDocument();
      expect(
        screen.getByText(
          `${rawWatermark} (${new Date(rawWatermark).toISOString()})`
        )
      ).toBeInTheDocument();
    });
  });

  it("with negative watermark", async () => {
    const rawWatermark = -1;
    render(<EdgeDetails edgeId="test-edge" watermarks={[rawWatermark]} />);
    await waitFor(() => {
      expect(screen.getByText("test-edge Edge")).toBeInTheDocument();
      expect(screen.getByText(`${rawWatermark}`)).toBeInTheDocument();
    });
  });

  it("renders ISB tab", async () => {
    render(
      <EdgeDetails
        namespaceId="test-namespace"
        pipelineId="test-pipeline"
        edgeId="test-edge"
        from="in"
        to="cat"
        watermarks={[]}
      />
    );

    fireEvent.click(screen.getByTestId("edge-isb-tab"));

    await waitFor(() => {
      expect(screen.getByText("KV Stores")).toBeInTheDocument();
      expect(
        screen.getByText("test-namespace-test-pipeline-in-cat_OT")
      ).toBeInTheDocument();
      expect(screen.getByText(/target vertex buffer/)).toBeInTheDocument();
    });

    fireEvent.click(screen.getByText("Refresh"));

    await waitFor(() => {
      expect(mockISBDebugRefresh).toHaveBeenCalledTimes(1);
    });
  });
});
