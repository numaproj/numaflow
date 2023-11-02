import React from "react";
import { render, screen, waitFor } from "@testing-library/react";
import "@testing-library/jest-dom";

import { ProcessingRates } from "./index";

const mockPipelineId = "simple-pipeline";
const mockVertexId = "in";
const mockVertexMetrics = {
  cat: [
    {
      pipeline: "simple-pipeline",
      vertex: "cat",
      processingRates: {
        "15m": 0,
        "1m": 0,
        "5m": 0,
        default: 0,
      },
    },
  ],
  in: [
    {
      pipeline: "simple-pipeline",
      vertex: "in",
      processingRates: {
        "15m": 10,
        "1m": 20,
        "5m": 30,
        default: 0,
      },
    },
  ],
  out: [
    {
      pipeline: "simple-pipeline",
      vertex: "out",
      processingRates: {
        "15m": 0,
        "1m": 0,
        "5m": 0,
        default: 0,
      },
    },
  ],
};

describe("ProcessingRates", () => {
  it("renders with empty processing rates", async () => {
    render(
      <ProcessingRates vertexId={""} pipelineId={""} vertexMetrics={[]} />
    );
    await waitFor(() => {
      expect(screen.getByText("Partition")).toBeInTheDocument();
      expect(screen.getByText("No metrics found")).toBeInTheDocument();
    });
  });

  it("renders with processing rates", async () => {
    render(
      <ProcessingRates
        vertexId={mockVertexId}
        pipelineId={mockPipelineId}
        vertexMetrics={mockVertexMetrics}
      />
    );
    await waitFor(() => {
      expect(screen.getByText("Partition")).toBeInTheDocument();
      expect(screen.getByText("20.00")).toBeInTheDocument();
      expect(screen.getByText("30.00")).toBeInTheDocument();
      expect(screen.getByText("10.00")).toBeInTheDocument();
    });
  });
  it("Renders when vertexData is null", async () => {
    render(
      <ProcessingRates
        vertexId={"zzz"}
        pipelineId={mockPipelineId}
        vertexMetrics={mockVertexMetrics}
      />
    );
    await waitFor(() => {
      expect(screen.getByText("No metrics found")).toBeInTheDocument();
    });
  });
});
