import React from "react";
import { render, screen, waitFor } from "@testing-library/react";
import "@testing-library/jest-dom";

import { Buffers } from "./index";

const mockBuffers = [
  {
    partition: 1000,
    isFull: false,
    ackPending: false,
    pending: false,
    bufferLength: 1001,
    bufferUsage: 1002,
    totalPendingMessages: 1003,
  },
  {
    partition: 1004,
    isFull: false,
    ackPending: false,
    pending: false,
    bufferLength: 1005,
    bufferUsage: 1006,
    totalPendingMessages: 1007,
  },
  {
    partition: 1008,
    isFull: true,
    ackPending: false,
    pending: false,
    bufferLength: 1009,
    bufferUsage: 1010,
    totalPendingMessages: 1011,
  },
];

describe("Buffers", () => {
  it("renders with empty buffers", async () => {
    render(<Buffers buffers={[]} />);
    await waitFor(() => {
      expect(screen.getByText("Partition")).toBeInTheDocument();
      expect(screen.getByText("isFull")).toBeInTheDocument();
      expect(screen.getByText("AckPending")).toBeInTheDocument();
      expect(
        screen.getByText("No buffer information found")
      ).toBeInTheDocument();
    });
  });

  it("renders with buffers", async () => {
    render(<Buffers buffers={mockBuffers} />);
    await waitFor(() => {
      expect(screen.getByText("1001")).toBeInTheDocument();
      expect(screen.getByText("100200.00%")).toBeInTheDocument();
      expect(screen.getByText("1005")).toBeInTheDocument();
      expect(screen.getByText("100600.00%")).toBeInTheDocument();
      expect(screen.getAllByText("no").length).toBeGreaterThan(0);
    });
  });
});
