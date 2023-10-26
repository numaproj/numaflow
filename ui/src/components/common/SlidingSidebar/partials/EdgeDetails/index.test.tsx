import React from "react";
import { render, screen, waitFor } from "@testing-library/react";
import { EdgeDetails } from "./index";

import "@testing-library/jest-dom";

describe("EdgeDetails", () => {
  beforeEach(() => {
    jest.clearAllMocks();
  });

  it("no watermarks", async () => {
    render(<EdgeDetails edgeId="test-edge" watermarks={[]} />);
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
});
