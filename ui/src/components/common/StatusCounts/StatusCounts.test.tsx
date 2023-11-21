import React from "react";
import { render, screen } from "@testing-library/react";
import "@testing-library/jest-dom";

import { StatusCounts } from "./StatusCounts";

describe("StatusCounts", () => {
  it("Renders", () => {
    render(<StatusCounts counts={{ healthy: 1, warning: 2, critical: 3 }} />);
    expect(screen.getByText("healthy")).toBeInTheDocument();
    expect(screen.getByText(": 1")).toBeInTheDocument();
    expect(screen.getByText("warning")).toBeInTheDocument();
    expect(screen.getByText(": 2")).toBeInTheDocument();
    expect(screen.getByText("critical")).toBeInTheDocument();
    expect(screen.getByText(": 3")).toBeInTheDocument();
  });

  it("Renders when counts are zero", () => {
    render(<StatusCounts counts={{ healthy: 0, warning: 0, critical: 0 }} />);
    expect(screen.getByText("healthy")).toBeInTheDocument();
    expect(screen.getAllByText(": 0").length).toBe(3);
    expect(screen.getByText("warning")).toBeInTheDocument();
    expect(screen.getByText("critical")).toBeInTheDocument();
  });
});
