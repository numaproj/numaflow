import React from "react";
import { render, screen } from "@testing-library/react";
import "@testing-library/jest-dom";

import { IndicatorStatus, StatusIndicator } from "./StatusIndicator";

describe("StatusIndicator", () => {
  it("Renders", () => {
    render(<StatusIndicator status={IndicatorStatus.HEALTHY} />);
    expect(screen.getByTestId("HEALTHY")).toBeInTheDocument();
  });

  it("Renders when status is warning", () => {
    render(<StatusIndicator status={IndicatorStatus.WARNING} />);
    expect(screen.getByTestId("WARNING")).toBeInTheDocument();
  });

  it("Renders when status is critical", () => {
    render(<StatusIndicator status={IndicatorStatus.CRITICAL} />);
    expect(screen.getByTestId("CRITICAL")).toBeInTheDocument();
  });
});
