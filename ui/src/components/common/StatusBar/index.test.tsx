import React from "react";
import { render, screen } from "@testing-library/react";
import "@testing-library/jest-dom";

import { StatusBar } from "./index";

describe("StatusBar", () => {
  it("Renders", () => {
    render(<StatusBar healthy={1} warning={2} critical={3} />);
    expect(screen.getByText("Healthy")).toBeInTheDocument();
    expect(screen.getByText("Warning")).toBeInTheDocument();
  });
});
