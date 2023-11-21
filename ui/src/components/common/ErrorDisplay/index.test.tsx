import React from "react";
import { render, screen, waitFor } from "@testing-library/react";
import "@testing-library/jest-dom";

import { ErrorDisplay } from "./index";

describe("ErrorDisplay", () => {
  it("renders", async () => {
    render(<ErrorDisplay title={"test"} message={"test error"} />);
    await waitFor(() => {
      expect(screen.getByText("test")).toBeInTheDocument();
      expect(screen.getByText("test error")).toBeInTheDocument();
    });
  });
});
