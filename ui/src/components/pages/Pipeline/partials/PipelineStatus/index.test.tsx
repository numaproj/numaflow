import React from "react";
import { render, screen } from "@testing-library/react";
import "@testing-library/jest-dom";
import { AppContext } from "../../../../../App";
import { BrowserRouter } from "react-router-dom";
import { PipelineStatus } from "./index";

describe("PipelineStatus", () => {
  it("should render the component", () => {
    render(
      <AppContext.Provider value="healthy">
        <BrowserRouter>
          <PipelineStatus
            status="Running"
            healthStatus="healthy"
            healthData={{
              resourceHealthStatus: "healthy",
              resourceHealthMessage: "Pipeline simple-pipeline is healthy",
              dataHealthStatus: "healthy",
              dataHealthMessage: "Pipeline data flow is healthy",
            }}
          />
        </BrowserRouter>
      </AppContext.Provider>
    );

    expect(screen.getByText("STATUS")).toBeInTheDocument();
  });
});
