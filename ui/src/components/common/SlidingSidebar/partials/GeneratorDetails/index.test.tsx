import React from "react";
import { render, screen, waitFor } from "@testing-library/react";
import { GeneratorDetails } from "./index";

import "@testing-library/jest-dom";

// Mock GeneratorUpdate
jest.mock("./partials/GeneratorUpdate", () => {
  const originalModule = jest.requireActual("./partials/GeneratorUpdate");
  // Mock any module exports here
  return {
    __esModule: true,
    ...originalModule,
    // Named export mocks
    GeneratorUpdate: () => (
      <div data-testid="generator-update-mock">Mocked</div>
    ),
  };
});

describe("GeneratorDetails", () => {
  beforeEach(() => {
    jest.clearAllMocks();
  });

  it("renders spec tab", async () => {
    render(<GeneratorDetails vertexId="test-vertex" generatorDetails={{}} />);

    await waitFor(() => {
      expect(screen.getByText("Generator Vertex")).toBeInTheDocument();
    });
  });
});
