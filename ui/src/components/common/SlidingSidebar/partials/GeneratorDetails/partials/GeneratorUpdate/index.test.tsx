import React from "react";
import { render, screen, waitFor } from "@testing-library/react";
import { GeneratorUpdate } from "./index";

import "@testing-library/jest-dom";

// Mock SpecEditor
jest.mock("../../../../../SpecEditor", () => {
  const originalModule = jest.requireActual("../../../../../SpecEditor");
  // Mock any module exports here
  return {
    __esModule: true,
    ...originalModule,
    // Named export mocks
    SpecEditor: (props: any) => (
      <div data-testid="spec-editor-mock">{JSON.stringify(props)}</div>
    ),
  };
});

describe("GeneratorUpdate", () => {
  beforeEach(() => {
    jest.clearAllMocks();
  });

  it("renders speceditor", async () => {
    render(
      <GeneratorUpdate generatorId="test-vertex" generatorSpec={"test-spec"} />
    );

    await waitFor(() => {
      expect(
        screen.getByText(`{"initialYaml":"test-spec","viewType":0}`)
      ).toBeInTheDocument();
    });
  });
});
