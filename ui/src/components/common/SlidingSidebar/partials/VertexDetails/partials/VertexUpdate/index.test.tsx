import React from "react";
import {
  fireEvent,
  render,
  screen,
  waitFor,
  act,
} from "@testing-library/react";
import { VertexUpdate } from "./index";
import fetch from "jest-fetch-mock";
import { BrowserRouter, MemoryRouter, useLocation } from "react-router-dom";

import "@testing-library/jest-dom";

const SearchProbe = () => {
  const location = useLocation();
  return <div data-testid="location-search">{location.search}</div>;
};

// Mock SpecEditor
jest.mock("../../../../../SpecEditor", () => {
  const originalModule = jest.requireActual("../../../../../SpecEditor");
  const react = jest.requireActual("react");
  // Mock any module exports here
  return {
    __esModule: true,
    ...originalModule,
    // Named export mocks
    SpecEditor: (props: any) => {
      const [mutated, setMutated] = react.useState(false);
      const handleMutateClick = react.useCallback(() => {
        props.onMutatedChange(mutated);
        setMutated(!mutated);
      }, [mutated, props.onMutatedChange]);
      return (
        <div data-testid="spec-editor-mock">
          <div>{JSON.stringify(props.validationMessage)}</div>
          <div>{JSON.stringify(props.statusIndicator)}</div>
          <div>{props.initialYaml}</div>
          <div data-testid="spec-editor-initial-line">
            {String(props.initialLine ?? "")}
          </div>
          <div data-testid="spec-editor-initial-end-line">
            {String(props.initialEndLine ?? "")}
          </div>
          <button
            data-testid="spec-editor-reset"
            onClick={props.onResetApplied}
          />
          <button
            data-testid="spec-editor-validate"
            onClick={() => {
              props.onValidate("test");
            }}
          />
          <button
            data-testid="spec-editor-submit"
            onClick={() => {
              props.onSubmit("test");
            }}
          />
          <button
            data-testid="spec-editor-mutated"
            onClick={handleMutateClick}
          />
          <button
            data-testid="spec-editor-select-range"
            onClick={() => props.onSelectionLineChange?.(12, 15)}
          />
          <button
            data-testid="spec-editor-select-single"
            onClick={() => props.onSelectionLineChange?.(12, 12)}
          />
          <button
            data-testid="spec-editor-cursor"
            onClick={() => props.onCursorLineChange?.(15)}
          />
        </div>
      );
    },
  };
});

const renderLinkedSpec = (search: string) =>
  render(
    <MemoryRouter
      initialEntries={[`/${search.startsWith("?") ? search : `?${search}`}`]}
    >
      <SearchProbe />
      <VertexUpdate
        namespaceId="test-namespace"
        pipelineId="test-pipeline"
        vertexId="test-vertex"
        vertexSpec="test-spec"
        setModalOnClose={jest.fn()}
        refresh={jest.fn()}
      />
    </MemoryRouter>
  );

describe("VertexUpdate", () => {
  beforeEach(() => {
    jest.clearAllMocks();
    fetch.resetMocks();
  });

  it("renders title and spec editor", async () => {
    const mockSetModalOnClose = jest.fn();
    render(
      <VertexUpdate
        namespaceId="test-namespace"
        pipelineId="test-pipeline"
        vertexId="test-vertex"
        vertexSpec="test-spec"
        setModalOnClose={mockSetModalOnClose}
        refresh={jest.fn()}
      />,
      { wrapper: BrowserRouter }
    );
    await waitFor(() => {
      expect(screen.getByTestId("spec-editor-reset")).toBeInTheDocument();
    });
    // Click reset
    act(() => {
      const resetBtn = screen.getByTestId("spec-editor-reset");
      fireEvent.click(resetBtn);
    });
    // Fire mutation change twice to run both branches
    act(() => {
      const mutationBtn = screen.getByTestId("spec-editor-mutated");
      fireEvent.click(mutationBtn);
    });
    expect(mockSetModalOnClose).toHaveBeenCalledWith(undefined);
    mockSetModalOnClose.mockClear();
    act(() => {
      const mutationBtn = screen.getByTestId("spec-editor-mutated");
      fireEvent.click(mutationBtn);
    });
    expect(mockSetModalOnClose).toHaveBeenCalledWith({
      iconType: "warn",
      message: "Are you sure you want to discard your changes?",
    });
  });

  it("validation success", async () => {
    fetch.mockResponseOnce(JSON.stringify({ data: {} }));
    const mockSetModalOnClose = jest.fn();
    render(
      <VertexUpdate
        namespaceId="test-namespace"
        pipelineId="test-pipeline"
        vertexId="test-vertex"
        vertexSpec="test-spec"
        setModalOnClose={mockSetModalOnClose}
        refresh={jest.fn()}
      />,
      { wrapper: BrowserRouter }
    );
    await waitFor(() => {
      expect(screen.getByTestId("spec-editor-reset")).toBeInTheDocument();
    });
    // Click reset
    act(() => {
      const validateBtn = screen.getByTestId("spec-editor-validate");
      fireEvent.click(validateBtn);
    });
    await waitFor(() => {
      expect(
        screen.getByText(
          `{"type":"success","message":"Successfully validated"}`
        )
      ).toBeInTheDocument();
    });
  });

  it("validation failure", async () => {
    fetch.mockResponseOnce(JSON.stringify({ errMsg: "failed" }));
    const mockSetModalOnClose = jest.fn();
    render(
      <VertexUpdate
        namespaceId="test-namespace"
        pipelineId="test-pipeline"
        vertexId="test-vertex"
        vertexSpec="test-spec"
        setModalOnClose={mockSetModalOnClose}
        refresh={jest.fn()}
      />,
      { wrapper: BrowserRouter }
    );
    await waitFor(() => {
      expect(screen.getByTestId("spec-editor-reset")).toBeInTheDocument();
    });
    // Click reset
    act(() => {
      const validateBtn = screen.getByTestId("spec-editor-validate");
      fireEvent.click(validateBtn);
    });
    await waitFor(() => {
      expect(
        screen.getByText(`{"type":"error","message":"Error: failed"}`)
      ).toBeInTheDocument();
    });
  });

  it("submit success", async () => {
    fetch.once(JSON.stringify({ data: {} })).once(JSON.stringify({ data: {} }));
    const mockRefresh = jest.fn();
    render(
      <VertexUpdate
        namespaceId="test-namespace"
        pipelineId="test-pipeline"
        vertexId="test-vertex"
        vertexSpec="test-spec"
        setModalOnClose={jest.fn()}
        refresh={mockRefresh}
      />,
      { wrapper: BrowserRouter }
    );
    await waitFor(() => {
      expect(screen.getByTestId("spec-editor-reset")).toBeInTheDocument();
    });
    // Click
    act(() => {
      const submitBtn = screen.getByTestId("spec-editor-submit");
      fireEvent.click(submitBtn);
    });
    await waitFor(() => {
      expect(
        screen.getByText(
          `{"submit":{"status":1,"message":"Vertex updated successfully","allowRetry":false}}`
        )
      ).toBeInTheDocument();
    });
    // Wait for onUpdateComplete call (after 1000ms setTimeout)
    await waitFor(
      () => {
        expect(mockRefresh).toHaveBeenCalledTimes(1);
      },
      { timeout: 2000 }
    );
  });

  it("submit failure", async () => {
    fetch.mockResponseOnce(JSON.stringify({ errMsg: "failed" }));
    const mockRefresh = jest.fn();
    render(
      <VertexUpdate
        namespaceId="test-namespace"
        pipelineId="test-pipeline"
        vertexId="test-vertex"
        vertexSpec="test-spec"
        setModalOnClose={jest.fn()}
        refresh={mockRefresh}
      />,
      { wrapper: BrowserRouter }
    );
    await waitFor(() => {
      expect(screen.getByTestId("spec-editor-reset")).toBeInTheDocument();
    });
    // Click reset
    act(() => {
      const submitBtn = screen.getByTestId("spec-editor-submit");
      fireEvent.click(submitBtn);
    });
    await waitFor(() => {
      expect(
        screen.getByText(`{"type":"error","message":"Error: failed"}`)
      ).toBeInTheDocument();
    });
  });

  it("writes a multi-line Spec selection to specLine and ignores cursor-only events", async () => {
    renderLinkedSpec("vertex=test-vertex&vertexTab=spec");
    await waitFor(() => {
      expect(screen.getByTestId("spec-editor-select-range")).toBeInTheDocument();
    });

    fireEvent.click(screen.getByTestId("spec-editor-select-range"));
    await waitFor(() => {
      expect(screen.getByTestId("location-search")).toHaveTextContent(
        "specLine=12-15"
      );
    });

    fireEvent.click(screen.getByTestId("spec-editor-cursor"));
    expect(screen.getByTestId("location-search")).toHaveTextContent(
      "specLine=12-15"
    );
    expect(screen.getByTestId("location-search")).not.toHaveTextContent(
      "specLine=15"
    );
  });

  it("writes a single-line Spec selection as specLine without a range", async () => {
    renderLinkedSpec("vertex=test-vertex&vertexTab=spec");
    await waitFor(() => {
      expect(
        screen.getByTestId("spec-editor-select-single")
      ).toBeInTheDocument();
    });

    fireEvent.click(screen.getByTestId("spec-editor-select-single"));
    await waitFor(() => {
      expect(screen.getByTestId("location-search")).toHaveTextContent(
        "specLine=12"
      );
    });
    expect(screen.getByTestId("location-search")).not.toHaveTextContent(
      "specLine=12-12"
    );
  });

  it("passes a deep-linked specLine range into the editor", async () => {
    renderLinkedSpec("vertex=test-vertex&vertexTab=spec&specLine=12-15");
    await waitFor(() => {
      expect(screen.getByTestId("spec-editor-initial-line")).toHaveTextContent(
        "12"
      );
      expect(
        screen.getByTestId("spec-editor-initial-end-line")
      ).toHaveTextContent("15");
    });
  });
});
