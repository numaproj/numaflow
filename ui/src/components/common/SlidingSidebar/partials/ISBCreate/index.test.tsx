import React from "react";
import {
  fireEvent,
  render,
  screen,
  waitFor,
  act,
} from "@testing-library/react";
import { ISBCreate } from "./index";
import fetch from "jest-fetch-mock";

import "@testing-library/jest-dom";

// Mock SpecEditor
jest.mock("../../../SpecEditor", () => {
  const originalModule = jest.requireActual("../../../SpecEditor");
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
        </div>
      );
    },
  };
});

describe("ISBCreate", () => {
  beforeEach(() => {
    jest.clearAllMocks();
    fetch.resetMocks();
  });

  it("renders title and spec editor", async () => {
    const mockSetModalOnClose = jest.fn();
    render(
      <ISBCreate
        namespaceId="test-namespace"
        viewType={0}
        onUpdateComplete={jest.fn()}
        setModalOnClose={mockSetModalOnClose}
      />
    );
    await waitFor(() => {
      expect(screen.getByText("Create ISB Service")).toBeInTheDocument();
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
      <ISBCreate
        namespaceId="test-namespace"
        viewType={0}
        onUpdateComplete={jest.fn()}
        setModalOnClose={mockSetModalOnClose}
      />
    );
    await waitFor(() => {
      expect(screen.getByText("Create ISB Service")).toBeInTheDocument();
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
      <ISBCreate
        namespaceId="test-namespace"
        viewType={0}
        onUpdateComplete={jest.fn()}
        setModalOnClose={mockSetModalOnClose}
      />
    );
    await waitFor(() => {
      expect(screen.getByText("Create ISB Service")).toBeInTheDocument();
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
    fetch.mockResponseOnce(JSON.stringify({ data: {} }));
    const mockUpdateComplete = jest.fn();
    render(
      <ISBCreate
        namespaceId="test-namespace"
        viewType={0}
        onUpdateComplete={mockUpdateComplete}
        setModalOnClose={jest.fn()}
      />
    );
    await waitFor(() => {
      expect(screen.getByText("Create ISB Service")).toBeInTheDocument();
      expect(screen.getByTestId("spec-editor-reset")).toBeInTheDocument();
    });
    // Click reset
    act(() => {
      const submitBtn = screen.getByTestId("spec-editor-submit");
      fireEvent.click(submitBtn);
    });
    await waitFor(() => {
      expect(
        screen.getByText(
          `{"submit":{"status":1,"message":"ISB Service created successfully","allowRetry":false}}`
        )
      ).toBeInTheDocument();
    });
    // Wait for onUpdateComplete call
    await new Promise((r) => setTimeout(r, 1000));
    expect(mockUpdateComplete).toHaveBeenCalledTimes(1);
  });

  it("submit failure", async () => {
    fetch.mockResponseOnce(JSON.stringify({ errMsg: "failed" }));
    const mockUpdateComplete = jest.fn();
    render(
      <ISBCreate
        namespaceId="test-namespace"
        viewType={0}
        onUpdateComplete={mockUpdateComplete}
        setModalOnClose={jest.fn()}
      />
    );
    await waitFor(() => {
      expect(screen.getByText("Create ISB Service")).toBeInTheDocument();
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
});
