import React from "react";
import {
  fireEvent,
  render,
  screen,
  waitFor,
  act,
} from "@testing-library/react";
import { ISBUpdate } from "./index";
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
          <div>{JSON.stringify(props.initialYaml)}</div>
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

describe("ISBUpdate", () => {
  beforeEach(() => {
    jest.clearAllMocks();
    fetch.resetMocks();
  });

  it("renders title and spec editor", async () => {
    const mockSetModalOnClose = jest.fn();
    render(
      <ISBUpdate
        initialYaml="test"
        namespaceId="test-namespace"
        isbId="test-isb"
        viewType={0}
        onUpdateComplete={jest.fn()}
        setModalOnClose={mockSetModalOnClose}
      />
    );
    await waitFor(() => {
      expect(screen.getByText(`Edit ISB Service: test-isb`)).toBeInTheDocument();
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
      <ISBUpdate
        initialYaml="test"
        namespaceId="test-namespace"
        isbId="test-isb"
        viewType={0}
        onUpdateComplete={jest.fn()}
        setModalOnClose={mockSetModalOnClose}
      />
    );
    await waitFor(() => {
      expect(screen.getByText(`Edit ISB Service: test-isb`)).toBeInTheDocument();
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
      <ISBUpdate
        initialYaml="test"
        namespaceId="test-namespace"
        isbId="test-isb"
        viewType={0}
        onUpdateComplete={jest.fn()}
        setModalOnClose={mockSetModalOnClose}
      />
    );
    await waitFor(() => {
      expect(screen.getByText(`Edit ISB Service: test-isb`)).toBeInTheDocument();
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
      <ISBUpdate
        initialYaml="test"
        namespaceId="test-namespace"
        isbId="test-isb"
        viewType={0}
        onUpdateComplete={mockUpdateComplete}
        setModalOnClose={jest.fn()}
      />
    );
    await waitFor(() => {
      expect(screen.getByText(`Edit ISB Service: test-isb`)).toBeInTheDocument();
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
          `{"submit":{"status":1,"message":"ISB Service updated successfully","allowRetry":false}}`
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
      <ISBUpdate
        initialYaml="test"
        namespaceId="test-namespace"
        isbId="test-isb"
        viewType={0}
        onUpdateComplete={mockUpdateComplete}
        setModalOnClose={jest.fn()}
      />
    );
    await waitFor(() => {
      expect(screen.getByText(`Edit ISB Service: test-isb`)).toBeInTheDocument();
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

  it("renders JetStream summary and RAFT debug information for read-only JetStream ISB", async () => {
    fetch.mockResponseOnce(
      JSON.stringify({
        data: {
          summary: [
            {
              server: "js-0",
              cluster: "default",
              streams: 7,
              consumers: 3,
              messages: 200003,
              bytes: 41523609,
              apiRequests: 65431,
              apiErrors: 12,
              apiErrorRate: 0.00018,
              metaLeader: true,
            },
            {
              server: "js-1",
              cluster: "default",
              streams: 7,
              consumers: 3,
              messages: 200003,
              bytes: 41523609,
              apiRequests: 35145,
              apiErrors: 0,
              apiErrorRate: 0,
              metaLeader: false,
            },
          ],
          raftMetaGroup: [
            {
              name: "js-0",
              id: "server-a",
              leader: true,
              current: true,
              online: true,
              lag: 0,
            },
          ],
        },
      })
    );
    fetch.mockResponseOnce(
      JSON.stringify({
        data: {
          summary: [
            {
              server: "js-0",
              cluster: "default",
              streams: 8,
              consumers: 4,
              messages: 300000,
              bytes: 1048576,
              apiRequests: 1000,
              apiErrors: 1,
              apiErrorRate: 0.001,
              metaLeader: true,
            },
          ],
          raftMetaGroup: [
            {
              name: "js-0",
              id: "server-a",
              leader: true,
              current: true,
              online: true,
              lag: 0,
            },
          ],
        },
      })
    );

    render(
      <ISBUpdate
        initialYaml={{
          metadata: { name: "test-isb" },
          spec: { jetstream: {} },
        }}
        namespaceId="test-namespace"
        isbId="test-isb"
        viewType={0}
        onUpdateComplete={jest.fn()}
        setModalOnClose={jest.fn()}
      />
    );

    await waitFor(() => {
      expect(screen.getByText("Spec")).toBeInTheDocument();
      expect(screen.getByText("JetStream")).toBeInTheDocument();
      expect(screen.getByText("Refresh")).toBeInTheDocument();
      expect(screen.queryByText("KV Stores")).not.toBeInTheDocument();
    });

    act(() => {
      fireEvent.click(screen.getByText("JetStream"));
    });

    await waitFor(() => {
      expect(screen.getByText("JetStream Summary")).toBeInTheDocument();
      expect(screen.getByText("RAFT Meta Group Information")).toBeInTheDocument();
      expect(screen.getAllByText("js-0").length).toBeGreaterThan(0);
      expect(screen.getByText("js-1")).toBeInTheDocument();
      expect(screen.getByText("400,006")).toBeInTheDocument();
      expect(screen.getByText("100,576")).toBeInTheDocument();
      expect(screen.getByText("12 / 0.018%")).toBeInTheDocument();
      expect(screen.getByText("12 / 0.012%")).toBeInTheDocument();
      expect(screen.queryByText("Memory")).not.toBeInTheDocument();
      expect(screen.queryByText("File")).not.toBeInTheDocument();
      expect(screen.queryByText("Stream Information")).not.toBeInTheDocument();
      expect(screen.queryByText("Consumer Information")).not.toBeInTheDocument();
    });

    act(() => {
      fireEvent.click(screen.getByText("Refresh"));
    });

    await waitFor(() => {
      expect(fetch).toHaveBeenCalledTimes(2);
      expect(screen.getAllByText("300,000")).toHaveLength(2);
      expect(screen.getAllByText("1 / 0.100%")).toHaveLength(2);
    });
  });
});
