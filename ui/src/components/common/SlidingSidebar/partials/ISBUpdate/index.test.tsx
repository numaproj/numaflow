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
import { MemoryRouter, Route, Switch } from "react-router-dom";

import "@testing-library/jest-dom";

const renderISBUpdate = (component: React.ReactElement) =>
  render(
    <MemoryRouter initialEntries={["/isb-services"]}>{component}</MemoryRouter>
  );

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
    renderISBUpdate(
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
    renderISBUpdate(
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
    renderISBUpdate(
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
    renderISBUpdate(
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
    renderISBUpdate(
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
            {
              server: "js-2",
              cluster: "default",
              streams: 0,
              consumers: 0,
              messages: 0,
              bytes: 0,
              apiRequests: 149687,
              apiErrors: 4,
              apiErrorRate: 0.0000267224,
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
              active: "450.793757ms",
              lag: 0,
            },
            {
              name: "js-1",
              id: "server-b",
              leader: false,
              current: true,
              online: true,
              active: "1m30s",
              lag: 2,
            },
            {
              name: "js-2",
              id: "server-c",
              leader: false,
              current: true,
              online: true,
              active: "1h2m3s",
              lag: 3,
            },
            {
              name: "js-3",
              id: "server-d",
              leader: false,
              current: true,
              online: true,
              active: "1.005s",
              lag: 4,
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
              active: "450.736306ms",
              lag: 0,
            },
          ],
        },
      })
    );

    renderISBUpdate(
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

    await waitFor(() => {
      expect(screen.getByText("JetStream Summary")).toBeInTheDocument();
      expect(screen.getByText("RAFT Meta Group Information")).toBeInTheDocument();
      expect(screen.getAllByText("js-0").length).toBeGreaterThan(0);
      expect(screen.getAllByText("js-1").length).toBeGreaterThan(0);
      expect(screen.getByText("400,006")).toBeInTheDocument();
      expect(screen.getByText("250,263")).toBeInTheDocument();
      expect(screen.getByText("12 / 0.018%")).toBeInTheDocument();
      expect(screen.getByText("4 / 0.003%")).toBeInTheDocument();
      expect(screen.getByText("16 / 0.006%")).toBeInTheDocument();
      expect(screen.getByText("Last Active")).toBeInTheDocument();
      expect(screen.getByText("450.79ms")).toBeInTheDocument();
      expect(screen.getByText("1m 30s")).toBeInTheDocument();
      expect(screen.getByText("1h 2m 3s")).toBeInTheDocument();
      expect(screen.getByText("1.01s")).toBeInTheDocument();
      expect(screen.queryByText("ID")).not.toBeInTheDocument();
      expect(screen.queryByText("server-a")).not.toBeInTheDocument();
      expect(
        screen.getByTestId("isb-debug-header-help-api-errors")
      ).toBeInTheDocument();
      expect(
        screen.getByTestId("isb-debug-header-help-current")
      ).toBeInTheDocument();
      expect(screen.getByTestId("isb-debug-header-help-lag")).toBeInTheDocument();
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
      expect(screen.getAllByText("1 / 0.10%")).toHaveLength(2);
    });
  });

  it("redirects to login on JetStream debug 401", async () => {
    fetch.mockResponseOnce("", { status: 401 });

    render(
      <MemoryRouter initialEntries={["/isb-services"]}>
        <Switch>
          <Route path="/login">
            <div>Login Page</div>
          </Route>
          <Route path="/isb-services">
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
          </Route>
        </Switch>
      </MemoryRouter>
    );

    await waitFor(() => {
      expect(screen.getByText("Login Page")).toBeInTheDocument();
      expect(
        screen.queryByText("Error loading JetStream information: Response code: 401")
      ).not.toBeInTheDocument();
    });
  });
});
