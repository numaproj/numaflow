import React from "react";
import {
  fireEvent,
  render,
  screen,
  waitFor,
  act,
} from "@testing-library/react";
import { K8sEvents } from "./index";
import fetch from "jest-fetch-mock";
import { BrowserRouter } from "react-router-dom";

import "@testing-library/jest-dom";

const MOCK_DATA = {
  data: [
    {
      timestamp: 1698427317000,
      type: "Warning",
      object: "Pod/pipe-with-generators-even-or-odd-0-yczsz",
      reason: "Unhealthy",
      message:
        'Readiness probe failed: Get "https://10.244.0.148:2469/readyz": net/http: request canceled (Client.Timeout exceeded while awaiting headers)',
    },
    {
      timestamp: 1698427317000,
      type: "Warning",
      object: "Pod/pipe-with-generators-even-or-odd-0-yczsz",
      reason: "Unhealthy",
      message:
        'Readiness probe failed: Get "https://10.244.0.148:2469/readyz": net/http: request canceled (Client.Timeout exceeded while awaiting headers)',
    },
    {
      timestamp: 1698427317000,
      type: "Warning",
      object: "Pod/pipe-with-generators-even-or-odd-0-yczsz",
      reason: "Unhealthy",
      message:
        'Readiness probe failed: Get "https://10.244.0.148:2469/readyz": net/http: request canceled (Client.Timeout exceeded while awaiting headers)',
    },
    {
      timestamp: 1698427317000,
      type: "Warning",
      object: "Pod/pipe-with-generators-even-or-odd-0-yczsz",
      reason: "Unhealthy",
      message:
        'Readiness probe failed: Get "https://10.244.0.148:2469/readyz": net/http: request canceled (Client.Timeout exceeded while awaiting headers)',
    },
    {
      timestamp: 1698427317000,
      type: "Warning",
      object: "Pod/pipe-with-generators-even-or-odd-0-yczsz",
      reason: "Unhealthy",
      message:
        'Readiness probe failed: Get "https://10.244.0.148:2469/readyz": net/http: request canceled (Client.Timeout exceeded while awaiting headers)',
    },
    {
      timestamp: 1698427317000,
      type: "Warning",
      object: "Pod/pipe-with-generators-even-or-odd-0-yczsz",
      reason: "Unhealthy",
      message:
        'Readiness probe failed: Get "https://10.244.0.148:2469/readyz": net/http: request canceled (Client.Timeout exceeded while awaiting headers)',
    },
    {
      timestamp: 1698427317000,
      type: "Warning",
      object: "Pod/pipe-with-generators-even-or-odd-0-yczsz",
      reason: "Unhealthy",
      message:
        'Readiness probe failed: Get "https://10.244.0.148:2469/readyz": net/http: request canceled (Client.Timeout exceeded while awaiting headers)',
    },
  ],
};

describe("K8sEvents", () => {
  beforeEach(() => {
    jest.clearAllMocks();
    fetch.resetMocks();
  });

  it("no events", async () => {
    fetch.mockResponseOnce(JSON.stringify({ data: [] }));
    render(<K8sEvents namespaceId="test-namespace" />, {
      wrapper: BrowserRouter,
    });
    await waitFor(() => {
      expect(screen.getByText("No events found")).toBeInTheDocument();
    });
  });

  it("w/ events", async () => {
    fetch.mockResponseOnce(JSON.stringify(MOCK_DATA));
    render(<K8sEvents namespaceId="test-namespace" />, {
      wrapper: BrowserRouter,
    });
    await waitFor(() => {
      expect(screen.queryAllByText("Unhealthy").length).toEqual(6);
    });
  });

  it("w/ events filtered", async () => {
    fetch.mockResponseOnce(JSON.stringify(MOCK_DATA));
    render(<K8sEvents namespaceId="test-namespace" />, {
      wrapper: BrowserRouter,
    });
    await waitFor(() => {
      expect(screen.queryAllByText("Unhealthy").length).toEqual(6);
    });
    act(() => {
      const normalFilter = screen.getByTestId("normal-filter");
      fireEvent.click(normalFilter);
    });
    await waitFor(() => {
      expect(screen.getByText("No events found")).toBeInTheDocument();
    });
  });

  it("w/ events page change", async () => {
    fetch.mockResponseOnce(JSON.stringify(MOCK_DATA));
    render(<K8sEvents namespaceId="test-namespace" />, {
      wrapper: BrowserRouter,
    });
    await waitFor(() => {
      expect(screen.queryAllByText("Unhealthy").length).toEqual(6);
    });
    act(() => {
      const nextPageButton = screen.getByRole("button", {
        name: "Go to next page",
      });
      fireEvent.click(nextPageButton);
    });
    await waitFor(() => {
      expect(screen.queryAllByText("Unhealthy").length).toEqual(1);
    });
  });

  // it("w/ error", async () => {
  //   fetch.mockResponseOnce(JSON.stringify({ errMsg: "failed" }));
  //   render(<K8sEvents excludeHeader namespaceId="test-namespace" />, {
  //     wrapper: BrowserRouter,
  //   });
  //   await waitFor(() => {
  //     expect(
  //       screen.getByText("Error loading events: failed")
  //     ).toBeInTheDocument();
  //   });
  // });
});
