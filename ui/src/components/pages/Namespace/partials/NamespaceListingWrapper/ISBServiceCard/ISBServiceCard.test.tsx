// Tests the ISBServiceCard component

import React, { act } from "react";
import { render, screen, waitFor } from "@testing-library/react";
import "@testing-library/jest-dom";
import userEvent from "@testing-library/user-event";
import { ISBServiceCard } from "./ISBServiceCard";
import { AppContext } from "../../../../../../App";
import { BrowserRouter } from "react-router-dom";

const mockSetSidebarProps = jest.fn();

const mockNamespace = "numaflow-system";

const mockData = {
  name: "default",
  status: "healthy",
  isbService: {
    kind: "InterStepBufferService",
    apiVersion: "numaflow.numaproj.io/v1alpha1",
    metadata: {
      name: "default",
      namespace: "numaflow-system",
      uid: "56a52b93-b9bf-481e-8556-2bad974805a4",
      resourceVersion: "331851",
      generation: 1,
      creationTimestamp: "2023-10-12T14:35:53Z",
      annotations: {
        "kubectl.kubernetes.io/last-applied-configuration":
          '{"apiVersion":"numaflow.numaproj.io/v1alpha1","kind":"InterStepBufferService","metadata":{"annotations":{},"name":"default","namespace":"numaflow-system"},"spec":{"jetstream":{"persistence":{"volumeSize":"3Gi"},"version":"latest"}}}\n',
      },
      finalizers: ["isbsvc-controller"],
      managedFields: [
        {
          manager: "kubectl-client-side-apply",
          operation: "Update",
          apiVersion: "numaflow.numaproj.io/v1alpha1",
          time: "2023-10-12T14:35:53Z",
          fieldsType: "FieldsV1",
          fieldsV1: {
            "f:metadata": {
              "f:annotations": {
                ".": {},
                "f:kubectl.kubernetes.io/last-applied-configuration": {},
              },
            },
            "f:spec": {
              ".": {},
              "f:jetstream": {
                ".": {},
                "f:persistence": {
                  ".": {},
                  "f:volumeSize": {},
                },
                "f:replicas": {},
                "f:version": {},
              },
            },
          },
        },
        {
          manager: "numaflow",
          operation: "Update",
          apiVersion: "numaflow.numaproj.io/v1alpha1",
          time: "2023-10-12T14:35:54Z",
          fieldsType: "FieldsV1",
          fieldsV1: {
            "f:metadata": {
              "f:finalizers": {
                ".": {},
                'v:"isbsvc-controller"': {},
              },
            },
          },
        },
        {
          manager: "numaflow",
          operation: "Update",
          apiVersion: "numaflow.numaproj.io/v1alpha1",
          time: "2023-10-30T13:32:18Z",
          fieldsType: "FieldsV1",
          fieldsV1: {
            "f:status": {
              ".": {},
              "f:conditions": {},
              "f:config": {
                ".": {},
                "f:jetstream": {
                  ".": {},
                  "f:auth": {
                    ".": {},
                    "f:basic": {
                      ".": {},
                      "f:password": {
                        ".": {},
                        "f:key": {},
                        "f:name": {},
                      },
                      "f:user": {
                        ".": {},
                        "f:key": {},
                        "f:name": {},
                      },
                    },
                  },
                  "f:streamConfig": {},
                  "f:url": {},
                },
              },
              "f:phase": {},
              "f:type": {},
            },
          },
          subresource: "status",
        },
      ],
    },
    spec: {
      jetstream: {
        version: "latest",
        replicas: 3,
        persistence: {
          volumeSize: "3Gi",
        },
      },
    },
    status: {
      conditions: [
        {
          type: "Configured",
          status: "True",
          lastTransitionTime: "2023-10-30T13:32:18Z",
          reason: "Successful",
          message: "Successful",
        },
        {
          type: "Deployed",
          status: "True",
          lastTransitionTime: "2023-10-30T13:32:18Z",
          reason: "Successful",
          message: "Successful",
        },
      ],
      phase: "Running",
      config: {
        jetstream: {
          url: "nats://isbsvc-default-js-svc.numaflow-system.svc:4222",
          auth: {
            basic: {
              user: {
                name: "isbsvc-default-js-client-auth",
                key: "client-auth-user",
              },
              password: {
                name: "isbsvc-default-js-client-auth",
                key: "client-auth-password",
              },
            },
          },
          streamConfig:
            "consumer:\n  ackwait: 60s\n  maxackpending: 25000\notbucket:\n  history: 1\n  maxbytes: 0\n  maxvaluesize: 0\n  replicas: 3\n  storage: 0\n  ttl: 3h\nprocbucket:\n  history: 1\n  maxbytes: 0\n  maxvaluesize: 0\n  replicas: 3\n  storage: 0\n  ttl: 72h\nstream:\n  duplicates: 60s\n  maxage: 72h\n  maxbytes: -1\n  maxmsgs: 100000\n  replicas: 3\n  retention: 0\n  storage: 0\n",
        },
      },
      type: "jetstream",
    },
  },
};

//Mock the DeleteModal component
jest.mock("../../DeleteModal", () => {
  return {
    DeleteModal: ({ _, onDeleteCompleted, onCancel }) => {
      return (
        <div data-testid="mock-delete-confirmation">
          <button onClick={onDeleteCompleted}>Mock Delete Confirmation</button>
          <button onClick={onCancel}>Mock Cancel</button>
        </div>
      );
    },
  };
});

const mockRefresh = jest.fn();

describe("ISBServiceCard", () => {
  beforeEach(() => {
    jest.clearAllMocks();
  });

  it("should render ISBServiceCard with correct data", async () => {
    render(
      <BrowserRouter>
        <AppContext.Provider
          value={{
            setSidebarProps: mockSetSidebarProps,
          }}
        >
          <ISBServiceCard
            namespace={mockNamespace}
            data={mockData}
            refresh={mockRefresh}
          />
        </AppContext.Provider>
      </BrowserRouter>
    );
    expect(screen.getByText("jetstream")).toBeInTheDocument();
    expect(screen.getByText("3")).toBeInTheDocument();
  });

  it("Tests if Delete exists and is clickable", async () => {
    render(
      <BrowserRouter>
        <AppContext.Provider
          value={{
            setSidebarProps: mockSetSidebarProps,
          }}
        >
          <ISBServiceCard
            namespace={mockNamespace}
            data={mockData}
            refresh={mockRefresh}
          />
        </AppContext.Provider>
      </BrowserRouter>
    );

    const deleteButton = screen.getByTestId("delete-isb");
    expect(deleteButton).toBeInTheDocument();
    await userEvent.click(deleteButton);
    expect(screen.getByTestId("mock-delete-confirmation")).toBeInTheDocument();
    await userEvent.click(screen.getByText("Mock Delete Confirmation"));
    expect(mockRefresh).toHaveBeenCalled();
  });
  it("Tests if Edit exists and is clickable", async () => {
    render(
      <BrowserRouter>
        <AppContext.Provider
          value={{
            setSidebarProps: mockSetSidebarProps,
          }}
        >
          <ISBServiceCard
            namespace={mockNamespace}
            data={mockData}
            refresh={mockRefresh}
          />
        </AppContext.Provider>
      </BrowserRouter>
    );

    const editButton = screen.getByTestId("edit-isb");
    expect(editButton).toBeInTheDocument();
    userEvent.click(editButton);
  });

  it("Tests clicking cancel on the delete confirmation", async () => {
    render(
      <BrowserRouter>
        <AppContext.Provider
          value={{
            setSidebarProps: mockSetSidebarProps,
          }}
        >
          <ISBServiceCard
            namespace={mockNamespace}
            data={mockData}
            refresh={mockRefresh}
          />
        </AppContext.Provider>
      </BrowserRouter>
    );

    const deleteButton = screen.getByTestId("delete-isb");
    expect(deleteButton).toBeInTheDocument();
    await act(() => {
      userEvent.click(deleteButton);
    });
    await waitFor(() => {
      expect(screen.getByText("Mock Cancel")).toBeInTheDocument();
    });
    await act(() => {
      userEvent.click(screen.getByText("Mock Cancel"));
    });
  });
});
