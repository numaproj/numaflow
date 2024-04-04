import React from "react";
import { render, screen } from "@testing-library/react";
import "@testing-library/jest-dom";
import { AppContext } from "../../../../../App";
import { BrowserRouter } from "react-router-dom";
import { PipelineISBStatus } from "./index";

const mockData = {
  name: "default",
  status: "healthy",
  isbService: {
    kind: "InterStepBufferService",
    apiVersion: "v1alpha1",
    metadata: {
      name: "default",
      namespace: "numaflow-system",
      uid: "55786992-26ce-47d8-860e-2aece99f8ba8",
      resourceVersion: "368601",
      generation: 1,
      creationTimestamp: "2023-10-31T14:13:25Z",
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
          time: "2023-10-31T14:13:25Z",
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
          time: "2023-10-31T14:13:27Z",
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
          time: "2023-10-31T14:13:27Z",
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
          lastTransitionTime: "2023-10-31T14:13:27Z",
          reason: "Successful",
          message: "Successful",
        },
        {
          type: "Deployed",
          status: "True",
          lastTransitionTime: "2023-10-31T14:13:27Z",
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
            "consumer:\n  ackwait: 60s\n  maxackpending: 25000\notbucket:\n  history: 1\n  maxbytes: 0\n  maxvaluesize: 0\n  replicas: 3\n  storage: 0\n  ttl: 3h\nprocbucket:\n  history: 1\n  maxbytes: 0\n  maxvaluesize: 0\n  replicas: 3\n  storage: 0\n  ttl: 72h\nstream:\n  duplicates: 60s\n  maxage: 72h\n  maxbytes: -1\n  maxmsgs: 100000\n  replicas: 3\n storage: 0\n",
        },
      },
      type: "jetstream",
    },
  },
};

describe("PipelineISBStatus", () => {
  it("should render the component", () => {
    render(
      <AppContext.Provider
        value={{
          isbData: mockData,
        }}
      >
        <BrowserRouter>
          <PipelineISBStatus isbData={mockData} />
        </BrowserRouter>
      </AppContext.Provider>
    );
    expect(screen.getByText("ISB SERVICES STATUS")).toBeInTheDocument();
  });

  it("Should render correctly when isbData is null", () => {
    render(
      <AppContext.Provider
        value={{
          isbData: null,
        }}
      >
        <BrowserRouter>
          <PipelineISBStatus isbData={null} />
        </BrowserRouter>
      </AppContext.Provider>
    );
    expect(screen.getByText("ISB SERVICES STATUS")).toBeInTheDocument();
    expect(screen.getByText("Unknown")).toBeInTheDocument();
    expect(screen.getByText("Unknown")).toBeInTheDocument();
  });
});
