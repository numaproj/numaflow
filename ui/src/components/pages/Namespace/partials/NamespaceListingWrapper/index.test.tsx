import React, { act } from "react";
import { fireEvent, render, screen } from "@testing-library/react";
import "@testing-library/jest-dom";
import { BrowserRouter } from "react-router-dom";

import { NamespaceListingWrapper } from "./index";
import { AppContext } from "../../../../../App";

const mockRefresh = jest.fn();
const mockData = {
  pipelinesCount: 4,
  pipelinesActiveCount: 3,
  pipelinesInactiveCount: 1,
  pipelinesHealthyCount: 3,
  pipelinesWarningCount: 0,
  pipelinesCriticalCount: 0,
  isbsCount: 5,
  isbsActiveCount: 5,
  isbsInactiveCount: 0,
  isbsHealthyCount: 5,
  isbsWarningCount: 0,
  isbsCriticalCount: 0,
  pipelineSummaries: [
    {
      name: "simple-pipeline",
      status: "healthy",
    },
    {
      name: "simple-pipeline-1",
      status: "warning",
    },
    {
      name: "simple-pipeline-2",
      status: "critical",
    },
  ],
};
const mockPipelineRawData = {
  "simple-pipeline": {
    name: "simple-pipeline",
    status: "healthy",
    pipeline: {
      kind: "Pipeline",
      apiVersion: "numaflow.numaproj.io/v1alpha1",
      metadata: {
        name: "simple-pipeline",
        namespace: "numaflow-system",
        uid: "c0513165-3bf3-44a6-b0a0-7b9cf9ae4de5",
        resourceVersion: "673823",
        generation: 2,
        creationTimestamp: "2023-11-02T14:43:32Z",
      },
      status: {
        conditions: [
          {
            type: "Configured",
            status: "True",
            lastTransitionTime: "2023-11-08T16:02:04Z",
            reason: "Successful",
            message: "Successful",
          },
          {
            type: "Deployed",
            status: "True",
            lastTransitionTime: "2023-11-08T16:02:04Z",
            reason: "Successful",
            message: "Successful",
          },
        ],
        phase: "Running",
        lastUpdated: "2023-11-08T16:02:04Z",
        vertexCount: 3,
        sourceCount: 1,
        sinkCount: 1,
        udfCount: 1,
      },
    },
  },
  "simple-pipeline-1": {
    name: "simple-pipeline-1",
    status: "warning",
    pipeline: {
      kind: "Pipeline",
      apiVersion: "numaflow.numaproj.io/v1alpha1",
      metadata: {
        name: "simple-pipeline-1",
        namespace: "numaflow-system",
        uid: "c0513165-3bf3-44a6-b0a0-7b9cf9ae4de5",
        resourceVersion: "673823",
        generation: 2,
        creationTimestamp: "2023-11-02T14:43:32Z",
      },
      status: {
        conditions: [
          {
            type: "Configured",
            status: "True",
            lastTransitionTime: "2023-11-08T16:02:04Z",
            reason: "Successful",
            message: "Successful",
          },
          {
            type: "Deployed",
            status: "True",
            lastTransitionTime: "2023-11-08T16:02:04Z",
            reason: "Successful",
            message: "Successful",
          },
        ],
        phase: "Paused",
        lastUpdated: "2023-11-08T16:02:04Z",
        vertexCount: 3,
        sourceCount: 1,
        sinkCount: 1,
        udfCount: 1,
      },
    },
  },
  "simple-pipeline-2": {
    name: "simple-pipeline-2",
    status: "critical",
    pipeline: {
      kind: "Pipeline",
      apiVersion: "numaflow.numaproj.io/v1alpha1",
      metadata: {
        name: "simple-pipeline-1",
        namespace: "numaflow-system",
        uid: "c0513165-3bf3-44a6-b0a0-7b9cf9ae4de5",
        resourceVersion: "673823",
        generation: 2,
        creationTimestamp: "2023-11-02T14:43:32Z",
      },
      status: {
        conditions: [
          {
            type: "Configured",
            status: "True",
            lastTransitionTime: "2023-11-08T16:02:04Z",
            reason: "Successful",
            message: "Successful",
          },
          {
            type: "Deployed",
            status: "True",
            lastTransitionTime: "2023-11-08T16:02:04Z",
            reason: "Successful",
            message: "Successful",
          },
        ],
        phase: "Stopped",
        lastUpdated: "2023-11-08T16:02:04Z",
        vertexCount: 3,
        sourceCount: 1,
        sinkCount: 1,
        udfCount: 1,
      },
    },
  },
};

const mockISBRawData = {
  default: {
    name: "default",
    status: "healthy",
    isbService: {
      kind: "InterStepBufferService",
      apiVersion: "numaflow.numaproj.io/v1alpha1",
      metadata: {
        name: "default",
        namespace: "numaflow-system",
        uid: "cc3f20f7-4d36-4e9c-8804-155248da7948",
        resourceVersion: "673825",
        generation: 1,
        creationTimestamp: "2023-11-02T14:43:25Z",
        annotations: {
          "kubectl.kubernetes.io/last-applied-configuration":
            '{"apiVersion":"numaflow.numaproj.io/v1alpha1","kind":"InterStepBufferService","metadata":{"annotations":{},"name":"default","namespace":"numaflow-system"},"spec":{"jetstream":{"persistence":{"volumeSize":"3Gi"},"version":"latest"}}}\n',
        },
        finalizers: ["isbsvc-controller"],
      },
      status: {
        conditions: [
          {
            type: "Configured",
            status: "True",
            lastTransitionTime: "2023-11-08T16:02:04Z",
            reason: "Successful",
            message: "Successful",
          },
          {
            type: "Deployed",
            status: "True",
            lastTransitionTime: "2023-11-08T16:02:04Z",
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
  },
};

describe("NamespaceListingWrapper", () => {
  it("renders without crashing", async () => {
    await act(async () => {
      render(
        <BrowserRouter>
          <NamespaceListingWrapper
            namespace="numaflow-system"
            data={mockData}
            refresh={mockRefresh}
            isbData={mockISBRawData}
            pipelineData={mockPipelineRawData}
          />
        </BrowserRouter>
      );
    });
    expect(
      screen.getAllByTestId("namespace-pipeline-listing")[0]
    ).toBeInTheDocument();
  });

  it("Searches for a pipeline", async () => {
    await act(async () => {
      render(
        <AppContext.Provider value="">
          <BrowserRouter>
            <NamespaceListingWrapper
              namespace="numaflow-system"
              data={mockData}
              refresh={mockRefresh}
              isbData={mockISBRawData}
              pipelineData={mockPipelineRawData}
            />
          </BrowserRouter>
        </AppContext.Provider>
      );
    });
    const searchBar = screen
      .getAllByTestId("namespace-pipeline-listing")[0]
      .querySelector("input");
    searchBar &&
      fireEvent.change(searchBar, { target: { value: "simple-pipeline-1" } });
    expect(
      screen
        .getAllByTestId("namespace-pipeline-listing")[0]
        .querySelector("input")?.value
    ).toBe("simple-pipeline-1");
    expect(screen.getByText("simple-pipeline-1")).toBeInTheDocument();
    searchBar && fireEvent.change(searchBar, { target: { value: "zzz" } });
    expect(
      screen
        .getAllByTestId("namespace-pipeline-listing")[0]
        .querySelector("input")?.value
    ).toBe("zzz");
  });

  it("Tests handleTabChange", async () => {
    await act(async () => {
      render(
        <AppContext.Provider value="">
          <BrowserRouter>
            <NamespaceListingWrapper
              namespace="numaflow-system"
              data={mockData}
              refresh={mockRefresh}
              isbData={mockISBRawData}
              pipelineData={mockPipelineRawData}
            />
          </BrowserRouter>
        </AppContext.Provider>
      );
    });
    const tabs = screen
      .getAllByTestId("namespace-pipeline-listing")[0]
      .querySelectorAll("button");
    tabs.forEach((tab) => {
      fireEvent.click(tab);
    });
  });
});
