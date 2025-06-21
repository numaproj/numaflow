import React, { act } from "react";
import { render, screen, waitFor } from "@testing-library/react";
import "@testing-library/jest-dom";
import { AppContext } from "../../../../../App";
import { BrowserRouter } from "react-router-dom";
import { PipelineSummaryStatus } from "./index";
import { useParams } from "react-router-dom";

jest.mock("react-router-dom", () => ({
  ...jest.requireActual("react-router-dom"),
  useParams: jest.fn(),
}));

const mockRefresh = jest.fn();
const mockPipeline = {
  kind: "Pipeline",
  apiVersion: "numaflow.numaproj.io/v1alpha1",
  metadata: {
    name: "simple-pipeline",
    namespace: "numaflow-system",
    uid: "9a3fa74f-d51e-4e67-a842-b32f531aa8fd",
    resourceVersion: "381794",
    generation: 54,
    creationTimestamp: "2023-10-31T14:13:34Z",
    annotations: {
      "kubectl.kubernetes.io/last-applied-configuration":
        '{"apiVersion":"numaflow.numaproj.io/v1alpha1","kind":"Pipeline","metadata":{"annotations":{},"name":"simple-pipeline","namespace":"numaflow-system"},"spec":{"edges":[{"from":"in","to":"cat"},{"from":"cat","to":"out"}],"vertices":[{"name":"in","source":{"generator":{"duration":"1s","rpu":5}}},{"name":"cat","udf":{"builtin":{"name":"cat"}}},{"name":"out","sink":{"log":{}}}]}}\n',
    },
    finalizers: ["pipeline-controller"],
    managedFields: [
      {
        manager: "kubectl-client-side-apply",
        operation: "Update",
        apiVersion: "numaflow.numaproj.io/v1alpha1",
        time: "2023-10-31T14:13:34Z",
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
            "f:edges": {},
            "f:lifecycle": {
              ".": {},
              "f:deleteGracePeriodSeconds": {},
              "f:pauseGracePeriodSeconds": {},
            },
            "f:limits": {
              ".": {},
              "f:bufferMaxLength": {},
              "f:bufferUsageLimit": {},
              "f:readBatchSize": {},
              "f:readTimeout": {},
            },
            "f:watermark": {
              ".": {},
              "f:disabled": {},
              "f:maxDelay": {},
            },
          },
        },
      },
      {
        manager: "numaflow",
        operation: "Update",
        apiVersion: "numaflow.numaproj.io/v1alpha1",
        time: "2023-10-31T16:57:10Z",
        fieldsType: "FieldsV1",
        fieldsV1: {
          "f:metadata": {
            "f:finalizers": {
              ".": {},
              'v:"pipeline-controller"': {},
            },
          },
          "f:spec": {
            "f:lifecycle": {
              "f:desiredPhase": {},
            },
            "f:vertices": {},
          },
        },
      },
      {
        manager: "numaflow",
        operation: "Update",
        apiVersion: "numaflow.numaproj.io/v1alpha1",
        time: "2023-10-31T16:57:19Z",
        fieldsType: "FieldsV1",
        fieldsV1: {
          "f:status": {
            ".": {},
            "f:conditions": {},
            "f:lastUpdated": {},
            "f:phase": {},
            "f:sinkCount": {},
            "f:sourceCount": {},
            "f:udfCount": {},
            "f:vertexCount": {},
          },
        },
        subresource: "status",
      },
    ],
  },
  spec: {
    vertices: [
      {
        name: "in",
        source: {
          generator: {
            rpu: 5,
            duration: "1s",
            msgSize: 8,
          },
        },
        scale: {},
      },
      {
        name: "cat",
        udf: {
          container: null,
          builtin: {
            name: "cat",
          },
          groupBy: null,
        },
        scale: {},
      },
      {
        name: "out",
        sink: {
          log: {},
        },
        scale: {},
      },
    ],
    edges: [
      {
        from: "in",
        to: "cat",
        conditions: null,
      },
      {
        from: "cat",
        to: "out",
        conditions: null,
      },
    ],
    lifecycle: {
      deleteGracePeriodSeconds: 30,
      desiredPhase: "Running",
      pauseGracePeriodSeconds: 30,
    },
    limits: {
      readBatchSize: 500,
      bufferMaxLength: 30000,
      bufferUsageLimit: 80,
      readTimeout: "1s",
    },
    watermark: {
      maxDelay: "0s",
    },
  },
  status: {
    conditions: [
      {
        type: "Configured",
        status: "True",
        lastTransitionTime: "2023-10-31T16:57:19Z",
        reason: "Successful",
        message: "Successful",
      },
      {
        type: "Deployed",
        status: "True",
        lastTransitionTime: "2023-10-31T16:57:19Z",
        reason: "Successful",
        message: "Successful",
      },
    ],
    phase: "Running",
    lastUpdated: "2023-10-31T16:57:19Z",
    vertexCount: 3,
    sourceCount: 1,
    sinkCount: 1,
    udfCount: 1,
  },
};

describe("PipelineSummaryStatus", () => {
  it("should render the component", () => {
    (useParams as jest.Mock).mockReturnValue({
      namespaceId: "numaflow-system",
      pipelineId: "simple-pipeline",
    });
    render(
      <AppContext.Provider value="healthy">
        <BrowserRouter>
          <PipelineSummaryStatus
            pipelineId="simple-pipeline"
            pipeline={mockPipeline}
            lag={0}
            refresh={mockRefresh}
          />
        </BrowserRouter>
      </AppContext.Provider>
    );

    expect(screen.getByText("SUMMARY")).toBeInTheDocument();
  });

  it("Should click on the spec button", async () => {
    (useParams as jest.Mock).mockReturnValue({
      namespaceId: "numaflow-system",
      pipelineId: "simple-pipeline",
    });
    render(
      <AppContext.Provider value={{ setSidebarProps: jest.fn() }}>
        <BrowserRouter>
          <PipelineSummaryStatus
            pipelineId="simple-pipeline"
            pipeline={mockPipeline}
            lag={0}
            refresh={mockRefresh}
          />
        </BrowserRouter>
      </AppContext.Provider>
    );

    await waitFor(() => {
      expect(screen.getByTestId("pipeline-spec-click")).toBeInTheDocument();
    });
    await act(async () => {
      screen.getByTestId("pipeline-spec-click").click();
    });
  });

  it("Should click on the spec button but not open", async () => {
    (useParams as jest.Mock).mockReturnValue({
      namespaceId: undefined,
    });
    render(
      <AppContext.Provider value={{ setSidebarProps: jest.fn() }}>
        <BrowserRouter>
          <PipelineSummaryStatus
            pipelineId="simple-pipeline"
            pipeline={mockPipeline}
            lag={0}
            refresh={mockRefresh}
          />
        </BrowserRouter>
      </AppContext.Provider>
    );

    await waitFor(() => {
      expect(screen.getByTestId("pipeline-spec-click")).toBeInTheDocument();
    });
    await act(async () => {
      screen.getByTestId("pipeline-spec-click").click();
    });
  });
});
