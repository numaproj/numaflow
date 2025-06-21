import React, { act } from "react";
import { render, screen } from "@testing-library/react";
import "@testing-library/jest-dom";
import { BrowserRouter } from "react-router-dom";

import { Namespaces } from "./index";
import { useNamespaceSummaryFetch } from "../../../utils/fetchWrappers/namespaceSummaryFetch";

window.ResizeObserver = class ResizeObserver {
  observe() {
    // do nothing
  }
  unobserve() {
    // do nothing
  }
  disconnect() {
    // do nothing
  }
};
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
      name: "simple-pipeline-25",
      status: "inactive",
    },
    {
      name: "simple-pipeline-3",
      status: "healthy",
    },
    {
      name: "simple-pipeline-6",
      status: "healthy",
    },
  ],
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
        resourceVersion: "654518",
        generation: 1,
        creationTimestamp: "2023-11-02T14:43:25Z",
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
            time: "2023-11-02T14:43:25Z",
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
            time: "2023-11-02T14:43:27Z",
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
            time: "2023-11-07T20:15:21Z",
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
            lastTransitionTime: "2023-11-07T20:15:21Z",
            reason: "Successful",
            message: "Successful",
          },
          {
            type: "Deployed",
            status: "True",
            lastTransitionTime: "2023-11-07T20:15:21Z",
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
        resourceVersion: "654509",
        generation: 2,
        creationTimestamp: "2023-11-02T14:43:32Z",
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
            time: "2023-11-02T14:43:32Z",
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
                  "f:desiredPhase": {},
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
            time: "2023-11-02T14:43:32Z",
            fieldsType: "FieldsV1",
            fieldsV1: {
              "f:metadata": {
                "f:finalizers": {
                  ".": {},
                  'v:"pipeline-controller"': {},
                },
              },
              "f:spec": {
                "f:vertices": {},
              },
            },
          },
          {
            manager: "numaflow",
            operation: "Update",
            apiVersion: "numaflow.numaproj.io/v1alpha1",
            time: "2023-11-07T20:15:20Z",
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
            lastTransitionTime: "2023-11-07T20:15:20Z",
            reason: "Successful",
            message: "Successful",
          },
          {
            type: "Deployed",
            status: "True",
            lastTransitionTime: "2023-11-07T20:15:20Z",
            reason: "Successful",
            message: "Successful",
          },
        ],
        phase: "Running",
        lastUpdated: "2023-11-07T20:15:20Z",
        vertexCount: 3,
        sourceCount: 1,
        sinkCount: 1,
        udfCount: 1,
      },
    },
  },
};
const mockRefresh = jest.fn();

//Mocking namespaceSummaryFetch
jest.mock("../../../utils/fetchWrappers/namespaceSummaryFetch");

jest.mock("../../common/SummaryPageLayout", () => ({
  SummaryPageLayout: (props) => {
    const customComponentInstance = props.summarySections.find(
      (section) => section.customComponent
    );
    return (
      <div data-testid="summary-page-layout">
        <button
          data-testid="spl-button"
          onClick={customComponentInstance.customComponent.props.onClick()}
        ></button>
      </div>
    );
  },
  SummarySection: () => <div data-testid="summary-section"></div>,
  SummarySectionType: () => <div data-testid="summary-section-type"></div>,
}));

//Mock useParam
jest.mock("react-router-dom", () => ({
  ...jest.requireActual("react-router-dom"),
  useParams: () => ({
    namespaceId: "default",
  }),
}));

describe("Namespace", () => {
  it("renders the namespace page", async () => {
    useNamespaceSummaryFetch.mockReturnValue({
      useNamespaceSummaryFetch: () => ({
        data: mockData,
        pipelineRawData: mockPipelineRawData,
        isbRawData: mockISBRawData,
        loading: false,
        error: null,
        refresh: mockRefresh,
      }),
    });
    render(
      <BrowserRouter>
        <Namespaces />
      </BrowserRouter>
    );
    await act(() => {
      expect(screen.getByTestId("summary-page-layout")).toBeInTheDocument();
    });
  });

  it("Tests handleK8s method getting called", async () => {
    useNamespaceSummaryFetch.mockReturnValue({
      data: mockData,
      pipelineRawData: mockPipelineRawData,
      isbRawData: mockISBRawData,
      loading: false,
      error: null,
      refresh: mockRefresh,
    });
    render(
      <BrowserRouter>
        <Namespaces />
      </BrowserRouter>
    );
    await act(() => {
      expect(screen.getByTestId("spl-button")).toBeInTheDocument();
      screen.getByTestId("spl-button").click();
    });
  });
});
