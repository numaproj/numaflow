import React from "react";
import { render, screen, fireEvent, within } from "@testing-library/react";
import "@testing-library/jest-dom";
import { PipelineCard } from "./index";
import { AppContext } from "../../../../../App";
import { BrowserRouter } from "react-router-dom";

const mockSetSidebarProps = jest.fn();

const mockNamespace = "test-namespace";
const mockData = {
  name: "simple-pipeline",
  status: "inactive",
  pipeline: {
    kind: "Pipeline",
    apiVersion: "numaflow.numaproj.io/v1alpha1",
    metadata: {
      name: "simple-pipeline",
      namespace: "numaflow-system",
      uid: "87775ef4-fd4b-497e-b40d-7ba47b821a92",
      resourceVersion: "30209",
      generation: 13,
      creationTimestamp: "2023-10-12T14:36:00Z",
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
          time: "2023-10-12T14:36:00Z",
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
          time: "2023-10-12T16:38:25Z",
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
          time: "2023-10-13T14:36:41Z",
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
        desiredPhase: "Paused",
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
          lastTransitionTime: "2023-10-13T14:36:41Z",
          reason: "Successful",
          message: "Successful",
        },
        {
          type: "Deployed",
          status: "True",
          lastTransitionTime: "2023-10-13T14:36:41Z",
          reason: "Successful",
          message: "Successful",
        },
      ],
      phase: "Paused",
      lastUpdated: "2023-10-13T14:36:41Z",
      vertexCount: 3,
      sourceCount: 1,
      sinkCount: 1,
      udfCount: 1,
    },
  },
};
const mockStatusData = {
  name: "simple-pipeline",
  status: "inactive",
  pipeline: {
    kind: "Pipeline",
    apiVersion: "numaflow.numaproj.io/v1alpha1",
    metadata: {
      name: "simple-pipeline",
      namespace: "numaflow-system",
      uid: "87775ef4-fd4b-497e-b40d-7ba47b821a92",
      resourceVersion: "30209",
      generation: 13,
      creationTimestamp: "2023-10-12T14:36:00Z",
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
          time: "2023-10-12T14:36:00Z",
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
          time: "2023-10-12T16:38:25Z",
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
          time: "2023-10-13T14:36:41Z",
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
        desiredPhase: "Paused",
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
          lastTransitionTime: "2023-10-13T14:36:41Z",
          reason: "Successful",
          message: "Successful",
        },
        {
          type: "Deployed",
          status: "True",
          lastTransitionTime: "2023-10-13T14:36:41Z",
          reason: "Successful",
          message: "Successful",
        },
      ],
      phase: "Paused",
      lastUpdated: "2023-10-13T14:36:41Z",
      vertexCount: 3,
      sourceCount: 1,
      sinkCount: 1,
      udfCount: 1,
    },
  },
};
const mockISBData = {
  name: "default",
  status: "healthy",
  isbService: {
    kind: "InterStepBufferService",
    apiVersion: "numaflow.numaproj.io/v1alpha1",
    metadata: {
      name: "default",
      namespace: "numaflow-system",
      uid: "56a52b93-b9bf-481e-8556-2bad974805a4",
      resourceVersion: "30210",
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
          time: "2023-10-13T14:36:41Z",
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
          lastTransitionTime: "2023-10-13T14:36:41Z",
          reason: "Successful",
          message: "Successful",
        },
        {
          type: "Deployed",
          status: "True",
          lastTransitionTime: "2023-10-13T14:36:41Z",
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

describe("PipelineCard", () => {
  beforeEach(() => {
    jest.clearAllMocks();
  });

  it("renders correctly", async () => {
    render(
      <AppContext.Provider
        value={{
          setSidebarProps: mockSetSidebarProps,
          systemInfo: {
            managedNamespace: "numaflow-system",
            namespaced: false,
          },
          systemInfoError: {},
        }}
      >
        <BrowserRouter>
          <PipelineCard
            namespace={mockNamespace}
            data={mockData}
            statusData={mockStatusData}
            isbData={mockISBData}
          />
        </BrowserRouter>
      </AppContext.Provider>
    );

    expect(await screen.findByText("simple-pipeline")).toBeInTheDocument();
    expect(await screen.findByText("Paused")).toBeInTheDocument();
    expect(await screen.findByText("3")).toBeInTheDocument();
  });

  it("renders correctly when namespace is not managed", async () => {
    render(
      <AppContext.Provider
        value={{
          setSidebarProps: mockSetSidebarProps,
          systemInfo: {
            managedNamespace: "numaflow-system",
            namespaced: false,
          },
          systemInfoError: {},
        }}
      >
        <BrowserRouter>
          <PipelineCard
            namespace={mockNamespace}
            data={mockData}
            statusData={mockStatusData}
            isbData={mockISBData}
          />
        </BrowserRouter>
      </AppContext.Provider>
    );

    expect(await screen.findByText("simple-pipeline")).toBeInTheDocument();
    expect(await screen.findByText("Paused")).toBeInTheDocument();
    expect(await screen.findByText("3")).toBeInTheDocument();
  });

  it("Tests handleEditChange", async () => {
    const { findByTestId } = render(
      <AppContext.Provider
        value={{
          setSidebarProps: mockSetSidebarProps,
          systemInfo: {
            managedNamespace: "numaflow-system",
            namespaced: false,
          },
          systemInfoError: {},
        }}
      >
        <BrowserRouter>
          <PipelineCard
            namespace={mockNamespace}
            data={mockData}
            statusData={mockStatusData}
            isbData={mockISBData}
          />
        </BrowserRouter>
      </AppContext.Provider>
    );

    const wrapperNode = await findByTestId("pipeline-card-edit-select");
    const button = within(wrapperNode).getByRole("combobox");

    fireEvent.mouseDown(button);
    expect(await screen.findByText("Pipeline")).toBeInTheDocument();
    fireEvent.click((await screen.findAllByText("Pipeline"))[0]);

    expect(await screen.findByText("simple-pipeline")).toBeInTheDocument();
  });
  it("Tests handleEditChange for isb", async () => {
    const { findByTestId } = render(
      <AppContext.Provider
        value={{
          setSidebarProps: mockSetSidebarProps,
          systemInfo: {
            managedNamespace: "numaflow-system",
            namespaced: false,
          },
          systemInfoError: {},
        }}
      >
        <BrowserRouter>
          <PipelineCard
            namespace={mockNamespace}
            data={mockData}
            statusData={mockStatusData}
            isbData={mockISBData}
          />
        </BrowserRouter>
      </AppContext.Provider>
    );

    const wrapperNode = await findByTestId("pipeline-card-edit-select");
    const button = within(wrapperNode).getByRole("combobox");

    fireEvent.mouseDown(button);
    expect(await screen.findByText("ISB Service")).toBeInTheDocument();
    fireEvent.click((await screen.findAllByText("ISB Service"))[0]);

    expect(await screen.findByText("simple-pipeline")).toBeInTheDocument();
  });

  it("Tests if the component renders correctly when isbData is null", async () => {
    render(
      <AppContext.Provider
        value={{
          setSidebarProps: mockSetSidebarProps,
          systemInfo: {
            managedNamespace: "numaflow-system",
            namespaced: false,
          },
          systemInfoError: {},
        }}
      >
        <BrowserRouter>
          <PipelineCard
            namespace={mockNamespace}
            data={mockData}
            statusData={mockStatusData}
            isbData={null}
          />
        </BrowserRouter>
      </AppContext.Provider>
    );

    expect(await screen.findByText("simple-pipeline")).toBeInTheDocument();
    expect(await screen.findByText("Paused")).toBeInTheDocument();
  });
});
