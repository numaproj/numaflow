import React from "react";
import { render, screen } from "@testing-library/react";
import "@testing-library/jest-dom";
import { AppContext } from "../../../../../App";
import { BrowserRouter } from "react-router-dom";
import { PipelineISBSummaryStatus } from "./index";
import userEvent from "@testing-library/user-event";
import { SidebarType } from "../../../../common/SlidingSidebar";

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
            "consumer:\n  ackwait: 60s\n  maxackpending: 25000\notbucket:\n  history: 1\n  maxbytes: 0\n  maxvaluesize: 0\n  replicas: 3\n  storage: 0\n  ttl: 3h\nprocbucket:\n  history: 1\n  maxbytes: 0\n  maxvaluesize: 0\n  replicas: 3\n  storage: 0\n  ttl: 72h\nstream:\n  duplicates: 60s\n  maxage: 72h\n  maxbytes: -1\n  maxmsgs: 100000\n  replicas: 3\n  retention: 0\n  storage: 0\n",
        },
      },
      type: "jetstream",
    },
  },
};

describe("PipelineISBSummaryStatus", () => {
  const mockSetSidebarProps = jest.fn();

  beforeEach(() => {
    jest.clearAllMocks();
  });

  it("should render the ISB summary status", () => {
    render(
      <AppContext.Provider value={{ setSidebarProps: mockSetSidebarProps }}>
        <BrowserRouter>
          <PipelineISBSummaryStatus
            isbData={mockData}
            namespaceId="numaflow-system"
          />
        </BrowserRouter>
      </AppContext.Provider>
    );
    expect(screen.getByText("ISB SERVICES SUMMARY")).toBeInTheDocument();
    expect(screen.getByText("default")).toBeInTheDocument();
    expect(screen.getByText("3")).toBeInTheDocument();
    expect(screen.getByText("jetstream")).toBeInTheDocument();
    expect(screen.getByText("View More")).toBeInTheDocument();
  });

  it("should render the ISB summary status with unknown values", () => {
    render(
      <AppContext.Provider value={{ setSidebarProps: mockSetSidebarProps }}>
        <BrowserRouter>
          <PipelineISBSummaryStatus isbData={null} />
        </BrowserRouter>
      </AppContext.Provider>
    );
    expect(screen.getByText("ISB SERVICES SUMMARY")).toBeInTheDocument();
    expect(screen.queryByText("View More")).not.toBeInTheDocument();
  });

  it("should open ISB details sidebar when View More is clicked", async () => {
    render(
      <AppContext.Provider value={{ setSidebarProps: mockSetSidebarProps }}>
        <BrowserRouter>
          <PipelineISBSummaryStatus
            isbData={mockData}
            namespaceId="numaflow-system"
          />
        </BrowserRouter>
      </AppContext.Provider>
    );

    await userEvent.click(screen.getByTestId("pipeline-isb-view-more"));

    expect(mockSetSidebarProps).toHaveBeenCalledWith(
      expect.objectContaining({
        type: SidebarType.ISB_UPDATE,
        specEditorProps: expect.objectContaining({
          titleOverride: "ISB Service Details: default",
          initialYaml: mockData.isbService,
          namespaceId: "numaflow-system",
          isbId: "default",
          viewType: 0,
        }),
      })
    );
  });
});
