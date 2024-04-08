import React from "react";
import { act, render, screen, waitFor } from "@testing-library/react";
import "@testing-library/jest-dom";

import { SpecEditor } from "./index";

const onValidate = jest.fn();
const onSubmit = jest.fn();
const onResetApplied = jest.fn();
const onMutatedChange = jest.fn();

const mockYaml = {
  kind: "Pipeline",
  apiVersion: "numaflow.numaproj.io/v1alpha1",
  metadata: {
    name: "simple-pipeline",
    namespace: "numaflow-system",
    uid: "c0513165-3bf3-44a6-b0a0-7b9cf9ae4de5",
    resourceVersion: "492810",
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
        time: "2023-11-02T14:43:32Z",
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
      retryInterval: "0.001s",
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
        lastTransitionTime: "2023-11-02T14:43:32Z",
        reason: "Successful",
        message: "Successful",
      },
      {
        type: "Deployed",
        status: "True",
        lastTransitionTime: "2023-11-02T14:43:32Z",
        reason: "Successful",
        message: "Successful",
      },
    ],
    phase: "Running",
    lastUpdated: "2023-11-02T14:43:32Z",
    vertexCount: 3,
    sourceCount: 1,
    sinkCount: 1,
    udfCount: 1,
  },
};

describe("SpecEditor", () => {
  it("renders", async () => {
    render(<SpecEditor />);
    await waitFor(() => {
      expect(screen.getByTestId("spec-editor")).toBeInTheDocument();
    });
  });

  it("renders with yaml", async () => {
    render(
      <SpecEditor
        initialYaml={mockYaml}
        onValidate={onValidate}
        onSubmit={onSubmit}
        onResetApplied={onResetApplied}
        onMutatedChange={onMutatedChange}
        loading={false}
        viewType={2}
        allowNonMutatedSubmit={false}
      />
    );
    await waitFor(() => {
      expect(screen.getByTestId("spec-editor")).toBeInTheDocument();
    });
  });

  it("Clicks on validate button", async () => {
    render(
      <SpecEditor
        initialYaml={mockYaml}
        onValidate={onValidate}
        onSubmit={onSubmit}
        onResetApplied={onResetApplied}
        onMutatedChange={onMutatedChange}
        loading={false}
        viewType={2}
        allowNonMutatedSubmit={false}
      />
    );
    await waitFor(() => {
      expect(
        screen.getByTestId("spec-editor-validate-button")
      ).toBeInTheDocument();
    });
    const validateButton = screen.getByTestId("spec-editor-validate-button");
    validateButton.click();
  });

  it("Tests handleEditToggle", async () => {
    render(
      <SpecEditor
        initialYaml={mockYaml}
        onValidate={onValidate}
        onSubmit={onSubmit}
        onResetApplied={onResetApplied}
        onMutatedChange={onMutatedChange}
        loading={false}
        viewType={1}
        allowNonMutatedSubmit={false}
      />
    );
    await waitFor(() => {
      expect(screen.getByTestId("spec-editor-edit-btn")).toBeInTheDocument();
    });
    await act(async () => {
      screen.getByTestId("spec-editor-edit-btn").click();
    });
  });

  it("Tests handleValidate", async () => {
    render(
      <SpecEditor
        initialYaml={mockYaml}
        onValidate={onValidate}
        onSubmit={onSubmit}
        onResetApplied={onResetApplied}
        onMutatedChange={onMutatedChange}
        loading={false}
        viewType={1}
        allowNonMutatedSubmit={false}
      />
    );
    await waitFor(() => {
      expect(
        screen.getByTestId("spec-editor-validate-button")
      ).toBeInTheDocument();
    });
    await act(async () => {
      screen.getByTestId("spec-editor-validate-button").click();
    });
  });

  it("Renders without initialYaml", () => {
    render(
      <SpecEditor
        initialYaml={null}
        onValidate={onValidate}
        onSubmit={onSubmit}
        onResetApplied={onResetApplied}
        onMutatedChange={onMutatedChange}
        loading={false}
        viewType={1}
        allowNonMutatedSubmit={false}
        mutationKey="45"
      />
    );
    expect(screen.getByTestId("spec-editor")).toBeInTheDocument();
  });

  it("Renders when initialYaml is a string", () => {
    render(
      <SpecEditor
        initialYaml={"test"}
        onValidate={onValidate}
        onSubmit={onSubmit}
        onResetApplied={onResetApplied}
        onMutatedChange={onMutatedChange}
        loading={false}
        viewType={1}
        allowNonMutatedSubmit={false}
      />
    );
    expect(screen.getByTestId("spec-editor")).toBeInTheDocument();
  });
});
