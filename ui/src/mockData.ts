export const SAMPLE_NAMESPACES = ["numaflow-system"];

export const SAMPLE_PIPELINES = [
  {
    kind: "Pipeline",
    apiVersion: "numaflow.numaproj.io/v1alpha1",
    metadata: {
      name: "simple-pipeline",
      namespace: "numaflow-system",
      uid: "6bef81cc-590b-4dd9-9e7f-cdfaaf6aec68",
      resourceVersion: "3135",
      generation: 1,
      creationTimestamp: "2022-04-25T17:55:25Z",
      annotations: {
        "kubectl.kubernetes.io/last-applied-configuration":
          '{"apiVersion":"numaflow.numaproj.io/v1alpha1","kind":"Pipeline","metadata":{"annotations":{},"name":"simple-pipeline","namespace":"numaflow-system"},"spec":{"edges":[{"from":"input","to":"cat"},{"from":"cat","to":"output"}],"vertices":[{"name":"input","source":{"generator":{"duration":"1s","rpu":5}}},{"name":"cat","udf":{"builtin":{"name":"cat"}}},{"name":"output","sink":{"log":{}}}]}}\n',
      },
      finalizers: ["pipeline-controller"],
      managedFields: [
        {
          manager: "kubectl-client-side-apply",
          operation: "Update",
          apiVersion: "numaflow.numaproj.io/v1alpha1",
          time: "2022-04-25T17:55:25Z",
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
              },
              "f:limits": {
                ".": {},
                "f:bufferMaxLength": {},
                "f:bufferUsageLimit": {},
                "f:readBatchSize": {},
              },
              "f:vertices": {},
            },
          },
        },
        {
          manager: "numaflow",
          operation: "Update",
          apiVersion: "numaflow.numaproj.io/v1alpha1",
          time: "2022-04-25T17:55:26Z",
          fieldsType: "FieldsV1",
          fieldsV1: {
            "f:metadata": {
              "f:finalizers": { ".": {}, 'v:"pipeline-controller"': {} },
            },
            "f:status": {
              ".": {},
              "f:conditions": {},
              "f:lastUpdated": {},
              "f:phase": {},
            },
          },
        },
      ],
    },
    spec: {
      vertices: [
        {
          name: "input",
          source: { generator: { rpu: 5, duration: "1s", msgSize: 8 } },
        },
        { name: "cat", udf: { container: null, builtin: { name: "cat" } } },
        { name: "output", sink: { log: {} } },
      ],
      edges: [
        { from: "input", to: "cat", conditions: null },
        { from: "cat", to: "output", conditions: null },
      ],
      lifecycle: { deleteGracePeriodSeconds: 30, desiredPhase: "Running" },
      limits: {
        readBatchSize: 100,
        bufferMaxLength: 10000,
        bufferUsageLimit: 80,
      },
    },
    status: {
      conditions: [
        {
          type: "Configured",
          status: "True",
          lastTransitionTime: "2022-04-25T17:55:26Z",
          reason: "Successful",
          message: "Successful",
        },
        {
          type: "Deployed",
          status: "True",
          lastTransitionTime: "2022-04-25T17:55:26Z",
          reason: "Successful",
          message: "Successful",
        },
      ],
      phase: "Running",
      lastUpdated: "2022-04-25T17:55:26Z",
    },
  },
];
