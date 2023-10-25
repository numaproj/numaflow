// import { Pipeline } from "./index";
// import { render, screen, waitFor } from "@testing-library/react";
// import { usePipelineViewFetch } from "../../../utils/fetcherHooks/pipelineViewFetch";

// global.ResizeObserver = require("resize-observer-polyfill");

import "@testing-library/jest-dom"

// jest.mock("react-router-dom", () => ({
//   ...jest.requireActual("react-router-dom"),
//   useParams: () => ({
//     namespaceId: "numaflow-system",
//     pipelineId: "simple-pipeline",
//   }),
// }));

// jest.mock("../../../utils/fetcherHooks/pipelineViewFetch");
// const mockedUsePipelineViewFetch = usePipelineViewFetch as jest.MockedFunction<
//   typeof usePipelineViewFetch
// >;

describe("Pipeline", () => {
  test.todo('please update');
//   it("Load Graph screen", async () => {
//     mockedUsePipelineViewFetch.mockReturnValue({
//       pipeline: {
//         metadata: {
//           name: "simple-pipeline",
//           namespace: "default",
//           uid: "c376880e-8d4f-495c-821f-666c72ed2a43",
//           resourceVersion: "854",
//           generation: 2,
//           creationTimestamp: "2023-06-06T05:18:34Z",
//           annotations: {
//             "kubectl.kubernetes.io/last-applied-configuration":
//               '{"apiVersion":"numaflow.numaproj.io/v1alpha1","kind":"Pipeline","metadata":{"annotations":{},"name":"simple-pipeline","namespace":"default"},"spec":{"edges":[{"from":"in","to":"cat"},{"from":"cat","to":"out"}],"vertices":[{"name":"in","source":{"generator":{"duration":"1s","rpu":5}}},{"name":"cat","udf":{"builtin":{"name":"cat"}}},{"name":"out","sink":{"log":{}}}]}}\n',
//           },
//           finalizers: ["pipeline-controller"],
//           managedFields: [
//             {
//               manager: "kubectl-client-side-apply",
//               operation: "Update",
//               apiVersion: "numaflow.numaproj.io/v1alpha1",
//               time: "2023-06-06T05:18:34Z",
//               fieldsType: "FieldsV1",
//               fieldsV1: {
//                 "f:metadata": {
//                   "f:annotations": {
//                     ".": {},
//                     "f:kubectl.kubernetes.io/last-applied-configuration": {},
//                   },
//                 },
//                 "f:spec": {
//                   ".": {},
//                   "f:edges": {},
//                   "f:lifecycle": {
//                     ".": {},
//                     "f:deleteGracePeriodSeconds": {},
//                     "f:desiredPhase": {},
//                   },
//                   "f:limits": {
//                     ".": {},
//                     "f:bufferMaxLength": {},
//                     "f:bufferUsageLimit": {},
//                     "f:readBatchSize": {},
//                     "f:readTimeout": {},
//                   },
//                   "f:watermark": {
//                     ".": {},
//                     "f:disabled": {},
//                     "f:maxDelay": {},
//                   },
//                 },
//               },
//             },
//             {
//               manager: "numaflow",
//               operation: "Update",
//               apiVersion: "numaflow.numaproj.io/v1alpha1",
//               time: "2023-06-06T05:18:34Z",
//               fieldsType: "FieldsV1",
//               fieldsV1: {
//                 "f:metadata": {
//                   "f:finalizers": {
//                     ".": {},
//                     'v:"pipeline-controller"': {},
//                   },
//                 },
//                 "f:spec": {
//                   "f:vertices": {},
//                 },
//               },
//             },
//             {
//               manager: "numaflow",
//               operation: "Update",
//               apiVersion: "numaflow.numaproj.io/v1alpha1",
//               time: "2023-06-06T05:18:34Z",
//               fieldsType: "FieldsV1",
//               fieldsV1: {
//                 "f:status": {
//                   ".": {},
//                   "f:conditions": {},
//                   "f:lastUpdated": {},
//                   "f:phase": {},
//                   "f:sinkCount": {},
//                   "f:sourceCount": {},
//                   "f:udfCount": {},
//                   "f:vertexCount": {},
//                 },
//               },
//               subresource: "status",
//             },
//           ],
//         },
//         spec: {
//           vertices: [
//             {
//               name: "in",
//               source: {
//                 generator: {
//                   rpu: 5,
//                   duration: "1s",
//                   msgSize: 8,
//                 },
//               },
//             },
//             {
//               name: "cat",
//               udf: {
//                 container: null,
//                 builtin: {
//                   name: "cat",
//                 },
//                 groupBy: null,
//               },
//             },
//             {
//               name: "out",
//               sink: {
//                 log: {},
//               },
//             },
//           ],
//           edges: [
//             {
//               from: "in",
//               to: "cat",
//               conditions: null,
//             },
//             {
//               from: "cat",
//               to: "out",
//               conditions: null,
//             },
//           ],
//           watermark: {
//             maxDelay: "0s",
//           },
//         },
//         status: {
//           conditions: [
//             {
//               type: "Configured",
//               status: "True",
//               lastTransitionTime: "2023-06-06T05:18:34Z",
//               reason: "Successful",
//               message: "Successful",
//             },
//             {
//               type: "Deployed",
//               status: "True",
//               lastTransitionTime: "2023-06-06T05:18:34Z",
//               reason: "Successful",
//               message: "Successful",
//             },
//           ],
//           phase: "Running",
//           lastUpdated: "2023-06-06T05:18:34Z",
//           vertexCount: 3,
//           sourceCount: 1,
//           sinkCount: 1,
//           udfCount: 1,
//         },
//       },
//       vertices: [
//         {
//           id: "in",
//           data: {
//             name: "in",
//             podnum: 0,
//             source: {
//               name: "in",
//               source: {
//                 generator: {
//                   rpu: 5,
//                   duration: "1s",
//                   msgSize: 8,
//                 },
//               },
//               scale: {},
//             },
//             vertexMetrics: null,
//             buffers: null,
//           },
//           position: {
//             x: 0,
//             y: 0,
//           },
//           draggable: false,
//           type: "source",
//         },
//         {
//           id: "cat",
//           data: {
//             name: "cat",
//             podnum: 0,
//             udf: {
//               name: "cat",
//               udf: {
//                 container: null,
//                 builtin: {
//                   name: "cat",
//                 },
//                 groupBy: null,
//               },
//               scale: {},
//             },
//             vertexMetrics: null,
//             buffers: [
//               {
//                 pipeline: "simple-pipeline",
//                 bufferName: "default-simple-pipeline-cat-0",
//                 pendingCount: 0,
//                 ackPendingCount: 4,
//                 totalMessages: 4,
//                 bufferLength: 30000,
//                 bufferUsageLimit: 0.8,
//                 bufferUsage: 0.00013333333333333334,
//                 isFull: false,
//               },
//             ],
//           },
//           position: {
//             x: 0,
//             y: 0,
//           },
//           draggable: false,
//           type: "udf",
//         },
//         {
//           id: "out",
//           data: {
//             name: "out",
//             podnum: 0,
//             sink: {
//               name: "out",
//               sink: {
//                 log: {},
//               },
//               scale: {},
//             },
//             test: "out",
//             vertexMetrics: null,
//             buffers: [
//               {
//                 pipeline: "simple-pipeline",
//                 bufferName: "default-simple-pipeline-out-0",
//                 pendingCount: 0,
//                 ackPendingCount: 0,
//                 totalMessages: 0,
//                 bufferLength: 30000,
//                 bufferUsageLimit: 0.8,
//                 bufferUsage: 0,
//                 isFull: false,
//               },
//             ],
//           },
//           position: {
//             x: 0,
//             y: 0,
//           },
//           draggable: false,
//           type: "sink",
//         },
//       ],
//       edges: [
//         {
//           id: "in-cat",
//           source: "in",
//           target: "cat",
//           data: {
//             conditions: null,
//             backpressureLabel: 4,
//             edgeWatermark: null,
//           },
//           animated: true,
//           type: "custom",
//         },
//         {
//           id: "cat-out",
//           source: "cat",
//           target: "out",
//           data: {
//             conditions: null,
//             backpressureLabel: 0,
//             edgeWatermark: null,
//           },
//           animated: true,
//           type: "custom",
//         },
//       ],
//       pipelineErr: undefined,
//       buffersErr: undefined,
//       podsErr: undefined,
//       metricsErr: undefined,
//       watermarkErr: undefined,
//       loading: false,
//     });
//     render(<Pipeline />);
//     await waitFor(() =>
//       expect(screen.getByTestId("pipeline")).toBeInTheDocument()
//     );
//   });
//   it("Load Pipeline screen", async () => {
//     mockedUsePipelineViewFetch.mockReturnValue({
//       pipeline: undefined,
//       vertices: undefined,
//       edges: undefined,
//       pipelineErr: [],
//       buffersErr: [],
//       podsErr: [],
//       metricsErr: [],
//       watermarkErr: [],
//       loading: true,
//     });
//     render(<Pipeline />);
//     await waitFor(() => expect(screen.queryByTestId("pipeline")).toBeNull());
//   });
});
