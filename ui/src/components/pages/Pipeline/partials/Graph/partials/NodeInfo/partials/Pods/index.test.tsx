// import { fireEvent, render, screen, waitFor } from "@testing-library/react";
// import { Pods } from "./index";
// import { usePodsViewFetch } from "../../../../../../../../../utils/fetcherHooks/podsViewFetch";
// import {
//   Pod,
//   PodContainerSpec,
//   PodDetail,
// } from "../../../../../../../../../types/declarations/pods";

import "@testing-library/jest-dom"

// const podContainerSpec: PodContainerSpec = {
//   name: "numa",
//   cpuParsed: 100,
//   memoryParsed: 100,
// };
// const containerSpecMap = new Map<string, PodContainerSpec>([
//   ["numa", podContainerSpec],
//   ["udf", podContainerSpec],
// ]);

// const pod = {
//   name: "simple-pipeline-infer-0-xah5w",
//   containers: ["numa", "udf"],
//   containerSpecMap: containerSpecMap,
// };
// const podDetail = {
//   name: "simple-pipeline-infer-0-xah5w",
//   containerMap: containerSpecMap,
// };

// const pods: Pod[] = [pod];

// const podsDetails = new Map<string, PodDetail>([
//   ["simple-pipeline-infer-0-xah5w", podDetail],
// ]);

// jest.mock("react-router-dom", () => ({
//   ...jest.requireActual("react-router-dom"),
//   useParams: () => ({
//     namespaceId: "numaflow-system",
//     pipelineId: "simple-pipeline",
//     vertexId: "infer",
//   }),
// }));

// jest.mock("../../../../../../../../../utils/fetcherHooks/podsViewFetch");
// const mockedUsePodsViewFetch = usePodsViewFetch as jest.MockedFunction<
//   typeof usePodsViewFetch
// >;

describe("Pods", () => {
  test.todo('please update');
//   it("loads pods view", async () => {
//     mockedUsePodsViewFetch.mockReturnValue({
//       pods: pods,
//       podsDetails: podsDetails,
//       podsErr: undefined,
//       podsDetailsErr: undefined,
//       loading: false,
//     });
//     render(
//       <Pods
//         namespaceId={"numaflow-system"}
//         pipelineId={"simple-pipeline"}
//         vertexId={"infer"}
//       />
//     );
//     await waitFor(() =>
//       expect(
//         screen.getByTestId("pods-searchablePodsHeatMap")
//       ).toBeInTheDocument()
//     );
//     await waitFor(() =>
//       expect(screen.getByTestId("pods-containers")).toBeInTheDocument()
//     );
//     await waitFor(() =>
//       expect(screen.getByTestId("pods-poddetails")).toBeInTheDocument()
//     );
//     await waitFor(() =>
//       expect(
//         screen.getByTestId("hexagon_simple-pipeline-infer-0-xah5w-cpu")
//       ).toBeInTheDocument()
//     );
//     fireEvent.click(
//       screen.getByTestId("hexagon_simple-pipeline-infer-0-xah5w-cpu")
//     );
//     await waitFor(() =>
//       expect(
//         screen.getByTestId("simple-pipeline-infer-0-xah5w-numa")
//       ).toBeInTheDocument()
//     );
//     fireEvent.click(screen.getByTestId("simple-pipeline-infer-0-xah5w-numa"));
//   });
//   it("pods error screen - api errors", async () => {
//     mockedUsePodsViewFetch.mockReturnValue({
//       pods: pods,
//       podsDetails: podsDetails,
//       podsErr: ["some error"],
//       podsDetailsErr: ["some error"],
//       loading: false,
//     });
//     render(
//       <Pods
//         namespaceId={"numaflow-system"}
//         pipelineId={"simple-pipeline"}
//         vertexId={"infer"}
//       />
//     );
//     await waitFor(() =>
//       expect(screen.getByTestId("pods-error")).toBeInTheDocument()
//     );
//   });
//   it("pods error screen - missing info", () => {
//     render(
//       <Pods
//         namespaceId={undefined}
//         pipelineId={undefined}
//         vertexId={undefined}
//       />
//     );
//     waitFor(() =>
//       expect(screen.getByTestId("pods-error-missing")).toBeInTheDocument()
//     );
//   });
});
