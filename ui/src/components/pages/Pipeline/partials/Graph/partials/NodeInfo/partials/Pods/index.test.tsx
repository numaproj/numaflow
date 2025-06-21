import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { Pods } from "./index";
import { usePodsViewFetch } from "../../../../../../../../../utils/fetcherHooks/podsViewFetch";
import {
  Pod,
  PodContainerSpec,
  PodDetail,
} from "../../../../../../../../../types/declarations/pods";
import { TextEncoder, TextDecoder } from "util";
import "@testing-library/jest-dom";
import { act } from "react-test-renderer";

Object.assign(global, { TextDecoder, TextEncoder });

const podContainerSpec: PodContainerSpec = {
  name: "numa",
  cpuParsed: 100,
  memoryParsed: 100,
};
const containerSpecMap = new Map<string, PodContainerSpec>([
  ["numa", podContainerSpec],
  ["udf", podContainerSpec],
]);

const pod = {
  name: "simple-pipeline-infer-0-xah5w",
  containers: ["numa", "udf"],
  containerSpecMap: containerSpecMap,
};
const podDetail = {
  name: "simple-pipeline-infer-0-xah5w",
  containerMap: containerSpecMap,
};

const pod1 = {
  name: "simple-pipeline-infer-1-xah5w",
  containers: ["numa", "udf"],
  containerSpecMap: containerSpecMap,
};
const podDetail1 = {
  name: "simple-pipeline-infer-1-xah5w",
  containerMap: containerSpecMap,
};

const pods: Pod[] = [pod, pod1];

const podsDetails = new Map<string, PodDetail>([
  ["simple-pipeline-infer-0-xah5w", podDetail],
  ["simple-pipeline-infer-1-xah5w", podDetail1],
]);

jest.mock("react-router-dom", () => ({
  ...jest.requireActual("react-router-dom"),
  useParams: () => ({
    namespaceId: "numaflow-system",
    pipelineId: "simple-pipeline",
    vertexId: "infer",
  }),
}));

jest.mock("../../../../../../../../../utils/fetcherHooks/podsViewFetch");
const mockedUsePodsViewFetch = usePodsViewFetch as jest.MockedFunction<
  typeof usePodsViewFetch
>;

describe("Pods", () => {
  let originFetch: any;
  beforeEach(() => {
    originFetch = (global as any).fetch;
  });
  afterEach(() => {
    (global as any).fetch = originFetch;
  });
  it("loads pods view", async () => {
    mockedUsePodsViewFetch.mockReturnValue({
      pods: pods,
      podsDetails: podsDetails,
      podsErr: undefined,
      podsDetailsErr: undefined,
      loading: false,
    });
    const mRes = {
      body: new ReadableStream({
        start(controller) {
          controller.enqueue(
            Buffer.from(
              `{"level":"info","ts":"2023-09-04T11:50:19.712416709Z","logger":"numaflow.Source-processor","caller":"publish/publisher.go:180","msg":"Skip publishing the new watermark because it's older than the current watermark","pipeline":"simple-pipeline","vertex":"in","entityID":"simple-pipeline-in-0","otStore":"default-simple-pipeline-in-cat_OT","hbStore":"default-simple-pipeline-in-cat_PROCESSORS","toVertexPartitionIdx":0,"entity":"simple-pipeline-in-0","head":1693828217394,"new":-1}`
            )
          );
          controller.close();
        },
      }),
      ok: true,
    };
    const mockedFetch = jest.fn().mockResolvedValue(mRes as any);
    (global as any).fetch = mockedFetch;
    await act(async () => {
      render(
        <Pods
          namespaceId={"numaflow-system"}
          pipelineId={"simple-pipeline"}
          vertexId={"infer"}
        />
      );
    });
    await waitFor(() =>
      expect(
        screen.getByTestId("pods-searchablePodsHeatMap")
      ).toBeInTheDocument()
    );
    await waitFor(() =>
      expect(screen.getByTestId("pods-containers")).toBeInTheDocument()
    );
    await waitFor(() =>
      expect(screen.getByTestId("pods-poddetails")).toBeInTheDocument()
    );
    await waitFor(() =>
      expect(
        screen.getByTestId("hexagon_simple-pipeline-infer-0-xah5w-cpu")
      ).toBeInTheDocument()
    );
    expect(mockedFetch).toBeCalledTimes(3);
    fireEvent.click(
      screen.getByTestId("hexagon_simple-pipeline-infer-0-xah5w-cpu")
    );
    await waitFor(() =>
      expect(
        screen.getByTestId("simple-pipeline-infer-0-xah5w-numa")
      ).toBeInTheDocument()
    );
    expect(mockedFetch).toBeCalledTimes(5);
    fireEvent.click(screen.getByTestId("simple-pipeline-infer-0-xah5w-numa"));
    expect(mockedFetch).toBeCalledTimes(5);
    const dropdown = screen.getByRole("button", { name: "Open" });
    fireEvent.click(dropdown);
    expect(mockedFetch).toBeCalledTimes(5);
    const option2 = screen.getByText("simple-pipeline-infer-1-xah5w");
    await act(async () => {
      fireEvent.click(option2);
    });
    expect(mockedFetch).toBeCalledTimes(7);
  });
  it("pods error screen - api errors", async () => {
    mockedUsePodsViewFetch.mockReturnValue({
      pods: pods,
      podsDetails: podsDetails,
      podsErr: ["some error"],
      podsDetailsErr: ["some error"],
      loading: false,
    });
    render(
      <Pods
        namespaceId={"numaflow-system"}
        pipelineId={"simple-pipeline"}
        vertexId={"infer"}
      />
    );
    await waitFor(() =>
      expect(screen.getByTestId("pods-error")).toBeInTheDocument()
    );
  });
  it("pods error screen - missing info", () => {
    render(
      <Pods
        namespaceId={undefined}
        pipelineId={undefined}
        vertexId={undefined}
      />
    );
    waitFor(() =>
      expect(screen.getByTestId("pods-error-missing")).toBeInTheDocument()
    );
  });
});
