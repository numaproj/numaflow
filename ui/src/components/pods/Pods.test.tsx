import {Pods} from "./Pods";
import {usePodsFetch} from "../../utils/fetchWrappers/podsFetch";
import {usePodsDetailFetch} from "../../utils/fetchWrappers/podsDetailFetch";
import {render, screen, waitFor} from "@testing-library/react";
import {Pod, PodContainerSpec, PodDetail} from "../../utils/models/pods";

const podContainerSpec: PodContainerSpec = {
    name: "numa"
}
const containerSpecMap = new Map<string, PodContainerSpec>([
    ["simple-pipeline-infer-0-xah5w", podContainerSpec]
]);

const pod = {
    "name": "simple-pipeline-infer-0-xah5w",
    "containers": ["numa", "udf"],
    "containerSpecMap": containerSpecMap
}
const podDetail = {"name": "simple-pipeline-infer-0-xah5w", "containerMap": containerSpecMap}

const pods: Pod[] = [pod]

const podDetailMap = new Map<string, PodDetail>([
    ["simple-pipeline-infer-0-xah5w", podDetail]
]);

jest.mock("react-router-dom", () => ({
    ...jest.requireActual("react-router-dom"),
    useParams: () => ({
        namespaceId: "numaflow-system",
        pipelineId: "simple-pipeline",
        vertexId: "infer"
    })
}));

jest.mock("../../utils/fetchWrappers/podsFetch");
const mockedUsePodsFetch = usePodsFetch as jest.MockedFunction<typeof usePodsFetch>;

jest.mock("../../utils/fetchWrappers/podsDetailFetch");
const mockedUsePodDetailFetch = usePodsDetailFetch as jest.MockedFunction<typeof usePodsDetailFetch>;

describe("Pods", () => {
    it("loads screen", async () => {
        mockedUsePodsFetch.mockReturnValue({pods: pods, error: false, loading: false})
        mockedUsePodDetailFetch.mockReturnValue({podsDetailMap: podDetailMap, error: false, loading: false})
        render(<Pods namespaceId={"numaflow-system"} pipelineId={"simple-pipeline"} vertexId={"infer"} />)
        await waitFor(() => expect(screen.getByTestId("pod-detail")).toBeInTheDocument());
        await waitFor(() => expect(screen.getByTestId("searchable-pods")).toBeInTheDocument());


    })
    it("pods loading screen", async () => {
        mockedUsePodsFetch.mockReturnValue({pods: pods, error: false, loading: true})
        mockedUsePodDetailFetch.mockReturnValue({podsDetailMap: podDetailMap, error: false, loading: false})
        render(<Pods namespaceId={"numaflow-system"} pipelineId={"simple-pipeline"} vertexId={"infer"} />)
        await waitFor(() => expect(screen.getByTestId("progress")).toBeInTheDocument());

    })

    it("pods error screen", async () => {
        mockedUsePodsFetch.mockReturnValue({pods: pods, error: true, loading: false})
        mockedUsePodDetailFetch.mockReturnValue({podsDetailMap: podDetailMap, error: false, loading: false})
        render(<Pods namespaceId={"numaflow-system"} pipelineId={"simple-pipeline"} vertexId={"infer"} />)
        await waitFor(() => expect(screen.getByTestId("pods-error")).toBeInTheDocument());

    })

    it("pods detail error screen", async () => {
        mockedUsePodsFetch.mockReturnValue({pods: pods, error: false, loading: false})
        mockedUsePodDetailFetch.mockReturnValue({podsDetailMap: podDetailMap, error: true, loading: false})
        render(<Pods namespaceId={"numaflow-system"} pipelineId={"simple-pipeline"} vertexId={"infer"} />)
        await waitFor(() => expect(screen.getByTestId("pods-detail-error")).toBeInTheDocument());

    })
})