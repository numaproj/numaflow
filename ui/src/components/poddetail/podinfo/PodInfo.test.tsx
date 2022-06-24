import {PodInfo} from "./PodInfo"
import {render, screen} from "@testing-library/react";
import {PodContainerSpec} from "../../../utils/models/pods";

const podContainerSpec: PodContainerSpec = {
    name: "main"
}
const containerSpecMap = new Map<string, PodContainerSpec>([
    ["simple-pipeline-infer-0-xah5w", podContainerSpec]
]);


const pod = {
    "name": "simple-pipeline-infer-0-xah5w",
    "containers": ["main", "udf"],
    "containerSpecMap": containerSpecMap
}
const podDetail = {"name": "simple-pipeline-infer-0-xah5w", "containerMap": containerSpecMap}
const containerName = "main"

describe("PodInfo screen", () => {
    it("loads screen", () => {
        render(
            <PodInfo pod={pod} podDetail={podDetail} containerName={containerName}/>)
        expect(screen.getByTestId("podInfo")).toBeInTheDocument();
        expect(screen.getByTestId("podInfo")).toBeVisible();
    });
})