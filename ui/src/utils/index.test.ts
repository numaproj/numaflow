import { handleCopy, isDev, findSuffix, quantityToScalar, getPodContainerUsePercentages } from "./index"
import {Pod, PodContainerSpec, PodDetail} from "./models/pods";

const podContainerSpec: PodContainerSpec = {
    name: "numa",
    cpuParsed: 34,
    memoryParsed:50
}
const containerSpecMap = new Map<string, PodContainerSpec>([
    ["simple-pipeline-infer-0-xah5w", podContainerSpec]
]);

const pod: Pod = {
    name: "simple-pipeline-infer-0-xah5w",
    containers:["numa","udf"],
    containerSpecMap: containerSpecMap
}

const podContainerSpec1: PodContainerSpec = {
    name: "numa",
    cpuParsed: 12,
    memoryParsed:15
}

const containerMap = new Map<string, PodContainerSpec>([
    ["simple-pipeline-infer-0-xah5w", podContainerSpec1]
]);


const podDetail = {"name":"simple-pipeline-infer-0-xah5w","containerMap":containerMap}



describe("index", () => {

    it("isDev", () => {
        expect(isDev())
    })

    it("findSuffix", () => {
        expect(findSuffix("10n")).toEqual("n")
        expect(findSuffix("10")).toEqual("")

    })

    it("quantityToScalar", () => {
        expect(quantityToScalar("10")).toEqual(10)
    })

    it("getPodContainerUsePercentages", () => {
        expect(getPodContainerUsePercentages(pod, podDetail, "numa")).toEqual({"cpuPercent": undefined, "memoryPercent": undefined})
    })
})