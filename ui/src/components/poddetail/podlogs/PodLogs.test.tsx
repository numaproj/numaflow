import {PodLogs} from "./PodLogs"
import {render} from "@testing-library/react"

describe("PodLogs", () => {
    it("Load PodLogs screen", async () => {
        render(<PodLogs namespaceId={"numaflow-system"} containerName={"numa"}
                        podName={"simple-pipeline-infer-0-xah5w"}/>)
    })
})
