import { render, screen, waitFor } from "@testing-library/react";
import "@testing-library/jest-dom";
import { ContainerInfo } from "./index";
import {
  ContainerInfoProps,
  PodContainerSpec,
  PodSpecificInfoProps,
} from "../../../../../../../../../../../../../types/declarations/pods";

const podContainerSpec: PodContainerSpec = {
  name: "numa",
  cpu: "100m",
  memory: "100Mi",
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
const podDetails = {
  name: "simple-pipeline-infer-0-xah5w",
  containerMap: containerSpecMap,
};
const containerName = "numa";

describe("ContainerInfo screen", () => {
  it("loads screen", async () => {
    render(
      <ContainerInfo
        namespaceId={"numaflow-system"}
        pipelineId={"simple-pipeline"}
        vertexId={"infer"}
        type={"udf"}
        pod={pod}
        podDetails={podDetails}
        containerName={containerName}
        containerInfo={{} as ContainerInfoProps}
        podSpecificInfo={{} as PodSpecificInfoProps}
      />
    );
    await waitFor(() =>
      expect(screen.getByTestId("containerInfo")).toBeInTheDocument()
    );
    await waitFor(() =>
      expect(screen.getByTestId("containerInfo")).toBeVisible()
    );
  });
});
