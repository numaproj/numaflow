import { render, screen } from "@testing-library/react";
import "@testing-library/jest-dom";
import { PodInfo } from "./index";
import { PodContainerSpec } from "../../../../../../../../../../../../../types/declarations/pods";

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

describe("PodInfo screen", () => {
  it("loads screen", () => {
    render(
      <PodInfo
        pod={pod}
        podDetails={podDetails}
        containerName={containerName}
      />
    );
    expect(screen.getByTestId("podInfo")).toBeInTheDocument();
    expect(screen.getByTestId("podInfo")).toBeVisible();
  });
});
