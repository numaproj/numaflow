import React from "react";
import { render, screen } from "@testing-library/react";
import "@testing-library/jest-dom";
import { PodDetail } from "./index";
import { PodContainerSpec } from "../../../../../../../../../../../types/declarations/pods";
import { TextDecoder, TextEncoder } from "util";
import { act } from "react-test-renderer";

Object.assign(global, { TextDecoder, TextEncoder });

const podContainerSpec: PodContainerSpec = {
  name: "numa",
};
const containerSpecMap = new Map<string, PodContainerSpec>([
  ["simple-pipeline-infer-0-xah5w", podContainerSpec],
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
const namespaceId = "numaflow-system";

describe("PodDetail screen", () => {
  let originFetch: any;
  beforeEach(() => {
    originFetch = (global as any).fetch;
  });
  afterEach(() => {
    (global as any).fetch = originFetch;
  });
  it("loads screen", async () => {
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
        <PodDetail
          namespaceId={namespaceId}
          pod={pod}
          podDetails={podDetails}
          containerName={containerName}
        />
      );
    });
    expect(screen.getByText("Container Logs")).toBeInTheDocument();
    expect(mockedFetch).toBeCalledTimes(1);
  });
  it("returns null when pod search is null", () => {
    const { container } = render(
      <PodDetail
        namespaceId={namespaceId}
        pod={null}
        podDetails={podDetails}
        containerName={containerName}
      />
    );
    expect(container).toBeEmptyDOMElement();
  });
});
