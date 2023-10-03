import { fireEvent, render, screen } from "@testing-library/react";
import { act } from "react-test-renderer";
import { TextEncoder, TextDecoder } from "util";
import { PodLogs } from "./index";

Object.assign(global, { TextDecoder, TextEncoder });

describe("PodLogs", () => {
  let originFetch: any;
  beforeEach(() => {
    originFetch = (global as any).fetch;
  });
  afterEach(() => {
    (global as any).fetch = originFetch;
  });

  it("Load PodLogs screen", async () => {
    const mRes = {
      body: new ReadableStream({
        start(controller) {
          controller.enqueue(
            Buffer.from(
              `{"level":"info","ts":"2023-09-04T11:50:19.712416709Z","logger":"numaflow.Source-processor","caller":"publish/publisher.go:180","msg":"Skip publishing the new watermark because it's older than the current watermark","pipeline":"simple-pipeline","vertex":"in","entityID":"simple-pipeline-in-0","otStore":"default-simple-pipeline-in-cat_OT","hbStore":"default-simple-pipeline-in-cat_PROCESSORS","toVertexPartitionIdx":0,"entity":"simple-pipeline-in-0","head":1693828217394,"new":-1}`
            )
          );
          controller.enqueue(
            Buffer.from(
              `{"level":"error","ts":"2023-09-04T11:50:19.712416709Z","logger":"numaflow.Source-processor","caller":"publish/publisher.go:180","msg":"Skip publishing the new watermark because it's older than the current watermark","pipeline":"simple-pipeline","vertex":"in","entityID":"simple-pipeline-in-0","otStore":"default-simple-pipeline-in-cat_OT","hbStore":"default-simple-pipeline-in-cat_PROCESSORS","toVertexPartitionIdx":0,"entity":"simple-pipeline-in-0","head":1693828217394,"new":-1}`
            )
          );
          controller.enqueue(
            Buffer.from(
              `{"level":"warn","ts":"2023-09-04T11:50:19.712416709Z","logger":"numaflow.Source-processor","caller":"publish/publisher.go:180","msg":"Skip publishing the new watermark because it's older than the current watermark","pipeline":"simple-pipeline","vertex":"in","entityID":"simple-pipeline-in-0","otStore":"default-simple-pipeline-in-cat_OT","hbStore":"default-simple-pipeline-in-cat_PROCESSORS","toVertexPartitionIdx":0,"entity":"simple-pipeline-in-0","head":1693828217394,"new":-1}`
            )
          );
          controller.enqueue(
            Buffer.from(
              `{"level":"debug","ts":"2023-09-04T11:50:19.712416709Z","logger":"numaflow.Source-processor","caller":"publish/publisher.go:180","msg":"Skip publishing the new watermark because it's older than the current watermark","pipeline":"simple-pipeline","vertex":"in","entityID":"simple-pipeline-in-0","otStore":"default-simple-pipeline-in-cat_OT","hbStore":"default-simple-pipeline-in-cat_PROCESSORS","toVertexPartitionIdx":0,"entity":"simple-pipeline-in-0","head":1693828217394,"new":-1}`
            )
          );
          controller.close();
        },
      }),
      ok: true,
    };
    const mockedFetch = jest.fn().mockResolvedValue(mRes as any);
    (global as any).fetch = mockedFetch;
    let container;
    await act(async () => {
      const { container: cont } = render(
        <PodLogs
          namespaceId={"numaflow-system"}
          containerName={"numa"}
          podName={"simple-pipeline-infer-0-xah5w"}
        />
      );
      container = cont;
    });

    expect(mockedFetch).toBeCalledTimes(1);

    //search for logs
    fireEvent.change(
      container.getElementsByClassName(
        "MuiInputBase-input css-yz9k0d-MuiInputBase-input"
      )[0],
      { target: { value: "load" } }
    );
    //search for logs not present
    fireEvent.change(
      container.getElementsByClassName(
        "MuiInputBase-input css-yz9k0d-MuiInputBase-input"
      )[0],
      { target: { value: "xyz" } }
    );
    expect(screen.getByText("No logs matching search.")).toBeVisible();
    //negate logs search
    fireEvent.click(
      container.getElementsByClassName(
        "PrivateSwitchBase-input css-1m9pwf3"
      )[0],
      { target: { value: true } }
    );
    //clear search
    expect(screen.getByTestId("clear-button")).toBeVisible();
    fireEvent.click(screen.getByTestId("clear-button"));
    //pause logs
    expect(screen.getByTestId("pause-button")).toBeVisible();
    act(() => {
      fireEvent.click(screen.getByTestId("pause-button"));
      //play logs
      fireEvent.click(screen.getByTestId("pause-button"));
    });
    //toggle theme
    expect(screen.getByTestId("color-mode-button")).toBeVisible();
    fireEvent.click(screen.getByTestId("color-mode-button"));
    //toggle logs order
    expect(screen.getByTestId("order-button")).toBeVisible();
    fireEvent.click(screen.getByTestId("order-button"));
  });

  it("Trigger PodLogs parsing error", async () => {
    const mRes = {
      body: new ReadableStream({
        start(controller) {
          controller.enqueue(Buffer.from("something"));
          controller.close();
        },
      }),
      ok: true,
    };
    const mockedFetch = jest.fn().mockResolvedValueOnce(mRes as any);
    (global as any).fetch = mockedFetch;
    await act(async () => {
      render(
        <PodLogs
          namespaceId={"numaflow-system"}
          containerName={"numa"}
          podName={"simple-pipeline-infer-0-xah5w"}
        />
      );
    });

    expect(mockedFetch).toBeCalledTimes(1);
  });
});
