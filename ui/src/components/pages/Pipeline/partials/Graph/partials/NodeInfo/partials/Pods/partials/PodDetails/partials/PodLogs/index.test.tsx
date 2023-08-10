import { fireEvent, render, screen } from "@testing-library/react";
import { PodLogs } from "./index";

describe("PodLogs", () => {
  it("Load PodLogs screen", async () => {
    const { debug, container } = render(
      <PodLogs
        namespaceId={"numaflow-system"}
        containerName={"numa"}
        podName={"simple-pipeline-infer-0-xah5w"}
      />
    );
    //search for logs
    fireEvent.change(
      container.getElementsByClassName(
        "MuiInputBase-input css-yz9k0d-MuiInputBase-input"
      )[0],
      { target: { value: "load" } }
    );
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
    //pause
    fireEvent.click(screen.getByTestId("pause-button"));
    //unpause
    fireEvent.click(screen.getByTestId("pause-button"));
  });
});
