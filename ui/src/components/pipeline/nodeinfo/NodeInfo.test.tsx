import NodeInfo from "./NodeInfo";
import { render, screen } from "@testing-library/react";
import { BrowserRouter } from "react-router-dom";

describe("NodeInfo", () => {
  const node = {
    id: "node",
    position: undefined,
    data: {
      source: { generator: "5" },
      udf: { builtin: "cat" },
      sink: { log: "{}" },
    },
  };
  it("loads", () => {
    render(
      <BrowserRouter>
        <NodeInfo
          node={node}
          namespaceId={"namespace"}
          pipelineId={"pipeline-1"}
        />
      </BrowserRouter>
    );
    expect(screen.getByTestId("vertex-info")).toBeVisible();
    expect(screen.getByTestId("pods-view")).toBeVisible();
  });

  it("namespace empty", () => {
    render(
      <BrowserRouter>
        <NodeInfo node={node} namespaceId={""} pipelineId={"pipeline-1"} />
      </BrowserRouter>
    );
    expect(screen.queryByTestId("dialog")).toBeNull();
  });
});
