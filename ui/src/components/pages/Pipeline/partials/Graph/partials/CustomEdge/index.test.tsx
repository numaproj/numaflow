import { render, screen, waitFor } from "@testing-library/react";
import { Position } from "@reactflow/core";
import { ReactFlowProvider } from "reactflow";
import CustomEdge from "./index";

describe("Graph screen test", () => {
  it("Straight edge not full", async () => {
    render(
      <ReactFlowProvider>
        <CustomEdge
          id={"first-second"}
          sourceX={240}
          sourceY={36}
          targetX={334}
          targetY={36}
          sourcePosition={Position.Right}
          targetPosition={Position.Left}
          data={{
            backpressureLabel: "0",
            edgeWatermark: null,
          }}
          source={"first"}
          target={"second"}
        />
      </ReactFlowProvider>
    );
    await waitFor(() =>
      expect(screen.getByTestId(`first-second`)).toBeInTheDocument()
    );
  });
  it("Straight edge but full", async () => {
    render(
      <ReactFlowProvider>
        <CustomEdge
          id={"first-second"}
          sourceX={240}
          sourceY={36}
          targetX={334}
          targetY={36}
          sourcePosition={Position.Right}
          targetPosition={Position.Left}
          data={{
            isFull: "true",
            backpressureLabel: "0",
            edgeWatermark: {
              isWaterMarkEnabled: true,
              watermarks: [1690792996319],
            },
          }}
          source={"first"}
          target={"second"}
        />
      </ReactFlowProvider>
    );
    await waitFor(() =>
      expect(screen.getByTestId(`first-second`)).toBeInTheDocument()
    );
  });
  it("Edge branches into two", async () => {
    render(
      <ReactFlowProvider>
        <CustomEdge
          id={"first-second"}
          sourceX={240}
          sourceY={36}
          targetX={334}
          targetY={40}
          sourcePosition={Position.Right}
          targetPosition={Position.Left}
          data={{
            isFull: "false",
            backpressureLabel: "0",
            edgeWatermark: null,
          }}
          source={"first"}
          target={"second"}
        />
        <CustomEdge
          id={"first-third"}
          sourceX={240}
          sourceY={36}
          targetX={334}
          targetY={32}
          sourcePosition={Position.Right}
          targetPosition={Position.Left}
          data={{
            isFull: "false",
            backpressureLabel: "0",
            edgeWatermark: null,
          }}
          source={"first"}
          target={"third"}
        />
      </ReactFlowProvider>
    );
    await waitFor(() =>
      expect(screen.getByTestId(`first-second`)).toBeInTheDocument()
    );
    await waitFor(() =>
      expect(screen.getByTestId(`first-third`)).toBeInTheDocument()
    );
  });
});
