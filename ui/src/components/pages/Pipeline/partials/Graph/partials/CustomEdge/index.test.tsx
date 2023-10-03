import { render, screen, waitFor } from "@testing-library/react";
import { Position } from "@reactflow/core";
import { ReactFlowProvider } from "reactflow";
import CustomEdge from "./index";

describe("Graph screen test", () => {
  it("Fwd edge not full", async () => {
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
            edgeWatermark: {
              isWaterMarkEnabled: true,
            },
            fwdEdge: true,
            fromNodeOutDegree: 1,
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
  it("Fwd edge but full with delay in mo", async () => {
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
              watermarks: [Date.now() - 2678400000],
            },
            fwdEdge: true,
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
  it("Edge branches with delays in ms, sec, min, hr, d", async () => {
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
            edgeWatermark: {
              isWaterMarkEnabled: true,
              watermarks: [Date.now() - 1],
            },
            fwdEdge: true,
          }}
          source={"first"}
          target={"second"}
        />
        <CustomEdge
          id={"first-third"}
          sourceX={240}
          sourceY={36}
          targetX={334}
          targetY={38}
          sourcePosition={Position.Right}
          targetPosition={Position.Left}
          data={{
            isFull: "false",
            backpressureLabel: "0",
            edgeWatermark: {
              isWaterMarkEnabled: true,
              watermarks: [Date.now() - 1000],
            },
            fwdEdge: true,
          }}
          source={"first"}
          target={"third"}
        />
        <CustomEdge
          id={"first-fourth"}
          sourceX={240}
          sourceY={36}
          targetX={334}
          targetY={36}
          sourcePosition={Position.Right}
          targetPosition={Position.Left}
          data={{
            isFull: "false",
            backpressureLabel: "0",
            edgeWatermark: {
              isWaterMarkEnabled: true,
              watermarks: [Date.now() - 60000],
            },
            fwdEdge: true,
          }}
          source={"first"}
          target={"fourth"}
        />
        <CustomEdge
          id={"first-fifth"}
          sourceX={240}
          sourceY={36}
          targetX={334}
          targetY={34}
          sourcePosition={Position.Right}
          targetPosition={Position.Left}
          data={{
            isFull: "false",
            backpressureLabel: "0",
            edgeWatermark: {
              isWaterMarkEnabled: true,
              watermarks: [Date.now() - 3600000],
            },
            fwdEdge: true,
          }}
          source={"first"}
          target={"fifth"}
        />
        <CustomEdge
          id={"first-sixth"}
          sourceX={240}
          sourceY={36}
          targetX={334}
          targetY={32}
          sourcePosition={Position.Right}
          targetPosition={Position.Left}
          data={{
            isFull: "false",
            backpressureLabel: "0",
            edgeWatermark: {
              isWaterMarkEnabled: true,
              watermarks: [Date.now() - 86400000],
            },
            fwdEdge: true,
          }}
          source={"first"}
          target={"sixth"}
        />
      </ReactFlowProvider>
    );
    await waitFor(() =>
      expect(screen.getByTestId(`first-second`)).toBeInTheDocument()
    );
    await waitFor(() =>
      expect(screen.getByTestId(`first-third`)).toBeInTheDocument()
    );
    await waitFor(() =>
      expect(screen.getByTestId(`first-fourth`)).toBeInTheDocument()
    );
    await waitFor(() =>
      expect(screen.getByTestId(`first-fifth`)).toBeInTheDocument()
    );
    await waitFor(() =>
      expect(screen.getByTestId(`first-sixth`)).toBeInTheDocument()
    );
  });
  it("Forward edges with out-degree > 1", async () => {
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
            isFull: "true",
            backpressureLabel: "0",
            edgeWatermark: {
              isWaterMarkEnabled: true,
              watermarks: [Date.now() - 2678400000],
            },
            fwdEdge: true,
            fromNodeOutDegree: 2,
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
            isFull: "true",
            backpressureLabel: "0",
            edgeWatermark: {
              isWaterMarkEnabled: true,
              watermarks: [Date.now() - 2678400000],
            },
            fwdEdge: true,
            fromNodeOutDegree: 2,
          }}
          source={"first"}
          target={"third"}
        />
      </ReactFlowProvider>
    );
    await waitFor(() => {
      expect(screen.getByTestId(`first-second`)).toBeInTheDocument();
      expect(screen.getByTestId(`first-third`)).toBeInTheDocument();
    });
  });
  it("Back edges", async () => {
    render(
      <ReactFlowProvider>
        <CustomEdge
          id={"first-second"}
          sourceX={334}
          sourceY={36}
          targetX={240}
          targetY={36}
          sourcePosition={Position.Right}
          targetPosition={Position.Left}
          data={{
            isFull: "true",
            backpressureLabel: "0",
            edgeWatermark: {
              isWaterMarkEnabled: true,
              watermarks: [Date.now() - 2678400000],
            },
            backEdge: true,
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
  it("Self edges", async () => {
    render(
      <ReactFlowProvider>
        <CustomEdge
          id={"first-first"}
          sourceX={334}
          sourceY={36}
          targetX={300}
          targetY={36}
          sourcePosition={Position.Top}
          targetPosition={Position.Top}
          data={{
            isFull: "true",
            backpressureLabel: "0",
            edgeWatermark: {
              isWaterMarkEnabled: true,
              watermarks: [Date.now() - 2678400000],
            },
            selfEdge: true,
          }}
          source={"first"}
          target={"second"}
        />
      </ReactFlowProvider>
    );
    await waitFor(() =>
      expect(screen.getByTestId(`first-first`)).toBeInTheDocument()
    );
  });
});
