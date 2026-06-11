import React from "react";
import { render, screen, waitFor } from "@testing-library/react";
import { PipelineISBDebugInfo } from "./index";

import "@testing-library/jest-dom";

describe("PipelineISBDebugInfo", () => {
  it("renders loading state", async () => {
    render(<PipelineISBDebugInfo loading />);

    await waitFor(() => {
      expect(screen.getByRole("progressbar")).toBeInTheDocument();
    });
  });

  it("renders error state", async () => {
    render(<PipelineISBDebugInfo loading={false} error="boom" />);

    await waitFor(() => {
      expect(screen.getByText("Error loading ISB information: boom")).toBeInTheDocument();
    });
  });

  it("renders streams consumers kv stores and monitor errors", async () => {
    render(
      <PipelineISBDebugInfo
        loading={false}
        streams={{
          streams: [
            {
              namespace: "ns",
              pipeline: "pl",
              vertex: "cat",
              partition: 0,
              stream: "ns-pl-cat-0",
              subjects: ["ns-pl-cat-0"],
              messages: 10,
              bytes: 1024,
              consumerCount: 1,
              firstSeq: 1,
              lastSeq: 10,
              storage: "file",
              replicas: 3,
              retention: "workqueue",
              leader: "js-0",
              scope: "vertex",
              sharedByInboundEdges: false,
            },
          ],
          errors: [{ pod: "js-1", message: "connection refused" }],
        }}
        consumers={{
          consumers: [
            {
              namespace: "ns",
              pipeline: "pl",
              vertex: "cat",
              partition: 0,
              stream: "ns-pl-cat-0",
              consumer: "ns-pl-cat-0",
              durable: "ns-pl-cat-0",
              filterSubject: "ns-pl-cat-0",
              ackPolicy: "explicit",
              deliverPolicy: "all",
              ackWaitSeconds: 30,
              maxAckPending: 25000,
              numAckPending: 2,
              numRedelivered: 1,
              numWaiting: 0,
              numPending: 8,
              deliveredConsumerSeq: 10,
              deliveredStreamSeq: 10,
              ackFloorConsumerSeq: 8,
              ackFloorStreamSeq: 8,
              leader: "js-0",
              scope: "vertex",
              sharedByInboundEdges: false,
            },
          ],
        }}
        kvStores={{
          kvStores: [
            {
              namespace: "ns",
              pipeline: "pl",
              scope: "edge",
              direction: "read",
              vertex: "cat",
              from: "in",
              to: "cat",
              bucket: "ns-pl-in-cat_OT",
              stream: "KV_ns-pl-in-cat_OT",
              values: 2,
              bytes: 2048,
              history: 1,
              ttlSeconds: 3600,
              replicas: 3,
              storage: "file",
            },
          ],
        }}
      />
    );

    await waitFor(() => {
      expect(screen.getByText("Stream Information")).toBeInTheDocument();
      expect(screen.getAllByText("ns-pl-cat-0").length).toBeGreaterThan(1);
      expect(screen.getByText("ns-pl-in-cat_OT")).toBeInTheDocument();
      expect(screen.getByText("connection refused")).toBeInTheDocument();
      expect(screen.getAllByText("1.00 KiB").length).toBeGreaterThan(0);
    });
  });

  it("renders shared edge scope notice", async () => {
    render(<PipelineISBDebugInfo loading={false} edgeScoped />);

    await waitFor(() => {
      expect(screen.getByText(/target vertex buffer/)).toBeInTheDocument();
    });
  });
});
