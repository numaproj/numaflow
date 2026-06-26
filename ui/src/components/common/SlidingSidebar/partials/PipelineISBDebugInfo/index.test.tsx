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
      expect(
        screen.getByText("ISB information is not available yet.")
      ).toBeInTheDocument();
      expect(screen.queryByText(/boom/)).not.toBeInTheDocument();
    });
  });

  it("renders neutral fallback for empty ISB information", async () => {
    render(
      <PipelineISBDebugInfo
        loading={false}
        streams={{ streams: [] }}
        consumers={{ consumers: [] }}
        kvStores={{ kvStores: [] }}
      />
    );

    await waitFor(() => {
      expect(
        screen.getByText("ISB information is not available yet.")
      ).toBeInTheDocument();
    });
  });

  it("renders unavailable message with monitor errors when there are no rows", async () => {
    render(
      <PipelineISBDebugInfo
        loading={false}
        streams={{
          streams: [],
          errors: [{ pod: "js-1", message: "connection refused" }],
        }}
      />
    );

    await waitFor(() => {
      expect(
        screen.getByText("ISB information is not available yet.")
      ).toBeInTheDocument();
      expect(
        screen.queryByText("Some ISB information is not available yet.")
      ).not.toBeInTheDocument();
      expect(screen.getByText("Monitor Errors")).toBeInTheDocument();
      expect(screen.getByText("connection refused")).toBeInTheDocument();
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
            {
              namespace: "ns",
              pipeline: "pl",
              vertex: "cat",
              partition: 1,
              stream: "ns-pl-cat-1",
              consumer: "ephemeral-consumer",
              ackPolicy: "explicit",
              deliverPolicy: "all",
              ackWaitSeconds: 30,
              maxAckPending: 25000,
              numAckPending: 0,
              numRedelivered: 0,
              numWaiting: 0,
              numPending: 0,
              deliveredConsumerSeq: 0,
              deliveredStreamSeq: 0,
              ackFloorConsumerSeq: 0,
              ackFloorStreamSeq: 0,
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
              ttlSeconds: 0,
              replicas: 3,
              storage: "file",
            },
          ],
        }}
      />
    );

    await waitFor(() => {
      expect(screen.getByText("Stream Information")).toBeInTheDocument();
      expect(screen.getByText("Messages")).toBeInTheDocument();
      expect(screen.getAllByText("Bytes").length).toBeGreaterThan(0);
      expect(screen.getByText("Consumers")).toBeInTheDocument();
      expect(screen.getAllByText("Replicas").length).toBeGreaterThan(0);
      expect(screen.getByText("Ack Pending")).toBeInTheDocument();
      expect(screen.getByText("Pending")).toBeInTheDocument();
      expect(
        screen.getByTestId("isb-debug-header-help-stream-messages")
      ).toBeInTheDocument();
      expect(
        screen.getByTestId("isb-debug-header-help-consumer-ack-pending")
      ).toBeInTheDocument();
      expect(
        screen.getByTestId("isb-debug-header-help-kv-ttl")
      ).toBeInTheDocument();
      expect(screen.getAllByText("ns-pl-cat-0").length).toBeGreaterThan(1);
      expect(screen.getByText("Yes")).toBeInTheDocument();
      expect(screen.getByText("No")).toBeInTheDocument();
      expect(screen.getByText("ns-pl-in-cat_OT")).toBeInTheDocument();
      expect(screen.getByText("connection refused")).toBeInTheDocument();
      expect(screen.getAllByText("1.00 KiB").length).toBeGreaterThan(0);
      expect(screen.getByText("0.00s")).toBeInTheDocument();
      expect(screen.queryByText("Subjects")).not.toBeInTheDocument();
      expect(screen.queryByText("Filter Subject")).not.toBeInTheDocument();
      expect(screen.queryByText("Storage")).not.toBeInTheDocument();
    });
  });

  it("renders shared edge scope notice", async () => {
    render(
      <PipelineISBDebugInfo
        loading={false}
        edgeScoped
        kvStores={{
          kvStores: [
            {
              namespace: "ns",
              pipeline: "pl",
              scope: "edge",
              from: "in",
              to: "cat",
              bucket: "ns-pl-in-cat_OT",
              stream: "KV_ns-pl-in-cat_OT",
              values: 2,
              bytes: 2048,
            },
          ],
        }}
      />
    );

    await waitFor(() => {
      expect(screen.getByText(/target vertex buffer/)).toBeInTheDocument();
      expect(
        screen.getByText("Some ISB information is not available yet.")
      ).toBeInTheDocument();
      expect(screen.getByText("ns-pl-in-cat_OT")).toBeInTheDocument();
      expect(
        screen.getAllByText("ISB information is not available yet.").length
      ).toBeGreaterThan(0);
    });
  });
});
