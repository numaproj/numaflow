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
              lastTimestamp: "2026-06-30T00:01:00Z",
              storage: "file",
              replicas: 3,
              retention: "workqueue",
              maxMessages: 100000,
              leader: "js-0",
              sourcePod: "js-0",
              collectedAt: "2026-06-30T01:02:03Z",
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
              sourcePod: "js-0",
              collectedAt: "2026-06-30T01:02:03Z",
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
              sourcePod: "js-0",
              collectedAt: "2026-06-30T01:02:03Z",
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
              leader: "js-0",
              sourcePod: "js-0",
              collectedAt: "2026-06-30T01:02:03Z",
            },
          ],
        }}
      />
    );

    await waitFor(() => {
      expect(screen.getByText("Stream Information")).toBeInTheDocument();
      expect(
        screen.getByTestId("isb-debug-consumers-table")
      ).toBeInTheDocument();
      expect(screen.getByTestId("isb-debug-streams-table")).toBeInTheDocument();
      expect(
        screen.getByTestId("isb-debug-kv-stores-table")
      ).toBeInTheDocument();
      expect(screen.getByText("Stored Messages")).toBeInTheDocument();
      expect(screen.getAllByText("Bytes").length).toBeGreaterThan(0);
      expect(screen.getByText("First Seq")).toBeInTheDocument();
      expect(screen.getByText("Last Seq")).toBeInTheDocument();
      expect(screen.getByText("Last Timestamp")).toBeInTheDocument();
      expect(screen.getByText("Consumer Count")).toBeInTheDocument();
      expect(screen.queryByText("Retention")).not.toBeInTheDocument();
      expect(screen.queryByText("Max Messages")).not.toBeInTheDocument();
      expect(screen.queryByText("Storage")).not.toBeInTheDocument();
      expect(screen.getAllByText("Replicas").length).toBeGreaterThan(0);
      expect(screen.getByText("Redelivered")).toBeInTheDocument();
      expect(screen.getByText("Waiting Pulls")).toBeInTheDocument();
      expect(screen.getByText("Delivered Seq")).toBeInTheDocument();
      expect(screen.getByText("Ack Floor Seq")).toBeInTheDocument();
      expect(screen.getByText("Ack Wait")).toBeInTheDocument();
      expect(screen.getByText("Max Ack Pending")).toBeInTheDocument();
      expect(
        screen.queryByTestId("isb-debug-header-help-consumer-ack-pending")
      ).not.toBeInTheDocument();
      expect(
        screen.queryByTestId("isb-debug-header-help-consumer-pending")
      ).not.toBeInTheDocument();
      expect(
        screen.getByTestId("isb-debug-header-help-stream-messages")
      ).toBeInTheDocument();
      expect(
        screen.getByTestId("isb-debug-header-help-consumer-redelivered")
      ).toBeInTheDocument();
      expect(
        screen.getByTestId("isb-debug-header-help-kv-ttl")
      ).toBeInTheDocument();
      expect(
        screen.getByText(/Diagnostic snapshot from JetStream monitor/)
      ).toBeInTheDocument();
      expect(screen.getByText(/Source pods: js-0/)).toBeInTheDocument();
      expect(screen.getAllByText("ns-pl-cat-0").length).toBeGreaterThan(1);
      expect(screen.getByText("ns-pl-in-cat_OT")).toBeInTheDocument();
      expect(screen.getByText("Scope")).toBeInTheDocument();
      expect(screen.getByText("edge")).toBeInTheDocument();
      expect(screen.getByText("connection refused")).toBeInTheDocument();
      expect(screen.getAllByText("1.00 KiB").length).toBeGreaterThan(0);
      expect(screen.getByText("0.00s")).toBeInTheDocument();
      expect(screen.queryByText("Subjects")).not.toBeInTheDocument();
      expect(screen.queryByText("Filter Subject")).not.toBeInTheDocument();
      expect(screen.queryByText("Durable")).not.toBeInTheDocument();
      expect(screen.queryByText("Ack Policy")).not.toBeInTheDocument();
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
