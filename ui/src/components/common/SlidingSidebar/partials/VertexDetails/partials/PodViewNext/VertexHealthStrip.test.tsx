import { fireEvent, render, screen } from "@testing-library/react";
import "@testing-library/jest-dom";
import { VertexHealthStrip } from "./VertexHealthStrip";
import { HealthCard, VertexHealth } from "../../../../../../../types/declarations/pods";

const cards: HealthCard[] = [
  {
    id: "latency",
    title: "Latency",
    value: "P99 4.29 s",
    detail: "slowest: sink P99 3.1 s",
    tone: "ok",
    sparkline: [10, 20, 30, 40, 50, 60, 70, 100],
    stages: [
      { name: "read", value: "12 ms" },
      { name: "sink", value: "3.10 s" },
    ],
    stagesNote: "Stages add up to 3.11 s; the rest of the batch time is spent waiting for messages.",
    inspect: { kind: "metrics", metric: "monovtx_processing_time_bucket" },
  },
  {
    id: "traffic",
    title: "Traffic",
    value: "15 msg/s",
    detail: "5m avg 15",
    tone: "ok",
    inspect: { kind: "tab", tab: "processingRates" },
  },
  {
    id: "backlog",
    title: "Backlog",
    value: "Pending not reported",
    detail: "Source does not report pending messages",
    tone: "neutral",
  },
  {
    id: "loss",
    title: "Message outcomes",
    value: "2 drops/s",
    detail: "2 active outcome types · hover for details",
    tone: "warn",
    breakdown: {
      title: "Message outcomes",
      items: [
        {
          name: "Duplicate-message drops",
          value: "2/s",
          description: "Duplicate source messages discarded by MonoVertex.",
          available: true,
        },
        {
          name: "Nacks / redeliveries",
          value: "1/s",
          description: "Messages negatively acknowledged for redelivery. A nack is not a confirmed drop.",
          available: true,
        },
        {
          name: "Fallback deliveries",
          value: "0/s",
          description: "Messages delivered to the fallback sink instead of the primary sink.",
          available: true,
        },
      ],
    },
    inspect: { kind: "metrics", metric: "monovtx_dropped_total" },
  },
  {
    id: "errors",
    title: "Errors",
    value: "1 unhealthy pod",
    detail: "4 restarts · 0 errors (24h)",
    tone: "critical",
    breakdown: {
      title: "Error diagnostics",
      items: [
        {
          name: "Critical runtime errors",
          value: "3/s",
          description: "Severe runtime errors that can interrupt message processing.",
          available: true,
        },
      ],
    },
    inspect: { kind: "critical-pods" },
  },
];

const health: VertexHealth = {
  tone: "critical",
  verdict: { label: "Data flow unknown (D4)", tone: "neutral" },
  replicas: { label: "3/3 ready · scaled 12d ago", tone: "ok" },
  meta: "trends over the last 30 minutes",
  cards,
};

describe("VertexHealthStrip", () => {
  it("renders the header and five cards without numeric prefixes", () => {
    render(<VertexHealthStrip health={health} onInspect={jest.fn()} />);

    expect(screen.getByText("Vertex health")).toBeInTheDocument();
    expect(screen.getByTestId("vertex-health-dot")).toHaveAttribute("data-tone", "critical");
    expect(screen.getByTestId("vertex-health-verdict")).toHaveTextContent("Data flow unknown (D4)");
    expect(screen.getByTestId("vertex-health-replicas")).toHaveTextContent("3/3 ready");
    expect(screen.getByText("trends over the last 30 minutes")).toBeInTheDocument();
    expect(screen.getByText("LATENCY")).toBeInTheDocument();
    expect(screen.getByText("ERRORS")).toBeInTheDocument();
    expect(screen.queryByText("01 · LATENCY")).not.toBeInTheDocument();
    cards.forEach((entry) =>
      expect(
        screen.getByTestId(`vertex-health-card-${entry.id}`).querySelector("svg.lucide")
      ).toBeInTheDocument()
    );
    expect(screen.getByTestId("vertex-health-card-loss").querySelector("p")).toHaveClass(
      "vertex-health-tone--warn"
    );
  });

  it("draws sparklines only for cards with a series", () => {
    render(<VertexHealthStrip health={health} onInspect={jest.fn()} />);

    expect(screen.getByTestId("vertex-health-sparkline-latency").children).toHaveLength(8);
    expect(screen.queryByTestId("vertex-health-sparkline-traffic")).not.toBeInTheDocument();
  });

  it("disables cards without an inspect target", () => {
    render(<VertexHealthStrip health={health} onInspect={jest.fn()} />);

    const backlog = screen.getByTestId("vertex-health-card-backlog");
    expect(backlog).toBeDisabled();
    expect(backlog).not.toHaveTextContent("Inspect →");
  });

  it("shows the per-stage breakdown on the latency card", async () => {
    render(<VertexHealthStrip health={health} onInspect={jest.fn()} />);

    fireEvent.mouseOver(screen.getByTestId("vertex-health-card-latency"));
    const stages = await screen.findByTestId("vertex-health-stages");
    expect(stages).toHaveTextContent("P99 by stage");
    expect(stages).toHaveTextContent("sink3.10 s");
    expect(stages).toHaveTextContent("waiting for messages");
  });

  it("shows all message outcome types and their explanations on the loss card", async () => {
    render(<VertexHealthStrip health={health} onInspect={jest.fn()} />);

    fireEvent.mouseOver(screen.getByTestId("vertex-health-card-loss"));
    const outcomes = await screen.findByTestId("vertex-health-breakdown-loss");
    expect(outcomes).toHaveTextContent("Message outcomes");
    expect(outcomes).toHaveTextContent("Duplicate-message drops");
    expect(outcomes).toHaveTextContent("2/s");
    expect(outcomes).toHaveTextContent("A nack is not a confirmed drop");
    expect(outcomes).toHaveTextContent("Fallback deliveries");
    expect(outcomes).toHaveTextContent("0/s");
    expect(outcomes).not.toHaveTextContent("Critical runtime errors");
  });

  it("shows critical runtime errors in the Errors breakdown", async () => {
    render(<VertexHealthStrip health={health} onInspect={jest.fn()} />);

    fireEvent.mouseOver(screen.getByTestId("vertex-health-card-errors"));
    const errors = await screen.findByTestId("vertex-health-breakdown-errors");
    expect(errors).toHaveTextContent("Error diagnostics");
    expect(errors).toHaveTextContent("Critical runtime errors");
    expect(errors).toHaveTextContent("3/s");
  });

  it("reports each card's inspect target", () => {
    const onInspect = jest.fn();
    render(<VertexHealthStrip health={health} onInspect={onInspect} />);

    ["latency", "traffic", "loss", "errors"].forEach((id) =>
      fireEvent.click(screen.getByTestId(`vertex-health-card-${id}`))
    );
    expect(onInspect.mock.calls.map(([target]) => target)).toEqual([
      cards[0].inspect,
      cards[1].inspect,
      cards[3].inspect,
      cards[4].inspect,
    ]);
  });
});
