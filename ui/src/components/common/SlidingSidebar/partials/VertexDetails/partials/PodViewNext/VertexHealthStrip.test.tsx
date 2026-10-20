import { fireEvent, render, screen } from "@testing-library/react";
import "@testing-library/jest-dom";
import { VertexHealthStrip } from "./VertexHealthStrip";
import { HealthCard, VertexHealth } from "../../../../../../../types/declarations/pods";

const cards: HealthCard[] = [
  {
    id: "latency",
    index: "01",
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
    index: "02",
    title: "Traffic",
    value: "15 msg/s",
    detail: "5m avg 15",
    tone: "ok",
    inspect: { kind: "tab", tab: "processingRates" },
  },
  {
    id: "backlog",
    index: "03",
    title: "Backlog",
    value: "Pending not reported",
    detail: "Source does not report pending messages",
    tone: "neutral",
  },
  {
    id: "loss",
    index: "04",
    title: "Data loss",
    value: "2 drops/s",
    detail: "nacks 0/s",
    tone: "critical",
    inspect: { kind: "metrics", metric: "monovtx_dropped_total" },
  },
  {
    id: "errors",
    index: "05",
    title: "Errors",
    value: "1 unhealthy pod",
    detail: "4 restarts · 0 errors (24h)",
    tone: "critical",
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
  it("renders the header and five cards with Figma labels and lucide icons", () => {
    render(<VertexHealthStrip health={health} onInspect={jest.fn()} />);

    expect(screen.getByText("Vertex health")).toBeInTheDocument();
    expect(screen.getByTestId("vertex-health-dot")).toHaveAttribute("data-tone", "critical");
    expect(screen.getByTestId("vertex-health-verdict")).toHaveTextContent("Data flow unknown (D4)");
    expect(screen.getByTestId("vertex-health-replicas")).toHaveTextContent("3/3 ready");
    expect(screen.getByText("trends over the last 30 minutes")).toBeInTheDocument();
    expect(screen.getByText("01 · LATENCY")).toBeInTheDocument();
    expect(screen.getByText("05 · ERRORS")).toBeInTheDocument();
    cards.forEach((entry) =>
      expect(
        screen.getByTestId(`vertex-health-card-${entry.id}`).querySelector("svg.lucide")
      ).toBeInTheDocument()
    );
    expect(screen.getByTestId("vertex-health-card-loss").querySelector("p")).toHaveClass(
      "vertex-health-tone--critical"
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
