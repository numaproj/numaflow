import { ReactElement } from "react";
import Tooltip from "@mui/material/Tooltip";
import { Activity, CircleAlert, Gauge, PackageX, TrendingUp } from "lucide-react";
import {
  HealthCard,
  InspectTarget,
  SignalId,
  VertexHealth,
} from "../../../../../../../types/declarations/pods";

import "./vertexHealth.css";

const ICONS: Record<SignalId, ReactElement> = {
  latency: <Activity size={16} aria-hidden />,
  traffic: <TrendingUp size={16} aria-hidden />,
  backlog: <Gauge size={16} aria-hidden />,
  loss: <PackageX size={16} aria-hidden />,
  errors: <CircleAlert size={16} aria-hidden />,
};

interface VertexHealthStripProps {
  health: VertexHealth;
  onInspect: (target: InspectTarget) => void;
}

function StageBreakdown({
  stages,
  note,
}: {
  stages: NonNullable<HealthCard["stages"]>;
  note?: string;
}) {
  return (
    <div className="vertex-health-stages" data-testid="vertex-health-stages">
      <span className="vertex-health-stages-title">P99 by stage</span>
      {stages.map((stage) => (
        <div key={stage.name} className="vertex-health-stage">
          <span>{stage.name}</span>
          <strong>{stage.value}</strong>
        </div>
      ))}
      {note && <span className="vertex-health-stages-note">{note}</span>}
    </div>
  );
}

function HealthCardButton({
  card,
  onInspect,
}: {
  card: HealthCard;
  onInspect: (target: InspectTarget) => void;
}) {
  const { inspect } = card;
  const button = (
    <button
      type="button"
      className={`vertex-health-card vertex-health-card--${card.id}`}
      data-testid={`vertex-health-card-${card.id}`}
      disabled={!inspect}
      aria-label={`${card.title}: ${card.value}. ${card.detail}`}
      onClick={() => inspect && onInspect(inspect)}
    >
      <div className="vertex-health-card-head">
        <span className="vertex-health-card-label">
          {card.index} · {card.title.toUpperCase()}
        </span>
        <span className="vertex-health-card-icon">{ICONS[card.id]}</span>
      </div>
      <div className="vertex-health-card-value">{card.value}</div>
      <p
        className={`vertex-health-card-detail vertex-health-tone--${card.tone}`}
        title={card.detail}
      >
        {card.detail}
      </p>
      <div className="vertex-health-card-foot">
        {card.sparkline && (
          <>
            <span className="vertex-health-card-dot" />
            <div className="vertex-health-sparkline" data-testid={`vertex-health-sparkline-${card.id}`}>
              {card.sparkline.map((height, index) => (
                <span key={index} style={{ height: `${height}%` }} />
              ))}
            </div>
          </>
        )}
        {inspect && <span className="vertex-health-card-inspect">Inspect →</span>}
      </div>
    </button>
  );
  if (!card.stages?.length) return button;
  return (
    <Tooltip
      title={<StageBreakdown stages={card.stages} note={card.stagesNote} />}
      arrow
      describeChild
      placement="bottom"
    >
      {button}
    </Tooltip>
  );
}

/** VertexHealthStrip summarizes vertex-level stream signals above the Beta Pod View fleet. */
export function VertexHealthStrip({ health, onInspect }: VertexHealthStripProps) {
  return (
    <section className="vertex-health" data-testid="vertex-health">
      <div className="vertex-health-header">
        <div className="vertex-health-heading">
          <span
            className={`vertex-health-dot vertex-health-dot--${health.tone}`}
            data-testid="vertex-health-dot"
            data-tone={health.tone}
          />
          <span className="vertex-health-title">Vertex health</span>
          {health.verdict && (
            <span
              className={`vertex-health-verdict vertex-health-tone--${health.verdict.tone}`}
              data-testid="vertex-health-verdict"
            >
              {health.verdict.label}
            </span>
          )}
        </div>
        <div className="vertex-health-heading">
          {health.replicas && (
            <span
              className={`vertex-health-badge vertex-health-badge--${health.replicas.tone}`}
              data-testid="vertex-health-replicas"
            >
              {health.replicas.label}
            </span>
          )}
          <span className="vertex-health-meta">{health.meta}</span>
        </div>
      </div>
      <div className="vertex-health-grid">
        {health.cards.map((card) => (
          <HealthCardButton key={card.id} card={card} onInspect={onInspect} />
        ))}
      </div>
    </section>
  );
}
