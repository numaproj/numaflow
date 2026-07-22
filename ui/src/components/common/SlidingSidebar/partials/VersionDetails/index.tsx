import React, { ReactNode } from "react";
import Box from "@mui/material/Box";
import Chip from "@mui/material/Chip";
import { ControllerInfo } from "../../../../../utils/models/controllerInfo";

import "./style.css";

// Server fields come from GET /api/v1/sysinfo (UX server binary).
export interface VersionDetailsProps {
  Version?: string;
  BuildDate?: string;
  GitCommit?: string;
  GitTag?: string;
  GitTreeState?: string;
  GoVersion?: string;
  Compiler?: string;
  Platform?: string;
  // Controller fields come from GET /api/v1/namespaces/:ns/controller-info.
  controllerInfo?: ControllerInfo;
}

function displayValue(value?: string): string {
  return value && value.trim() !== "" ? value : "unknown";
}

function InfoRow({
  label,
  value,
  mono,
  children,
}: {
  label: string;
  value?: string;
  mono?: boolean;
  children?: ReactNode;
}) {
  return (
    <div className="version-info-row">
      <span className="version-info-label">{label}</span>
      <span
        className={
          mono ? "version-info-value version-value-mono" : "version-info-value"
        }
        title={typeof value === "string" ? value : undefined}
      >
        {children ?? displayValue(value)}
      </span>
    </div>
  );
}

function Section({
  title,
  hero,
  children,
}: {
  title: string;
  hero?: string;
  children: ReactNode;
}) {
  return (
    <section className="version-section">
      <h3 className="version-section-title">{title}</h3>
      {hero !== undefined && (
        <div className="version-hero" title={hero}>
          {displayValue(hero)}
        </div>
      )}
      <div className="version-info-list">{children}</div>
    </section>
  );
}

export function VersionDetails(props: VersionDetailsProps) {
  const { controllerInfo } = props;
  const treeState = displayValue(props.GitTreeState);
  const isDirty = treeState.toLowerCase() === "dirty";
  const controllerScope = controllerInfo?.found
    ? controllerInfo.namespaced
      ? "Namespace"
      : "Cluster"
    : undefined;

  return (
    <Box
      className="version-details"
      sx={{
        display: "flex",
        flexDirection: "column",
        height: "100%",
        overflow: "auto",
      }}
    >
      <h2 className="version-header-text">Version details</h2>

      {/* Server version from /api/v1/sysinfo */}
      <Section title="Server" hero={props.Version}>
        <InfoRow label="Build date" value={props.BuildDate} />
        <InfoRow label="Git commit" value={props.GitCommit} mono />
        <InfoRow label="Git tag" value={props.GitTag} />
        <InfoRow label="Tree state" value={treeState}>
          <Chip
            size="small"
            label={treeState}
            color={isDirty ? "warning" : "default"}
            variant={isDirty ? "filled" : "outlined"}
            sx={{ fontSize: "1.1rem", height: "2.2rem" }}
          />
        </InfoRow>
        <InfoRow label="Go version" value={props.GoVersion} />
        <InfoRow label="Compiler" value={props.Compiler} />
        <InfoRow label="Platform" value={props.Platform} />
      </Section>

      {/* Controller version/scope from /api/v1/namespaces/:ns/controller-info */}
      <Section
        title="Controller"
        hero={controllerInfo?.found ? controllerInfo.version : undefined}
      >
        {controllerInfo?.found ? (
          <>
            <InfoRow label="Image" value={controllerInfo.image} mono />
            <InfoRow label="Scope" value={controllerScope}>
              <Chip
                size="small"
                label={controllerScope}
                color="primary"
                variant="outlined"
                sx={{ fontSize: "1.1rem", height: "2.2rem" }}
              />
            </InfoRow>
            <InfoRow
              label="Managed namespace"
              value={controllerInfo.managedNamespace || "—"}
            />
            <InfoRow label="Namespace" value={controllerInfo.namespace} />
            <InfoRow label="Deployment" value={controllerInfo.name} />
          </>
        ) : (
          <p className="version-empty-state">
            No controller found in this namespace
          </p>
        )}
      </Section>
    </Box>
  );
}
