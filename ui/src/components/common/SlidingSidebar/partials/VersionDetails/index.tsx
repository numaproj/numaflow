import React from "react";
import Box from "@mui/material/Box";
import Grid from "@mui/material/Grid";
import Paper from "@mui/material/Paper";
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

const paperStyle = {
  display: "flex",
  padding: "1.6rem",
  overflow: "scroll",
};

const keyStyle = {
  fontWeight: "bold",
  marginRight: "1rem",
  minWidth: "10rem",
};

const serverVersionDetails = [
  "Version",
  "BuildDate",
  "GitCommit",
  "GitTag",
  "GitTreeState",
  "GoVersion",
  "Compiler",
  "Platform",
] as const;

function DetailRow({ label, value }: { label: string; value?: string }) {
  return (
    <Grid item xs={12}>
      <Paper elevation={0} sx={paperStyle}>
        <span style={keyStyle}>{label}</span>
        <span>{value ?? "unknown"}</span>
      </Paper>
    </Grid>
  );
}

export function VersionDetails(props: VersionDetailsProps) {
  const { controllerInfo } = props;
  const controllerScope = controllerInfo?.found
    ? controllerInfo.namespaced
      ? "Namespace"
      : "Cluster"
    : undefined;

  return (
    <Box
      sx={{
        display: "flex",
        flexDirection: "column",
        height: "100%",
        overflow: "auto",
      }}
    >
      {/* Server version from /api/v1/sysinfo */}
      <span className="version-header-text">Numaflow Server Version</span>
      <Grid
        container
        spacing={2}
        sx={{
          marginTop: "0.8rem",
          justifyContent: "center",
          fontSize: "1.3rem",
        }}
      >
        {serverVersionDetails.map((detail) => (
          <DetailRow
            key={detail}
            label={detail}
            value={props?.[detail] ?? "unknown"}
          />
        ))}
      </Grid>

      {/* Controller version/scope from /api/v1/namespaces/:ns/controller-info */}
      <span
        className="version-header-text"
        style={{ marginTop: "2.4rem" }}
      >
        Numaflow Controller
      </span>
      <Grid
        container
        spacing={2}
        sx={{
          marginTop: "0.8rem",
          justifyContent: "center",
          fontSize: "1.3rem",
          paddingBottom: "1.6rem",
        }}
      >
        {controllerInfo?.found ? (
          <>
            <DetailRow label="Version" value={controllerInfo.version} />
            <DetailRow label="Image" value={controllerInfo.image} />
            <DetailRow label="Scope" value={controllerScope} />
            <DetailRow
              label="Managed Namespace"
              value={controllerInfo.managedNamespace || "—"}
            />
            <DetailRow label="Namespace" value={controllerInfo.namespace} />
            <DetailRow label="Deployment" value={controllerInfo.name} />
          </>
        ) : (
          <Grid item xs={12}>
            <Paper elevation={0} sx={paperStyle}>
              <span>No controller found in this namespace</span>
            </Paper>
          </Grid>
        )}
      </Grid>
    </Box>
  );
}
