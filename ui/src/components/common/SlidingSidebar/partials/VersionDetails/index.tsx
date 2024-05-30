import React from "react";
import Box from "@mui/material/Box";
import Grid from "@mui/material/Grid";
import Paper from "@mui/material/Paper";

import "./style.css";

export interface VersionDetailsProps {
  Version?: string;
  BuildDate?: string;
  GitCommit?: string;
  GitTag?: string;
  GitTreeState?: string;
  GoVersion?: string;
  Compiler?: string;
  Platform?: string;
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

const versionDetails = [
  "Version",
  "BuildDate",
  "GitCommit",
  "GitTag",
  "GitTreeState",
  "GoVersion",
  "Compiler",
  "Platform",
] as const;

export function VersionDetails(props: VersionDetailsProps) {
  return (
    <Box
      sx={{
        display: "flex",
        flexDirection: "column",
        height: "100%",
      }}
    >
      <span className="version-header-text">Numaflow Version</span>
      <Grid
        container
        spacing={2}
        sx={{
          marginTop: "0.8rem",
          justifyContent: "center",
          fontSize: "1.3rem",
        }}
      >
        {versionDetails.map((detail) => (
          <Grid item xs={12} key={detail}>
            <Paper elevation={0} sx={paperStyle}>
              <span style={keyStyle}>{detail}</span>
              <span>{props?.[detail] ?? "unknown"}</span>
            </Paper>
          </Grid>
        ))}
      </Grid>
    </Box>
  );
}
