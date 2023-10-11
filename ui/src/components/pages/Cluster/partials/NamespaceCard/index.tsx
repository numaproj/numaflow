import React from "react";
import Box from "@mui/material/Box";
import Paper from "@mui/material/Paper";
import Grid from "@mui/material/Grid";
import { Link } from "react-router-dom";
import { StatusBar } from "../../../../common/StatusBar";
import { NamespaceCardProps } from "../../../../../types/declarations/cluster";

import "./style.css";
import {
  CRITICAL,
  HEALTHY,
  IconsStatusMap,
  WARNING,
} from "../../../../../utils";

export function NamespaceCard({ data }: NamespaceCardProps) {
  return (
    <Link to={`/namespaces/${data.name}`} style={{ textDecoration: "none" }}>
      <Paper
        sx={{
          display: "flex",
          flexDirection: "column",
          padding: "1.5rem",
          minWidth: "27.1875rem",
        }}
      >
        <span className="namespace-card-name">{data.name}</span>
        <Box
          sx={{
            display: "flex",
            flexDirection: "row",
            marginTop: "1.6875rem",
          }}
        >
          <span className="namespace-card-section-title">Pipelines:</span>
          <span className="namespace-card-section-title-value">
            {data.pipelinesCount}
          </span>
        </Box>
        <Grid
          container
          rowSpacing={0}
          columnSpacing={0}
          sx={{
            borderBottom: "0.03125rem solid #8D9096",
            marginTop: "0.5rem",
          }}
        >
          <Grid item xs={2}>
            <span className="namespace-card-section-text-14-normal">
              Status:
            </span>
          </Grid>
          <Grid item xs={4}>
            <Box
              sx={{
                display: "flex",
                flexDirection: "row",
                borderRight: "0.03125rem solid #8D9096",
                width: "67%",
              }}
            >
              <span className="namespace-card-section-text-14-bold">
                {data?.pipelinesActiveCount}
              </span>
              <span className="namespace-card-section-text-14-normal namespace-card-group-spacing">
                Active
              </span>
            </Box>
          </Grid>
          <Grid item xs={4}>
            <Box sx={{ display: "flex", flexDirection: "row" }}>
              <span className="namespace-card-section-text-14-bold">
                {data?.pipelinesInactiveCount}
              </span>
              <span className="namespace-card-section-text-14-normal namespace-card-group-spacing">
                Non-Active
              </span>
            </Box>
          </Grid>
        </Grid>
        <Grid
          container
          rowSpacing={0}
          columnSpacing={0}
          sx={{
            borderBottom: "0.03125rem solid #8D9096",
            marginTop: "0.5rem",
          }}
        >
          <Grid item xs={2} sx={{ display: "flex", alignItems: "center" }}>
            <span className="namespace-card-section-text-14-normal">
              Health:
            </span>
          </Grid>
          <Grid item xs={8} sx={{ padding: "0.3125rem 0" }}>
            <Grid container sx={{ flexDirection: "row" }}>
              <Grid
                item
                xs={4}
                sx={{
                  display: "flex",
                  flexDirection: "row",
                  alignItems: "center",
                }}
              >
                <span className="namespace-card-section-text-14-normal">
                  Healthy
                </span>
                <img
                  src={IconsStatusMap[HEALTHY]}
                  alt="Healthy"
                  className={"status-logo"}
                />
                <span className={"status-count"}>
                  : {data?.pipelinesHealthyCount}
                </span>
              </Grid>
              <Grid
                item
                xs={4}
                sx={{
                  display: "flex",
                  flexDirection: "row",
                  alignItems: "center",
                }}
              >
                <span className="namespace-card-section-text-14-normal">
                  Warning
                </span>
                <img
                  src={IconsStatusMap[WARNING]}
                  alt="Warning"
                  className={"status-logo"}
                />
                <span className={"status-count"}>
                  : {data?.pipelinesWarningCount}
                </span>
              </Grid>
              <Grid
                item
                xs={4}
                sx={{
                  display: "flex",
                  flexDirection: "row",
                  alignItems: "center",
                }}
              >
                <span className="namespace-card-section-text-14-normal">
                  Critical
                </span>
                <img
                  src={IconsStatusMap[CRITICAL]}
                  alt="Critical"
                  className={"status-logo"}
                />
                <span className={"status-count"}>
                  : {data?.pipelinesCriticalCount}
                </span>
              </Grid>
            </Grid>
          </Grid>
        </Grid>
        <Box
          sx={{
            display: "flex",
            flexDirection: "row",
            marginTop: "1.6875rem",
          }}
        >
          <span className="namespace-card-section-title">ISB Services:</span>
          <span className="namespace-card-section-title-value">
            {data.isbsCount}
          </span>
        </Box>
        <Grid
          container
          rowSpacing={0}
          columnSpacing={0}
          sx={{
            borderBottom: "0.03125rem solid #8D9096",
            marginTop: "0.5rem",
          }}
        >
          <Grid item xs={2}>
            <span className="namespace-card-section-text-14-normal">
              Status:
            </span>
          </Grid>
          <Grid item xs={4}>
            <Box
              sx={{
                display: "flex",
                flexDirection: "row",
                borderRight: "0.03125rem solid #8D9096",
                width: "67%",
              }}
            >
              <span className="namespace-card-section-text-14-bold">
                {data?.isbsActiveCount}
              </span>
              <span className="namespace-card-section-text-14-normal namespace-card-group-spacing">
                Live
              </span>
            </Box>
          </Grid>
          <Grid item xs={4}>
            <Box sx={{ display: "flex", flexDirection: "row" }}>
              <span className="namespace-card-section-text-14-bold">
                {data?.isbsInactiveCount}
              </span>
              <span className="namespace-card-section-text-14-normal namespace-card-group-spacing">
                Not-Live
              </span>
            </Box>
          </Grid>
        </Grid>
        <Grid
          container
          rowSpacing={0}
          columnSpacing={0}
          sx={{
            marginTop: "0.5rem",
          }}
        >
          <Grid item xs={2} sx={{ display: "flex", alignItems: "center" }}>
            <span className="namespace-card-section-text-14-normal">
              Health:
            </span>
          </Grid>
          <Grid item xs={8} sx={{ padding: "0.3125rem 0" }}>
            <Grid container sx={{ flexDirection: "row" }}>
              <Grid
                item
                xs={4}
                sx={{
                  display: "flex",
                  flexDirection: "row",
                  alignItems: "center",
                }}
              >
                <span className="namespace-card-section-text-14-normal">
                  Healthy
                </span>
                <img
                  src={IconsStatusMap[HEALTHY]}
                  alt="Healthy"
                  className={"status-logo"}
                />
                <span className={"status-count"}>
                  : {data?.isbsHealthyCount}
                </span>
              </Grid>
              <Grid
                item
                xs={4}
                sx={{
                  display: "flex",
                  flexDirection: "row",
                  alignItems: "center",
                }}
              >
                <span className="namespace-card-section-text-14-normal">
                  Warning
                </span>
                <img
                  src={IconsStatusMap[WARNING]}
                  alt="Warning"
                  className={"status-logo"}
                />
                <span className={"status-count"}>
                  : {data?.isbsWarningCount}
                </span>
              </Grid>
              <Grid
                item
                xs={4}
                sx={{
                  display: "flex",
                  flexDirection: "row",
                  alignItems: "center",
                }}
              >
                <span className="namespace-card-section-text-14-normal">
                  Critical
                </span>
                <img
                  src={IconsStatusMap[CRITICAL]}
                  alt="Critical"
                  className={"status-logo"}
                />
                <span className={"status-count"}>
                  : {data?.isbsCriticalCount}
                </span>
              </Grid>
            </Grid>
          </Grid>
        </Grid>
      </Paper>
    </Link>
  );
}
