import React from "react";
import Paper from "@mui/material/Paper";
import { Link } from "react-router-dom";
import { PipelineCardProps } from "../../../../../types/declarations/namespace";

import "./style.css";

export function PipelineCard({ namespace, data }: PipelineCardProps) {
  return (
    <Paper
      sx={{
        display: "flex",
        flexDirection: "column",
        padding: "1.5rem",
        width: "100%",
      }}
    >
      <Link
        to={`/namespaces/${namespace}/pipelines/${data.name}`}
        style={{ textDecoration: "none" }}
      >
        <span className="pipeline-card-name">{data.name}</span>
      </Link>
    </Paper>
  );
}
