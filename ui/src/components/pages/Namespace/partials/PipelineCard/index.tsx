import React from "react";
import Paper from "@mui/material/Paper";
import { Link } from "react-router-dom";
import { PipelineCardProps } from "../../../../../types/declarations/namespace";

import "./style.css";
import { Box, Button, ButtonGroup } from "@mui/material";

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
        <Box
          sx={{
            display: "flex",
            flexDirection: "row",
            flexGrow: 1,
          }}
        >
          <Box
            sx={{
              display: "flex",
              flexDirection: "row",
              flexGrow: 1,
            }}
          >
            <span className="pipeline-card-name">{data.name}</span>
          </Box>
          <Box
            sx={{
              display: "flex",
              flexDirection: "row",
              flexGrow: 1,
              justifyContent: "flex-end",
            }}
          >
            <Button variant="contained" sx={{ marginRight: "10px" }}>
              Resume
            </Button>
            <Button variant="contained">Pause</Button>
          </Box>
        </Box>
      </Link>
    </Paper>
  );
}
