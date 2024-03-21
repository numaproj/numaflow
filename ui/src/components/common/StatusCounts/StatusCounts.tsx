import React from "react";
import { IconsStatusMap } from "../../../utils";
import Box from "@mui/material/Box";

import "./style.css";
export interface StatusCountsProps {
  counts: {
    healthy: number;
    warning: number;
    critical: number;
    [key: string]: number;
  };
}

export function StatusCounts(counts: StatusCountsProps) {
  return (
    <Box sx={{ display: "flex", flexDirection: "row", marginLeft: "0.5rem" }}>
      {Object.keys(counts.counts).map((key) => {
        return (
          <Box
            sx={{
              display: "flex",
              flexDirection: "row",
              alignItems: "center",
              justifyContent: "center",
              flexGrow: "1",
            }}
            key={key}
          >
            <Box sx={{ display: "flex", flexDirection: "column" }}>
              <Box sx={{ display: "flex", flexDirection: "row" }}>
                <img
                  src={
                    counts.counts[key] > 0
                      ? IconsStatusMap[key]
                      : IconsStatusMap[`${key}0`]
                  }
                  alt={key}
                  className="status-icon-img"
                />
                <span style={{ marginLeft: "0.5rem" }} className="bold-text">
                  : {counts.counts[key]}
                </span>
              </Box>
              <Box
                sx={{
                  display: "flex",
                  flexDirection: "row",
                  textTransform: "capitalize",
                  color: "#3C4348",
                  fontSize: "0.75rem",
                  fontWeight: "400",
                  lineHeight: "1.46875rem",
                  wordWrap: "break-word",
                }}
              >
                {key}
              </Box>
            </Box>
          </Box>
        );
      })}
    </Box>
  );
}
