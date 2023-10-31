import React from "react";
import Box from "@mui/material/Box";
import { Help } from "../../../Help";

import "./style.css";

export interface SummaryTitledValueProps {
  title: string;
  value: string | number;
  tooltip?: string;
}

export function SummaryTitledValue({
  title,
  value,
  tooltip,
}: SummaryTitledValueProps) {
  return (
    <Box
      sx={{
        display: "flex",
        flexDirection: "row",
        alignItems: "center",
        flexGrow: 1,
        justifyContent: "center",
      }}
    >
      <Box
        sx={{
          display: "flex",
          flexDirection: "column",
          flexGrow: 1,
          paddingLeft: "1rem"
        }}
      >
        <span className="summary-titled-value-title">{title}</span>
        <span className="summary-titled-value-value">{value}</span>
      </Box>
      {tooltip && (
        <Box
          sx={{
            display: "flex",
            flexDirection: "row",
            alignItems: "right",
            height: "100%",
            justifyContent: "flex-end",
            marginRight: "1rem",
          }}
        >
          <Help tooltip={tooltip} />
        </Box>
      )}
    </Box>
  );
}
