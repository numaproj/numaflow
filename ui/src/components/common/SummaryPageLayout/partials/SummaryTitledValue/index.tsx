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
        flexDirection: "column",
        alignItems: "center",
        flexGrow: 1,
        justifyContent: "center",
      }}
    >
      {tooltip && (
        <Box
          sx={{
            display: "flex",
            flexDirection: "row",
            alignItems: "right",
            width: "100%",
            justifyContent: "flex-end",
            paddingRight: "0.2rem",
            marginBottom: "-1rem",
            marginRight: "1rem",
          }}
        >
          <Help tooltip={tooltip} />
        </Box>
      )}
      <span className="summary-titled-value-title">{title}</span>
      <span className="summary-titled-value-value">{value}</span>
    </Box>
  );
}
