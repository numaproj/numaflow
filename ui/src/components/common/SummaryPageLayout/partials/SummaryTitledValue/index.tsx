import React from "react";
import Box from "@mui/material/Box";

import "./style.css";

export interface SummaryTitledValueProps {
  title: string;
  value: string | number;
}

export function SummaryTitledValue({ title, value }: SummaryTitledValueProps) {
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
      <span className="summary-titled-value-title">{title}</span>
      <span className="summary-titled-value-value">{value}</span>
    </Box>
  );
}
