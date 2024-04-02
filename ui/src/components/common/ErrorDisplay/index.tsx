import React from "react";
import Box from "@mui/material/Box";
import ErrorIcon from "../../../images/warning-triangle.png";

import "./style.css";

export interface ErrorDisplayProps {
  title: string;
  message: string;
}

export function ErrorDisplay({ title, message }: ErrorDisplayProps) {
  return (
    <Box
      sx={{
        display: "flex",
        flexDirection: "column",
        height: "100%",
        width: "100%",
        alignItems: "center",
        justifyContent: "center",
        maxWidth: "60rem",
      }}
    >
      <Box
        sx={{
          display: "flex",
          flexDirection: "row",
          alignItems: "center",
        }}
      >
        <img src={ErrorIcon} alt="Error" className="error-display-icon" />
        <Box
          sx={{
            display: "flex",
            flexDirection: "column",
            marginLeft: "0.8rem",
          }}
        >
          <span className="error-display-title">{title}</span>
          <span className="error-display-message">{message}</span>
        </Box>
      </Box>
    </Box>
  );
}
