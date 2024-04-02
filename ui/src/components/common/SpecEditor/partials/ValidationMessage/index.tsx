import React from "react";
import Box from "@mui/material/Box";
import SuccessIcon from "@mui/icons-material/CheckCircle";
import ErrorIcon from "../../../../../images/warning-triangle.png";

import "./style.css";

export interface ValidationMessageProps {
  type: "error" | "success";
  title: string;
  content: string | React.ReactNode;
}

export function ValidationMessage({
  type,
  title,
  content,
}: ValidationMessageProps) {
  return (
    <Box
      sx={{
        display: "flex",
        flexDirection: "row",
        alignItems: "center",
      }}
    >
      <Box
        sx={{
          display: "flex",
          flexDirection: "row",
        }}
      >
        {type === "error" && (
          <img
            src={ErrorIcon}
            alt="Error"
            className="validation-message-icon"
          />
        )}
        {type === "success" && (
          <SuccessIcon
            fontSize="large"
            color="success"
            sx={{ marginRight: "0.8rem", height: "3.5rem", width: "3.5rem" }}
          />
        )}
      </Box>
      <Box
        sx={{
          display: "flex",
          flexDirection: "column",
        }}
      >
        <span className="validation-message-title">{title}</span>
        <span className="validation-message-content">{content}</span>
      </Box>
    </Box>
  );
}
