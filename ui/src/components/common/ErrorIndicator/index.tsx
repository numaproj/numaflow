import React, { useContext, useCallback } from "react";
import Box from "@mui/material/Box";
import Paper from "@mui/material/Paper";
import Tooltip from "@mui/material/Tooltip";
import { AppContextProps } from "../../../types/declarations/app";
import { AppContext } from "../../../App";
import { SidebarType } from "../SlidingSidebar";
import ErrorOutlineIcon from "@mui/icons-material/ErrorOutline";
import ErrorIcon from "@mui/icons-material/Error";
import { ERRORS_TOOLTIP } from "../../../utils";

import "./style.css";

export function ErrorIndicator() {
  const { errors, setSidebarProps } = useContext<AppContextProps>(AppContext);

  const onErrorClick = useCallback(() => {
    setSidebarProps({
      type: SidebarType.ERRORS,
      slide: false,
    });
  }, []);

  return (
    <Tooltip
      title={<div className="error-tooltip">{ERRORS_TOOLTIP}</div>}
      arrow
    >
      <Paper
        elevation={1}
        sx={{
          cursor: "pointer",
          padding: "0.4rem 0.8rem",
        }}
        onClick={onErrorClick}
      >
        <Box
          sx={{ display: "flex", flexDirection: "row", alignItems: "center" }}
        >
          {errors && errors.length ? (
            <ErrorIcon
              sx={{ color: "#D52B1E", height: "2.4rem", width: "2.4rem" }}
            />
          ) : (
            <ErrorOutlineIcon
              sx={{ color: "#6B6C72", height: "2.4rem", width: "2.4rem" }}
            />
          )}
          {errors?.length ? (
            <span className="error-indicator-text">Error occurred</span>
          ) : undefined}
        </Box>
      </Paper>
    </Tooltip>
  );
}
