import React, { useMemo, useState, useCallback } from "react";
import Box from "@mui/material/Box";
import chevronLeft from "../../../images/chevron-m-left.png";
import chevronRight from "../../../images/chevron-m-right.png";

import "./style.css";

const SUMMARY_HEIGHT = "6.5625rem";
const COLLAPSED_HEIGHT = "2.25rem";

export interface SummaryPageLayoutProps {
  collapsable?: boolean;
  defaultCollapsed?: boolean;
  offsetOnCollapse?: boolean; // Add top margin to content when collapsed to avoid content overlap
  collapsedText?: string;
  summaryComponent: React.ReactNode;
  contentComponent: React.ReactNode;
}

export function SummaryPageLayout({
  collapsable = false,
  defaultCollapsed = false,
  offsetOnCollapse = false,
  collapsedText = "Details",
  summaryComponent,
  contentComponent,
}: SummaryPageLayoutProps) {
  const [collapsed, setCollapsed] = useState(collapsable && defaultCollapsed);

  const toggleCollapsed = useCallback(() => {
    if (!collapsable) {
      return;
    }
    setCollapsed((prev) => !prev);
  }, [collapsable]);

  const summary = useMemo(() => {
    if (collapsed) {
      return (
        <Box
          sx={{
            display: "flex",
            flexDirection: "row",
            height: COLLAPSED_HEIGHT,
            background: "#F8F8FB",
            boxShadow: "0px 4px 6px rgba(39, 76, 119, 0.16)",
            zIndex: 999,
            position: "fixed",
            top: "5.75rem",
            padding: "0 1.25rem",
            alignItems: "center",
          }}
        >
          <span className={"summary-collapsed-text"}>{collapsedText}</span>
          <img
            onClick={toggleCollapsed}
            src={chevronRight}
            alt="expand button"
            className={"summary-expand-button"}
          />
        </Box>
      );
    }
    return (
      <Box
        sx={{
          display: "flex",
          flexDirection: "row",
          width: "100%",
          minHeight: SUMMARY_HEIGHT,
          background: "#F8F8FB",
          boxShadow: "0px 3px 11px rgba(39, 76, 119, 0.16)",
          zIndex: 999,
          position: "fixed",
          top: "5.75rem",
          padding: "0.5rem"
        }}
      >
        <Box
          sx={{
            display: "flex",
            width: "100%",
          }}
        >
          {summaryComponent}
        </Box>
        {collapsable && (
          <img
            onClick={toggleCollapsed}
            src={chevronLeft}
            alt="collapse button"
            className={"summary-collapse-button"}
          />
        )}
      </Box>
    );
  }, [summaryComponent, collapsed, collapsable, toggleCollapsed, collapsedText]);

  const contentMargin = useMemo(() => {
    if (collapsed) {
      return offsetOnCollapse ? COLLAPSED_HEIGHT : undefined;
    }
    return SUMMARY_HEIGHT;
  }, [collapsed, offsetOnCollapse]);

  return (
    <Box className="SummaryPageLayout">
      {summary}
      <Box
        sx={{
          marginTop: contentMargin,
          paddingTop: "1.25rem",
        }}
        className="SummaryPageLayout__content"
      >
        {contentComponent}
      </Box>
    </Box>
  );
}
