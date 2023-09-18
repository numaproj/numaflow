import React, {
  useMemo,
  useState,
  useCallback,
  useEffect,
  useRef,
} from "react";
import Box from "@mui/material/Box";
import {
  SummaryTitledValueProps,
  SummaryTitledValue,
} from "./partials/SummaryTitledValue";
import {
  SummaryStatuses,
  SummaryStatusesProps,
} from "./partials/SummaryStatuses";
import chevronLeft from "../../../images/chevron-m-left.png";
import chevronRight from "../../../images/chevron-m-right.png";

import "./style.css";

const SUMMARY_HEIGHT = "6.5625rem";
const COLLAPSED_HEIGHT = "2.25rem";

const getSummaryComponent = (summarySections: SummarySection[]) => {
  // Build sections from props
  const components: React.ReactNode[] = [];
  let key: string;
  summarySections.forEach((section, index) => {
    switch (section.type) {
      case SummarySectionType.TITLED_VALUE:
        if (!section.titledValueProps) {
          key = "titled-value-missing";
          components.push(<div key={key}>Missing props</div>);
          break;
        }
        key = `titled-value-${section.titledValueProps.title}`;
        components.push(
          <SummaryTitledValue key={key} {...section.titledValueProps} />
        );
        break;
      case SummarySectionType.STATUSES:
        if (!section.statusesProps) {
          key = "statuses-missing";
          components.push(<div key={key}>Missing props</div>);
          break;
        }
        key = `statuses-${section.statusesProps.title}`;
        components.push(
          <SummaryStatuses key={key} {...section.statusesProps} />
        );
        break;
      case SummarySectionType.CUSTOM:
        if (!section.customComponent) {
          key = "custom-missing";
          components.push(<div key={key}>Missing props</div>);
          break;
        }
        components.push(section.customComponent);
        break;
      default:
        key = "unknown";
        components.push(<div key={key}>Missing props</div>);
        break;
    }
    // Add separator if not last section
    if (index !== summarySections.length - 1) {
      components.push(
        <div
          key={`${key}-separator`}
          className="summary-page-layout-separator"
        />
      );
    }
  });
  return (
    <Box
      sx={{
        display: "flex",
        flexDirection: "row",
        alignItems: "center",
        width: "100%",
        marginLeft: "2rem",
        flexWrap: "wrap",
        justifyContent: "space-around",
      }}
    >
      {components}
    </Box>
  );
};

export enum SummarySectionType {
  TITLED_VALUE,
  STATUSES,
  CUSTOM,
}
export interface SummarySection {
  type: SummarySectionType;
  titledValueProps?: SummaryTitledValueProps;
  statusesProps?: SummaryStatusesProps;
  customComponent?: React.ReactNode;
}
export interface SummaryPageLayoutProps {
  collapsable?: boolean;
  defaultCollapsed?: boolean;
  offsetOnCollapse?: boolean; // Add top margin to content when collapsed to avoid content overlap
  collapsedText?: string;
  summarySections: SummarySection[];
  contentComponent: React.ReactNode;
}

export function SummaryPageLayout({
  collapsable = false,
  defaultCollapsed = false,
  offsetOnCollapse = false,
  collapsedText = "Details",
  summarySections,
  contentComponent,
}: SummaryPageLayoutProps) {
  const [collapsed, setCollapsed] = useState(collapsable && defaultCollapsed);
  const sumaryRef = useRef<any>();
  const [summaryHeight, setSummaryHeight] = useState(0);

  // Resize observer to update content margin when summary height changes
  useEffect(() => {
    if (!sumaryRef.current) {
      return;
    }
    const resizeObserver = new ResizeObserver(() => {
      setSummaryHeight(sumaryRef.current.offsetHeight);
    });
    resizeObserver.observe(sumaryRef.current);
    return function cleanup() {
      resizeObserver.disconnect();
    };
  }, [sumaryRef.current]);

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
          ref={sumaryRef}
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
          <span className={"summary-page-layout-collapsed-text"}>
            {collapsedText}
          </span>
          <img
            onClick={toggleCollapsed}
            src={chevronRight}
            alt="expand button"
            className={"summary-page-layout-expand-button"}
          />
        </Box>
      );
    }
    return (
      <Box
        ref={sumaryRef}
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
          padding: "0.5rem",
        }}
      >
        <Box
          sx={{
            display: "flex",
            width: "100%",
          }}
        >
          {getSummaryComponent(summarySections)}
        </Box>
        {collapsable && (
          <img
            onClick={toggleCollapsed}
            src={chevronLeft}
            alt="collapse button"
            className={"summary-page-layout-collapse-button"}
          />
        )}
      </Box>
    );
  }, [
    summarySections,
    collapsed,
    collapsable,
    toggleCollapsed,
    collapsedText,
    sumaryRef,
  ]);

  const contentMargin = useMemo(() => {
    if (collapsed) {
      return offsetOnCollapse ? `${summaryHeight}px` : undefined;
    }
    return `${summaryHeight}px`;
  }, [summaryHeight, collapsed, offsetOnCollapse]);

  return (
    <Box>
      {summary}
      <Box
        sx={{
          marginTop: contentMargin,
          paddingTop: "1.25rem",
        }}
      >
        {contentComponent}
      </Box>
    </Box>
  );
}
