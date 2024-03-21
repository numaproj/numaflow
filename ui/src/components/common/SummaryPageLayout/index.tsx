import React, {
  useMemo,
  useState,
  useCallback,
  useEffect,
  useRef,
  useContext,
} from "react";
import Box from "@mui/material/Box";
import {
  SummaryTitledValue,
  SummaryTitledValueProps,
} from "./partials/SummaryTitledValue";
import {
  SummaryStatuses,
  SummaryStatusesProps,
} from "./partials/SummaryStatuses";
import { AppContext } from "../../../App";
import { AppContextProps } from "../../../types/declarations/app";
import chevronLeft from "../../../images/chevron-m-left.png";
import chevronRight from "../../../images/chevron-m-right.png";

import "./style.css";

export enum SummarySectionType {
  TITLED_VALUE,
  STATUSES,
  CUSTOM,
  COLLECTION,
}

export interface SummarySection {
  type: SummarySectionType;
  titledValueProps?: SummaryTitledValueProps;
  statusesProps?: SummaryStatusesProps;
  customComponent?: React.ReactNode;
  collectionSections?: SummarySection[];
}

export interface SummaryPageLayoutProps {
  excludeContentMargin?: boolean;
  collapsable?: boolean;
  defaultCollapsed?: boolean;
  offsetOnCollapse?: boolean; // Add top margin to content when collapsed to avoid content overlap
  collapsedText?: string;
  summarySections: SummarySection[];
  contentComponent: React.ReactNode;
  contentPadding?: boolean;
  contentHideOverflow?: boolean;
}

export const CollapseContext = React.createContext(false);

const SUMMARY_HEIGHT = "6.5625rem";
const COLLAPSED_HEIGHT = "2.25rem";

const getSectionComponentAndKey = (
  section: SummarySection,
  sectionIndex: number
) => {
  let key: string;
  const collectionComponents: React.ReactNode[] = [];
  switch (section.type) {
    case SummarySectionType.TITLED_VALUE:
      if (!section.titledValueProps) {
        key = "titled-value-missing";
        return {
          key,
          component: <div key={key}>Missing props</div>,
        };
      }
      key = `titled-value-${section.titledValueProps.title}`;
      return {
        key,
        component: (
          <SummaryTitledValue key={key} {...section.titledValueProps} />
        ),
      };
    case SummarySectionType.STATUSES:
      if (!section.statusesProps) {
        key = "statuses-missing";
        return {
          key,
          component: <div key={key}>Missing props</div>,
        };
      }
      key = `statuses-${section.statusesProps.title}`;
      return {
        key,
        component: <SummaryStatuses key={key} {...section.statusesProps} />,
      };
    case SummarySectionType.CUSTOM:
      if (!section.customComponent) {
        key = "custom-missing";
        return {
          key,
          component: <div key={key}>Missing props</div>,
        };
      }
      key = `custom-${sectionIndex}`;
      return {
        key,
        component: section.customComponent,
      };
    case SummarySectionType.COLLECTION:
      if (!section.collectionSections || !section.collectionSections.length) {
        key = "collection-missing";
        return {
          key,
          component: <div key={key}>Missing props</div>,
        };
      }
      section.collectionSections.forEach((collectionSection, index) => {
        if (!section.collectionSections?.length) {
          // Added for undefined TS check
          return;
        }
        const { key: collectionKey, component } = getSectionComponentAndKey(
          collectionSection,
          index
        );
        collectionComponents.push(component);
        // Add separator if not last section
        if (index < section.collectionSections.length - 1) {
          collectionComponents.push(
            <div
              key={`${collectionKey}-separator`}
              className="summary-page-layout-separator"
            />
          );
        }
      });
      key = `collection-${sectionIndex}}`;
      return {
        key,
        component: (
          <Box
            key={key}
            sx={{
              display: "flex",
              flexDirection: "row",
              flexGrow: 1,
              justifyContent: "space-around",
            }}
          >
            {collectionComponents}
          </Box>
        ),
      };
    default:
      key = "unknown";
      return {
        key,
        component: <div key={key}>Missing props</div>,
      };
  }
};

const getSummaryComponent = (summarySections: SummarySection[]) => {
  // Build sections from props
  const components: React.ReactNode[] = [];
  summarySections.forEach((section, index) => {
    const { key, component } = getSectionComponentAndKey(section, index);
    // Add separator if not last section
    if (index !== summarySections.length - 1) {
      components.push(
        <Box
          sx={{
            display: "flex",
            flexDirection: "row",
            flexGrow: "1",
          }}
          key={key}
        >
          {component}
          <div
            key={`${key}-separator`}
            className="summary-page-layout-separator"
          />
        </Box>
      );
    } else {
      // Assigning key to the last component
      components.push(<Box key={key}>{component}</Box>);
    }
  });
  return (
    <Box
      sx={{
        display: "flex",
        flexDirection: "row",
        alignItems: "center",
        width: "100%",
        flexWrap: "wrap",
        justifyContent: "space-around",
      }}
    >
      {components}
    </Box>
  );
};

export function SummaryPageLayout({
  excludeContentMargin = false,
  collapsable = false,
  defaultCollapsed = false,
  offsetOnCollapse = false,
  collapsedText = "Details",
  summarySections,
  contentComponent,
  contentPadding = true,
  contentHideOverflow = false,
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
      setSummaryHeight(sumaryRef?.current?.offsetHeight);
    });
    resizeObserver.observe(sumaryRef?.current);
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

  const { isPlugin } = useContext<AppContextProps>(AppContext);

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
            boxShadow: "0 0.25rem 0.375rem rgba(39, 76, 119, 0.16)",
            zIndex: (theme) => theme.zIndex.drawer - 1,
            position: isPlugin ? undefined : "fixed",
            top: isPlugin ? "2.05rem" : "6.25rem",
            padding: "0 1.25rem",
            alignItems: "center",
            borderBottomLeftRadius: "1.25rem",
            borderBottomRightRadius: "1.25rem",
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
          boxShadow: "0 0.1875rem 0.6875rem rgba(39, 76, 119, 0.16)",
          zIndex: (theme) => theme.zIndex.drawer - 1,
          position: isPlugin ? undefined : "fixed",
          top: isPlugin ? "2rem" : "5.75rem",
        }}
      >
        <Box
          sx={{
            display: "flex",
            width: "100%",
            padding: "0rem",
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
    if (excludeContentMargin) return 0;
    if (collapsed) {
      return offsetOnCollapse ? `${summaryHeight}px` : undefined;
    }
    return `${summaryHeight}px`;
  }, [summaryHeight, collapsed, offsetOnCollapse]);

  return (
    <Box
      sx={
        contentHideOverflow
          ? { height: "100%", overflow: "hidden" }
          : { height: "100%" }
      }
      data-testid="summary-page-layout"
    >
      {summary}
      <Box
        sx={{
          marginTop: contentMargin,
          paddingTop: contentPadding ? "1.25rem" : "0",
          height: "100%",
        }}
      >
        <CollapseContext.Provider value={collapsed}>
          {contentComponent}
        </CollapseContext.Provider>
      </Box>
    </Box>
  );
}
