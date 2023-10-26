import React, { useCallback, useContext, useMemo } from "react";
import { useParams } from "react-router-dom";
import Box from "@mui/material/Box";
import CircularProgress from "@mui/material/CircularProgress";
import { useNamespaceSummaryFetch } from "../../../utils/fetchWrappers/namespaceSummaryFetch";
import {
  SummaryPageLayout,
  SummarySection,
  SummarySectionType,
} from "../../common/SummaryPageLayout";
import { AppContext } from "../../../App";
import { AppContextProps } from "../../../types/declarations/app";
import { SidebarType } from "../../common/SlidingSidebar";
import { NamespacePipelineListing } from "./partials/NamespacePipelineListing";
import { ErrorDisplay } from "../../common/ErrorDisplay";
import { NamespaceSummaryData } from "../../../types/declarations/namespace";

import "./style.css";

const defaultNamespaceSummaryData: NamespaceSummaryData = {
  pipelinesCount: 0,
  pipelinesActiveCount: 0,
  pipelinesInactiveCount: 0,
  pipelinesHealthyCount: 0,
  pipelinesWarningCount: 0,
  pipelinesCriticalCount: 0,
  isbsCount: 0,
  isbsActiveCount: 0,
  isbsInactiveCount: 0,
  isbsHealthyCount: 0,
  isbsWarningCount: 0,
  isbsCriticalCount: 0,
  pipelineSummaries: [],
  pipelineRawData: null,
  isbRawData: null,
};

export function Namespaces() {
  const { namespaceId } = useParams();
  const { setSidebarProps, addError } = useContext<AppContextProps>(AppContext);
  const { data, pipelineRawData, isbRawData, loading, error, refresh } =
    useNamespaceSummaryFetch({
      namespace: namespaceId || "",
      loadOnRefresh: false,
      addError,
    });
  const handleK8sEventsClick = useCallback(() => {
    if (!namespaceId || !setSidebarProps) {
      return;
    }
    setSidebarProps({
      type: SidebarType.NAMESPACE_K8s,
      k8sEventsProps: { namespaceId },
    });
  }, [namespaceId, setSidebarProps]);
  const defaultPipelinesData = useMemo(() => {
    return [
      // Pipelines collection
      {
        type: SummarySectionType.COLLECTION,
        collectionSections: [
          {
            type: SummarySectionType.TITLED_VALUE,
            titledValueProps: {
              title: "PIPELINES",
              value: 0,
            },
          },
          {
            type: SummarySectionType.STATUSES,
            statusesProps: {
              title: "PIPELINES STATUS",
              active: 0,
              inActive: 0,
              healthy: 0,
              warning: 0,
              critical: 0,
            },
          },
        ],
      },
      // ISBs collection
      {
        type: SummarySectionType.COLLECTION,
        collectionSections: [
          {
            type: SummarySectionType.TITLED_VALUE,
            titledValueProps: {
              title: "ISB SERVICES",
              value: 0,
            },
          },
          {
            type: SummarySectionType.STATUSES,
            statusesProps: {
              title: "ISB SERVICES STATUS",
              active: 0,
              inActive: 0,
              healthy: 0,
              warning: 0,
              critical: 0,
            },
          },
        ],
      },
      {
        type: SummarySectionType.CUSTOM,
        customComponent: (
          <div className="namespace-k8s-events" onClick={handleK8sEventsClick}>
            K8s Events
          </div>
        ),
      },
    ];
  }, [handleK8sEventsClick]);

  const summarySections: SummarySection[] = useMemo(() => {
    if (loading) {
      return [
        {
          type: SummarySectionType.CUSTOM,
          customComponent: <CircularProgress key="ns-summary-spinner" />,
        },
      ];
    }
    if (error) {
      return [
        {
          type: SummarySectionType.CUSTOM,
          customComponent: (
            <ErrorDisplay
              key="ns-summary-error"
              title="Error loading namespace summary"
              message={error}
            />
          ),
        },
      ];
    }
    if (!data) {
      return defaultPipelinesData;
    }
    return [
      // Pipelines collection
      {
        type: SummarySectionType.COLLECTION,
        collectionSections: [
          {
            type: SummarySectionType.TITLED_VALUE,
            titledValueProps: {
              title: "PIPELINES",
              value: data.pipelinesCount,
            },
          },
          {
            type: SummarySectionType.STATUSES,
            statusesProps: {
              title: "PIPELINES STATUS",
              active: data.pipelinesActiveCount,
              inActive: data.pipelinesInactiveCount,
              healthy: data.pipelinesHealthyCount,
              warning: data.pipelinesWarningCount,
              critical: data.pipelinesCriticalCount,
            },
          },
        ],
      },
      // ISBs collection
      {
        type: SummarySectionType.COLLECTION,
        collectionSections: [
          {
            type: SummarySectionType.TITLED_VALUE,
            titledValueProps: {
              title: "ISB SERVICES",
              value: data.isbsCount,
            },
          },
          {
            type: SummarySectionType.STATUSES,
            statusesProps: {
              title: "ISB SERVICES STATUS",
              active: data.isbsActiveCount,
              inActive: data.isbsInactiveCount,
              healthy: data.isbsHealthyCount,
              warning: data.isbsWarningCount,
              critical: data.isbsCriticalCount,
            },
          },
        ],
      },
      {
        type: SummarySectionType.CUSTOM,
        customComponent: (
          <div className="namespace-k8s-events" onClick={handleK8sEventsClick}>
            K8s Events
          </div>
        ),
      },
    ];
  }, [data, loading, error, handleK8sEventsClick]);

  const content = useMemo(() => {
    if (error) {
      return (
        <Box
          sx={{
            display: "flex",
            flexDirection: "column",
            height: "100%",
            width: "100%",
            justifyContent: "center",
            alignItems: "center",
          }}
        >
          <ErrorDisplay
            title="Error loading namespace pipelines"
            message={error}
          />
        </Box>
      );
    }
    if (loading) {
      return (
        <Box
          sx={{
            display: "flex",
            height: "100%",
            width: "100%",
            justifyContent: "center",
            alignItems: "center",
          }}
        >
          <CircularProgress />
        </Box>
      );
    }
    return (
      <NamespacePipelineListing
        namespace={namespaceId || ""}
        data={data ? data : defaultNamespaceSummaryData}
        pipelineData={pipelineRawData}
        isbData={isbRawData}
        refresh={refresh}
      />
    );
  }, [error, loading, data, namespaceId, pipelineRawData, isbRawData, refresh]);

  return (
    <SummaryPageLayout
      summarySections={summarySections}
      contentComponent={content}
    />
  );
}
