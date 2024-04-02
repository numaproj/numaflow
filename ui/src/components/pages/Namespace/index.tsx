import React, { useCallback, useContext, useMemo } from "react";
import { useLocation } from "react-router-dom";
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
import { ErrorDisplay } from "../../common/ErrorDisplay";
import { NamespaceSummaryData } from "../../../types/declarations/namespace";
import { NamespaceListingWrapper } from "./partials/NamespaceListingWrapper";
import {
  ISB_SERVICES_STATUS_TOOLTIP,
  ISB_SERVICES_TOOLTIP,
  PIPELINE_STATUS_TOOLTIP,
} from "../../../utils";

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

export interface NamespaceProps {
  namespaceId?: string;
}

export function Namespaces({ namespaceId: nsIdProp }: NamespaceProps) {
  const location = useLocation();
  const query = new URLSearchParams(location.search);
  const nsIdParam = query.get("namespace") || "";
  const namespaceId = nsIdProp || nsIdParam;
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
    const pipelines: string[] = [];
    const vertexMap = new Map<string, string[]>();
    if (pipelineRawData) {
      Object.keys(pipelineRawData).forEach((pipelineId) => {
        pipelines.push(pipelineId);
        const vertices =
          pipelineRawData[pipelineId]?.pipeline?.spec?.vertices || [];
        vertices.forEach((vertex: any) => {
          const listing = vertexMap.get(pipelineId) || [];
          listing.push(vertex.name);
          vertexMap.set(pipelineId, listing);
        });
      });
    }
    setSidebarProps({
      type: SidebarType.NAMESPACE_K8s,
      k8sEventsProps: {
        namespaceId,
        pipelineFilterOptions: pipelines,
        vertexFilterOptions: vertexMap,
      },
    });
  }, [namespaceId, setSidebarProps, pipelineRawData]);

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
              tooltip: PIPELINE_STATUS_TOOLTIP,
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
              tooltip: ISB_SERVICES_TOOLTIP,
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
              tooltip: ISB_SERVICES_STATUS_TOOLTIP,
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
              tooltip: PIPELINE_STATUS_TOOLTIP,
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
              tooltip: ISB_SERVICES_TOOLTIP,
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
              tooltip: ISB_SERVICES_STATUS_TOOLTIP,
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
      <NamespaceListingWrapper
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
