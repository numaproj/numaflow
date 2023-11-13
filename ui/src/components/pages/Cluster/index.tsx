import React, { useContext, useMemo } from "react";
import Box from "@mui/material/Box";
import CircularProgress from "@mui/material/CircularProgress";
import {
  SummaryPageLayout,
  SummarySection,
  SummarySectionType,
} from "../../common/SummaryPageLayout";
import { ClusterNamespaceListing } from "./partials/ClusterNamespaceListing";
import { ErrorDisplay } from "../../common/ErrorDisplay";
import { useClusterSummaryFetch } from "../../../utils/fetchWrappers/clusterSummaryFetch";
import { AppContextProps } from "../../../types/declarations/app";
import { AppContext } from "../../../App";
import {
  ISB_SERVICES_STATUS_TOOLTIP,
  ISB_SERVICES_TOOLTIP,
  PIPELINE_STATUS_TOOLTIP,
} from "../../../utils";

import "./style.css";

export function Cluster() {
  const { addError } = useContext<AppContextProps>(AppContext);
  const { data, loading, error } = useClusterSummaryFetch({
    loadOnRefresh: false,
    addError,
  });

  const summarySections: SummarySection[] = useMemo(() => {
    if (loading) {
      return [
        {
          type: SummarySectionType.CUSTOM,
          customComponent: <CircularProgress key="cluster-summary-loading" />,
        },
      ];
    }
    if (error) {
      return [
        {
          type: SummarySectionType.CUSTOM,
          customComponent: (
            <ErrorDisplay
              key="cluster-summary-error"
              title="Error loading cluster summary"
              message={error}
            />
          ),
        },
      ];
    }
    if (!data) {
      return [];
    }
    return [
      {
        type: SummarySectionType.TITLED_VALUE,
        titledValueProps: {
          title: "NAMESPACES",
          value: data.namespacesCount,
        },
      },
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
    ];
  }, [data, loading, error]);

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
            title="Error loading cluster namespaces"
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
          <CircularProgress data-testid="cluster-loading-icon" />
        </Box>
      );
    }
    if (!data) {
      return (
        <ErrorDisplay
          title="Error loading cluster namespaces"
          message="No resources found."
        />
      );
    }
    return <ClusterNamespaceListing data={data} />;
  }, [error, loading, data]);

  return (
    <SummaryPageLayout
      summarySections={summarySections}
      contentComponent={content}
    />
  );
}
