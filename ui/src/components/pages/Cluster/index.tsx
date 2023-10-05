import React, { useMemo } from "react";
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

import "./style.css";

export function Cluster() {
  const { data, loading, error } = useClusterSummaryFetch({
    loadOnRefresh: false,
  });

  const summarySections: SummarySection[] = useMemo(() => {
    if (loading) {
      return [
        {
          type: SummarySectionType.CUSTOM,
          customComponent: <CircularProgress />,
        },
      ];
    }
    if (error) {
      return [
        {
          type: SummarySectionType.CUSTOM,
          customComponent: (
            <ErrorDisplay
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
              activeText: "Live",
              inAcitveText: "Not-Live",
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
          <CircularProgress />
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
