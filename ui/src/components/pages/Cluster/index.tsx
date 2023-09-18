import React, { useMemo } from "react";
import Box from "@mui/material/Box";
import CircularProgress from "@mui/material/CircularProgress";
import {
  SummaryPageLayout,
  SummarySection,
  SummarySectionType,
} from "../../common/SummaryPageLayout";
import { ClusterNamespaceListing } from "./partials/ClusterNamespaceListing";
import { useClusterSummaryFetch } from "../../../utils/fetchWrappers/clusterSummaryFetch";

import "./style.css";

export function Cluster() {
  const { data, loading, error } = useClusterSummaryFetch({
    loadOnRefresh: false,
  });

  const summarySections: SummarySection[] = useMemo(() => {
    if (!data) {
      return [];
    }
    return [
      {
        type: SummarySectionType.TITLED_VALUE,
        titledValueProps: {
          title: "Namespaces",
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
              title: "Pipelines",
              value: data.pipelinesCount,
            },
          },
          {
            type: SummarySectionType.STATUSES,
            statusesProps: {
              title: "Pipelines Status",
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
              title: "ISB Services",
              value: data.isbsCount,
            },
          },
          {
            type: SummarySectionType.STATUSES,
            statusesProps: {
              title: "ISB Services Status",
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
  }, [data]);

  if (loading) {
    return (
      <Box sx={{ display: "flex", justifyContent: "center" }}>
        <CircularProgress />
      </Box>
    );
  }
  if (error) {
    return <div>{`Error loading cluster summary: ${error}`}</div>;
  }
  if (!data) {
    return <div>{`No resources found.`}</div>;
  }
  return (
    <Box>
      <SummaryPageLayout
        summarySections={summarySections}
        contentComponent={<ClusterNamespaceListing data={data} />}
      />
    </Box>
  );
}
