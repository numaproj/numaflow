import React, { useState, useEffect, useMemo } from "react";
import Box from "@mui/material/Box";
import CircularProgress from "@mui/material/CircularProgress";
import {
  SummaryPageLayout,
  SummarySection,
  SummarySectionType,
} from "../../common/SummaryPageLayout";
import { useClusterSummaryFetch } from "../../../utils/fetchWrappers/clusterSummaryFetch";

import "./style.css";

const DATA_REFRESH_INTERVAL = 15000; // ms

export function Cluster() {
  const [refreshKey, setRefreshKey] = useState(0);
  const { data, loading, error } = useClusterSummaryFetch({
    refreshKey,
    loadOnRefresh: false,
  });

  // Refresh data periodically
  useEffect(() => {
    setInterval(() => {
      setRefreshKey((prev) => prev + 1);
    }, DATA_REFRESH_INTERVAL);
  }, [refreshKey]);

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
        },
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
    return <div>{`Error loading cluster summary: no data returned.`}</div>;
  }
  return (
    <div>
      <SummaryPageLayout
        summarySections={summarySections}
        contentComponent={<div>TODO Content</div>}
      />
    </div>
  );
}
