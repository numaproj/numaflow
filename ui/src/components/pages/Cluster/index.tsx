import React, { useState, useEffect } from "react";
import Box from "@mui/material/Box";
import CircularProgress from "@mui/material/CircularProgress";
import { SummaryPageLayout } from "../../common/SummaryPageLayout";
import { useClusterSummaryFetch } from "../../../utils/fetchWrappers/clusterSummaryFetch";
import { ClusterSummary } from "./partials/ClusterSummary";

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
    <div className="Cluster">
      <SummaryPageLayout
        summaryComponent={<ClusterSummary data={data} />}
        contentComponent={<div>TODO Content</div>}
      />
    </div>
  );
}
