import React, { useMemo, useContext, useCallback } from "react";
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

import "./style.css";

export function Namespaces() {
  const { namespaceId } = useParams();
  const { setSidebarProps } = useContext<AppContextProps>(AppContext);
  const { data, pipelineRawData, isbRawData, loading, error, refresh } =
    useNamespaceSummaryFetch({
      namespace: namespaceId || "",
      loadOnRefresh: false,
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

  const summarySections: SummarySection[] = useMemo(() => {
    if (!data) {
      return [];
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
              linkComponent: (
                <div
                  className="namespace-k8s-events"
                  onClick={handleK8sEventsClick}
                >
                  K8s Event Logs
                </div>
              ),
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
  }, [data, handleK8sEventsClick]);

  if (loading) {
    return (
      <Box sx={{ display: "flex", justifyContent: "center" }}>
        <CircularProgress />
      </Box>
    );
  }
  if (error) {
    return <div>{`Error loading namespace summary: ${error}`}</div>;
  }
  if (!data) {
    return <div>{`No resources found.`}</div>;
  }
  return (
    <Box>
      <SummaryPageLayout
        summarySections={summarySections}
        contentComponent={
          <NamespacePipelineListing
            namespace={namespaceId || ""}
            data={data}
            pipelineData={pipelineRawData}
            isbData={isbRawData}
            refresh={refresh}
          />
        }
      />
    </Box>
  );
}
