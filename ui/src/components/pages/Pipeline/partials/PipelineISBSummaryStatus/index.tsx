import React, { useCallback, useContext } from "react";
import Box from "@mui/material/Box";
import { GetISBType, UNKNOWN } from "../../../../../utils";
import { AppContext } from "../../../../../App";
import { AppContextProps } from "../../../../../types/declarations/app";
import { IsbSummary } from "../../../../../types/declarations/pipeline";
import { SidebarType } from "../../../../common/SlidingSidebar";
import { ViewType } from "../../../../common/SpecEditor";

interface PipelineISBSummaryStatusProps {
  isbData?: IsbSummary | null;
  namespaceId?: string;
}

interface IsbSpecWithReplicas {
  [key: string]: { replicas?: number } | undefined;
}

export function PipelineISBSummaryStatus({
  isbData,
  namespaceId,
}: PipelineISBSummaryStatusProps) {
  const isbSpec = isbData?.isbService?.spec as unknown as
    | IsbSpecWithReplicas
    | undefined;
  const isbType = isbData?.isbService?.spec
    ? GetISBType(isbData.isbService.spec) || UNKNOWN
    : UNKNOWN;
  const { setSidebarProps } = useContext<AppContextProps>(AppContext);

  const handleViewMoreClick = useCallback(() => {
    if (!namespaceId || !isbData?.name || !setSidebarProps) {
      return;
    }
    setSidebarProps({
      type: SidebarType.ISB_UPDATE,
      specEditorProps: {
        titleOverride: `ISB Service Details: ${isbData.name}`,
        initialYaml: isbData.isbService,
        namespaceId,
        isbId: isbData.name,
        viewType: ViewType.READ_ONLY,
      },
    });
  }, [namespaceId, isbData, setSidebarProps]);

  return (
    <Box
      sx={{
        display: "flex",
        flexDirection: "column",
        marginTop: "0.6rem",
        flexGrow: 1,
        paddingLeft: "1.6rem",
      }}
    >
      <Box sx={{ display: "flex", flexDirection: "row" }}>
        <Box sx={{ width: "fit-content", marginRight: "2em", flexGrow: 1 }}>
          <span className="pipeline-status-title">ISB SERVICES SUMMARY</span>
          <Box
            sx={{
              display: "flex",
              flexDirection: "row",
              marginTop: "0.5rem",
            }}
          >
            <Box
              sx={{
                display: "flex",
                flexDirection: "column",
                marginLeft: "0.5rem",
              }}
            >
              <div className="pipeline-summary-text">
                <span className="pipeline-summary-subtitle">Name: </span>
              </div>
              <div className="pipeline-summary-text">
                <span className="pipeline-summary-subtitle">Type: </span>
              </div>
            </Box>
            <Box
              sx={{
                display: "flex",
                flexDirection: "column",
                marginLeft: "0.5rem",
              }}
            >
              <div className="isb-status-text">
                <span>{isbData?.name}</span>
              </div>
              <div className="isb-status-text">
                <span>{isbType}</span>
              </div>
            </Box>
            <Box
              sx={{
                display: "flex",
                flexDirection: "column",
                marginLeft: "3.2rem",
              }}
            >
              <div className="pipeline-summary-text">
                <span className="pipeline-summary-subtitle">
                  <div className="pipeline-summary-text">
                    <span className="pipeline-summary-subtitle">Size: </span>
                    <span>
                      {isbType && isbSpec?.[isbType]?.replicas
                        ? isbSpec[isbType]?.replicas
                        : UNKNOWN}
                    </span>
                  </div>
                </span>
              </div>
              {isbData?.name && (
                <div className="pipeline-summary-text">
                  <span className="pipeline-summary-subtitle">
                    <div
                      className="pipeline-onclick-events"
                      onClick={handleViewMoreClick}
                      data-testid="pipeline-isb-view-more"
                    >
                      View More
                    </div>
                  </span>
                </div>
              )}
            </Box>
          </Box>
        </Box>
      </Box>
    </Box>
  );
}
