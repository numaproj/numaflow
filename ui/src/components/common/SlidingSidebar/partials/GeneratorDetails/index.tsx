import React, { useCallback, useState } from "react";
import Tabs from "@mui/material/Tabs";
import Tab from "@mui/material/Tab";
import Box from "@mui/material/Box";
import { GeneratorUpdate } from "./partials/GeneratorUpdate";

import "./style.css";

const SPEC_TAB_INDEX = 0;

export enum VertexType {
  GENERATOR,
}

export interface GeneratorDetailsProps {
  namespaceId?: string;
  pipelineId?: string;
  vertexId: string;
  generatorDetails: any;
}

export function GeneratorDetails({
  vertexId,
  generatorDetails,
}: GeneratorDetailsProps) {
  const [tabValue, setTabValue] = useState(SPEC_TAB_INDEX);

  const handleTabChange = useCallback(
    (event: React.SyntheticEvent, newValue: number) => {
      setTabValue(newValue);
    },
    []
  );

  return (
    <Box
      sx={{
        display: "flex",
        flexDirection: "column",
        height: "100%",
      }}
    >
      <Box
        sx={{
          display: "flex",
          flexDirection: "row",
        }}
      >
        <span className="vertex-details-header-text">Generator Vertex</span>
      </Box>
      <Box sx={{ marginTop: "1rem", borderBottom: 1, borderColor: "divider" }}>
        <Tabs
          className="vertex-details-tabs"
          value={tabValue}
          onChange={handleTabChange}
        >
          <Tab
            className={
              tabValue === SPEC_TAB_INDEX
                ? "vertex-details-tab-selected"
                : "vertex-details-tab"
            }
            label="Spec"
          />
        </Tabs>
      </Box>
      <div
        className="vertex-details-tab-panel"
        role="tabpanel"
        hidden={tabValue !== SPEC_TAB_INDEX}
      >
        {tabValue === SPEC_TAB_INDEX && (
          <Box sx={{ height: "100%" }}>
            <GeneratorUpdate
              generatorId={vertexId}
              generatorSpec={generatorDetails?.data?.nodeInfo}
            />
          </Box>
        )}
      </div>
    </Box>
  );
}
