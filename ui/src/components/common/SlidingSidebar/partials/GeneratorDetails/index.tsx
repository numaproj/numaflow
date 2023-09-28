import React, { useCallback, useEffect, useMemo, useState } from "react";
import Tabs from "@mui/material/Tabs";
import Tab from "@mui/material/Tab";
import Box from "@mui/material/Box";
import { SpecEditor } from "../VertexDetails/partials/SpecEditor";

import "./style.css";

const SPEC_TAB_INDEX = 0;

export enum VertexType {
  GENERATOR,
}

export interface GeneratorDetailsProps {
  namespaceId: string;
  pipelineId: string;
  vertexId: string;
  generatorDetails: any;
}

export function GeneratorDetails({
  namespaceId,
  pipelineId,
  vertexId,
  generatorDetails,
}: GeneratorDetailsProps) {
  const [generatorSpec, setGeneratorSpec] = useState<any>();
  const [vertexType, setVertexType] = useState<VertexType | undefined>();
  const [tabValue, setTabValue] = useState(SPEC_TAB_INDEX);

  // Find the vertex spec by id
  useEffect(() => {
    setVertexType(VertexType.GENERATOR);
    setGeneratorSpec(generatorDetails?.data?.nodeInfo);
  }, [vertexId, generatorDetails]);

  const handleTabChange = useCallback(
    (event: React.SyntheticEvent, newValue: number) => {
      setTabValue(newValue);
    },
    []
  );

  const header = useMemo(() => {
    const headerContainerStyle = {
      display: "flex",
      flexDirection: "row",
    };
    const textClass = "vertex-details-header-text";
    switch (vertexType) {
      case VertexType.GENERATOR:
        return (
          <Box sx={headerContainerStyle}>
            <span className={textClass}>Generator Vertex</span>
          </Box>
        );
      default:
        return (
          <Box sx={headerContainerStyle}>
            <span className={textClass}>Vertex</span>
          </Box>
        );
    }
  }, [vertexType]);

  return (
    <Box
      sx={{
        display: "flex",
        flexDirection: "column",
        height: "100%",
      }}
    >
      {header}
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
            <SpecEditor vertexId={vertexId} vertexSpec={generatorSpec} />
          </Box>
        )}
      </div>
    </Box>
  );
}
