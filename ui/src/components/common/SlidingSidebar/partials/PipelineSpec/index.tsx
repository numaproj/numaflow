import React, { useMemo } from "react";
import Box from "@mui/material/Box";
import Paper from "@mui/material/Paper";
import YAML from "yaml";
import Editor from "@monaco-editor/react";
import CircularProgress from "@mui/material/CircularProgress";

import "./style.css";

export interface PiplineSpecProps {
  spec: any;
  titleOverride?: string;
}

export function PiplineSpec({ spec, titleOverride }: PiplineSpecProps) {
  const editor = useMemo(() => {
    if (!spec) {
      return <Box>Pipeline spec not found</Box>;
    }
    return (
      <Paper square elevation={0} sx={{ height: "100%", marginTop: "1rem" }}>
        <Editor
          height="100%"
          defaultLanguage="yaml"
          defaultValue={YAML.stringify(spec)}
          value={YAML.stringify(spec)}
          theme="github"
          options={{ domReadOnly: true, readOnly: true }}
          loading={
            <Box sx={{ display: "flex", justifyContent: "center" }}>
              <CircularProgress />
            </Box>
          }
        />
      </Paper>
    );
  }, [spec]);

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
        <span className="pipeline-spec-header-text">{titleOverride || "Pipeline Spec"}</span>
      </Box>
      {editor}
    </Box>
  );
}
