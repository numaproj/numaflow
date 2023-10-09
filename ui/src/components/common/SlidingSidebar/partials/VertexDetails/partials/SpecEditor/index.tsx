import React, { useMemo } from "react";
import Box from "@mui/material/Box";
import Paper from "@mui/material/Paper";
import YAML from "yaml";
import Editor from "@monaco-editor/react";
import CircularProgress from "@mui/material/CircularProgress";

import "./style.css";

export interface SpecEditorProps {
  vertexId: string;
  vertexSpec: any;
}

export function SpecEditor({ vertexId, vertexSpec }: SpecEditorProps) {
  const editor = useMemo(() => {
    if (!vertexSpec) {
      return <Box>Vertex spec not found</Box>;
    }
    return (
      <Paper square elevation={0} sx={{ height: "100%" }}>
        <Editor
          height="100%"
          defaultLanguage="yaml"
          defaultValue={YAML.stringify(vertexSpec)}
          value={YAML.stringify(vertexSpec)}
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
  }, [vertexId, vertexSpec]);

  return (
    <Box
      sx={{
        display: "flex",
        flexDirection: "column",
        height: "100%",
      }}
    >
      {editor}
    </Box>
  );
}
