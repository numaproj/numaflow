import React from "react";
import Box from "@mui/material/Box";
import { SpecEditor, ViewType } from "../../../../../SpecEditor";

import "./style.css";

export interface GeneratorUpdateProps {
  generatorId: string;
  generatorSpec: any;
}

export function GeneratorUpdate({ generatorSpec }: GeneratorUpdateProps) {
  return (
    <Box
      sx={{
        display: "flex",
        flexDirection: "column",
        height: "100%",
      }}
    >
      <SpecEditor initialYaml={generatorSpec} viewType={ViewType.READ_ONLY} />
    </Box>
  );
}
