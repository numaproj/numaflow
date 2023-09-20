import React, { useState, useEffect, useCallback } from "react";
import Box from "@mui/material/Box";

import "./style.css";

export interface NamespaceK8sProps {
  namespaceId: string;
}

export function NamespaceK8s({ namespaceId }: NamespaceK8sProps) {
  return (
    <Box
      sx={{
        display: "flex",
        flexDirection: "column",
      }}
    >
      TODO CONTENT
    </Box>
  );
}
