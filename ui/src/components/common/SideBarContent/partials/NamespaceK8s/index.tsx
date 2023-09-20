import React, { useState, useEffect, useCallback } from "react";
import Box from "@mui/material/Box";
import { useNamespaceK8sEventsFetch } from "../../../../../utils/fetchWrappers/namespaceK8sEventsFetch"

import "./style.css";

export interface NamespaceK8sProps {
  namespaceId: string;
}

export function NamespaceK8s({ namespaceId }: NamespaceK8sProps) {
  const { data, loading, error } = useNamespaceK8sEventsFetch({ namespace: namespaceId })
  console.log("data", data);
  console.log("loading", loading);
  console.log("error", error);
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
