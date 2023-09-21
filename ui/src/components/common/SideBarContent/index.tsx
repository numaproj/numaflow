import React from "react";
import Box from "@mui/material/Box";

import "./style.css";

export enum SideBarType {
  NAMESPACE_K8s,
}

export interface NamespaceK8sProps {
  namespaceId: string;
}

export interface SideBarProps {
  type: SideBarType;
  namespaceK8sProps?: NamespaceK8sProps;
}

export function SideBarContent({ type, namespaceK8sProps }: SideBarProps) {
  return (
    <Box
      sx={{
        display: "flex",
        flexDirection: "column",
        paddingTop: "5.8125rem",
        backgroundColor: "#F8F8FB",
      }}
    >
      TODO CONTENT
    </Box>
  );
}
