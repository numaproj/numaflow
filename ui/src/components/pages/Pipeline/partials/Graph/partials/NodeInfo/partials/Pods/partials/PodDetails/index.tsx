import React from "react";
import Box from "@mui/material/Box";
import { PodLogs } from "./partials/PodLogs";
import { PodDetailProps } from "../../../../../../../../../../../types/declarations/pods";

export function PodDetail({
  namespaceId,
  type,
  containerName,
  pod,
  focusControls,
}: PodDetailProps) {
  if (!pod) return null;

  return (
    <Box
      sx={{
        display: "flex",
        flexDirection: "column",
        width: "100%",
        height: "100%",
        minHeight: 0,
      }}
    >
      <PodLogs
        namespaceId={namespaceId}
        podName={pod.name}
        containerName={containerName}
        type={type}
        focusControls={focusControls}
      />
    </Box>
  );
}
