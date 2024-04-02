import Box from "@mui/material/Box";
import { PodLogs } from "./partials/PodLogs";
import { PodDetailProps } from "../../../../../../../../../../../types/declarations/pods";

const headerSx = {
  marginBottom: "1rem",
  fontWeight: "bold",
};

export function PodDetail({ namespaceId, containerName, pod }: PodDetailProps) {
  if (!pod) return null;

  return (
    <Box
      sx={{
        display: "flex",
        flexDirection: "column",
        width: "100%",
        marginTop: "1.6rem",
      }}
    >
      <Box sx={headerSx}>Container Logs</Box>
      <PodLogs
        namespaceId={namespaceId}
        podName={pod.name}
        containerName={containerName}
      />
    </Box>
  );
}
