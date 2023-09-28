import Box from "@mui/material/Box";
import { PodInfo } from "./partials/PodInfo";
import { PodLogs } from "./partials/PodLogs";
import { PodDetailProps } from "../../../../../../../../../../../types/declarations/pods";

const headerSx = {
  marginBottom: "0.625rem",
  fontWeight: "bold",
};

export function PodDetail({
  namespaceId,
  containerName,
  pod,
  podDetails,
}: PodDetailProps) {
  if (!pod) return null;

  return (
    <Box
      data-testid="podDetail"
      sx={{ display: "flex", flexDirection: "column", mb: 2 }}
    >
      <Box
        sx={{
          display: "flex",
          flexDirection: "column",
          width: "100%",
          marginTop: "1rem",
        }}
      >
        <Box sx={headerSx}>Pod Info</Box>
        <PodInfo
          pod={pod}
          podDetails={podDetails}
          containerName={containerName}
        />
      </Box>
      <Box
        sx={{
          display: "flex",
          flexDirection: "column",
          width: "100%",
          marginTop: "1rem",
        }}
      >
        <Box sx={headerSx}>Pod Logs</Box>
        <PodLogs
          namespaceId={namespaceId}
          podName={pod.name}
          containerName={containerName}
        />
      </Box>
    </Box>
  );
}
