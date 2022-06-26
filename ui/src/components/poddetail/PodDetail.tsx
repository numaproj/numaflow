import Box from "@mui/material/Box";
import { PodInfo } from "./podinfo/PodInfo";
import { PodLogs } from "./podlogs/PodLogs";
import { Pod, PodDetail as PodDetailModel } from "../../utils/models/pods";

import "./PodDetail.css";

export interface PodDetailProps {
  namespaceId: string;
  containerName: string;
  pod: Pod;
  podDetail: PodDetailModel;
}

const headerSx = {
  marginBottom: "10px",
  fontWeight: "bold",
};

export function PodDetail({
  namespaceId,
  containerName,
  pod,
  podDetail,
}: PodDetailProps) {
  return (
    <Box
      data-testid="podDetail"
      sx={{ display: "flex", flexDirection: "column" }}
    >
      <Box sx={{ display: "flex", flexDirection: "column", width: "100%" }}>
        <Box sx={headerSx}>Pod Info</Box>
        <PodInfo
          pod={pod}
          podDetail={podDetail}
          containerName={containerName}
        />
      </Box>
      <Box sx={{ display: "flex", flexDirection: "column", width: "100%" }}>
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
