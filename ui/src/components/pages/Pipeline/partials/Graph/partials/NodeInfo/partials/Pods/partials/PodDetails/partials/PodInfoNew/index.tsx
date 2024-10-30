import {
  Box,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableRow,
} from "@mui/material";
import { PodSpecificInfoProps } from "../../../../../../../../../../../../../types/declarations/pods";

export function PodInfoNew({ podSpecificInfo }: PodSpecificInfoProps) {
  return (
    <Box sx={{ padding: "1.6rem" }}>
      <Box sx={{ fontWeight: 600 }}>Pod Info</Box>
      <Box
        data-testid="podInfo"
        sx={{
          display: "flex",
          flexDirection: "column",
          height: "100%",
          color: "#DCDCDC",
        }}
      >
        <TableContainer sx={{ maxHeight: "60rem", backgroundColor: "#FFF" }}>
          <Table stickyHeader>
            <TableBody>
              <TableRow>
                <TableCell sx={{ fontWeight: 600, width: "30rem" }}>
                  Name
                </TableCell>
                <TableCell>
                  {podSpecificInfo?.name?.slice(
                    0,
                    podSpecificInfo.name?.lastIndexOf("-")
                  )}
                </TableCell>
              </TableRow>
              <TableRow>
                <TableCell sx={{ fontWeight: 600, width: "30rem" }}>
                  Status
                </TableCell>
                <TableCell>{podSpecificInfo?.status || "Unknown"}</TableCell>
              </TableRow>
              {podSpecificInfo?.totalCPU && (
                <TableRow>
                  <TableCell sx={{ fontWeight: 600, width: "30rem" }}>
                    CPU
                  </TableCell>
                  <TableCell>{podSpecificInfo?.totalCPU}</TableCell>
                </TableRow>
              )}
              {podSpecificInfo?.totalMemory && (
                <TableRow>
                  <TableCell sx={{ fontWeight: 600, width: "30rem" }}>
                    Memory
                  </TableCell>
                  <TableCell>{podSpecificInfo?.totalMemory}</TableCell>
                </TableRow>
              )}
              {podSpecificInfo?.reason && (
                <TableRow>
                  <TableCell sx={{ fontWeight: 600, width: "30rem" }}>
                    Reason
                  </TableCell>
                  <TableCell>{podSpecificInfo?.reason}</TableCell>
                </TableRow>
              )}
              {podSpecificInfo?.message && (
                <TableRow>
                  <TableCell sx={{ fontWeight: 600, width: "30rem" }}>
                    Message
                  </TableCell>
                  <TableCell>{podSpecificInfo?.message}</TableCell>
                </TableRow>
              )}
              <TableRow>
                <TableCell sx={{ fontWeight: 600, width: "30rem" }}>
                  Restart Count
                </TableCell>
                <TableCell>{podSpecificInfo?.restartCount}</TableCell>
              </TableRow>
            </TableBody>
          </Table>
        </TableContainer>
      </Box>
    </Box>
  );
}
