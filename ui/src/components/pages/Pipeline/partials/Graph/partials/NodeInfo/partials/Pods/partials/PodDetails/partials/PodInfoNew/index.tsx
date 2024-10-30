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
                <TableCell sx={{ fontWeight: 600, minWidth: "30rem" }}>
                  Name
                </TableCell>
                <TableCell>
                  {podSpecificInfo?.Name?.slice(
                    0,
                    podSpecificInfo.Name?.lastIndexOf("-")
                  )}
                </TableCell>
              </TableRow>
              <TableRow>
                <TableCell sx={{ fontWeight: 600, minWidth: "30rem" }}>
                  Status
                </TableCell>
                <TableCell>{podSpecificInfo?.Status || "N/A"}</TableCell>
              </TableRow>
              {podSpecificInfo?.Message && (
                  <TableRow>
                  <TableCell sx={{ fontWeight: 600, minWidth: "30rem" }}>
                    Message
                  </TableCell>
                  <TableCell>{podSpecificInfo?.Message}</TableCell>
                </TableRow>
              )}

              {podSpecificInfo?.Reason && (
                <TableRow>
                  <TableCell sx={{ fontWeight: 600, minWidth: "30rem" }}>
                    Reason
                  </TableCell>
                  <TableCell>{podSpecificInfo?.Reason}</TableCell>
                </TableRow>

              )}

              <TableRow>
                <TableCell sx={{ fontWeight: 600, minWidth: "30rem" }}>
                  Restart Count
                </TableCell>
                <TableCell>{podSpecificInfo?.RestartCount}</TableCell>
              </TableRow>
            </TableBody>
          </Table>
        </TableContainer>
      </Box>
    </Box>
  );
}
