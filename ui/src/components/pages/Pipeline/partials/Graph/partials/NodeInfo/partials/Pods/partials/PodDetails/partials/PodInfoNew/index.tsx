import { PodSpecificInfoProps } from "../../../../../../../../../../../../../types/declarations/pods";
import { Box, Table, TableBody, TableCell, TableContainer, TableRow } from "@mui/material";


export function PodInfoNew({ podSpecificInfo}: PodSpecificInfoProps) {
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
                <TableCell sx={{ fontWeight: 600 }}>Name</TableCell>
                <TableCell>{podSpecificInfo?.Name}</TableCell>
              </TableRow>
              <TableRow>
                <TableCell sx={{ fontWeight: 600 }}>Status</TableCell>
                <TableCell>{podSpecificInfo?.Status || "N/A"}</TableCell>
              </TableRow>
              <TableRow>
                <TableCell sx={{ fontWeight: 600 }}>Condition</TableCell>
                <TableCell>{podSpecificInfo?.Condition || "N/A"}</TableCell>
              </TableRow>
              <TableRow>
                <TableCell sx={{ fontWeight: 600 }}>Message</TableCell>
                <TableCell>{podSpecificInfo?.Message || "N/A"}</TableCell>
              </TableRow>
              <TableRow>
                <TableCell sx={{ fontWeight: 600 }}>Reason</TableCell>
                <TableCell>{podSpecificInfo?.Reason || "N/A"}</TableCell>
              </TableRow>
              <TableRow>
                <TableCell sx={{ fontWeight: 600 }}>Restart Count: </TableCell>
                <TableCell>{podSpecificInfo?.RestartCount}</TableCell>
              </TableRow>
              <TableRow>
                <TableCell sx={{ fontWeight: 600 }}>Container Count:</TableCell>
                <TableCell>{podSpecificInfo?.ContainerCount}</TableCell>
              </TableRow>
            </TableBody>
          </Table>
        </TableContainer>
      </Box>
    </Box>
  );

}