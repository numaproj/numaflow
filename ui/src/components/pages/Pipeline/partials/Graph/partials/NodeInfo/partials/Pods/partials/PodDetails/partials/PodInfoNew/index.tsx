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
              <TableRow>
                <TableCell sx={{ fontWeight: 600, minWidth: "30rem" }}>
                  Message
                </TableCell>
                <TableCell>{podSpecificInfo?.Message || "N/A"}</TableCell>
              </TableRow>
              <TableRow>
                <TableCell sx={{ fontWeight: 600, minWidth: "30rem" }}>
                  Reason
                </TableCell>
                <TableCell>{podSpecificInfo?.Reason || "N/A"}</TableCell>
              </TableRow>
              {/* <TableRow>
                <TableCell sx={{ fontWeight: 600, minWidth: "30rem" }}>
                  Condition Type
                </TableCell>
                <TableCell>{podSpecificInfo?.Condition || "N/A"}</TableCell>
              </TableRow>
              <TableRow>
                <TableCell sx={{ fontWeight: 600, minWidth: "30rem" }}>
                  Last Transition Message
                </TableCell>
                <TableCell>{podSpecificInfo?.ConditionMessage || "N/A"}</TableCell>
              </TableRow>
              <TableRow>
                <TableCell sx={{ fontWeight: 600, minWidth: "30rem" }}>
                  Last Transition Reason
                </TableCell>
                <TableCell>{podSpecificInfo?.ConditionReason || "N/A"}</TableCell>
              </TableRow> */}
              <TableRow>
                <TableCell sx={{ fontWeight: 600, minWidth: "30rem" }}>
                  Containers Count
                </TableCell>
                <TableCell>{podSpecificInfo?.ContainerCount || "N/A"}</TableCell>
              </TableRow>
              <TableRow>
                <TableCell sx={{ fontWeight: 600, minWidth: "30rem" }}>
                  Restart Count
                </TableCell>
                <TableCell>{podSpecificInfo?.RestartCount}</TableCell>
              </TableRow>
              {/*<TableRow>*/}
              {/*  <TableCell sx={{ fontWeight: 600 }}>Container Count:</TableCell>*/}
              {/*  <TableCell>{podSpecificInfo?.ContainerCount}</TableCell>*/}
              {/*</TableRow>*/}
            </TableBody>
          </Table>
        </TableContainer>
      </Box>
    </Box>
  );
}
