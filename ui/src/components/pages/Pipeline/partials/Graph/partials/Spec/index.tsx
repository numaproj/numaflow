import {
  Box,
  Paper,
  Tab,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableRow,
  Tabs,
} from "@mui/material";
import TabPanel from "../../../../../../common/Tab-Panel";
import { a11yProps } from "../../../../../../../utils";
import { SpecProps } from "../../../../../../../types/declarations/graph";

export default function Spec(props: SpecProps) {
  const { pipeline } = props;

  const fontWeightStyle = { fontWeight: "bold" };

  return (
    <Box>
      <Box sx={{ borderBottom: 1, borderColor: "divider" }}>
        <Tabs value={0} aria-label="pipeline details">
          <Tab
            sx={fontWeightStyle}
            label="Pipeline Details"
            {...a11yProps(0)}
          />
        </Tabs>
      </Box>
      <TabPanel value={0} index={0}>
        <TableContainer
          component={Paper}
          sx={{ borderBottom: 1, borderColor: "divider", width: 600 }}
        >
          <Table
            sx={{ borderBottom: 1, borderColor: "divider" }}
            aria-label="pipeline-spec"
          >
            <TableBody>
              <TableRow data-testid="phase">
                <TableCell sx={fontWeightStyle}>Phase</TableCell>
                <TableCell align="left">{pipeline?.status?.phase}</TableCell>
              </TableRow>
              <TableRow data-testid="creation-timestamp">
                <TableCell sx={fontWeightStyle}>Creation Timestamp</TableCell>
                <TableCell align="left">
                  {pipeline?.metadata?.creationTimestamp}
                </TableCell>
              </TableRow>
              <TableRow data-testid="last-updated-timestamp">
                <TableCell sx={fontWeightStyle}>
                  Last Updated Timestamp
                </TableCell>
                <TableCell align="left">
                  {pipeline?.status?.lastUpdated}
                </TableCell>
              </TableRow>
            </TableBody>
          </Table>
        </TableContainer>
      </TabPanel>
    </Box>
  );
}
