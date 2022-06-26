import {
  Box,
  Paper,
  Tab,
  Table,
  TableCell,
  TableContainer,
  TableRow,
  Tabs,
} from "@mui/material";
import { SyntheticEvent, useState } from "react";
import TabPanel from "../tab-panel/TabPanel";
import { a11yProps } from "../../../utils";

interface SpecProps {
  pipeline: any;
}

export default function Spec(props: SpecProps) {
  const { pipeline } = props;

  const [value, setValue] = useState(0);

  const handleChange = (event: SyntheticEvent, newValue: number) => {
    setValue(newValue);
  };

  return (
    <div>
      <Box sx={{ borderBottom: 1, borderColor: "divider" }}>
        <Tabs
          value={value}
          onChange={handleChange}
          aria-label="basic tabs example"
        >
          <Tab
            sx={{
              fontWeight: "bold",
              color: "grey",
              fontFamily: "IBM Plex Sans",
            }}
            label="Pipeline Details"
            {...a11yProps(0)}
          />
        </Tabs>
      </Box>
      <TabPanel value={value} index={0}>
        <TableContainer
          component={Paper}
          sx={{ borderBottom: 1, borderColor: "divider", width: 500 }}
        >
          <Table
            sx={{ borderBottom: 1, borderColor: "divider" }}
            aria-label="pipeline-spec"
          >
            <TableRow data-testid="phase">
              <TableCell>Phase</TableCell>
              <TableCell align="left">{pipeline?.status?.phase}</TableCell>
            </TableRow>
            <TableRow data-testid="resource-version">
              <TableCell>Resource Version</TableCell>
              <TableCell align="left">
                {pipeline?.metadata?.resourceVersion}
              </TableCell>
            </TableRow>
            <TableRow data-testid="creation-timestamp">
              <TableCell>Creation Timestamp</TableCell>
              <TableCell align="left">
                {pipeline?.metadata?.creationTimestamp}
              </TableCell>
            </TableRow>
            <TableRow data-testid="last-updated-timestamp">
              <TableCell>Last Updated Timestamp</TableCell>
              <TableCell align="left">
                {pipeline?.status?.lastUpdated}
              </TableCell>
            </TableRow>
          </Table>
        </TableContainer>
      </TabPanel>
    </div>
  );
}
