import ReactJson from "react-json-view";
import { a11yProps, handleCopy } from "../../../utils";
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
import TabPanel from "../tab-panel/TabPanel";
import { SyntheticEvent, useState } from "react";
import { Edge } from "react-flow-renderer";

interface EdgeInfoProps {
  edge: Edge;
}

export default function EdgeInfo(props: EdgeInfoProps) {
  const { edge } = props;

  const [value, setValue] = useState(0);

  const handleChange = (event: SyntheticEvent, newValue: number) => {
    setValue(newValue);
  };

  let bufferUsage = "";
  if (typeof edge?.data?.bufferUsage !== "undefined") {
    bufferUsage = (edge?.data?.bufferUsage * 100).toFixed(2);
  }

  let isFull = "";

  if (edge?.data?.isFull) {
    isFull = "yes";
  } else {
    isFull = "no";
  }

  const label = `${edge?.id} Buffer`;

  return (
    <div>
      <Box sx={{ borderBottom: 1, borderColor: "divider" }}>
        <Tabs>
          <Tab label={label} {...a11yProps(0)} />
        </Tabs>
      </Box>
      <Box sx={{ borderBottom: 1, borderColor: "divider" }}>
        <Tabs
          value={value}
          onChange={handleChange}
          aria-label="basic tabs example"
        >
          {edge?.data && (
            <Tab
              style={{
                fontWeight: "bold",
              }}
              label="Info"
              {...a11yProps(0)}
            />
          )}
          {edge?.data?.conditions && (
            <Tab
              style={{
                fontWeight: "bold",
              }}
              data-testid="conditions"
              label="Conditions"
              {...a11yProps(1)}
            />
          )}
        </Tabs>
      </Box>
      {edge?.data && (
        <TabPanel value={value} index={0}>
          <TableContainer
            component={Paper}
            sx={{ borderBottom: 1, borderColor: "divider", width: 300 }}
          >
            <Table aria-label="edge-info">
              <TableRow data-testid="isFull">
                <TableCell>isFull</TableCell>
                <TableCell>{isFull}</TableCell>
              </TableRow>
              <TableRow data-testid="pending">
                <TableCell>Pending</TableCell>
                <TableCell>{edge?.data?.pending}</TableCell>
              </TableRow>
              <TableRow data-testid="ackPending">
                <TableCell>Ack Pending</TableCell>
                <TableCell>{edge?.data?.ackPending}</TableCell>
              </TableRow>
              <TableRow data-testid="bufferLength">
                <TableCell>Buffer Length</TableCell>
                <TableCell>{edge?.data?.bufferLength}</TableCell>
              </TableRow>
              <TableRow data-testid="usage">
                <TableCell>Buffer Usage</TableCell>
                <TableCell>{bufferUsage}%</TableCell>
              </TableRow>
            </Table>
          </TableContainer>
        </TabPanel>
      )}

      {edge?.data?.conditions && (
        <TabPanel value={value} index={1}>
          <ReactJson
            name="conditions"
            enableClipboard={handleCopy}
            theme="apathy:inverted"
            src={edge.data.conditions}
            style={{
              width: "100%",
              borderRadius: "4px",
              fontFamily: "IBM Plex Sans",
            }}
          />
        </TabPanel>
      )}
    </div>
  );
}
