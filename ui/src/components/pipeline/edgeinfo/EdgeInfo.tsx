import ReactJson from "react-json-view";
import { a11yProps, handleCopy } from "../../../utils";
import {
  Box,
  Paper,
  Tab,
  Table,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Tabs,
} from "@mui/material";
import TabPanel from "../tab-panel/TabPanel";
import { SyntheticEvent, useState } from "react";
import { Edge } from "reactflow";
import TableBody from "@mui/material/TableBody";

interface EdgeInfoProps {
  edge: Edge;
  edges: Edge[];
}

export default function EdgeInfo(props: EdgeInfoProps) {
  const { edge, edges } = props;

  const [value, setValue] = useState(0);

  const handleChange = (event: SyntheticEvent, newValue: number) => {
    setValue(newValue);
  };

  const label = `${edge?.id} Buffer`;

  return (
    <Box>
      <Box sx={{ borderBottom: 1, borderColor: "divider" }}>
        <Tabs value={value}>
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
          {edge?.data?.edgeWatermark && (
            <Tab
                style={{
                  fontWeight: "bold",
                }}
                label="Watermarks"
                {...a11yProps(1)}
            />
          )}
          {edge?.data?.conditions && (
            <Tab
              style={{
                fontWeight: "bold",
              }}
              data-testid="conditions"
              label="Conditions"
              {...a11yProps(2)}
            />
          )}
        </Tabs>
      </Box>
      {edge?.data && (
        <TabPanel value={value} index={0}>
          <TableContainer
            component={Paper}
            sx={{ borderBottom: 1, borderColor: "divider" }}
          >
            <Table aria-label="edge-info">
              <TableHead>
                <TableRow>
                  <TableCell >Edge</TableCell>
                  <TableCell >isFull</TableCell>
                  <TableCell >AckPending</TableCell>
                  <TableCell >Pending</TableCell>
                  <TableCell >Buffer Length</TableCell>
                  <TableCell >Buffer Usage</TableCell>
                  <TableCell >Total Messages</TableCell>
                </TableRow>
              </TableHead>
              <TableBody>
                {edges.map((singleEdge) => {
                  if (singleEdge?.source == edge?.data?.fromVertex && singleEdge?.target == edge?.data?.toVertex) {
                    let isFull;
                    if (singleEdge?.data?.isFull) {
                      isFull = "yes";
                    } else {
                      isFull = "no";
                    }
                    let bufferUsage = "";
                    if (typeof singleEdge?.data?.bufferUsage !== "undefined") {
                      bufferUsage = (singleEdge?.data?.bufferUsage * 100).toFixed(2);
                    }
                    return <TableRow>
                      <TableCell>{singleEdge.data.bufferName.slice(singleEdge.data.bufferName.indexOf('-') + 1)}</TableCell>
                      <TableCell data-testid="isFull">{isFull}</TableCell>
                      <TableCell data-testid="ackPending">{singleEdge.data.ackPending}</TableCell>
                      <TableCell data-testid="pending">{singleEdge.data.pending}</TableCell>
                      <TableCell data-testid="bufferLength">{singleEdge.data.bufferLength}</TableCell>
                      <TableCell data-testid="usage">{bufferUsage}%</TableCell>
                      <TableCell data-testid="totalMessages">{singleEdge.data.totalMessages}</TableCell>
                    </TableRow>
                  }
                })}
              </TableBody>
            </Table>
          </TableContainer>
        </TabPanel>
      )}

      {edge?.data?.edgeWatermark &&
          <TabPanel value={value} index={1}>
            <TableContainer
                component={Paper}
                sx={{ borderBottom: 1, borderColor: "divider", width: 400 }}
            >
              <Table aria-label="buffer-watermark">
                <TableHead>
                  <TableRow>
                    <TableCell>Partition</TableCell>
                    <TableCell >Watermark</TableCell>
                  </TableRow>
                </TableHead>
                <TableBody>
                  {edge?.data?.edgeWatermark?.watermarks &&
                      edge.data.edgeWatermark.watermarks.map((Watermark, idx) => (
                          <TableRow>
                            <TableCell >{idx}</TableCell>
                            <TableCell >{Watermark} ({new Date(Watermark).toISOString()})</TableCell>
                          </TableRow>
                      ))}
                </TableBody>
              </Table>
            </TableContainer>
          </TabPanel>
      }

      {edge?.data?.conditions && (
        <TabPanel value={value} index={2}>
          {edges.map((singleEdge) => {
            if (singleEdge?.data?.conditions && (singleEdge?.source == edge?.data?.fromVertex && singleEdge?.target == edge?.data?.toVertex)) {
              return <ReactJson
                  name="conditions"
                  enableClipboard={handleCopy}
                  theme="apathy:inverted"
                  src={singleEdge.data.conditions}
                  style={{
                    width: "100%",
                    borderRadius: "4px",
                    fontFamily: "IBM Plex Sans",
                  }}
              />
            }
          })}
        </TabPanel>
      )}
    </Box>
  );
}
