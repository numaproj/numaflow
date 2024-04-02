import { SyntheticEvent, useState } from "react";

import {
  Box,
  Tab,
  Tabs,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableRow,
  TableHead,
  Paper,
} from "@mui/material";
import ReactJson from "react-json-view";
import TabPanel from "../../../../../../common/Tab-Panel";
import { a11yProps, handleCopy } from "../../../../../../../utils";
import { Pods } from "./partials/Pods";
import { NodeInfoProps } from "../../../../../../../types/declarations/graph";

export default function NodeInfo(props: NodeInfoProps) {
  const { node, namespaceId, pipelineId } = props;

  const [value, setValue] = useState(0);

  const handleChange = (event: SyntheticEvent, newValue: number) => {
    setValue(newValue);
  };

  const label = node?.id + " Vertex";
  const fontWeightStyle = { fontWeight: "bold" };

  if (!namespaceId || !pipelineId) {
    return (
      <Box>
        <Box sx={{ borderBottom: 1, borderColor: "divider" }}>
          <Tabs value={0}>
            <Tab style={fontWeightStyle} label={label} />
          </Tabs>
        </Box>
        <Box
          sx={{ mx: 3, my: 2 }}
        >{`Missing namespace or pipeline information`}</Box>
      </Box>
    );
  }

  if (node?.data?.type === "sideInput") {
    // returning just spec in case of side input
    return (
      <Box>
        <Box sx={{ borderBottom: 1, borderColor: "divider" }}>
          <Tabs value={0}>
            <Tab style={fontWeightStyle} label={`${node?.id} Generator`} />
          </Tabs>
        </Box>
        <Box sx={{ borderBottom: 1, borderColor: "divider" }}>
          <Tabs value={0}>
            {node?.data?.nodeInfo && (
              <Tab
                style={fontWeightStyle}
                data-testid="spec"
                label="Spec"
                {...a11yProps(1)}
              />
            )}
          </Tabs>
        </Box>

        {node?.data?.nodeInfo && (
          <TabPanel value={value} index={0}>
            <ReactJson
              name="spec"
              enableClipboard={handleCopy}
              theme="apathy:inverted"
              src={node.data.nodeInfo}
              style={{
                width: "100%",
                borderRadius: "0.4rem",
                fontFamily: "IBM Plex Sans",
              }}
            />
          </TabPanel>
        )}
      </Box>
    );
  }

  return (
    <Box>
      <Box sx={{ borderBottom: 1, borderColor: "divider" }}>
        <Tabs value={0}>
          <Tab
            style={fontWeightStyle}
            data-testid={node?.id}
            label={label}
            {...a11yProps(0)}
          />
        </Tabs>
      </Box>
      <Box sx={{ borderBottom: 1, borderColor: "divider" }}>
        <Tabs
          value={value}
          onChange={handleChange}
          aria-label={`${label}-details`}
        >
          {node?.id && (
            <Tab
              style={fontWeightStyle}
              data-testid="pods-view"
              label="Pods View"
              {...a11yProps(0)}
            />
          )}
          {node?.data?.nodeInfo && (
            <Tab
              style={fontWeightStyle}
              data-testid="spec"
              label="Spec"
              {...a11yProps(1)}
            />
          )}
          {node?.data?.vertexMetrics && (
            <Tab
              style={fontWeightStyle}
              data-testid="processing-rates"
              label="Processing Rates"
              {...a11yProps(2)}
            />
          )}
          {node?.data?.buffers && (
            <Tab
              style={fontWeightStyle}
              data-testid="buffers"
              label="Buffers"
              {...a11yProps(3)}
            />
          )}
        </Tabs>
      </Box>

      {node?.id && (
        <TabPanel data-testid="link" value={value} index={0}>
          <Pods
            namespaceId={namespaceId}
            pipelineId={pipelineId}
            vertexId={node.id}
          />
        </TabPanel>
      )}

      {node?.data?.nodeInfo && (
        <TabPanel value={value} index={1}>
          <ReactJson
            name="spec"
            enableClipboard={handleCopy}
            theme="apathy:inverted"
            src={node.data.nodeInfo}
            style={{
              width: "100%",
              borderRadius: "0.4rem",
              fontFamily: "IBM Plex Sans",
            }}
          />
        </TabPanel>
      )}

      {node?.data?.vertexMetrics && (
        <TabPanel value={value} index={2}>
          {node.data.vertexMetrics?.podMetrics && (
            <TableContainer
              component={Paper}
              sx={{ borderBottom: 1, borderColor: "divider" }}
            >
              <Table aria-label="pod-backpressure">
                <TableHead>
                  <TableRow>
                    <TableCell style={fontWeightStyle}>Partition</TableCell>
                    <TableCell style={fontWeightStyle}>1m</TableCell>
                    <TableCell style={fontWeightStyle}>5m</TableCell>
                    <TableCell style={fontWeightStyle}>15m</TableCell>
                  </TableRow>
                </TableHead>
                <TableBody>
                  {node.data.vertexMetrics.podMetrics.map(
                    (podMetric: any, idx: number) => {
                      return (
                        <TableRow key={`vertex-processingRate-${idx}`}>
                          <TableCell>{idx}</TableCell>
                          <TableCell>
                            {"processingRates" in podMetric &&
                              podMetric["processingRates"]["1m"].toFixed(2)}
                            {!("processingRates" in podMetric) && -1}
                          </TableCell>
                          <TableCell>
                            {"processingRates" in podMetric &&
                              podMetric["processingRates"]["5m"].toFixed(2)}
                            {!("processingRates" in podMetric) && -1}
                          </TableCell>
                          <TableCell>
                            {"processingRates" in podMetric &&
                              podMetric["processingRates"]["15m"].toFixed(2)}
                            {!("processingRates" in podMetric) && -1}
                          </TableCell>
                        </TableRow>
                      );
                    }
                  )}
                </TableBody>
              </Table>
            </TableContainer>
          )}
          {!node.data.vertexMetrics?.podMetrics && <Box>{`No pods found`}</Box>}
        </TabPanel>
      )}

      {node?.data?.buffers && (
        <TabPanel value={value} index={3}>
          <TableContainer
            component={Paper}
            sx={{ borderBottom: 1, borderColor: "divider" }}
          >
            <Table aria-label="edge-info">
              <TableHead>
                <TableRow>
                  <TableCell style={fontWeightStyle}>Partition</TableCell>
                  <TableCell style={fontWeightStyle}>isFull</TableCell>
                  <TableCell style={fontWeightStyle}>AckPending</TableCell>
                  <TableCell style={fontWeightStyle}>Pending</TableCell>
                  <TableCell style={fontWeightStyle}>Buffer Length</TableCell>
                  <TableCell style={fontWeightStyle}>Buffer Usage</TableCell>
                  <TableCell style={fontWeightStyle}>
                    Total Pending Messages
                  </TableCell>
                </TableRow>
              </TableHead>
              <TableBody>
                {node.data.buffers.map((buffer: any, idx: number) => {
                  let isFull;
                  if (buffer?.isFull) {
                    isFull = "yes";
                  } else {
                    isFull = "no";
                  }
                  let bufferUsage = "";
                  if (typeof buffer?.bufferUsage !== "undefined") {
                    bufferUsage = (buffer?.bufferUsage * 100).toFixed(2);
                  }
                  return (
                    <TableRow key={`node-buffer-info-${idx}`}>
                      <TableCell>{buffer?.bufferName}</TableCell>
                      <TableCell data-testid="isFull">{isFull}</TableCell>
                      <TableCell data-testid="ackPending">
                        {buffer?.ackPendingCount}
                      </TableCell>
                      <TableCell data-testid="pending">
                        {buffer?.pendingCount}
                      </TableCell>
                      <TableCell data-testid="bufferLength">
                        {buffer?.bufferLength}
                      </TableCell>
                      <TableCell data-testid="usage">{bufferUsage}%</TableCell>
                      <TableCell data-testid="totalMessages">
                        {buffer?.totalMessages}
                      </TableCell>
                    </TableRow>
                  );
                })}
              </TableBody>
            </Table>
          </TableContainer>
        </TabPanel>
      )}
    </Box>
  );
}
