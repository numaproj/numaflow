import ReactJson from "react-json-view";
import { a11yProps, handleCopy } from "../../../utils";
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
import { SyntheticEvent, useState } from "react";
import TabPanel from "../../common/Tab-Panel";
import { Pods } from "../../pods/Pods";
import { Node } from "reactflow";

interface NodeInfoProps {
  node: Node;
  namespaceId: string | undefined;
  pipelineId: string | undefined;
}

export default function NodeInfo(props: NodeInfoProps) {
  const { node, namespaceId, pipelineId } = props;

  if (!namespaceId || !pipelineId) {
    return null;
  }

  const [value, setValue] = useState(0);

  const handleChange = (event: SyntheticEvent, newValue: number) => {
    setValue(newValue);
  };

  const label = node?.id + " Vertex";

  return (
    <Box>
      <Box sx={{ borderBottom: 1, borderColor: "divider" }}>
        <Tabs value={0}>
          <Tab data-testid={node?.id} label={label} {...a11yProps(0)} />
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
              data-testid="pods-view"
              style={{ fontWeight: "bold" }}
              label="Pods View"
              {...a11yProps(0)}
            />
          )}
          {node?.data && (
            <Tab
              data-testid="vertex-info"
              style={{ fontWeight: "bold" }}
              label="Spec"
              {...a11yProps(1)}
            />
          )}
          {node?.data?.vertexMetrics && (
            <Tab
              data-testid="processing-rates"
              style={{ fontWeight: "bold" }}
              label="Processing Rates"
              {...a11yProps(2)}
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
      {node?.data && (
        <TabPanel value={value} index={1}>
          <ReactJson
            name="spec"
            enableClipboard={handleCopy}
            theme="apathy:inverted"
            src={
              node.data?.source
                ? node.data.source
                : node.data?.udf
                ? node.data.udf
                : node.data.sink
            }
            style={{
              width: "100%",
              borderRadius: "4px",
              fontFamily: "IBM Plex Sans",
            }}
          />
        </TabPanel>
      )}
      {node?.data?.vertexMetrics && (
        <TabPanel value={value} index={2}>
          {node?.data?.vertexMetrics?.podMetrics && (
            <TableContainer
              component={Paper}
              sx={{ borderBottom: 1, borderColor: "divider" }}
            >
              <Table aria-label="pod-backpressure">
                <TableHead>
                  <TableRow>
                    <TableCell>Partition</TableCell>
                    <TableCell>1m</TableCell>
                    <TableCell>5m</TableCell>
                    <TableCell>15m</TableCell>
                  </TableRow>
                </TableHead>
                <TableBody>
                  {node.data.vertexMetrics.podMetrics.map((podMetric, idx) => {
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
                  })}
                </TableBody>
              </Table>
            </TableContainer>
          )}
          {!node?.data?.vertexMetrics?.podMetrics && (
            <Box>{`No pods found`}</Box>
          )}
        </TabPanel>
      )}
    </Box>
  );
}
