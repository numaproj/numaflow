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
import TabPanel from "../../common/Tab-Panel";
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

  const label = `${edge?.id} Edge`;

  return (
    <Box>
      <Box sx={{ borderBottom: 1, borderColor: "divider" }}>
        <Tabs value={0}>
          <Tab data-testid={edge?.id} label={label} {...a11yProps(0)} />
        </Tabs>
      </Box>
      <Box sx={{ borderBottom: 1, borderColor: "divider" }}>
        <Tabs
          value={value}
          onChange={handleChange}
          aria-label={`${label}-details`}
        >
          {edge?.data?.edgeWatermark && (
            <Tab
              style={{
                fontWeight: "bold",
              }}
              data-testid="watermarks"
              label="Watermarks"
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

      {edge?.data?.edgeWatermark && (
        <TabPanel value={value} index={0}>
          <TableContainer
            component={Paper}
            sx={{ borderBottom: 1, borderColor: "divider", width: 600 }}
          >
            <Table aria-label="buffer-watermark">
              <TableHead>
                <TableRow>
                  <TableCell>Partition</TableCell>
                  <TableCell>Watermark</TableCell>
                </TableRow>
              </TableHead>
              <TableBody>
                {edge.data.edgeWatermark?.watermarks &&
                  edge.data.edgeWatermark.watermarks.map((Watermark, idx) => (
                    <TableRow key={`edge-watermark-${idx}`}>
                      <TableCell>{idx}</TableCell>
                      <TableCell>
                        {Watermark} ({new Date(Watermark).toISOString()})
                      </TableCell>
                    </TableRow>
                  ))}
              </TableBody>
            </Table>
          </TableContainer>
        </TabPanel>
      )}

      {edge?.data?.conditions && (
        <TabPanel value={value} index={1}>
          {edges.map((singleEdge, idx) => {
            if (
              singleEdge?.data?.conditions &&
              singleEdge?.source == edge?.data?.fromVertex &&
              singleEdge?.target == edge?.data?.toVertex
            ) {
              return (
                <ReactJson
                  name="conditions"
                  key={`edge-condition-${idx}`}
                  enableClipboard={handleCopy}
                  theme="apathy:inverted"
                  src={singleEdge.data.conditions}
                  style={{
                    width: "100%",
                    borderRadius: "4px",
                    fontFamily: "IBM Plex Sans",
                  }}
                />
              );
            }
          })}
        </TabPanel>
      )}
    </Box>
  );
}
