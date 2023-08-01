import { SyntheticEvent, useState } from "react";

import {
  Box,
  Paper,
  Tab,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Tabs,
} from "@mui/material";
import ReactJson from "react-json-view";
import TabPanel from "../../../../../../common/Tab-Panel";
import { a11yProps, handleCopy } from "../../../../../../../utils";
import { EdgeInfoProps } from "../../../../../../../types/declarations/graph";

export default function EdgeInfo(props: EdgeInfoProps) {
  const { edge } = props;

  const [value, setValue] = useState(0);

  const handleChange = (event: SyntheticEvent, newValue: number) => {
    setValue(newValue);
  };

  const label = `${edge?.id} Edge`;
  const fontWeightStyle = { fontWeight: "bold" };

  return (
    <Box>
      <Box sx={{ borderBottom: 1, borderColor: "divider" }}>
        <Tabs value={0}>
          <Tab
            style={fontWeightStyle}
            data-testid={edge?.id}
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
          {edge?.data?.edgeWatermark && (
            <Tab
              style={fontWeightStyle}
              data-testid="watermarks"
              label="Watermarks"
              {...a11yProps(0)}
            />
          )}
          {edge?.data?.conditions && (
            <Tab
              style={fontWeightStyle}
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
            <Table aria-label="partition-watermark">
              <TableHead>
                <TableRow>
                  <TableCell style={fontWeightStyle}>Partition</TableCell>
                  <TableCell style={fontWeightStyle}>Watermark</TableCell>
                </TableRow>
              </TableHead>
              <TableBody>
                {edge.data.edgeWatermark?.watermarks &&
                  edge.data.edgeWatermark.watermarks.map((wmVal, idx) => {
                    return (
                      <TableRow key={`edge-watermark-${idx}`}>
                        <TableCell>{idx}</TableCell>
                        <TableCell>
                          {wmVal} ({new Date(wmVal).toISOString()})
                        </TableCell>
                      </TableRow>
                    );
                  })}
              </TableBody>
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
    </Box>
  );
}
