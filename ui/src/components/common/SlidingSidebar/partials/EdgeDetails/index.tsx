import React, { useState } from "react";
import Box from "@mui/material/Box";
import Tabs from "@mui/material/Tabs";
import Tab from "@mui/material/Tab";
import TableContainer from "@mui/material/TableContainer";
import Table from "@mui/material/Table";
import TableBody from "@mui/material/TableBody";
import TableCell from "@mui/material/TableCell";
import TableHead from "@mui/material/TableHead";
import TableRow from "@mui/material/TableRow";
import { usePipelineISBDebugFetch } from "../../../../../utils/fetchWrappers/pipelineISBDebugFetch";
import { PipelineISBDebugInfo } from "../PipelineISBDebugInfo";

import "./style.css";

const WATERMARKS_TAB_INDEX = 0;
const ISB_TAB_INDEX = 1;

export interface EdgeDetailsProps {
  namespaceId?: string;
  pipelineId?: string;
  edgeId: string;
  from?: string;
  to?: string;
  watermarks?: (string | number)[];
}

export function EdgeDetails({
  namespaceId,
  pipelineId,
  edgeId,
  from,
  to,
  watermarks,
}: EdgeDetailsProps) {
  const [tabValue, setTabValue] = useState<number>(WATERMARKS_TAB_INDEX);
  const {
    data: isbDebugData,
    loading: isbDebugLoading,
    error: isbDebugError,
    refresh: isbDebugRefresh,
  } = usePipelineISBDebugFetch({
    namespaceId,
    pipelineId,
    from,
    to,
    enabled: tabValue === ISB_TAB_INDEX,
  });

  return (
    <Box
      sx={{
        display: "flex",
        flexDirection: "column",
        height: "100%",
      }}
    >
      <Box
        sx={{
          display: "flex",
          flexDirection: "row",
        }}
      >
        <span className="edge-details-header-text">{`${edgeId} Edge`}</span>
      </Box>
      <Box
        sx={{ marginTop: "1.6rem", borderBottom: 1, borderColor: "divider" }}
      >
        <Tabs
          className="edge-details-tabs"
          value={tabValue}
          onChange={(_, newValue) => setTabValue(newValue)}
        >
          <Tab
            className={
              tabValue === WATERMARKS_TAB_INDEX
                ? "edge-details-tab-selected"
                : "edge-details-tab"
            }
            label="Watermarks"
            data-testid="edge-watermarks-tab"
          />
          <Tab
            className={
              tabValue === ISB_TAB_INDEX
                ? "edge-details-tab-selected"
                : "edge-details-tab"
            }
            label="ISB"
            data-testid="edge-isb-tab"
          />
        </Tabs>
      </Box>
      <div role="tabpanel" hidden={tabValue !== WATERMARKS_TAB_INDEX}>
        {tabValue === WATERMARKS_TAB_INDEX && (
          <TableContainer
            sx={{
              maxHeight: "60rem",
              backgroundColor: "#FFF",
              marginTop: "1.6rem",
            }}
          >
            <Table stickyHeader>
              <TableHead>
                <TableRow>
                  <TableCell>Partition</TableCell>
                  <TableCell>Watermark</TableCell>
                </TableRow>
              </TableHead>
              <TableBody>
                {(!watermarks || !watermarks.length) && (
                  <TableRow>
                    <TableCell colSpan={4} align="center">
                      No watermarks found
                    </TableCell>
                  </TableRow>
                )}
                {!!watermarks &&
                  !!watermarks.length &&
                  watermarks.map(
                    (watermark: string | number, index: number) => (
                      <TableRow key={index}>
                        <TableCell>{index}</TableCell>
                        <TableCell>
                          {Number(watermark) < 0
                            ? watermark
                            : `${watermark} (${new Date(
                                Number(watermark)
                              ).toISOString()})`}
                        </TableCell>
                      </TableRow>
                    )
                  )}
              </TableBody>
            </Table>
          </TableContainer>
        )}
      </div>
      <div role="tabpanel" hidden={tabValue !== ISB_TAB_INDEX}>
        {tabValue === ISB_TAB_INDEX && (
          <Box sx={{ marginTop: "1.6rem" }}>
            <PipelineISBDebugInfo
              streams={isbDebugData?.streams}
              consumers={isbDebugData?.consumers}
              kvStores={isbDebugData?.kvStores}
              loading={isbDebugLoading}
              error={isbDebugError}
              edgeScoped
              onRefresh={isbDebugRefresh}
            />
          </Box>
        )}
      </div>
    </Box>
  );
}
