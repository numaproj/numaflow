import React, { useState, useEffect, useMemo } from "react";
import Box from "@mui/material/Box";
import TableContainer from "@mui/material/TableContainer";
import Table from "@mui/material/Table";
import TableBody from "@mui/material/TableBody";
import TableCell from "@mui/material/TableCell";
import TableHead from "@mui/material/TableHead";
import TableRow from "@mui/material/TableRow";
import CircularProgress from "@mui/material/CircularProgress";
import { usePiplelineWatermarksFetch } from "../../../../../utils/fetchWrappers/piplelineWatermarksFetch";
import { PipelineWatermarks } from "../../../../../types/declarations/pipeline";

import "./style.css";

export interface EdgeDetailsProps {
  namespaceId: string;
  pipelineId: string;
  edgeId: string;
}

export function EdgeDetails({
  namespaceId,
  pipelineId,
  edgeId,
}: EdgeDetailsProps) {
  const [edgeWatermarks, setEdgeWatermarks] = useState<
    PipelineWatermarks | undefined
  >();
  const { data, loading, error } = usePiplelineWatermarksFetch({
    namespace: namespaceId,
    pipeline: pipelineId,
  });

  useEffect(() => {
    if (!data) {
      return;
    }
    setEdgeWatermarks(data.find((metric) => metric.edgeId === edgeId));
  }, [data, edgeId]);

  const content = useMemo(() => {
    if (loading) {
      return (
        <Box
          sx={{ display: "flex", justifyContent: "center", marginTop: "1rem" }}
        >
          <CircularProgress />
        </Box>
      );
    }
    if (error) {
      return (
        <Box
          sx={{ display: "flex", justifyContent: "center", marginTop: "1rem" }}
        >
          {`Error loading processing rates: ${error}`}
        </Box>
      );
    }
    if (!data || !edgeWatermarks) {
      return (
        <Box
          sx={{ display: "flex", justifyContent: "center", marginTop: "1rem" }}
        >
          {`No resources found.`}
        </Box>
      );
    }
    return (
      <TableContainer
        sx={{
          maxHeight: "37.5rem",
          backgroundColor: "#FFF",
          marginTop: "1rem",
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
            {!edgeWatermarks.watermarks.length && (
              <TableRow>
                <TableCell colSpan={4} align="center">
                  No watermarks found
                </TableCell>
              </TableRow>
            )}
            {!!edgeWatermarks.watermarks.length &&
              edgeWatermarks.watermarks.map((watermark) => (
                <TableRow key={watermark.partition}>
                  <TableCell>{watermark.partition}</TableCell>
                  <TableCell>{`${watermark.watermark} (${watermark.formattedWatermark})`}</TableCell>
                </TableRow>
              ))}
          </TableBody>
        </Table>
      </TableContainer>
    );
  }, [data, loading, error, edgeWatermarks]);

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
      {content}
    </Box>
  );
}
