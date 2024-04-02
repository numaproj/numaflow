import React from "react";
import Box from "@mui/material/Box";
import TableContainer from "@mui/material/TableContainer";
import Table from "@mui/material/Table";
import TableBody from "@mui/material/TableBody";
import TableCell from "@mui/material/TableCell";
import TableHead from "@mui/material/TableHead";
import TableRow from "@mui/material/TableRow";

import "./style.css";

export interface EdgeDetailsProps {
  edgeId: string;
  watermarks: number[];
}

export function EdgeDetails({ edgeId, watermarks }: EdgeDetailsProps) {
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
              watermarks.map((watermark: number, index: number) => (
                <TableRow key={index}>
                  <TableCell>{index}</TableCell>
                  <TableCell>
                    {watermark < 0
                      ? watermark
                      : `${watermark} (${new Date(watermark).toISOString()})`}
                  </TableCell>
                </TableRow>
              ))}
          </TableBody>
        </Table>
      </TableContainer>
    </Box>
  );
}
