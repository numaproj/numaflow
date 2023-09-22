import React, { useCallback, useMemo, useEffect } from "react";
import Box from "@mui/material/Box";
import Paper from "@mui/material/Paper";
import TableContainer from "@mui/material/TableContainer";
import Table from "@mui/material/Table";
import TableBody from "@mui/material/TableBody";
import TableCell from "@mui/material/TableCell";
import TableHead from "@mui/material/TableHead";
import TableRow from "@mui/material/TableRow";
import Pagination from "@mui/material/Pagination";
import CircularProgress from "@mui/material/CircularProgress";
import { K8sEvent } from "../../../../../types/declarations/namespace";
import { useNamespaceK8sEventsFetch } from "../../../../../utils/fetchWrappers/namespaceK8sEventsFetch";

import "./style.css";

const MAX_PAGE_SIZE = 7;

export interface NamespaceK8sProps {
  namespaceId: string;
}

export function NamespaceK8s({ namespaceId }: NamespaceK8sProps) {
  const [page, setPage] = React.useState(1);
  const [totalPages, setTotalPages] = React.useState(0);
  const [filteredEvents, setFilteredEvents] = React.useState<K8sEvent[]>([]);
  const { data, loading, error } = useNamespaceK8sEventsFetch({
    namespace: namespaceId,
  });

  // Update filtered events based on page selected
  useEffect(() => {
    if (!data) {
      setFilteredEvents([]);
      setPage(1);
      setTotalPages(0);
      return;
    }
    const filtered: K8sEvent[] = data.events;
    // Sort by timestamp (server gives in oldest to newest)
    filtered.reverse();

    // Break list into pages
    const pages = filtered.reduce((resultArray: any[], item, index) => {
      const chunkIndex = Math.floor(index / MAX_PAGE_SIZE);
      if (!resultArray[chunkIndex]) {
        resultArray[chunkIndex] = [];
      }
      resultArray[chunkIndex].push(item);
      return resultArray;
    }, []);

    if (page > pages.length) {
      // Reset to page 1 if current page is greater than total pages after filtering
      setPage(1);
    }
    // Set filtered namespaces with current page of namespaces
    setFilteredEvents(pages[page - 1] || []);
    setTotalPages(pages.length);
  }, [data, page]);

  const handlePageChange = useCallback(
    (event: React.ChangeEvent<unknown>, value: number) => {
      setPage(value);
    },
    []
  );

  const typeCounts = useMemo(() => {
    if (!data) {
      return undefined;
    }
    return (
      <Box
        sx={{
          display: "flex",
          flexDirection: "row",
          alignItems: "center",
        }}
      >
        <Box
          sx={{
            display: "flex",
            flexDirection: "row",
            alignItems: "center",
          }}
        >
          <span className="namespace-k8s-type-count-text">Normal</span>
          <div className="namespace-k8s-type-count-badge normal">
            {data.normalCount}
          </div>
        </Box>
        <Box
          sx={{
            display: "flex",
            flexDirection: "row",
            alignItems: "center",
            marginLeft: "1rem",
          }}
        >
          <span className="namespace-k8s-type-count-text">Warning</span>
          <div className="namespace-k8s-type-count-badge warn">
            {data.warningCount}
          </div>
        </Box>
      </Box>
    );
  }, [data]);

  const table = useMemo(() => {
    if (!data) {
      return undefined;
    }
    return (
      <Box
        sx={{
          display: "flex",
          flexDirection: "column",
          borderTop: "1px solid #DCDCDC",
          marginTop: "16px",
          height: "100%",
        }}
      >
        <TableContainer sx={{ height: "100%", backgroundColor: "#FFF" }}>
          <Table stickyHeader>
            <TableHead>
              <TableRow>
                <TableCell>Name</TableCell>
                <TableCell>Component</TableCell>
                <TableCell>Reason</TableCell>
                <TableCell>Message</TableCell>
              </TableRow>
            </TableHead>
            <TableBody>
              {!filteredEvents.length && (
                <TableRow>
                  <TableCell colSpan={4} align="center">
                    No events found
                  </TableCell>
                </TableRow>
              )}
              {!!filteredEvents.length &&
                filteredEvents.map((event) => (
                  <TableRow key={event.name}>
                    <TableCell>{event.name}</TableCell>
                    <TableCell>{event.component}</TableCell>
                    <TableCell>{event.reason}</TableCell>
                    <TableCell>{event.message}</TableCell>
                  </TableRow>
                ))}
            </TableBody>
          </Table>
        </TableContainer>
        <Box
          sx={{
            display: "flex",
            flexDirection: "row",
            justifyContent: "center",
            marginTop: "1rem",
          }}
        >
          <Pagination
            count={totalPages}
            page={page}
            onChange={handlePageChange}
            shape="rounded"
          />
        </Box>
      </Box>
    );
  }, [data, page, totalPages, filteredEvents, handlePageChange]);

  return (
    <Box
      sx={{
        display: "flex",
        flexDirection: "column",
        height: "100%",
      }}
    >
      <span className="namespace-k8s-title">Namespace K8s Event Logs</span>
      <Paper
        sx={{
          display: "flex",
          flexDirection: "column",
          padding: "1rem",
          marginTop: "2rem",
        }}
        elevation={0}
      >
        {loading && (
          <Box sx={{ display: "flex", justifyContent: "center" }}>
            <CircularProgress />
          </Box>
        )}
        {error && (
          <span className="namespace-k8s-error">{`Error loading events: ${error}`}</span>
        )}
        {typeCounts}
        {table}
      </Paper>
    </Box>
  );
}
