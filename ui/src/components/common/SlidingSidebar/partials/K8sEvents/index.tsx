import React, { useCallback, useMemo, useEffect, useState } from "react";
import Box from "@mui/material/Box";
import Paper from "@mui/material/Paper";
import TableContainer from "@mui/material/TableContainer";
import Table from "@mui/material/Table";
import TableBody from "@mui/material/TableBody";
import TableCell from "@mui/material/TableCell";
import TableHead from "@mui/material/TableHead";
import TableRow from "@mui/material/TableRow";
import Pagination from "@mui/material/Pagination";
import ToggleButtonGroup from "@mui/material/ToggleButtonGroup";
import ToggleButton from "@mui/material/ToggleButton";
import CircularProgress from "@mui/material/CircularProgress";
import { K8sEvent } from "../../../../../types/declarations/namespace";
import { useNamespaceK8sEventsFetch } from "../../../../../utils/fetchWrappers/namespaceK8sEventsFetch";

import "./style.css";

const MAX_PAGE_SIZE = 6;

export interface K8sEventsProps {
  namespaceId: string;
  excludeHeader?: boolean;
  square?: boolean;
}

export function K8sEvents({
  namespaceId,
  excludeHeader = false,
  square = false,
}: K8sEventsProps) {
  const [page, setPage] = useState(1);
  const [totalPages, setTotalPages] = useState(0);
  const [typeFilter, setTypeFilter] = useState<string | undefined>();
  const [filteredEvents, setFilteredEvents] = React.useState<K8sEvent[]>([]);
  const { data, loading, error } = useNamespaceK8sEventsFetch({
    namespace: namespaceId,
  });

  // Update filtered events based on page selected and filter
  useEffect(() => {
    if (!data) {
      setFilteredEvents([]);
      setPage(1);
      setTotalPages(0);
      return;
    }
    let filtered: K8sEvent[] = data.events;
    if (typeFilter) {
      filtered = filtered.filter(
        (event) => event.type.toLowerCase() === typeFilter
      );
    }

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
    // Set filtered namespaces with current page
    setFilteredEvents(pages[page - 1] || []);
    setTotalPages(pages.length);
  }, [data, page, typeFilter]);

  const handlePageChange = useCallback(
    (event: React.ChangeEvent<unknown>, value: number) => {
      setPage(value);
    },
    []
  );

  const handleTypeFilterChange = useCallback(
    (event: React.MouseEvent<HTMLElement>, value: string | undefined) => {
      setTypeFilter(value);
    },
    []
  );

  const typeCounts = useMemo(() => {
    if (!data) {
      return undefined;
    }

    return (
      <ToggleButtonGroup
        value={typeFilter}
        exclusive
        onChange={handleTypeFilterChange}
      >
        <ToggleButton value="normal" aria-label="normal events filter">
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
        </ToggleButton>
        <ToggleButton value="warning" aria-label="warning events filter">
          <Box
            sx={{
              display: "flex",
              flexDirection: "row",
              alignItems: "center",
            }}
          >
            <span className="namespace-k8s-type-count-text">Warning</span>
            <div className="namespace-k8s-type-count-badge warn">
              {data.warningCount}
            </div>
          </Box>
        </ToggleButton>
      </ToggleButtonGroup>
    );
  }, [data, typeFilter, handleTypeFilterChange]);

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
        <TableContainer sx={{ maxHeight: "37.5rem", backgroundColor: "#FFF" }}>
          <Table stickyHeader>
            <TableHead>
              <TableRow>
                <TableCell>Timestamp</TableCell>
                <TableCell>Type</TableCell>
                <TableCell>Object</TableCell>
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
                  <TableRow key={event.eventKey}>
                    <TableCell>{event.timestamp}</TableCell>
                    <TableCell>{event.type}</TableCell>
                    <TableCell>{event.object}</TableCell>
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
      {!excludeHeader && (
        <span className="namespace-k8s-title">K8s Events</span>
      )}
      <Paper
        sx={{
          display: "flex",
          flexDirection: "column",
          padding: "1rem",
          marginTop: excludeHeader ? "0" : "2rem",
        }}
        square={square}
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
