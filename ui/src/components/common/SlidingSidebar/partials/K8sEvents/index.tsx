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
import Select, { SelectChangeEvent } from "@mui/material/Select";
import MenuItem from "@mui/material/MenuItem";
import { K8sEvent } from "../../../../../types/declarations/namespace";
import { ErrorDisplay } from "../../../ErrorDisplay";
import { useNamespaceK8sEventsFetch } from "../../../../../utils/fetchWrappers/namespaceK8sEventsFetch";

import "./style.css";

const MAX_PAGE_SIZE = 6;
const DEFAULT_FILTER_VALUE = "All";

export interface K8sEventsProps {
  namespaceId: string;
  pipelineId?: string;
  vertexId?: string;
  headerText?: string;
  excludeHeader?: boolean;
  square?: boolean;
  pipelineFilterOptions?: string[];
  vertexFilterOptions?: Map<string, string[]>;
}

export function K8sEvents({
  namespaceId,
  pipelineId,
  vertexId,
  headerText = "Namespace K8s Events",
  excludeHeader = false,
  square = false,
  pipelineFilterOptions = [],
  vertexFilterOptions = new Map<string, string[]>(),
}: K8sEventsProps) {
  const [selectedPipeline, setSelectedPipeline] =
    useState<string>(DEFAULT_FILTER_VALUE);
  const [selectedVertex, setSelectedVertex] =
    useState<string>(DEFAULT_FILTER_VALUE);
  const [page, setPage] = useState(1);
  const [totalPages, setTotalPages] = useState(0);
  const [typeFilter, setTypeFilter] = useState<string | undefined>();
  const [filteredEvents, setFilteredEvents] = useState<K8sEvent[]>([]);
  const { data, loading, error } = useNamespaceK8sEventsFetch({
    namespace: namespaceId,
    pipeline:
      selectedPipeline === DEFAULT_FILTER_VALUE ? pipelineId : selectedPipeline,
    vertex: selectedVertex === DEFAULT_FILTER_VALUE ? vertexId : selectedVertex,
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

  const handlePipelineFilterChange = useCallback(
    (e: SelectChangeEvent<string>) => {
      setSelectedPipeline(e.target.value);
      setSelectedVertex(DEFAULT_FILTER_VALUE);
    },
    []
  );

  const handleVertexFilterChange = useCallback(
    (e: SelectChangeEvent<string>) => {
      setSelectedVertex(e.target.value);
    },
    []
  );

  const typeCounts = useMemo(() => {
    return (
      <ToggleButtonGroup
        value={typeFilter}
        exclusive
        onChange={handleTypeFilterChange}
      >
        <ToggleButton
          value="normal"
          aria-label="normal events filter"
          data-testid="normal-filter"
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
              {data?.normalCount || 0}
            </div>
          </Box>
        </ToggleButton>
        <ToggleButton
          value="warning"
          aria-label="warning events filter"
          data-testid="warn-filter"
        >
          <Box
            sx={{
              display: "flex",
              flexDirection: "row",
              alignItems: "center",
            }}
          >
            <span className="namespace-k8s-type-count-text">Warning</span>
            <div className="namespace-k8s-type-count-badge warn">
              {data?.warningCount || 0}
            </div>
          </Box>
        </ToggleButton>
      </ToggleButtonGroup>
    );
  }, [data, typeFilter, handleTypeFilterChange]);

  const table = useMemo(() => {
    if (loading) {
      return (
        <Box
          sx={{
            display: "flex",
            justifyContent: "center",
            alignItems: "center",
            height: "100%",
          }}
        >
          <CircularProgress />
        </Box>
      );
    }
    if (error) {
      return (
        <Box
          sx={{
            display: "flex",
            flexDirection: "column",
            height: "100%",
            width: "100%",
            justifyContent: "center",
            alignItems: "center",
          }}
        >
          <ErrorDisplay title="Error loading events" message={error} />
        </Box>
      );
    }
    return (
      <Box
        sx={{
          display: "flex",
          flexDirection: "column",
          borderTop: "1px solid #DCDCDC",
          marginTop: "1.6rem",
          height: "100%",
        }}
      >
        <TableContainer
          sx={{
            maxHeight: "60rem",
            backgroundColor: "#FFF",
          }}
        >
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
            marginTop: "1.6rem",
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
  }, [
    error,
    loading,
    data,
    page,
    totalPages,
    filteredEvents,
    handlePageChange,
  ]);

  const filters = useMemo(() => {
    const selectContainerStyle = {
      display: "flex",
      flexDirection: "column",
      marginRight: "0.8rem",
      fontSize: "1.6rem",
    };
    const selectStyle = {
      background: "#fff",
      border: "1px solid #6B6C72",
      height: "3.4rem",
      fontSize: "1.6rem",
    };
    if (vertexId) {
      // Given vertex, no filters
      return undefined;
    }
    if (pipelineId) {
      // Pipeline given
      if (
        !vertexFilterOptions.has(pipelineId) ||
        !vertexFilterOptions.get(pipelineId)?.length
      ) {
        // No vertex options
        return undefined;
      }
      // Vertex options available
      return (
        <Box sx={selectContainerStyle}>
          <label style={{ color: "#6B6C72" }}>Vertex</label>
          <Select
            label="Vertex"
            defaultValue={DEFAULT_FILTER_VALUE}
            value={selectedVertex}
            inputProps={{
              name: "Vertex",
              id: "Vertex",
            }}
            style={selectStyle}
            onChange={handleVertexFilterChange}
          >
            {(vertexFilterOptions.has(pipelineId)
              ? [
                  DEFAULT_FILTER_VALUE,
                  ...(vertexFilterOptions.get(pipelineId) || []),
                ]
              : [DEFAULT_FILTER_VALUE]
            ).map((pipeline) => (
              <MenuItem key={pipeline} value={pipeline}>
                {pipeline}
              </MenuItem>
            ))}
          </Select>
        </Box>
      );
    }
    // No pipeline given
    if (!pipelineFilterOptions.length) {
      // No filter options
      return undefined;
    }
    if (
      !selectedPipeline ||
      !vertexFilterOptions.has(selectedPipeline) ||
      !vertexFilterOptions.get(selectedPipeline)?.length
    ) {
      // No pipeline selected or one selected and no vertex options, just show pipeline selector
      return (
        <Box sx={selectContainerStyle}>
          <label style={{ color: "#6B6C72" }}>Pipeline</label>
          <Select
            label="Pipeline"
            defaultValue={DEFAULT_FILTER_VALUE}
            value={selectedPipeline}
            inputProps={{
              name: "Pipeline",
              id: "Pipeline",
            }}
            style={selectStyle}
            onChange={handlePipelineFilterChange}
          >
            {[DEFAULT_FILTER_VALUE, ...pipelineFilterOptions].map(
              (pipeline) => (
                <MenuItem key={pipeline} value={pipeline}>
                  {pipeline}
                </MenuItem>
              )
            )}
          </Select>
        </Box>
      );
    }
    // Pipeline selected and vertex options available
    return (
      <Box
        sx={{
          display: "flex",
          flexDirection: "row",
        }}
      >
        <Box sx={selectContainerStyle}>
          <label style={{ color: "#6B6C72" }}>Pipeline</label>
          <Select
            label="Pipeline"
            defaultValue={DEFAULT_FILTER_VALUE}
            value={selectedPipeline}
            inputProps={{
              name: "Pipeline",
              id: "Pipeline",
            }}
            style={selectStyle}
            onChange={handlePipelineFilterChange}
          >
            {[DEFAULT_FILTER_VALUE, ...pipelineFilterOptions].map(
              (pipeline) => (
                <MenuItem key={pipeline} value={pipeline}>
                  {pipeline}
                </MenuItem>
              )
            )}
          </Select>
        </Box>
        <Box sx={selectContainerStyle}>
          <label style={{ color: "#6B6C72" }}>Vertex</label>
          <Select
            label="Vertex"
            defaultValue={DEFAULT_FILTER_VALUE}
            value={selectedVertex}
            inputProps={{
              name: "Vertex",
              id: "Vertex",
            }}
            style={selectStyle}
            onChange={handleVertexFilterChange}
          >
            {(vertexFilterOptions.has(selectedPipeline)
              ? [
                  DEFAULT_FILTER_VALUE,
                  ...(vertexFilterOptions.get(selectedPipeline) || []),
                ]
              : [DEFAULT_FILTER_VALUE]
            ).map((pipeline) => (
              <MenuItem key={pipeline} value={pipeline}>
                {pipeline}
              </MenuItem>
            ))}
          </Select>
        </Box>
      </Box>
    );
  }, [
    vertexId,
    pipelineId,
    pipelineFilterOptions,
    vertexFilterOptions,
    selectedPipeline,
    selectedVertex,
    handlePipelineFilterChange,
    handleVertexFilterChange,
  ]);

  if (vertexId && !pipelineId) {
    return <Box>Missing Props</Box>;
  }

  return (
    <Box
      sx={{
        display: "flex",
        flexDirection: "column",
        height: "100%",
      }}
    >
      {!excludeHeader && (
        <span className="namespace-k8s-title">{headerText}</span>
      )}
      <Paper
        sx={{
          display: "flex",
          flexDirection: "column",
          padding: "1.6rem",
          marginTop: excludeHeader ? "0" : "3.2rem",
          height: "100%",
        }}
        square={square}
        elevation={0}
      >
        <Box sx={{ display: "flex", flexDirection: "row" }}>
          {filters}
          {typeCounts}
        </Box>
        {table}
      </Paper>
    </Box>
  );
}
