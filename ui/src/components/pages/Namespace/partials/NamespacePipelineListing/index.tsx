import React, { useState, useEffect, useCallback, useMemo } from "react";
import Box from "@mui/material/Box";
import Pagination from "@mui/material/Pagination";
import Grid from "@mui/material/Grid";
import { DebouncedSearchInput } from "../../../../common/DebouncedSearchInput";
import { PipelineCard } from "../PipelineCard";
import { createSvgIcon } from "@mui/material/utils";
import {
  NamespacePipelineListingProps,
  NamespacePipelineSummary,
} from "../../../../../types/declarations/namespace";

import "./style.css";
import {
  Button,
  MenuItem,
  Select,
  TableCell,
  TableSortLabel,
} from "@mui/material";

const MAX_PAGE_SIZE = 4;
export const HEALTH = ["All", "Healthy", "Warning", "Critical"];
export const STATUS = ["All", "Running", "Stopped", "Paused"];
export const sortOptions = [
  {
    label: "Last Updated",
    value: "lastUpdated",
    sortOrder: "desc",
  },
  {
    label: "Last Created",
    value: "lastCreated",
    sortOrder: "desc",
  },
  {
    label: "A-Z",
    value: "alphabeltical",
    sortOrder: "asc",
  },
];

const PlusIcon = createSvgIcon(
  // credit: plus icon from https://heroicons.com/
  <svg
    xmlns="http://www.w3.org/2000/svg"
    fill="none"
    viewBox="0 0 24 24"
    strokeWidth={1.5}
    stroke="currentColor"
  >
    <path
      strokeLinecap="round"
      strokeLinejoin="round"
      d="M12 4.5v15m7.5-7.5h-15"
    />
  </svg>,
  "Plus"
);

export function NamespacePipelineListing({
  namespace,
  data,
  pipelineData,
  isbData,
}: NamespacePipelineListingProps) {
  const [search, setSearch] = useState("");
  const [health, setHealth] = useState(HEALTH[0]);
  const [status, setStatus] = useState(STATUS[0]);
  const [page, setPage] = useState(1);
  const [orderBy, setOrderBy] = useState("lastUpdated");
  const [order, setOrder] = useState("desc");
  const [totalPages, setTotalPages] = useState(
    Math.ceil(data.pipelinesCount / MAX_PAGE_SIZE)
  );
  const [filteredPipelines, setFilteredPipelines] = useState<
    NamespacePipelineSummary[]
  >([]);
  // Update filtered pipelines based on search and page selected
  useEffect(() => {
    let filtered: NamespacePipelineSummary[] = data.pipelineSummaries;
    if (search) {
      // Filter by search
      filtered = data.pipelineSummaries.filter((p) => p.name.includes(search));
    }
    // Sort by name
    filtered.sort((a, b) => (a.name > b.name ? 1 : -1)); // TODO take sorting options into account

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
      // Reset to page 1 if current page is greater than total pages after filterting
      setPage(1);
    }
    // Set filtered namespaces with current page of pipelines
    setFilteredPipelines(pages[page - 1] || []);
    setTotalPages(pages.length);
  }, [data, search, page]);

  const handlePageChange = useCallback(
    (event: React.ChangeEvent<unknown>, value: number) => {
      setPage(value);
    },
    []
  );
  const handleSortChange = useCallback(
    (
      event: React.MouseEvent<HTMLAnchorElement | HTMLSpanElement>,
      value: string
    ) => {
      setOrderBy(value);
      setOrder(order === "asc" ? "desc" : "asc");
    },
    [order]
  );
  const listing = useMemo(() => {
    if (!filteredPipelines.length) {
      return (
        <Box
          sx={{
            display: "flex",
            flexDirection: "row",
            justifyContent: "center",
            margin: "0.5rem 0 1.5rem 0",
          }}
        >
          <span className="ns-pipeline-listing-table-title">
            No pipelines found
          </span>
        </Box>
      );
    }
    console.log("isbData", isbData);
    console.log("pipelineData", pipelineData);
    return (
      <Grid
        container
        rowSpacing={1}
        columnSpacing={1}
        wrap="wrap"
        sx={{
          margin: "0.5rem 0 1.5rem 0",
        }}
      >
        {filteredPipelines.map((p: NamespacePipelineSummary) => {
          return (
            <Grid key={`pipeline-${p.name}`} item xs={12}>
              <PipelineCard
                namespace={namespace}
                data={p}
                statusData={pipelineData ? pipelineData[p.name] : {}}
                isbData={
                  isbData
                    ? isbData[
                        pipelineData[p.name]?.pipeline?.metadata?.namespace
                      ]
                    : {}
                }
              />
            </Grid>
          );
        })}
      </Grid>
    );
  }, [filteredPipelines, namespace]);

  return (
    <Box
      sx={{
        display: "flex",
        flexDirection: "column",
        padding: "0 2.625rem",
      }}
    >
      <Box sx={{ display: "flex", flexDirection: "row", alignItems: "end" }}>
        <Box
          sx={{
            display: "flex",
            flexDirection: "row",
            flexGrow: 1,
            height: "44px",
            justifyItems: "end",
          }}
        >
          <DebouncedSearchInput
            placeHolder="Search for pipeline"
            onChange={setSearch}
          />
        </Box>
        <Box
          sx={{
            display: "flex",
            flexDirection: "row",
            flexGrow: 1,
          }}
        >
          <Box
            sx={{
              display: "flex",
              flexDirection: "column",
              flexGrow: 1,
            }}
          >
            <label style={{ color: "#6B6C72" }}>Health</label>
            <Select
              label="Health"
              defaultValue="All"
              inputProps={{
                name: "Health",
                id: "health",
              }}
              style={{
                width: "224px",
                background: "#fff",
                border: "1px solid #6B6C72",
                height: "34px",
              }}
              onChange={(e) => setHealth(e.target.value)}
            >
              {HEALTH.map((health) => (
                <MenuItem key={health} value={health}>
                  {health}
                </MenuItem>
              ))}
            </Select>
          </Box>
          <Box
            sx={{
              display: "flex",
              flexDirection: "column",
              flexGrow: 1,
            }}
          >
            <label style={{ color: "#6B6C72" }}>Status</label>
            <Select
              label="Status"
              defaultValue="All"
              inputProps={{
                name: "Status",
                id: "health",
              }}
              style={{
                width: "224px",
                background: "#fff",
                border: "1px solid #6B6C72",
                height: "34px",
              }}
              onChange={(e) => setStatus(e.target.value)}
            >
              {STATUS.map((status) => (
                <MenuItem key={status} value={status}>
                  {status}
                </MenuItem>
              ))}
            </Select>
          </Box>
        </Box>
      </Box>
      <Box sx={{ display: "flex", flexDirection: "row", marginTop: "2rem" }}>
        <Box sx={{ display: "flex", flexDirection: "row", flexGrow: 1 }}>
          <span className="ns-pipeline-listing-table-title">Pipelines</span>
        </Box>

        <Box
          sx={{
            display: "flex",
            flexDirection: "row",
            flexGrow: 1,
          }}
        >
          {sortOptions.map((option) => {
            return (
              <TableCell key={option.value} padding="normal">
                <TableSortLabel
                  active={orderBy === option.value}
                  onClick={(event) => handleSortChange(event, option.value)}
                  direction={
                    orderBy === option.value
                      ? (order as "desc" | "asc" | undefined)
                      : "asc"
                  }
                >
                  <span>{option.label}</span>
                </TableSortLabel>
              </TableCell>
            );
          })}
        </Box>
        <Box
          sx={{
            display: "flex",
            flexDirection: "row",
            flexGrow: 1,
            justifyContent: "flex-end",
          }}
        >
          <Button
            variant="outlined"
            startIcon={<PlusIcon />}
            size="small"
            sx={{ marginRight: "10px" }}
          >
            Create Pipeline
          </Button>
          <Button variant="outlined" startIcon={<PlusIcon />} size="small">
            Create ISB
          </Button>
        </Box>
      </Box>
      {listing}
      <Box
        sx={{
          display: "flex",
          flexDirection: "row",
          justifyContent: "center",
          marginBottom: "1.5rem",
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
}
