import React, { useState, useEffect, useCallback, useMemo } from "react";
import Box from "@mui/material/Box";
import { MenuItem, Select } from "@mui/material";
import Pagination from "@mui/material/Pagination";
import Grid from "@mui/material/Grid";
import { DebouncedSearchInput } from "../../../../common/DebouncedSearchInput";
import { NamespaceCard } from "../NamespaceCard";
import { ErrorIndicator } from "../../../../common/ErrorIndicator";
import {
  ClusterNamespaceListingProps,
  ClusterNamespaceSummary,
} from "../../../../../types/declarations/cluster";
import {
  ACTIVE,
  ALL,
  CRITICAL,
  HEALTHY,
  INACTIVE,
  INACTIVE_STATUS,
  NO_PIPELINES,
  WARNING,
  WITH_PIPELINES,
} from "../../../../../utils";

import "./style.css";

const MAX_PAGE_SIZE = 6;

const HEALTH = [ALL, HEALTHY, WARNING, CRITICAL, INACTIVE_STATUS];

export function ClusterNamespaceListing({
  data,
}: ClusterNamespaceListingProps) {
  const [search, setSearch] = useState("");
  const [page, setPage] = useState(1);
  const nFilter =
    data?.nameSpaceSummaries?.filter((ns) => !ns.isEmpty).length > 0
      ? WITH_PIPELINES
      : ALL;
  const [namespaceFilter, setNamespaceFilter] = useState(nFilter);
  const [healthFilter, setHealthFilter] = useState(ALL);
  const [statusFilter, setStatusFilter] = useState(ALL);
  const [totalPages, setTotalPages] = useState(
    Math.ceil(data.namespacesCount / MAX_PAGE_SIZE)
  );
  const [filteredNamespaces, setFilteredNamespaces] = useState<
    ClusterNamespaceSummary[]
  >([]);

  // Update filtered namespaces based on search and page selected
  useEffect(() => {
    let filtered: ClusterNamespaceSummary[] = data.nameSpaceSummaries;
    if (search) {
      // Filter by search
      filtered = data.nameSpaceSummaries.filter((ns) =>
        ns.name.includes(search)
      );
    }
    // Sort by name
    filtered?.sort((a, b) => (a.name > b.name ? 1 : -1));

    //Filter based on the empty pipelines filter
    if (namespaceFilter === WITH_PIPELINES) {
      filtered = filtered.filter((ns) => !ns.isEmpty);
    } else if (namespaceFilter === NO_PIPELINES) {
      filtered = filtered.filter((ns) => ns.isEmpty);
    }

    //Filter namespaces with pipelines based on health
    filtered = filtered.filter((ns) => {
      if (healthFilter === HEALTHY) {
        return ns.pipelinesHealthyCount > 0;
      } else if (healthFilter === WARNING) {
        return ns.pipelinesWarningCount > 0;
      } else if (healthFilter === CRITICAL) {
        return ns.pipelinesCriticalCount > 0;
      } else if (healthFilter === INACTIVE_STATUS) {
        return ns.pipelinesInactiveCount > 0;
      } else {
        return true;
      }
    });

    //Filter namespaces with pipelines based on status
    filtered = filtered.filter((ns) => {
      if (statusFilter === ACTIVE) {
        return ns.pipelinesActiveCount > 0;
      } else if (statusFilter === INACTIVE) {
        return ns.pipelinesInactiveCount > 0;
      } else {
        return true;
      }
    });

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
    // Set filtered namespaces with current page of namespaces
    setFilteredNamespaces(pages[page - 1] || []);
    setTotalPages(pages.length);
  }, [data, search, page, namespaceFilter, healthFilter, statusFilter]);

  const handlePageChange = useCallback(
    (_: React.ChangeEvent<unknown>, value: number) => {
      setPage(value);
    },
    []
  );

  const handleNamespaceFilterChange = useCallback((event: any) => {
    setNamespaceFilter(event.target.value);
  }, []);

  const handleHealthFilterChange = useCallback((event: any) => {
    setHealthFilter(event.target.value);
  }, []);

  const handleStatusFilterChange = useCallback((event: any) => {
    setStatusFilter(event.target.value);
  }, []);

  const listing = useMemo(() => {
    if (!filteredNamespaces.length) {
      return (
        <Box
          sx={{
            display: "flex",
            flexDirection: "row",
            justifyContent: "center",
            margin: "0.8rem 0 2.4rem 0",
          }}
        >
          <span className="cluster-ns-listing-table-title">
            No namespaces found
          </span>
        </Box>
      );
    }
    return (
      <Grid
        container
        rowSpacing={1}
        columnSpacing={1}
        wrap="wrap"
        sx={{
          margin: "0.8rem 0 2.4rem 0",
        }}
      >
        {filteredNamespaces.map((ns: ClusterNamespaceSummary) => {
          return (
            <Grid
              key={`ns-${ns.name}`}
              item
              xl={4}
              lg={4}
              md={6}
              sm={12}
              xs={12}
            >
              <NamespaceCard data={ns} />
            </Grid>
          );
        })}
      </Grid>
    );
  }, [filteredNamespaces]);

  const MenuStyle = { fontSize: "1.6rem" };

  return (
    <Box
      sx={{
        display: "flex",
        flexDirection: "column",
        padding: "0 4.2rem",
      }}
    >
      <Box
        sx={{
          display: "flex",
          flexDirection: "row",
          justifyContent: "space-between",
        }}
      >
        <DebouncedSearchInput
          placeHolder="Search for namespace"
          onChange={setSearch}
        />
        <Box
          sx={{
            display: "flex",
            flexDirection: "row",
            flexGrow: 1,
            marginLeft: "3.2rem",
          }}
        >
          <Box
            sx={{
              display: "flex",
              flexDirection: "column",
              flexGrow: 0.15,
            }}
          >
            <label style={{ color: "#6B6C72", fontSize: "1.6rem" }}>
              Namespaces
            </label>
            <Select
              id="namespace-empty-filter"
              value={namespaceFilter}
              label=""
              onChange={handleNamespaceFilterChange}
              style={{
                width: "22.4rem",
                background: "#fff",
                border: "1px solid #6B6C72",
                height: "3.4rem",
                marginRight: "0.8rem",
                fontSize: "1.6rem",
              }}
            >
              <MenuItem value={ALL} sx={MenuStyle}>
                {ALL}
              </MenuItem>
              <MenuItem value={WITH_PIPELINES} sx={MenuStyle}>
                {WITH_PIPELINES}
              </MenuItem>
              <MenuItem value={NO_PIPELINES} sx={MenuStyle}>
                {NO_PIPELINES}
              </MenuItem>
            </Select>
          </Box>
          <Box
            sx={{
              display: "flex",
              flexDirection: "column",
              flexGrow: 0.15,
            }}
          >
            <label style={{ color: "#6B6C72", fontSize: "1.6rem" }}>
              Health
            </label>
            <Select
              label="Health"
              defaultValue="All"
              inputProps={{
                name: "Health",
                id: "health",
              }}
              style={{
                width: "22.4rem",
                background: "#fff",
                border: "1px solid #6B6C72",
                height: "3.4rem",
                marginRight: "0.8rem",
                textTransform: "capitalize",
                fontSize: "1.6rem",
              }}
              onChange={handleHealthFilterChange}
            >
              {HEALTH.map((health) => (
                <MenuItem
                  key={health}
                  value={health}
                  sx={{ textTransform: "capitalize", fontSize: "1.6rem" }}
                >
                  {health}
                </MenuItem>
              ))}
            </Select>
          </Box>
          <Box
            sx={{
              display: "flex",
              flexDirection: "column",
              flexGrow: 0.15,
              marginRight: "32rem",
              textTransform: "capitalize",
            }}
          >
            <label style={{ color: "#6B6C72", fontSize: "1.6rem" }}>
              Status
            </label>
            <Select
              label="Status"
              defaultValue="All"
              inputProps={{
                name: "Status",
                id: "status",
              }}
              style={{
                width: "22.4rem",
                background: "#fff",
                border: "1px solid #6B6C72",
                height: "3.4rem",
                textTransform: "capitalize",
                fontSize: "1.6rem",
              }}
              onChange={handleStatusFilterChange}
            >
              <MenuItem value={ALL} sx={{ fontSize: "1.6rem" }}>
                {ALL}
              </MenuItem>
              <MenuItem
                value={ACTIVE}
                sx={{ textTransform: "capitalize", fontSize: "1.6rem" }}
              >
                {ACTIVE}
              </MenuItem>
              <MenuItem
                value={INACTIVE}
                sx={{ textTransform: "capitalize", fontSize: "1.6rem" }}
              >
                {INACTIVE}
              </MenuItem>
            </Select>
          </Box>
        </Box>
        <Box>
          <ErrorIndicator />
        </Box>
      </Box>
      <Box sx={{ display: "flex", flexDirection: "row", marginTop: "3.2rem" }}>
        <span className="cluster-ns-listing-table-title">Namespaces</span>
      </Box>
      {listing}
      <Box
        sx={{
          display: "flex",
          flexDirection: "row",
          justifyContent: "center",
          marginBottom: "2.4rem",
        }}
      >
        <Pagination
          count={totalPages}
          page={page}
          onChange={handlePageChange}
          shape="rounded"
          sx={{
            "& .MuiPaginationItem-root": {
              fontSize: "1.4rem",
            },
            "& .MuiPaginationItem-icon": {
              height: "2rem",
              width: "2rem",
            },
          }}
        />
      </Box>
    </Box>
  );
}
