import React, { useState, useEffect, useCallback, useMemo } from "react";
import Box from "@mui/material/Box";
import Pagination from "@mui/material/Pagination";
import Grid from "@mui/material/Grid";
import { DebouncedSearchInput } from "../../../../common/DebouncedSearchInput";
import { NamespaceCard } from "../NamespaceCard";
import { ErrorIndicator } from "../../../../common/ErrorIndicator";
import {
  ClusterNamespaceListingProps,
  ClusterNamespaceSummary,
} from "../../../../../types/declarations/cluster";

import "./style.css";

const MAX_PAGE_SIZE = 6;

export function ClusterNamespaceListing({
  data,
}: ClusterNamespaceListingProps) {
  const [search, setSearch] = useState("");
  const [page, setPage] = useState(1);
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
    filtered.sort((a, b) => (a.name > b.name ? 1 : -1));

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
  }, [data, search, page]);

  const handlePageChange = useCallback(
    (event: React.ChangeEvent<unknown>, value: number) => {
      setPage(value);
    },
    []
  );

  const listing = useMemo(() => {
    if (!filteredNamespaces.length) {
      return (
        <Box
          sx={{
            display: "flex",
            flexDirection: "row",
            justifyContent: "center",
            margin: "0.5rem 0 1.5rem 0",
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
          margin: "0.5rem 0 1.5rem 0",
        }}
      >
        {filteredNamespaces.map((ns: ClusterNamespaceSummary) => {
          return (
            <Grid
              key={`ns-${ns.name}`}
              item
              xl={3}
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

  return (
    <Box
      sx={{ display: "flex", flexDirection: "column", padding: "0 2.625rem" }}
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
        <Box>
          <ErrorIndicator />
        </Box>
      </Box>
      <Box sx={{ display: "flex", flexDirection: "row", marginTop: "2rem" }}>
        <span className="cluster-ns-listing-table-title">Namespaces</span>
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
