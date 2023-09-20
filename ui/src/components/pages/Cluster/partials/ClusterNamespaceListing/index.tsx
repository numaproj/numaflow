import React, { useState, useEffect, useCallback } from "react";
import Box from "@mui/material/Box";
import Pagination from "@mui/material/Pagination";
import { ClusterSummaryData } from "../../../../../utils/fetchWrappers/clusterSummaryFetch";
import { DebouncedSearchInput } from "../../../../common/DebouncedSearchInput";

import "./style.css";

export interface ClusterNamespaceListingProps {
  data: ClusterSummaryData;
}

const MAX_PAGE_SIZE = 6;

export function ClusterNamespaceListing({
  data,
}: ClusterNamespaceListingProps) {
  const [search, setSearch] = useState("");
  const [page, setPage] = useState(1);
  const [totalPages, setTotalPages] = useState(
    Math.ceil(data.namespacesCount / MAX_PAGE_SIZE)
  );

  // Update total pages
  useEffect(() => {
    const pageCount = Math.ceil(data.namespacesCount / MAX_PAGE_SIZE);
    setTotalPages(pageCount);
    if (page > pageCount) {
      // Reset page if current page is greater than total pages
      setPage(1);
    }
  }, [data, page]);

  const handlePageChange = useCallback(
    (event: React.ChangeEvent<unknown>, value: number) => {
      setPage(value);
    },
    []
  );

  return (
    <Box
      sx={{ display: "flex", flexDirection: "column", padding: "0 2.625rem" }}
    >
      <Box sx={{ display: "flex", flexDirection: "row" }}>
        <DebouncedSearchInput
          placeHolder="Search for namespace"
          onChange={setSearch}
        />
      </Box>
      <Box sx={{ display: "flex", flexDirection: "row", marginTop: "2rem" }}>
        <span className="cluster-ns-listing-table-title">Namespace</span>
      </Box>
      <Box>TODO table</Box>
      <Box sx={{ display: "flex", flexDirection: "row", justifyContent: "center" }}>
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
