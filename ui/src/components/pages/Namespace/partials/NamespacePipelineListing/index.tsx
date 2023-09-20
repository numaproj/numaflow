import React, { useState, useEffect, useCallback, useMemo } from "react";
import Box from "@mui/material/Box";
import Pagination from "@mui/material/Pagination";
import Grid from "@mui/material/Grid";
import { DebouncedSearchInput } from "../../../../common/DebouncedSearchInput";
import { PipelineCard } from "../PipelineCard";
import {
  NamespacePipelineListingProps,
  NamespacePipelineSummary,
} from "../../../../../types/declarations/namespace";

import "./style.css";

const MAX_PAGE_SIZE = 4;

export function NamespacePipelineListing({
  namespace,
  data,
}: NamespacePipelineListingProps) {
  const [search, setSearch] = useState("");
  const [page, setPage] = useState(1);
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
      filtered = data.pipelineSummaries.filter((p) =>
        p.name.includes(search)
      );
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
            <Grid
              key={`pipeline-${p.name}`}
              item
              xs={12}
            >
              <PipelineCard namespace={namespace} data={p} />
            </Grid>
          );
        })}
      </Grid>
    );
  }, [filteredPipelines, namespace]);

  return (
    <Box
      sx={{ display: "flex", flexDirection: "column", padding: "0 2.625rem" }}
    >
      <Box sx={{ display: "flex", flexDirection: "row" }}>
        <DebouncedSearchInput
          placeHolder="Search for pipeline"
          onChange={setSearch}
        />
      </Box>
      <Box sx={{ display: "flex", flexDirection: "row", marginTop: "2rem" }}>
        <span className="ns-pipeline-listing-table-title">Pipelines</span>
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
