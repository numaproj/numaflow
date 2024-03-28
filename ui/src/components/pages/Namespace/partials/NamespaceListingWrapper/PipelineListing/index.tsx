// eslint-disable-next-line @typescript-eslint/ban-ts-comment
// @ts-nocheck
import { Box, Grid, Pagination } from "@mui/material";
import React, { useCallback, useEffect, useMemo, useState } from "react";
import { MAX_PAGE_SIZE } from "../index";
import {
  ALL,
  ALPHABETICAL_SORT,
  ASC,
  DEFAULT_ISB,
  LAST_UPDATED_SORT,
  UNKNOWN,
} from "../../../../../../utils";
import { ListingProps } from "../ISBListing";
import { PipelineData } from "../PipelinesTypes";
import { PipelineCard } from "../../PipelineCard";

interface PipelineListingProps extends ListingProps {
  pipelineData: Map<string, PipelineData> | undefined;
  isbData: any;
  totalCount: number;
}

export function PipelineListing({
  statusFilter,
  healthFilter,
  pipelineData,
  refresh,
  orderBy,
  namespace,
  totalCount,
  search,
  isbData,
}: PipelineListingProps) {
  const [filteredPipelines, setFilteredPipelines] = useState<PipelineData[]>(
    []
  );
  const [page, setPage] = useState(1);
  const [totalPages, setTotalPages] = useState(
    Math.ceil(totalCount / MAX_PAGE_SIZE)
  );
  const handlePageChange = useCallback(
    (_: React.ChangeEvent<unknown>, value: number) => {
      setPage(value);
    },
    []
  );
  const listing = useMemo(() => {
    if (!filteredPipelines || !filteredPipelines.length) {
      return (
        <Box
          sx={{
            display: "flex",
            flexDirection: "row",
            justifyContent: "center",
            margin: "0.8rem 0 2.4rem 0",
          }}
        >
          <span className="ns-pipeline-listing-table-title">
            "No pipelines found"
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
        {filteredPipelines.map((p: PipelineData) => {
          const isbName = pipelineData
            ? pipelineData[p.name]?.pipeline?.spec
                ?.interStepBufferServiceName || DEFAULT_ISB
            : DEFAULT_ISB;
          return (
            <Grid key={`pipeline-${p.name}`} item xs={12}>
              <PipelineCard
                namespace={namespace}
                data={p}
                statusData={pipelineData ? pipelineData[p.name] : {}}
                isbData={isbData ? isbData[isbName] : {}}
                refresh={refresh}
              />
            </Grid>
          );
        })}
      </Grid>
    );
  }, [filteredPipelines, namespace, refresh]);
  useEffect(() => {
    let filtered: PipelineData[] = Object.values(
      pipelineData ? pipelineData : {}
    );
    if (search) {
      // Filter by search
      filtered = filtered.filter((p: PipelineData) => p.name.includes(search));
    }
    // Sorting
    if (orderBy.value === ALPHABETICAL_SORT) {
      filtered?.sort((a: PipelineData, b: PipelineData) => {
        if (orderBy.sortOrder === ASC) {
          return a.name > b.name ? 1 : -1;
        } else {
          return a.name < b.name ? 1 : -1;
        }
      });
    } else if (orderBy.value === LAST_UPDATED_SORT) {
      filtered?.sort((a: PipelineData, b: PipelineData) => {
        if (orderBy.sortOrder === ASC) {
          return a?.pipeline?.status?.lastUpdated >
            b?.pipeline?.status?.lastUpdated
            ? 1
            : -1;
        } else {
          return a?.pipeline?.status?.lastUpdated <
            b?.pipeline?.status?.lastUpdated
            ? 1
            : -1;
        }
      });
    } else {
      filtered?.sort((a: PipelineData, b: PipelineData) => {
        if (orderBy.sortOrder === ASC) {
          return Date.parse(a?.pipeline?.metadata?.creationTimestamp) >
            Date.parse(b?.pipeline?.metadata?.creationTimestamp)
            ? 1
            : -1;
        } else {
          return Date.parse(a?.pipeline?.metadata?.creationTimestamp) <
            Date.parse(b?.pipeline?.metadata?.creationTimestamp)
            ? 1
            : -1;
        }
      });
    }
    //Filter by health
    if (healthFilter !== ALL) {
      filtered = filtered.filter((p) => {
        const status = p?.status || UNKNOWN;
        if (status.toLowerCase() === healthFilter.toLowerCase()) {
          return true;
        } else {
          return false;
        }
      });
    }

    //Filter by status
    if (statusFilter !== ALL) {
      filtered = filtered.filter((p) => {
        const currentStatus = p?.pipeline?.status?.phase || UNKNOWN;
        if (currentStatus.toLowerCase() === statusFilter.toLowerCase()) {
          return true;
        } else {
          return false;
        }
      });
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
      // Reset to page 1 if current page is greater than total pages after filterting
      setPage(1);
    }
    // Set filtered namespaces with current page of pipelines
    setFilteredPipelines(pages[page - 1] || []);
    setTotalPages(pages.length);
  }, [
    search,
    page,
    pipelineData,
    isbData,
    orderBy,
    healthFilter,
    statusFilter,
  ]);

  return (
    <>
      <Box data-testid="namespace-pipeline-listing">{listing}</Box>
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
    </>
  );
}
