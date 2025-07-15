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
import { MonoVertexData, PipelineData } from "../PipelinesTypes";
import { PipelineCard } from "../../PipelineCard";
import { MonoVertexCard } from "../../MonoVertexCard";

interface PipelineListingProps extends ListingProps {
  pipelineData: Map<string, PipelineData> | undefined;
  isbData: any;
  monoVertexData: Map<string, MonoVertexData> | undefined;
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
  monoVertexData,
}: PipelineListingProps) {
  const [filteredPipelines, setFilteredPipelines] = useState<
    (PipelineData | MonoVertexData)[]
  >([]);
  const [page, setPage] = useState(1);
  const [totalPages, setTotalPages] = useState(
    Math.ceil(totalCount / MAX_PAGE_SIZE)
  );
  const [pipelineHealthMap, setPipelineHealthMap] = useState<Record<string, string>>({});
  const [monoVertexHealthMap, setMonoVertexHealthMap] = useState<Record<string, string>>({});

  const handlePageChange = useCallback(
    (_: React.ChangeEvent<unknown>, value: number) => {
      setPage(value);
    },
    []
  );

  useEffect(() => {
    let filtered: (PipelineData | MonoVertexData)[] = Object.values(
      pipelineData ? pipelineData : {}
    );
    filtered = [
      ...filtered,
      ...Object.values(monoVertexData ? monoVertexData : {}),
    ];
    if (search) {
      // Filter by search
      filtered = filtered.filter((p: PipelineData | MonoVertexData) =>
        p.name.includes(search)
      );
    }
    // Sorting
    if (orderBy.value === ALPHABETICAL_SORT) {
      filtered?.sort(
        (
          a: PipelineData | MonoVertexData,
          b: PipelineData | MonoVertexData
        ) => {
          if (orderBy.sortOrder === ASC) {
            return a.name > b.name ? 1 : -1;
          } else {
            return a.name < b.name ? 1 : -1;
          }
        }
      );
    } else if (orderBy.value === LAST_UPDATED_SORT) {
      filtered?.sort(
        (
          a: PipelineData | MonoVertexData,
          b: PipelineData | MonoVertexData
        ) => {
          const aType = a?.pipeline ? "pipeline" : "monoVertex";
          const bType = b?.pipeline ? "pipeline" : "monoVertex";
          if (orderBy.sortOrder === ASC) {
            return Date.parse(a?.[aType]?.status?.lastUpdated) >
              Date.parse(b?.[bType]?.status?.lastUpdated)
              ? 1
              : -1;
          } else {
            return Date.parse(a?.[aType]?.status?.lastUpdated) <
              Date.parse(b?.[bType]?.status?.lastUpdated)
              ? 1
              : -1;
          }
        }
      );
    } else {
      filtered?.sort(
        (
          a: PipelineData | MonoVertexData,
          b: PipelineData | MonoVertexData
        ) => {
          const aType = a?.pipeline ? "pipeline" : "monoVertex";
          const bType = b?.pipeline ? "pipeline" : "monoVertex";
          if (orderBy.sortOrder === ASC) {
            return Date.parse(a?.[aType]?.metadata?.creationTimestamp) >
              Date.parse(b?.[bType]?.metadata?.creationTimestamp)
              ? 1
              : -1;
          } else {
            return Date.parse(a?.[aType]?.metadata?.creationTimestamp) <
              Date.parse(b?.[bType]?.metadata?.creationTimestamp)
              ? 1
              : -1;
          }
        }
      );
    }
    //Filter by health
    if (healthFilter !== ALL) {
      filtered = filtered.filter((p) => {
        let healthStatus = UNKNOWN;
        if (p?.pipeline){
          healthStatus = pipelineHealthMap[p?.name] || UNKNOWN;
        } else {
          healthStatus = monoVertexHealthMap[p?.name] || UNKNOWN;
        }
        if (healthStatus.toLowerCase() === healthFilter.toLowerCase()) {
          return true;
        } else {
          return false;
        }
      });
    }

    //Filter by status
    if (statusFilter !== ALL) {
      filtered = filtered.filter((p) => {
        const type = p?.pipeline ? "pipeline" : "monoVertex";
        const currentStatus = p?.[type]?.status?.phase || UNKNOWN;
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
    monoVertexData,
    orderBy,
    healthFilter,
    statusFilter,
    pipelineHealthMap,
    monoVertexHealthMap,
  ]);


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
        {filteredPipelines.map((p: PipelineData | MonoVertexData) => {
          if (p?.pipeline) {
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
                  setPipelineHealthMap={setPipelineHealthMap} 
                />
              </Grid>
            );
          }
          return (
            <Grid key={`mono-vertex-${p.name}`} item xs={12}>
              <MonoVertexCard
                namespace={namespace}
                data={p}
                statusData={monoVertexData ? monoVertexData[p.name] : {}}
                refresh={refresh}
                setMonoVertexHealthMap={setMonoVertexHealthMap}
              />
            </Grid>
          );
        })}
      </Grid>
    );
  }, [
    filteredPipelines,
    namespace,
    pipelineData,
    isbData,
    monoVertexData,
    refresh,
    pipelineHealthMap,
    setPipelineHealthMap,
    setMonoVertexHealthMap,
    monoVertexHealthMap,
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
