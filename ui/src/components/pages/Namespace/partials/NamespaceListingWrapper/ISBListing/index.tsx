import { Box, Grid, Pagination } from "@mui/material";
import React, { useCallback, useEffect, useMemo, useState } from "react";
import { ISBServicesListing } from "../ISBServiceTypes";
import { ISBServiceCard } from "../ISBServiceCard/ISBServiceCard";
import { MAX_PAGE_SIZE } from "../index";
import {
  ALL,
  ALPHABETICAL_SORT,
  ASC,
  LAST_UPDATED_SORT,
  UNKNOWN,
} from "../../../../../../utils";

export interface ListingProps {
  statusFilter: string;
  healthFilter: string;
  refresh: () => void;
  orderBy: any;
  namespace: string;
  search: string;
}

interface ISBListingProps extends ListingProps {
  isbData: any;
  totalCount: number;
}

export function ISBListing({
  statusFilter,
  healthFilter,
  isbData,
  refresh,
  orderBy,
  namespace,
  totalCount,
  search,
}: ISBListingProps) {
  const [filteredISBServices, setFilteredISBServices] = useState<
    ISBServicesListing[]
  >([]);
  const [page, setPage] = useState(1);
  const [totalPages, setTotalPages] = useState(
    Math.ceil(totalCount / MAX_PAGE_SIZE)
  );
  const handlePageChange = useCallback(
    (event: React.ChangeEvent<unknown>, value: number) => {
      setPage(value);
    },
    []
  );
  const listing = useMemo(() => {
    if (!isbData) {
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
            "No ISB Services found"
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
        {filteredISBServices.map((p: ISBServicesListing) => {
          return (
            <Grid key={`isb-service-${p.name}`} item xs={12}>
              <ISBServiceCard
                namespace={namespace}
                data={p}
                refresh={refresh}
              />
            </Grid>
          );
        })}
      </Grid>
    );
  }, [filteredISBServices, namespace, refresh]);

  useEffect(() => {
    let filtered: ISBServicesListing[] = Object.values(isbData ? isbData : {});
    if (search) {
      // Filter by search
      filtered = filtered.filter((p: ISBServicesListing) =>
        p.name.includes(search)
      );
    }
    // Sorting
    if (orderBy.value === ALPHABETICAL_SORT) {
      filtered?.sort((a: ISBServicesListing, b: ISBServicesListing) => {
        if (orderBy.sortOrder === ASC) {
          return a.name > b.name ? 1 : -1;
        } else {
          return a.name < b.name ? 1 : -1;
        }
      });
    } else if (orderBy.value === LAST_UPDATED_SORT) {
      filtered?.sort((a: ISBServicesListing, b: ISBServicesListing) => {
        if (orderBy.sortOrder === ASC) {
          if (!a?.isbService?.status?.conditions) {
            return 1;
          }
          if (!b?.isbService?.status?.conditions) {
            return -1;
          }
          return new Date(
            a?.isbService?.status?.conditions[
              a?.isbService?.status?.conditions?.length - 1
            ]?.lastTransitionTime
          ) >
            new Date(
              b?.isbService?.status?.conditions[
                b?.isbService?.status?.conditions?.length - 1
              ]?.lastTransitionTime
            )
            ? 1
            : -1;
        } else {
          if (!a?.isbService?.status?.conditions) {
            return -1;
          }
          if (!b?.isbService?.status?.conditions) {
            return 1;
          }
          return new Date(
            a?.isbService?.status?.conditions[
              a?.isbService?.status?.conditions?.length - 1
            ]?.lastTransitionTime
          ) <
            new Date(
              b?.isbService?.status?.conditions[
                b?.isbService?.status?.conditions?.length - 1
              ]?.lastTransitionTime
            )
            ? 1
            : -1;
        }
      });
    } else {
      filtered?.sort((a: ISBServicesListing, b: ISBServicesListing) => {
        if (orderBy.sortOrder === ASC) {
          return new Date(a?.isbService?.metadata?.creationTimestamp) >
            new Date(b?.isbService?.metadata?.creationTimestamp)
            ? 1
            : -1;
        } else {
          return new Date(a?.isbService?.metadata?.creationTimestamp) <
            new Date(b?.isbService?.metadata?.creationTimestamp)
            ? 1
            : -1;
        }
      });
    }
    //Filter by health
    if (healthFilter !== ALL) {
      filtered = filtered.filter((p: ISBServicesListing) => {
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
      filtered = filtered.filter((p: ISBServicesListing) => {
        const currentStatus = p?.isbService?.status?.phase || UNKNOWN;
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
    setFilteredISBServices(pages[page - 1] || []);
    setTotalPages(pages.length);
  }, [search, page, isbData, orderBy, healthFilter, statusFilter]);

  return (
    <>
      <Box data-testid="namespace-isb-listing">{listing}</Box>
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
