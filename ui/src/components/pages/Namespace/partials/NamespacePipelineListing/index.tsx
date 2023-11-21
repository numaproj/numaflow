import React, {
  useState,
  useEffect,
  useCallback,
  useMemo,
  useContext,
} from "react";
import Box from "@mui/material/Box";
import Tabs from "@mui/material/Tabs";
import Tab from "@mui/material/Tab";
import Pagination from "@mui/material/Pagination";
import Grid from "@mui/material/Grid";
import ArrowDownwardIcon from "@mui/icons-material/ArrowDownward";
import ArrowUpwardIcon from "@mui/icons-material/ArrowUpward";
import { DebouncedSearchInput } from "../../../../common/DebouncedSearchInput";
import { PipelineCard } from "../PipelineCard";
import { createSvgIcon } from "@mui/material/utils";
import { NamespacePipelineListingProps } from "../../../../../types/declarations/namespace";
import { PipelineData } from "./PipelinesTypes";
import { AppContextProps } from "../../../../../types/declarations/app";
import { AppContext } from "../../../../../App";
import { SidebarType } from "../../../../common/SlidingSidebar";
import { ViewType } from "../../../../common/SpecEditor";
import { Button, MenuItem, Select } from "@mui/material";
import { ErrorIndicator } from "../../../../common/ErrorIndicator";
import { ISBServicesListing } from "./ISBServiceTypes";
import { ISBServiceCard } from "./ISBServiceCard/ISBServiceCard";
import {
  ALL,
  ALPHABETICAL_SORT,
  ASC,
  CRITICAL,
  DEFAULT_ISB,
  DELETING,
  DESC,
  FAILED,
  HEALTHY,
  INACTIVE_STATUS,
  LAST_CREATED_SORT,
  LAST_UPDATED_SORT,
  PAUSED,
  PAUSING,
  RUNNING,
  StatusString,
  STOPPED,
  UNKNOWN,
  WARNING,
} from "../../../../../utils";

import "./style.css";

const MAX_PAGE_SIZE = 4;
export const HEALTH = [
  ALL,
  HEALTHY,
  WARNING,
  CRITICAL,
  INACTIVE_STATUS,
  UNKNOWN,
];
export const STATUS = [
  ALL,
  RUNNING,
  STOPPED,
  PAUSING,
  PAUSED,
  DELETING,
  FAILED,
  UNKNOWN,
];

const sortOptions = [
  {
    label: "Last Updated",
    value: LAST_UPDATED_SORT,
    sortOrder: DESC,
  },
  {
    label: "Last Created",
    value: LAST_CREATED_SORT,
    sortOrder: DESC,
  },
  {
    label: "A-Z",
    value: ALPHABETICAL_SORT,
    sortOrder: ASC,
  },
];
export const PIPELINE = "pipeline";
export const ISB_SERVICES = "isb_services";
export const NamespacePipelineListingTabs = [
  {
    label: "Pipelines",
    value: PIPELINE,
  },
  {
    label: "ISB Services",
    value: ISB_SERVICES,
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
  refresh,
}: NamespacePipelineListingProps) {
  const { setSidebarProps } = useContext<AppContextProps>(AppContext);
  const [pipelineSearch, setPipelineSearch] = useState("");
  const [isbSearch, setIsbSearch] = useState("");
  const [health, setHealth] = useState(ALL);
  const [status, setStatus] = useState(ALL);
  const [pipelinePage, setPipelinePage] = useState(1);
  const [isbPage, setIsbPage] = useState(1);
  const [orderBy, setOrderBy] = useState({
    value: ALPHABETICAL_SORT,
    sortOrder: ASC,
  });
  const [totalPipelinePages, setTotalPipelinePages] = useState(
    Math.ceil(data.pipelinesCount / MAX_PAGE_SIZE)
  );
  const [totalIsbPages, setTotalIsbPages] = useState(
    Math.ceil(data?.isbsCount / MAX_PAGE_SIZE)
  );
  const [filteredPipelines, setFilteredPipelines] = useState<PipelineData[]>(
    []
  );
  const [filteredISBServices, setFilteredISBServices] = useState<
    ISBServicesListing[]
  >([]);
  const [tabValue, setTabValue] = useState(
    NamespacePipelineListingTabs[0].value
  );
  // Update filtered pipelines based on search and page selected
  useEffect(() => {
    if (tabValue === PIPELINE) {
      let filtered: PipelineData[] = Object.values(
        pipelineData ? pipelineData : {}
      );
      if (pipelineSearch) {
        // Filter by search
        filtered = filtered.filter((p: PipelineData) =>
          p.name.includes(pipelineSearch)
        );
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
      if (health !== "All") {
        filtered = filtered.filter((p) => {
          const status = p?.status || UNKNOWN;
          if (status.toLowerCase() === health.toLowerCase()) {
            return true;
          } else {
            return false;
          }
        });
      }

      //Filter by status
      if (status !== "All") {
        filtered = filtered.filter((p) => {
          const currentStatus = p?.pipeline?.status?.phase || UNKNOWN;
          if (currentStatus.toLowerCase() === status.toLowerCase()) {
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

      if (pipelinePage > pages.length) {
        // Reset to page 1 if current page is greater than total pages after filterting
        setPipelinePage(1);
      }
      // Set filtered namespaces with current page of pipelines
      setFilteredPipelines(pages[pipelinePage - 1] || []);
      setTotalPipelinePages(pages.length);
    } else {
      let filtered: ISBServicesListing[] = Object.values(
        isbData ? isbData : {}
      );
      if (isbSearch) {
        // Filter by search
        filtered = filtered.filter((p: ISBServicesListing) =>
          p.name.includes(isbSearch)
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
      if (health !== "All") {
        filtered = filtered.filter((p: ISBServicesListing) => {
          const status = p?.status || UNKNOWN;
          if (status.toLowerCase() === health.toLowerCase()) {
            return true;
          } else {
            return false;
          }
        });
      }

      //Filter by status
      if (status !== "All") {
        filtered = filtered.filter((p: ISBServicesListing) => {
          const currentStatus = p?.isbService?.status?.phase || UNKNOWN;
          if (currentStatus.toLowerCase() === status.toLowerCase()) {
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

      if (isbPage > pages.length) {
        // Reset to page 1 if current page is greater than total pages after filterting
        setIsbPage(1);
      }
      // Set filtered namespaces with current page of pipelines
      setFilteredISBServices(pages[isbPage - 1] || []);
      setTotalIsbPages(pages.length);
    }
  }, [
    data,
    pipelineSearch,
    isbSearch,
    pipelinePage,
    isbPage,
    pipelineData,
    isbData,
    orderBy,
    health,
    status,
    tabValue,
  ]);

  const handlePageChange = useCallback(
    (_: React.ChangeEvent<unknown>, value: number) => {
      if (tabValue === PIPELINE) {
        setPipelinePage(value);
      } else {
        setIsbPage(value);
      }
    },
    [tabValue]
  );
  const handleSortChange = useCallback(
    (
      _: React.MouseEvent<HTMLAnchorElement | HTMLSpanElement>,
      value: string
    ) => {
      setOrderBy({
        value: value,
        sortOrder: orderBy.sortOrder === ASC ? DESC : ASC,
      });
    },
    [orderBy]
  );
  const listing = useMemo(() => {
    if (!filteredPipelines.length && !filteredISBServices.length) {
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
            {tabValue === PIPELINE
              ? "No pipelines found"
              : "No ISB Services found"}
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
        {tabValue === PIPELINE &&
          filteredPipelines.map((p: PipelineData) => {
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
        {tabValue === ISB_SERVICES &&
          filteredISBServices.map((p: ISBServicesListing) => {
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
  }, [filteredPipelines, filteredISBServices, namespace, refresh]);

  const handleHealthFilterChange = useCallback(
    (e) => {
      setHealth(e.target.value);
    },
    [health, tabValue]
  );

  const handleStatusFilterChange = useCallback(
    (e) => {
      setStatus(e.target.value);
    },
    [status, tabValue]
  );

  const handleCreatePipelineComplete = useCallback(() => {
    refresh();
    setTabValue(PIPELINE);
    if (!setSidebarProps) {
      return;
    }
    // Close sidebar and change sort to show new pipeline
    setSidebarProps(undefined);
    setOrderBy({
      value: LAST_UPDATED_SORT,
      sortOrder: DESC,
    });
  }, [setSidebarProps, refresh]);

  const handleCreatePiplineClick = useCallback(() => {
    if (!setSidebarProps) {
      return;
    }
    setSidebarProps({
      type: SidebarType.PIPELINE_CREATE,
      specEditorProps: {
        namespaceId: namespace,
        viewType: ViewType.EDIT,
        onUpdateComplete: handleCreatePipelineComplete,
      },
    });
  }, [setSidebarProps, handleCreatePipelineComplete, namespace]);

  const handleCreateISBComplete = useCallback(() => {
    refresh();
    setTabValue(ISB_SERVICES);
    if (!setSidebarProps) {
      return;
    }
    // Close sidebar and change sort to show new pipeline
    setSidebarProps(undefined);
  }, [setSidebarProps, refresh]);

  const handleCreateISBClick = useCallback(() => {
    if (!setSidebarProps) {
      return;
    }
    setSidebarProps({
      type: SidebarType.ISB_CREATE,
      specEditorProps: {
        namespaceId: namespace,
        viewType: ViewType.EDIT,
        onUpdateComplete: handleCreateISBComplete,
      },
    });
  }, [setSidebarProps, handleCreateISBComplete, namespace]);

  const handleTabsChange = useCallback((_, newValue) => {
    setTabValue(newValue);
  }, []);

  useEffect(() => {
    setPipelineSearch("");
    setHealth(ALL);
    setStatus(ALL);
    setOrderBy({
      value: ALPHABETICAL_SORT,
      sortOrder: ASC,
    });
  }, [tabValue]);

  return (
    <Box
      sx={{
        display: "flex",
        flexDirection: "column",
        padding: "0 2.625rem",
      }}
      data-testid="namespace-pipeline-listing"
    >
      <Box
        sx={{
          display: "flex",
          flexDirection: "row",
          justifyContent: "space-between",
        }}
      >
        <Box
          sx={{
            display: "flex",
            flexDirection: "row",
            alignItems: "flex-start",
            width: "100%",
          }}
        >
          <DebouncedSearchInput
            placeHolder={
              tabValue === PIPELINE
                ? "Search for pipeline"
                : "Search for ISB service"
            }
            onChange={tabValue === PIPELINE ? setPipelineSearch : setIsbSearch}
            key={tabValue === PIPELINE ? "pipeline" : "isb"}
          />

          <Box
            sx={{
              display: "flex",
              flexDirection: "column",
              marginLeft: "1rem",
            }}
          >
            <label style={{ color: "#6B6C72" }}>Health</label>
            <Select
              label="Health"
              defaultValue={ALL}
              value={health}
              inputProps={{
                name: "Health",
                id: "health",
              }}
              style={{
                width: "14rem",
                background: "#fff",
                border: "1px solid #6B6C72",
                height: "2.125rem",
              }}
              onChange={handleHealthFilterChange}
            >
              {HEALTH.map((health) => (
                <MenuItem
                  key={health}
                  value={health}
                  sx={{ textTransform: "capitalize" }}
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
              marginLeft: "1rem",
              marginRight: "20rem",
            }}
          >
            <label style={{ color: "#6B6C72" }}>Status</label>
            <Select
              label="Status"
              defaultValue={ALL}
              value={status}
              inputProps={{
                name: "Status",
                id: "health",
              }}
              style={{
                width: "14rem",
                background: "#fff",
                border: "1px solid #6B6C72",
                height: "2.125rem",
              }}
              onChange={handleStatusFilterChange}
            >
              {STATUS.map((status) => (
                <MenuItem
                  key={status}
                  value={status}
                  sx={{ textTransform: "capitalize" }}
                >
                  {StatusString[status]}
                </MenuItem>
              ))}
            </Select>
          </Box>
        </Box>
        <Box>
          <ErrorIndicator />
        </Box>
      </Box>
      <Box
        sx={{
          display: "flex",
          flexDirection: "row",
          marginTop: "2rem",
          borderBottom: "1px solid #DBD9D2",
        }}
      >
        <Box
          sx={{
            display: "flex",
            flexDirection: "row",
            flexGrow: 1,
            justifyContent: "space-between",
          }}
        >
          <Box>
            <Tabs value={tabValue} onChange={handleTabsChange}>
              <Tab
                value={NamespacePipelineListingTabs[0].value}
                label={NamespacePipelineListingTabs[0].label}
              />
              <Tab
                value={NamespacePipelineListingTabs[1].value}
                label={NamespacePipelineListingTabs[1].label}
              />
            </Tabs>
          </Box>
          <Box
            sx={{
              display: "flex",
              flexDirection: "row",
              textDecoration: "none",
              marginBottom: "0.7rem",
            }}
          >
            <Button
              variant="outlined"
              startIcon={<PlusIcon />}
              size="medium"
              sx={{
                marginRight: "0.625rem",
                justifyContent: "flex-end",
              }}
              onClick={handleCreatePiplineClick}
            >
              Create Pipeline
            </Button>
            <Button
              variant="outlined"
              startIcon={<PlusIcon />}
              size="small"
              onClick={handleCreateISBClick}
              sx={{ justifyContent: "flex-end" }}
            >
              Create ISB Service
            </Button>
          </Box>
        </Box>
      </Box>
      <Box
        sx={{
          display: "flex",
          flexDirection: "row",
          flexGrow: 1,
          justifyContent: "center",
          marginTop: "1.5rem",
        }}
      >
        {sortOptions.map((option) => {
          return (
            <Button
              sx={{ color: "#393A3D" }}
              onClick={(e) => {
                handleSortChange(e, option.value);
              }}
              key={option.value}
              variant="text"
            >
              {option.label}{" "}
              {orderBy.value === option.value ? (
                orderBy.sortOrder === ASC ? (
                  <ArrowUpwardIcon fontSize="small" />
                ) : (
                  <ArrowDownwardIcon fontSize="small" />
                )
              ) : (
                ""
              )}
            </Button>
          );
        })}
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
          count={tabValue === PIPELINE ? totalPipelinePages : totalIsbPages}
          page={tabValue === PIPELINE ? pipelinePage : isbPage}
          onChange={handlePageChange}
          shape="rounded"
        />
      </Box>
    </Box>
  );
}
