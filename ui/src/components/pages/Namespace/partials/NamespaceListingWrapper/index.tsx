import React, { useState, useCallback, useContext } from "react";
import Box from "@mui/material/Box";
import Tabs from "@mui/material/Tabs";
import Tab from "@mui/material/Tab";
import ArrowDownwardIcon from "@mui/icons-material/ArrowDownward";
import ArrowUpwardIcon from "@mui/icons-material/ArrowUpward";
import { DebouncedSearchInput } from "../../../../common/DebouncedSearchInput";
import { createSvgIcon } from "@mui/material/utils";
import { NamespacePipelineListingProps } from "../../../../../types/declarations/namespace";
import { AppContextProps } from "../../../../../types/declarations/app";
import { AppContext } from "../../../../../App";
import { SidebarType } from "../../../../common/SlidingSidebar";
import { ViewType } from "../../../../common/SpecEditor";
import { Button, MenuItem, Select } from "@mui/material";
import { ErrorIndicator } from "../../../../common/ErrorIndicator";
import {
  ALL,
  ALPHABETICAL_SORT,
  ASC,
  CRITICAL,
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
import { PipelineListing } from "./PipelineListing";
import { ISBListing } from "./ISBListing";

export const MAX_PAGE_SIZE = 4;
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

export function NamespaceListingWrapper({
  namespace,
  data,
  pipelineData,
  isbData,
  refresh,
}: NamespacePipelineListingProps) {
  const { setSidebarProps, isReadOnly } =
    useContext<AppContextProps>(AppContext);
  const [search, setSearch] = useState("");
  const [health, setHealth] = useState(ALL);
  const [status, setStatus] = useState(ALL);
  const [orderBy, setOrderBy] = useState({
    value: ALPHABETICAL_SORT,
    sortOrder: ASC,
  });

  const [tabValue, setTabValue] = useState(
    NamespacePipelineListingTabs[0].value
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

  const handleHealthFilterChange = useCallback(
    (e: any) => {
      setHealth(e.target.value);
    },
    [health]
  );

  const handleStatusFilterChange = useCallback(
    (e: any) => {
      setStatus(e.target.value);
    },
    [status]
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

  const handleTabsChange = useCallback((_: any, newValue: any) => {
    setTabValue(newValue);
  }, []);

  return (
    <Box
      sx={{
        display: "flex",
        flexDirection: "column",
        padding: "0 4.2rem",
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
            onChange={setSearch}
          />

          <Box
            sx={{
              display: "flex",
              flexDirection: "column",
              marginLeft: "1.6rem",
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
              marginLeft: "1.6rem",
              marginRight: "32rem",
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
                id: "health",
              }}
              style={{
                width: "22.4rem",
                background: "#fff",
                border: "1px solid #6B6C72",
                height: "3.4rem",
                fontSize: "1.6rem",
              }}
              onChange={handleStatusFilterChange}
            >
              {STATUS.map((status) => (
                <MenuItem
                  key={status}
                  value={status}
                  sx={{
                    textTransform: "capitalize",
                    fontSize: "1.6rem",
                  }}
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
          marginTop: "3.2rem",
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
                sx={{
                  fontSize: "1.4rem",
                }}
                value={NamespacePipelineListingTabs[0].value}
                label={NamespacePipelineListingTabs[0].label}
              />
              <Tab
                sx={{
                  fontSize: "1.4rem",
                }}
                value={NamespacePipelineListingTabs[1].value}
                label={NamespacePipelineListingTabs[1].label}
              />
            </Tabs>
          </Box>
          {!isReadOnly && (
            <Box
              sx={{
                display: "flex",
                flexDirection: "row",
                textDecoration: "none",
                marginBottom: "1.12rem",
              }}
            >
              <Button
                variant="outlined"
                startIcon={<PlusIcon />}
                size="medium"
                sx={{
                  marginRight: "1rem",
                  justifyContent: "flex-end",
                  fontSize: "1.4rem",
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
                sx={{ justifyContent: "flex-end", fontSize: "1.4rem" }}
              >
                Create ISB Service
              </Button>
            </Box>
          )}
        </Box>
      </Box>
      <Box
        sx={{
          display: "flex",
          flexDirection: "row",
          flexGrow: 1,
          justifyContent: "center",
          marginTop: "2.4rem",
        }}
      >
        {sortOptions.map((option) => {
          return (
            <Button
              sx={{ color: "#393A3D", fontSize: "1.4rem" }}
              onClick={(e) => {
                handleSortChange(e, option.value);
              }}
              key={option.value}
              variant="text"
            >
              {option.label}{" "}
              {orderBy.value === option.value ? (
                orderBy.sortOrder === ASC ? (
                  <ArrowUpwardIcon fontSize="large" />
                ) : (
                  <ArrowDownwardIcon fontSize="large" />
                )
              ) : (
                ""
              )}
            </Button>
          );
        })}
      </Box>
      <Box>
        {tabValue === PIPELINE ? (
          <PipelineListing
            pipelineData={pipelineData}
            isbData={isbData}
            totalCount={data.pipelinesCount}
            statusFilter={status}
            healthFilter={health}
            refresh={refresh}
            orderBy={orderBy}
            namespace={namespace}
            search={search}
          />
        ) : (
          <ISBListing
            isbData={isbData}
            totalCount={data.isbsCount}
            statusFilter={status}
            healthFilter={health}
            refresh={refresh}
            orderBy={orderBy}
            namespace={namespace}
            search={search}
          />
        )}
      </Box>
    </Box>
  );
}
