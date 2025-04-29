import React, {
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useState,
} from "react";
import { Button, Popover, MenuItem, Box, IconButton } from "@mui/material";
import AddIcon from "@mui/icons-material/Add";
import CloseIcon from "@mui/icons-material/Close";
import ArrowForwardIcon from "@mui/icons-material/ArrowForward";
import {
  POD_CPU_UTILIZATION,
  POD_MEMORY_UTILIZATION,
} from "../../../utils/constants";
import { AppContextProps } from "../../../../../../../../../../../../../../../../types/declarations/app";
import { AppContext } from "../../../../../../../../../../../../../../../../App";
import { getBaseHref } from "../../../../../../../../../../../../../../../../utils";

import "./style.css";

export interface FiltersDropdownProps {
  items: any;
  namespaceId: string;
  pipelineId: string;
  type: string;
  vertexId?: string;
  setFilters: any;
  selectedPodName?: string;
  isFilterFocused: boolean;
  setFilterFocused: any;
  metric: any;
}

const periodData = [
  { name: "default" },
  { name: "1m" },
  { name: "5m" },
  { name: "15m" },
];

const FiltersDropdown = ({
  items,
  namespaceId,
  pipelineId,
  type,
  vertexId,
  setFilters,
  selectedPodName,
  isFilterFocused,
  setFilterFocused,
  metric,
}: FiltersDropdownProps) => {
  const { host } = useContext<AppContextProps>(AppContext);
  const [anchorEl, setAnchorEl] = useState(null);
  const [selectedFilters, setSelectedFilters] = useState<any[]>([]);
  const [activeFilters, setActiveFilters] = useState<any[]>([]);
  const [podsData, setPodsData] = useState<any[]>([]);

  useEffect(() => {
    const filtersMap = selectedFilters.reduce((acc, filter) => {
      const [key, value] = filter.split(":");
      if (key && value) {
        acc[key] = value;
      }
      return acc;
    }, {});

    setFilters((prevState: any) => ({
      ...prevState,
      ...filtersMap,
    }));
  }, [selectedFilters, setFilters]);

  const fetchPodsData = useCallback(
    (callback: (data: any[] | null) => void) => {
      const fetchData = async () => {
        try {
          const response = await fetch(
            `${host}${getBaseHref()}/api/v1/namespaces/${namespaceId}/${
              type === "monoVertex"
                ? `mono-vertices/${pipelineId}/pods`
                : `pipelines/${pipelineId}/vertices/${vertexId}/pods`
            }`
          );
          if (!response.ok) {
            callback(null);
            return;
          }
          const data = await response.json();
          const formattedData =
            data && data.data
              ? data.data
                  .filter((pod: any) => !pod?.metadata?.name.includes("daemon"))
                  .map((pod: any) => ({
                    containerNames: [
                      ...(pod?.spec?.containers?.map((ele: any) => ele.name) ||
                        []),
                      ...(pod?.spec?.initContainers
                        ?.filter(
                          (initContainer: any) =>
                            initContainer?.name !== "monitor" &&
                            initContainer?.restartPolicy === "Always"
                        )
                        ?.map((ele: any) => ele.name) || []),
                    ],
                    name: pod?.metadata?.name,
                  }))
              : [];
          callback(formattedData);
        } catch (error) {
          callback(null);
        }
      };

      fetchData();
    },
    [host, namespaceId, pipelineId, type]
  );

  useEffect(() => {
    fetchPodsData((data) => {
      if (data) {
        setPodsData(data);
      }
    });
  }, [fetchPodsData]);

  const getFilterValues = useCallback(
    (filterName: string) => {
      switch (filterName) {
        case "container":
          // get containers of selected pod
          return podsData
            .find((pod: any) => pod.name === selectedPodName)
            ?.containerNames?.map((containerName: any) => ({
              name: containerName,
            }));
        case "pod":
          return podsData;
        case "period":
          return periodData?.filter((p) => p?.name !== "default");
        default:
          return null;
      }
    },
    [podsData]
  );

  const getNestedFilterList = useMemo(() => {
    return items?.map((item: any) => {
      return {
        name: item?.name,
        subfilters: getFilterValues(item?.name),
      };
    });
  }, [items, getFilterValues]);

  const handleClick = (event: any) => {
    setAnchorEl(event?.currentTarget);
  };

  const handleClose = () => {
    setAnchorEl(null);
    setActiveFilters([]);
  };

  // Handle selection of filters
  const handleFilterSelect = (filter: any, level: number) => {
    if (filter?.subfilters) {
      setActiveFilters((prev) => [...prev.slice(0, level), filter]);
    } else {
      setSelectedFilters((prev) => [
        ...prev.filter(
          (filter) => !filter.startsWith(`${activeFilters[0]?.name}:`)
        ),
        `${activeFilters[0]?.name}:${filter.name}`,
      ]);
      handleClose();
    }
  };

  // Remove selected filter
  const handleRemoveFilter = (filterName: string, metric: any) => {
    const key = filterName.split(":")[0];
    setSelectedFilters((prev) =>
      prev.filter((filter) => filter !== filterName)
    );
    setFilters((prev: any) => {
      // eslint-disable-next-line @typescript-eslint/no-unused-vars
      const { [key]: _, ...rest } = prev;
      // Check if the key is "pod" and the pattern name is "pod_cpu_memory_utilization"
      if (
        key === "pod" &&
        [POD_CPU_UTILIZATION, POD_MEMORY_UTILIZATION]?.includes(
          metric?.display_name
        )
      ) {
        const newValue =
          type === "monoVertex"
            ? `${pipelineId}-.*`
            : `${pipelineId}-${vertexId}-.*`;
        return { ...rest, [key]: newValue };
      }
      return rest;
    });
    setFilterFocused(false);
  };

  // Render the dynamic filter menu levels
  const renderMenuItems = (filters: any, level: number) => {
    return filters.map((filter: any, index: number) => (
      <MenuItem
        className={"filter-menu-item"}
        key={index}
        onClick={() => handleFilterSelect(filter, level)}
      >
        <Box className={"filter-menu-item-name"}>{filter?.name}</Box>
        {level === 0 && (
          <ArrowForwardIcon className={"filter-menu-item-forward-icon"} />
        )}
      </MenuItem>
    ));
  };

  return (
    <Box
      className={"filters-dropdown-container"}
      sx={{
        border: isFilterFocused ? "0.2rem solid #0077c5" : "0.1rem solid black",
      }}
      onFocus={() => setFilterFocused(true)}
      onBlur={() => setFilterFocused(false)}
    >
      {selectedFilters.map((filter, index) => (
        <Box
          className={"filters-dropdown-selected-filters-container"}
          key={index}
        >
          <Box className={"filters-dropdown-selected-filter-value"}>
            {filter}
          </Box>
          <IconButton
            size="small"
            onClick={() => handleRemoveFilter(filter, metric)}
          >
            <CloseIcon fontSize="small" />
          </IconButton>
        </Box>
      ))}

      <Button
        variant="outlined"
        startIcon={<AddIcon sx={{ color: "#0077C5" }} />}
        onClick={handleClick}
        className={"filters-dropdown-add-filter-button"}
      >
        Add Filter
      </Button>

      <Popover
        open={Boolean(anchorEl)}
        anchorEl={anchorEl}
        onClose={handleClose}
        sx={{ mt: "1.75rem" }}
        anchorOrigin={{
          vertical: "bottom",
          horizontal: "left",
        }}
      >
        <Box
          sx={{
            display: "flex",
          }}
        >
          {/* Render the first level */}
          <Box>{renderMenuItems(getNestedFilterList, 0)}</Box>

          {/* Render sub levels dynamically based on activeFilters */}
          {activeFilters.map((filter: any, index) => (
            <Box
              className={"filters-dropdown-active-filters-options"}
              key={index}
            >
              {renderMenuItems(filter?.subfilters, index + 1)}
            </Box>
          ))}
        </Box>
      </Popover>
    </Box>
  );
};

export default FiltersDropdown;
