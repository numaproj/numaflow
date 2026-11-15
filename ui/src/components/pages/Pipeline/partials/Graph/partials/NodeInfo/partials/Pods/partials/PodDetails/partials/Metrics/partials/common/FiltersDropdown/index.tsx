import React, {
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useRef,
  useState,
} from "react";
import { Button, Popover, MenuItem, Box, IconButton } from "@mui/material";
import FormControl from "@mui/material/FormControl";
import InputLabel from "@mui/material/InputLabel";
import OutlinedInput from "@mui/material/OutlinedInput";
import AddIcon from "@mui/icons-material/Add";
import ArrowDropDown from "@mui/icons-material/ArrowDropDown";
import CloseIcon from "@mui/icons-material/Close";
import ArrowForwardIcon from "@mui/icons-material/ArrowForward";
import {
  POD_CPU_UTILIZATION,
  POD_MEMORY_UTILIZATION,
} from "../../../utils/constants";
import { AppContextProps } from "../../../../../../../../../../../../../../../../types/declarations/app";
import { AppContext } from "../../../../../../../../../../../../../../../../App";
import { getBaseHref } from "../../../../../../../../../../../../../../../../utils";
import { parseMetricFilters } from "../../../../../../../../../../../../../../../../utils/observabilityURLState";

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
  initialFilters?: string | null;
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
  initialFilters,
}: FiltersDropdownProps) => {
  const { host } = useContext<AppContextProps>(AppContext);
  const [anchorEl, setAnchorEl] = useState<HTMLElement | null>(null);
  const formRef = useRef<HTMLDivElement>(null);
  const [selectedFilters, setSelectedFilters] = useState<any[]>(() =>
    (initialFilters || "").split(",").filter(Boolean)
  );
  const [activeFilters, setActiveFilters] = useState<any[]>([]);
  const [podsData, setPodsData] = useState<any[]>([]);

  useEffect(() => {
    const next = (initialFilters || "").split(",").filter(Boolean);
    setSelectedFilters((prev) =>
      prev.join(",") === next.join(",") ? prev : next
    );
  }, [initialFilters]);

  useEffect(() => {
    const filtersMap = parseMetricFilters(selectedFilters.join(","));

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

  const handleClick = () => {
    setAnchorEl(formRef.current);
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
    <FormControl
      ref={formRef}
      className="filters-dropdown-form"
      focused={Boolean(anchorEl) || isFilterFocused}
      onFocus={() => setFilterFocused(true)}
      onBlur={() => setFilterFocused(false)}
    >
      <InputLabel shrink htmlFor="filters-input" className="dropdown-input-label">
        Filters
      </InputLabel>
      <OutlinedInput
        id="filters-input"
        notched
        label="Filters"
        readOnly
        className="filters-dropdown-input dropdown-select-value"
        onClick={handleClick}
        startAdornment={
          <Box className="filters-dropdown-content">
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
                  onClick={(event) => {
                    event.stopPropagation();
                    handleRemoveFilter(filter, metric);
                  }}
                >
                  <CloseIcon fontSize="small" />
                </IconButton>
              </Box>
            ))}
            <Button
              variant="text"
              disableRipple
              startIcon={<AddIcon />}
              onClick={(event) => {
                event.stopPropagation();
                handleClick();
              }}
              className={"filters-dropdown-add-filter-button"}
            >
              Add Filter
            </Button>
          </Box>
        }
        endAdornment={<ArrowDropDown className="filters-dropdown-caret" />}
        inputProps={{ "aria-label": "Filters", tabIndex: -1 }}
      />

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
    </FormControl>
  );
};

export default FiltersDropdown;
