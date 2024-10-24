import React, {
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useState,
} from "react";
import {
  Button,
  Popover,
  MenuItem,
  Typography,
  Box,
  IconButton,
} from "@mui/material";
import AddIcon from "@mui/icons-material/Add";
import CloseIcon from "@mui/icons-material/Close";
import ArrowForwardIcon from "@mui/icons-material/ArrowForward";
import { AppContextProps } from "../../../../../../../../types/declarations/app";
import { AppContext } from "../../../../../../../../App";
import { getBaseHref } from "../../../../../../../../utils";

export interface FiltersDropdownProps {
  items: any;
  namespaceId: string;
  pipelineId: string;
  type: string;
  setFilters: any;
}

const FiltersDropdown = ({
  items,
  namespaceId,
  pipelineId,
  type,
  setFilters,
}: FiltersDropdownProps) => {
  const { host } = useContext<AppContextProps>(AppContext);
  const [anchorEl, setAnchorEl] = useState(null);
  const [selectedFilters, setSelectedFilters] = useState<any[]>([]);
  const [activeFilters, setActiveFilters] = useState<any[]>([]);
  const [podsData, setPodsData] = useState<any[]>([]);
  console.log(selectedFilters);

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
              type === "monoVertex" ? "mono-vertices" : "pipeline"
            }/${pipelineId}/pods`
          );
          if (!response.ok) {
            callback(null);
            return;
          }
          const data = await response.json();
          const formattedData = data?.data
            ?.filter((pod: any) => !pod?.metadata?.name.includes("daemon"))
            .map((pod: any) => ({
              name: pod?.metadata?.name,
            }));
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
        case "pod":
          return podsData;
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
  const handleRemoveFilter = (filterName: string) => {
    setSelectedFilters((prev) =>
      prev.filter((filter) => filter !== filterName)
    );
    const key = filterName.split(":")[0];
    setFilters((prev: any) => {
      const { [key]: _, ...rest } = prev;
      return rest;
    });
  };

  // Render the dynamic filter menu levels
  const renderMenuItems = (filters: any, level: number) => {
    return filters.map((filter: any, index: number) => (
      <MenuItem
        key={index}
        onClick={() => handleFilterSelect(filter, level)}
        sx={{
          width: "fit-content",
          minWidth: "15rem",
          fontSize: "1.6rem",
          display: "flex",
          justifyContent: "space-between",
        }}
      >
        <Box sx={{ textOverflow: "ellipsis" }}>{filter?.name}</Box>
        {level === 0 && (
          <ArrowForwardIcon
            sx={{
              height: "2.4rem",
              width: "2.4rem",
              color: "#0077C5",
            }}
          />
        )}
      </MenuItem>
    ));
  };

  return (
    <Box
      sx={{
        display: "flex",
        alignItems: "center",
        width: "100%",
        border: "0.1rem solid black",
        borderRadius: "0.5rem",
        height: "4rem",
        p: "1rem",
      }}
    >
      {selectedFilters.map((filter, index) => (
        <Box
          key={index}
          sx={{
            display: "flex",
            alignItems: "center",
            p: "0.5rem 1rem",
            backgroundColor: "#f0f0f0",
            borderRadius: "0.4rem",
            mr: "1rem",
          }}
        >
          <Typography variant="body1">{filter}</Typography>
          <IconButton
            size="small"
            onClick={() => handleRemoveFilter(filter)}
            sx={{ marginLeft: "0.5rem" }}
          >
            <CloseIcon fontSize="small" />
          </IconButton>
        </Box>
      ))}

      <Button variant="outlined" startIcon={<AddIcon />} onClick={handleClick}>
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

          {/* Render sublevels dynamically based on activeFilters */}
          {activeFilters.map((filter: any, index) => (
            <Box key={index}>
              {renderMenuItems(filter?.subfilters, index + 1)}
            </Box>
          ))}
        </Box>
      </Popover>
    </Box>
  );
};

export default FiltersDropdown;
