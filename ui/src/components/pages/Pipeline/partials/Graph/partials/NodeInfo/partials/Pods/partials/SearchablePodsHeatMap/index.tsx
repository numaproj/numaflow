import { useState, useEffect, useCallback, ChangeEvent } from "react";
import Box from "@mui/material/Box";
import Paper from "@mui/material/Paper";
import IconButton from "@mui/material/IconButton";
import ClearIcon from "@mui/icons-material/Clear";
import InputBase from "@mui/material/InputBase";
import { PodsHeatMap } from "./partials/PodsHeatMap";
import {
  Pod,
  SearchablePodsHeatMapProps,
} from "../../../../../../../../../../../types/declarations/pods";

import "./style.css";

export const SearchablePodsHeatMap = ({
  pods,
  podsDetailsMap,
  onPodClick,
  selectedPod,
  setSelectedPod,
}: SearchablePodsHeatMapProps) => {
  const [search, setSearch] = useState<string>("");
  const [filteredPods, setFilteredPods] = useState<Pod[]>(pods);

  useEffect(() => {
    if (!search) {
      setFilteredPods(pods);
      if (pods?.length) setSelectedPod(pods[0]);
      return;
    }

    const filteredPods = [];

    pods?.forEach((pod) => {
      if (pod?.name.toLowerCase().includes(search)) {
        filteredPods.push(pod);
      }
    });

    if (filteredPods?.length > 0) setSelectedPod(filteredPods[0]);
    else setSelectedPod(undefined);

    setFilteredPods(filteredPods);
  }, [pods, search]);

  const handleSearchChange = useCallback(
    (event: ChangeEvent<HTMLInputElement>) => {
      setSearch(event.target.value);
    },
    []
  );

  const handleSearchClear = useCallback(() => {
    setSearch("");
  }, []);

  return (
    <Box
      data-testid={"searchable-pods"}
      sx={{ display: "flex", flexDirection: "column", mb: 2 }}
    >
      <Box sx={{ display: "flex", flexDirection: "row", mb: "1rem" }}>
        <Paper
          className="Pods-search"
          variant="outlined"
          sx={{
            p: "0.125rem 0.25rem",
            display: "flex",
            alignItems: "center",
            width: 400,
          }}
        >
          <InputBase
            sx={{ ml: 1, flex: 1 }}
            data-testid={"searchable-pods-input"}
            placeholder="Search pods"
            value={search}
            onChange={handleSearchChange}
          />
          <IconButton onClick={handleSearchClear}>
            <ClearIcon />
          </IconButton>
        </Paper>
      </Box>
      <div className="pod-exp-text">
        Click on different hexagons under CPU/MEM display to switch to different
        Pods
      </div>
      {filteredPods?.length > 0 && (
        <PodsHeatMap
          pods={filteredPods}
          podsDetailsMap={podsDetailsMap}
          onPodClick={onPodClick}
          selectedPod={selectedPod}
        />
      )}
      {filteredPods?.length === 0 && (
        <Box
          sx={{
            textAlign: "center",
            color: "text.secondary",
            mb: "1rem",
          }}
        >
          No pods matching search
        </Box>
      )}
    </Box>
  );
};
