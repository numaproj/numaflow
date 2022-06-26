import { useState, useEffect, useCallback, ChangeEvent } from "react";
import Box from "@mui/material/Box";
import Paper from "@mui/material/Paper";
import InputBase from "@mui/material/InputBase";
import IconButton from "@mui/material/IconButton";
import ClearIcon from "@mui/icons-material/Clear";
import { EventType } from "@visx/event/lib/types";
import { PodsHeatMap } from "./PodsHeatMap";
import { Pod, PodDetail } from "../../utils/models/pods";

import "./SearchablePodsHeatMap.css";

interface PodsHeatMapProps {
  pods: Pod[];
  podsDetailMap: Map<string, PodDetail>;
  onPodClick: (e: Element | EventType, pod: Pod) => void;
  selectedPod: Pod | undefined;
  setSelectedPod: any;
}

export const SearchablePodsHeatMap = ({
  pods,
  podsDetailMap,
  onPodClick,
  selectedPod,
  setSelectedPod,
}: PodsHeatMapProps) => {
  const [search, setSearch] = useState<string>("");
  const [filteredPods, setFilteredPods] = useState<Pod[]>(pods);

  useEffect(() => {
    if (!search) {
      setFilteredPods(pods);
      return;
    }

    const filteredPods = [];

    pods.map((pod) => {
      if (pod.name.toLowerCase().includes(search)) {
        filteredPods.push(pod);
        setSelectedPod(pod);
      }
    });

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
      sx={{ display: "flex", flexDirection: "column" }}
    >
      <Box
        sx={{
          marginBottom: "10px",
          fontWeight: "bold",
        }}
      >
        Pods
      </Box>
      <Box sx={{ display: "flex", flexDirection: "row", marginBottom: "15px" }}>
        <Paper
          className="Pods-search"
          variant="outlined"
          sx={{
            p: "2px 4px",
            display: "flex",
            alignItems: "center",
            width: 400,
          }}
        >
          <InputBase
            sx={{ ml: 1, flex: 1 }}
            placeholder="Search pods"
            value={search}
            onChange={handleSearchChange}
          />
          <IconButton onClick={handleSearchClear}>
            <ClearIcon />
          </IconButton>
        </Paper>
      </Box>
      <PodsHeatMap
        pods={filteredPods}
        podsDetailMap={podsDetailMap}
        onPodClick={onPodClick}
        selectedPod={selectedPod}
      />
    </Box>
  );
};
