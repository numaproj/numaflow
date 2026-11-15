import { SyntheticEvent } from "react";
import Autocomplete from "@mui/material/Autocomplete";
import Box from "@mui/material/Box";
import TextField from "@mui/material/TextField";
import { Pod } from "../../../../../../../../../../../types/declarations/pods";
import { Containers } from "../Containers";

interface PodFocusControlsProps {
  pods: Pod[];
  selectedPod: Pod;
  selectedContainer?: string;
  onPodSelect: (pod: Pod) => void;
  onContainerSelect: (container: string) => void;
}

/** PodFocusControls is shared by Pod View presenters inside the expanded logs dialog. */
export function PodFocusControls({
  pods,
  selectedPod,
  selectedContainer,
  onPodSelect,
  onContainerSelect,
}: PodFocusControlsProps) {
  const handlePodChange = (
    _event: SyntheticEvent,
    podName: string | null
  ) => {
    const pod = pods.find((item) => item.name === podName);
    if (pod) onPodSelect(pod);
  };

  return (
    <>
      <Box className="PodLogs-focus-context-pod">
        <span className="PodLogs-focus-context-label">Pod</span>
        <Autocomplete
          options={pods.map((pod) => pod.name)}
          getOptionLabel={(option: string) => option}
          disableClearable
          id="focus-pod-select"
          data-testid="logs-focus-pod-select"
          ListboxProps={{
            sx: { fontSize: "1.2rem", maxHeight: "24rem", overflow: "auto" },
          }}
          componentsProps={{
            popper: { sx: { zIndex: (theme) => theme.zIndex.modal + 4 } },
          }}
          sx={{
            width: "100%",
            minWidth: 0,
            "& .MuiOutlinedInput-root": {
              height: "3.2rem",
              fontSize: "1.2rem",
              paddingTop: 0,
              paddingBottom: 0,
            },
            "& .MuiAutocomplete-input": { textOverflow: "ellipsis" },
          }}
          autoHighlight
          onChange={handlePodChange}
          value={selectedPod.name}
          renderInput={(params) => (
            <TextField
              {...params}
              variant="outlined"
              size="small"
              title={selectedPod.name}
              inputProps={{
                ...params.inputProps,
                "aria-label": "Select pod",
                autoComplete: "new-password",
                style: { fontSize: "1.2rem" },
              }}
            />
          )}
        />
      </Box>
      <Box className="PodLogs-focus-context-container">
        <span className="PodLogs-focus-context-label">Container</span>
        <Box data-testid="logs-focus-containers">
          <Containers
            pod={selectedPod}
            containerName={selectedContainer || selectedPod.containers[0] || ""}
            handleContainerClick={onContainerSelect}
          />
        </Box>
      </Box>
    </>
  );
}
