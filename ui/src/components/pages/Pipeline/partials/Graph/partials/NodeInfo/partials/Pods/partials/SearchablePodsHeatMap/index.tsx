import Box from "@mui/material/Box";
import { PodsHeatMap } from "./partials/PodsHeatMap";
import { SearchablePodsHeatMapProps } from "../../../../../../../../../../../types/declarations/pods";

import "./style.css";

export const SearchablePodsHeatMap = ({
  pods,
  podsDetailsMap,
  onPodClick,
  selectedPod,
}: SearchablePodsHeatMapProps) => {
  return (
    pods?.length > 0 && (
      <Box sx={{ display: "flex", mb: "0.75rem", width: "100%" }}>
        <Box sx={{ fontWeight: "600", width: "24%", mr: "1%" }}>
          Select a pod by resource
        </Box>
        <Box sx={{ width: "75%" }}>
          <PodsHeatMap
            pods={pods}
            podsDetailsMap={podsDetailsMap}
            onPodClick={onPodClick}
            selectedPod={selectedPod}
          />
        </Box>
      </Box>
    )
  );
};
