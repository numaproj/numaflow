import { useEffect } from "react";
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

  //
  return (
    <Box sx={{ paddingBottom: "5px" }}>
      {pods?.length > 0 && (
        <Box sx={{ display: "flex", flexDirection: "row" }}>
          <Box sx={{ paddingBottom: "10px", fontWeight: "600", width: "8rem" }}>
            <span>Select a pod by resource</span>
          </Box>
          <PodsHeatMap
            pods={pods}
            podsDetailsMap={podsDetailsMap}
            onPodClick={onPodClick}
            selectedPod={selectedPod}
          />
        </Box>
      )}
    </Box>
  );
};
