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
  setSelectedPod,
}: SearchablePodsHeatMapProps) => {
  // let count = 100;
  // while (count > 0 && pods.length < 100) {
  //   const a = JSON.parse(JSON.stringify(pods[0]));
  //   a.name = `${a.name}${count}`;
  //   pods.push(a);
  //   const b = JSON.parse(JSON.stringify(podsDetailsMap.get(pods[0].name)));
  //   b.containerSpecMap = new Map(podsDetailsMap.get(pods[0].name).containerMap);
  //   if (b) {
  //     b.name = a.name;
  //     podsDetailsMap.set(a.name, b);
  //   }
  //   count--;
  // }

  useEffect(() => {
    if (pods?.length) setSelectedPod(pods[0]);
    return;
  }, [pods]);

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
