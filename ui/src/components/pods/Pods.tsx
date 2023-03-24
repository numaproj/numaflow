import { useState, useEffect, useMemo, useCallback } from "react";
import Box from "@mui/material/Box";
import Chip from "@mui/material/Chip";
import Stack from "@mui/material/Stack";
import CircularProgress from "@mui/material/CircularProgress";
import { EventType } from "@visx/event/lib/types";
import { SearchablePodsHeatMap } from "./SearchablePodsHeatMap";
import { PodDetail } from "../poddetail/PodDetail";
import { Pod } from "../../utils/models/pods";
import { usePodsFetch } from "../../utils/fetchWrappers/podsFetch";
import { usePodsDetailFetch } from "../../utils/fetchWrappers/podsDetailFetch";
import { Hexagon } from "../../utils/models/hexagonHeatMap";

import "./Pods.css";
import {notifyError} from "../../utils/error";

interface PodsProps {
  namespaceId: string;
  pipelineId: string;
  vertexId: string;
}

export function Pods(props: PodsProps) {
  const { namespaceId, pipelineId, vertexId } = props;

  if (!namespaceId || !pipelineId || !vertexId) {
    return <Box>Missing data.</Box>;
  }

  const [selectedPod, setSelectedPod] = useState<Pod | undefined>();
  const [selectedContainer, setSelectedContainer] = useState<
    string | undefined
  >();
  const [podsDetailsRequestKey, setPodsDetailsRequestKey] = useState(
    `${Date.now()}`
  );

  const {
    pods,
    loading: podsLoading,
    error: podsError,
  } = usePodsFetch(namespaceId, pipelineId, vertexId);

  const {
    podsDetailMap,
    loading: podsDetailLoading,
    error: podsDetailError,
  } = usePodsDetailFetch(namespaceId, podsDetailsRequestKey);

  useEffect(() => {
    // Refresh pod details every x ms
    const interval = setInterval(() => {
      setPodsDetailsRequestKey(`${Date.now()}`);
    }, 30000);
    return () => {
      clearInterval(interval);
    };
  }, []);

  // This useEffect notifies about the errors while querying for the pods of a vertex
  useEffect(() => {
    if (podsError){
      notifyError([{
        error: `Failed to get pods for ${vertexId} vertex`,
        options: {toastId: `${vertexId}-pod`, autoClose: 5000}
      }]);
    }
  }, [podsError]);

  useEffect(() => {
    if (pods.length) {
      setSelectedPod(pods[0]);
      setSelectedContainer(pods[0].containers[0]);
    } else {
      setSelectedPod(undefined);
      setSelectedContainer(undefined);
    }
  }, [pods]);

  useEffect(() => {
    if (podsDetailError){
      notifyError([{
        error: `Failed to get pod details for ${vertexId} vertex`,
        options: {toastId: `${vertexId}-pod-detail`, autoClose: 5000}
      }]);
    }
  }, [podsDetailError]);

  const handlePodClick = useCallback((e: Element | EventType, p: Hexagon) => {
    setSelectedPod(p.data.pod);
    setSelectedContainer(p.data.pod.containers[0]);
  }, []);

  const handContainerClick = useCallback((containerName: string) => {
    setSelectedContainer(containerName);
  }, []);

  const containerSelector = useMemo(() => {
    const podsContainerSx = {
      marginBottom: "15px",
      height: "66px",
    };
    if (!selectedPod) {
      return <Box sx={podsContainerSx}>Loading containers...</Box>;
    }
    return (
      <Box sx={podsContainerSx}>
        <Box
          sx={{
            marginBottom: "10px",
            fontWeight: "bold",
          }}
        >
          Containers
        </Box>
        <Stack  direction="row" spacing={1}>
          {selectedPod.containers?.map((c: string) => {
            return (
              <Chip key={c}
                label={c}
                variant={selectedContainer === c ? undefined : "outlined"}
                onClick={() => handContainerClick(c)}
              />
            );
          })}
        </Stack>
      </Box>
    );
  }, [selectedPod, selectedContainer]);

  const podDetail = useMemo(() => {
    if (!selectedPod || !selectedContainer || !podsDetailMap) {
      return;
    }
    const podDetail = podsDetailMap.get(selectedPod.name);
    if (!podDetail) {
      return;
    }
    return (
      <Box data-testid={"pod-detail"} sx={{ marginTop: "15px" }}>
        <PodDetail
          namespaceId={namespaceId}
          containerName={selectedContainer}
          pod={selectedPod}
          podDetail={podDetail}
        />
      </Box>
    );
  }, [namespaceId, selectedPod, selectedContainer, podsDetailMap]);

  if (podsLoading || (!podsDetailMap && podsDetailLoading)) {
    return (
      <Box
        data-testid={"progress"}
        sx={{ display: "flex", justifyContent: "center", marginTop: "10%" }}
      >
        <CircularProgress />
      </Box>
    );
  }

  if (podsError || pods.length === 0 || podsDetailError || !podsDetailMap) {
    const errors = [];

    if (pods.length === 0) {
      errors.push(<Box  key = {"no pods"}>{`No pods found`}</Box>);
    }

    if (podsError) {
      errors.push(
        <Box
            key={podsError}
          data-testid={"pods-error"}
        >{`Pods listing error: ${podsError}`}</Box>
      );
    }
    if (podsDetailError) {
      errors.push(
        <Box
            key = {podsDetailError}
          data-testid={"pods-detail-error"}
        >{`Pods metric error: ${podsDetailError}`}</Box>
      );
    } else if (!podsDetailMap) {
      errors.push(<Box  key = {podsDetailError}>{`Pods metric map undefined`}</Box>);
    }
    return <Box>{errors}</Box>;
  } else {
    let found = false;
    pods.forEach((pod) => {
      if (podsDetailMap.has(pod.name)) {
        found = true;
      }
    })

    if (!found) {
      return <Box  key = {"load-pods"}>{`Pods are not up yet...`}</Box>;
    }
  }

  return (
    <Box>
      <SearchablePodsHeatMap
        pods={pods}
        podsDetailMap={podsDetailMap}
        onPodClick={handlePodClick}
        selectedPod={selectedPod}
        setSelectedPod={setSelectedPod}
      />
      {containerSelector}
      {podDetail}
    </Box>
  );
}
