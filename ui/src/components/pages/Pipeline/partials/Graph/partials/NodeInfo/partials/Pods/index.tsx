import { useState, useEffect, useMemo, useCallback } from "react";

import Box from "@mui/material/Box";
import Paper from "@mui/material/Paper";
import CircularProgress from "@mui/material/CircularProgress";
import { EventType } from "@visx/event/lib/types";
import { Containers } from "./partials/Containers";
import { PodDetail } from "./partials/PodDetails";
import { SearchablePodsHeatMap } from "./partials/SearchablePodsHeatMap";
import { usePodsViewFetch } from "../../../../../../../../../utils/fetcherHooks/podsViewFetch";
import { notifyError } from "../../../../../../../../../utils/error";
import {
  Hexagon,
  Pod,
  PodsProps,
} from "../../../../../../../../../types/declarations/pods";

export function Pods(props: PodsProps) {
  const { namespaceId, pipelineId, vertexId } = props;

  if (!namespaceId || !pipelineId || !vertexId) {
    return (
      <Box data-testid={"pods-error-missing"} sx={{ mx: 2, my: 2 }}>
        {`Missing namespace, pipeline or vertex information`}
      </Box>
    );
  }

  const [selectedPod, setSelectedPod] = useState<Pod | undefined>(undefined);
  const [selectedContainer, setSelectedContainer] = useState<
    string | undefined
  >(undefined);

  const { pods, podsDetails, podsErr, podsDetailsErr, loading } =
    usePodsViewFetch(
      namespaceId,
      pipelineId,
      vertexId,
      setSelectedPod,
      setSelectedContainer
    );

  // This useEffect notifies about the errors while querying for the pods of the vertex
  useEffect(() => {
    if (podsErr) notifyError(podsErr);
  }, [podsErr]);

  // This useEffect notifies about the errors while querying for the pods details of the vertex
  useEffect(() => {
    if (podsDetailsErr) notifyError(podsDetailsErr);
  }, [podsDetailsErr]);

  const handlePodClick = useCallback((e: Element | EventType, p: Hexagon) => {
    setSelectedPod(p?.data?.pod);
    setSelectedContainer(p?.data?.pod?.containers[0]);
  }, []);

  const handleContainerClick = useCallback((containerName: string) => {
    setSelectedContainer(containerName);
  }, []);

  const containerSelector = useMemo(() => {
    return (
      <Box data-testid={"pods-containers"} sx={{ mt: 2 }}>
        <Containers
          pod={selectedPod}
          containerName={selectedContainer}
          handleContainerClick={handleContainerClick}
        />
      </Box>
    );
  }, [selectedPod, selectedContainer]);

  const podDetail = useMemo(() => {
    const selectedPodDetails = podsDetails?.get(selectedPod?.name);
    return (
      <Box data-testid={"pods-poddetails"} sx={{ mt: 2 }}>
        <PodDetail
          namespaceId={namespaceId}
          containerName={selectedContainer}
          pod={selectedPod}
          podDetails={selectedPodDetails}
        />
      </Box>
    );
  }, [namespaceId, selectedPod, selectedContainer, podsDetails]);

  if (loading) {
    return (
      <Box data-testid={"pods-loading"} sx={{ my: 2 }}>
        Loading pods view...
        <CircularProgress size={16} sx={{ mx: 2 }} />
      </Box>
    );
  }

  if (podsErr || podsDetailsErr) {
    return (
      <Box
        data-testid={"pods-error"}
        sx={{ mx: 2, my: 2 }}
      >{`Failed to get pods details`}</Box>
    );
  }

  return (
    <Paper square elevation={0} sx={{ padding: "1rem" }}>
      <Box data-testid={"pods-searchablePodsHeatMap"} sx={{ mt: 2 }}>
        <SearchablePodsHeatMap
          pods={pods}
          podsDetailsMap={podsDetails}
          onPodClick={handlePodClick}
          selectedPod={selectedPod}
          setSelectedPod={setSelectedPod}
        />
      </Box>
      {containerSelector}
      {podDetail}
    </Paper>
  );
}
