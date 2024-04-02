// eslint-disable-next-line @typescript-eslint/ban-ts-comment
// @ts-nocheck
import { useState, useEffect, useMemo, useCallback, ChangeEvent } from "react";
import Box from "@mui/material/Box";
import Paper from "@mui/material/Paper";
import CircularProgress from "@mui/material/CircularProgress";
import Autocomplete from "@mui/material/Autocomplete";
import TextField from "@mui/material/TextField";
import { EventType } from "@visx/event/lib/types";
import { Containers } from "./partials/Containers";
import { PodDetail } from "./partials/PodDetails";
import { SearchablePodsHeatMap } from "./partials/SearchablePodsHeatMap";
import { PodInfo } from "./partials/PodDetails/partials/PodInfo";
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
      selectedPod,
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
      <Box sx={{ display: "flex", flexDirection: "row" }}>
        <Box sx={{ fontWeight: "600", width: "12.8rem" }}>
          <span>Select a container</span>
        </Box>
        <Box data-testid={"pods-containers"} sx={{ mt: 2 }}>
          <Containers
            pod={selectedPod}
            containerName={selectedContainer}
            handleContainerClick={handleContainerClick}
          />
        </Box>
      </Box>
    );
  }, [selectedPod, selectedContainer]);

  const podDetail = useMemo(() => {
    const selectedPodDetails = podsDetails?.get(selectedPod?.name);
    return (
      <Box
        data-testid={"pods-poddetails"}
        sx={{ mt: 2, border: "1px solid #E0E0E0", padding: "1.6rem" }}
      >
        <PodDetail
          namespaceId={namespaceId}
          containerName={selectedContainer}
          pod={selectedPod}
          podDetails={selectedPodDetails}
        />
      </Box>
    );
  }, [namespaceId, selectedPod, selectedContainer, podsDetails]);

  const handleSearchChange = useCallback(
    (event: ChangeEvent<HTMLInputElement>, newValue: string | null) => {
      if (newValue) {
        if (pods) {
          setSelectedPod(pods?.find((pod) => pod.name === newValue));
        }
      }
    },
    [pods]
  );

  const defaultProps = useMemo(() => {
    return {
      options: pods?.map((pod) => pod.name) as string[],
      getOptionLabel: (option: string) => option,
    };
  }, [pods]);

  const podSearchDetails = (
    <Box sx={{ display: "flex", flexDirection: "row" }}>
      <Box sx={{ fontWeight: "600", width: "12.8rem" }}>
        <span>Select a pod by name</span>
      </Box>
      <Box
        data-testid={"searchable-pods"}
        sx={{
          display: "flex",
          flexDirection: "row",
          mb: 2,
          justifyContent: "space-between",
        }}
      >
        <Box sx={{ paddingBottom: "1rem" }}>
          {pods && selectedPod && (
            <Autocomplete
              {...defaultProps}
              disablePortal
              disableClearable
              id="pod-select"
              ListboxProps={{
                sx: { fontSize: "1.6rem" },
              }}
              sx={{
                width: 300,
                border: "1px solid #E0E0E0",
                "& .MuiOutlinedInput-root": {
                  borderRadius: "0",
                },
              }}
              autoHighlight
              onChange={handleSearchChange}
              value={selectedPod?.name}
              renderInput={(params) => (
                <TextField
                  {...params}
                  variant="outlined"
                  id="outlined-basic"
                  inputProps={{
                    ...params.inputProps,
                    autoComplete: "new-password", // disable autocomplete and autofill
                    style: { fontSize: "1.6rem" },
                  }}
                />
              )}
            />
          )}
        </Box>
      </Box>
    </Box>
  );

  const selectedPodDetails = useMemo(
    () => podsDetails?.get(selectedPod?.name),
    [podsDetails, selectedPod]
  );

  if (loading) {
    return (
      <Box data-testid={"pods-loading"} sx={{ my: 2 }}>
        Loading pods view...
        <CircularProgress size={16} sx={{ mx: 2 }} />
      </Box>
    );
  }

  if (podsErr || podsDetailsErr || !pods?.length) {
    return (
      <Box
        data-testid={"pods-error"}
        sx={{ mx: 2, my: 2 }}
      >{`Failed to get pods details`}</Box>
    );
  }

  return (
    <Paper square elevation={0} sx={{ padding: "1.6rem" }}>
      <Box sx={{ display: "flex", flexDirection: "row" }}>
        <Box
          sx={{
            width: "70%",
            border: "1px solid #E0E0E0",
            marginRight: "1.6rem",
            display: "flex",
            flexDirection: "column",
            padding: "1.6rem",
            justifyContent: "space-evenly",
          }}
          data-testid={"pods-searchablePodsHeatMap"}
        >
          {podSearchDetails}
          <SearchablePodsHeatMap
            pods={pods}
            podsDetailsMap={podsDetails}
            onPodClick={handlePodClick}
            selectedPod={selectedPod}
          />
          {containerSelector}
        </Box>
        <Box sx={{ width: "30%", border: "1px solid #E0E0E0" }}>
          <Box
            sx={{
              display: "flex",
              flexDirection: "column",
              width: "100%",
              marginTop: "1.6rem",
            }}
          >
            <PodInfo
              pod={selectedPod}
              podDetails={selectedPodDetails}
              containerName={selectedContainer}
            />
          </Box>
        </Box>
      </Box>
      {podDetail}
    </Paper>
  );
}
