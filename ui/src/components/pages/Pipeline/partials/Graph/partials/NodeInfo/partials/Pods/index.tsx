// eslint-disable-next-line @typescript-eslint/ban-ts-comment
// @ts-nocheck
import {
  ChangeEvent,
  useCallback,
  useMemo,
} from "react";
import Box from "@mui/material/Box";
import Paper from "@mui/material/Paper";
import CircularProgress from "@mui/material/CircularProgress";
import Autocomplete from "@mui/material/Autocomplete";
import TextField from "@mui/material/TextField";
import { Containers } from "./partials/Containers";
import { PodDetail } from "./partials/PodDetails";
import { SearchablePodsHeatMap } from "./partials/SearchablePodsHeatMap";
import { ContainerInfo } from "./partials/PodDetails/partials/ContainerInfo";
import { PodsProps } from "../../../../../../../../../types/declarations/pods";
import { PodFocusControls } from "./partials/PodFocusControls";
import { usePodViewState } from "./usePodViewState";

export function Pods(props: PodsProps) {
  const { namespaceId, pipelineId, vertexId, type } = props;

  if (!namespaceId || !pipelineId || !vertexId) {
    return (
      <Box data-testid={"pods-error-missing"} sx={{ mx: 2, my: 2 }}>
        {`Missing namespace, pipeline or vertex information`}
      </Box>
    );
  }

  const {
    pods,
    podsDetails,
    podsErr,
    loading,
    selectedPod,
    selectedContainer,
    selectedPodDetails,
    containerInfo,
    podSpecificInfo,
    selectPodFromFleet,
    selectPodFromSearch,
    selectContainer,
  } = usePodViewState(props);

  const handlePodClick = useCallback((_event: Element, p: any) => {
    const nextPod = p?.data?.pod;
    if (nextPod) selectPodFromFleet(nextPod);
  }, [selectPodFromFleet]);

  const handleContainerClick = useCallback((containerName: string) => {
    selectContainer(containerName);
  }, [selectContainer]);

  const containerSelector = useMemo(() => {
    return (
      <Box sx={{ display: "flex", width: "100%" }}>
        <Box sx={{ fontWeight: "600", width: "24%", mr: "1%" }}>
          Select a container
        </Box>
        <Box data-testid={"pods-containers"} sx={{ width: "75%" }}>
          <Containers
            pod={selectedPod}
            containerName={selectedContainer}
            handleContainerClick={handleContainerClick}
          />
        </Box>
      </Box>
    );
  }, [selectedPod, selectedContainer, handleContainerClick]);

  const focusControls = useMemo(() => {
    if (!pods || !selectedPod) {
      return null;
    }
    return (
      <PodFocusControls
        pods={pods}
        selectedPod={selectedPod}
        selectedContainer={selectedContainer}
        onPodSelect={selectPodFromSearch}
        onContainerSelect={selectContainer}
      />
    );
  }, [
    pods,
    selectedPod,
    selectedContainer,
    selectPodFromSearch,
    selectContainer,
  ]);

  const podDetail = useMemo(() => {
    return (
      <Box
        data-testid={"pods-poddetails"}
        sx={{ height: "100%", width: "100%", border: "1px solid #E0E0E0" }}
      >
        <PodDetail
          namespaceId={namespaceId}
          pipelineId={pipelineId}
          type={type}
          containerName={selectedContainer}
          pod={selectedPod}
          vertexId={vertexId}
          focusControls={focusControls}
        />
      </Box>
    );
  }, [
    namespaceId,
    pipelineId,
    type,
    selectedContainer,
    selectedPod,
    vertexId,
    focusControls,
  ]);

  const handleSearchChange = useCallback(
    (event: ChangeEvent<HTMLInputElement>, newValue: string | null) => {
      if (newValue) {
        if (pods) {
          const pod = pods?.find((pod) => pod.name === newValue);
          selectPodFromSearch(pod);
        }
      }
    },
    [pods, selectPodFromSearch]
  );

  const podSearchDetails = (
    <Box
      sx={{
        display: "flex",
        mb: "0.75rem",
        width: "100%",
      }}
    >
      <Box sx={{ fontWeight: "600", width: "24%", mr: "1%" }}>
        Select a pod by name
      </Box>
      <Box data-testid={"searchable-pods"} sx={{ width: "75%" }}>
        <Box>
          {pods && selectedPod && (
            <Autocomplete
              options={pods.map((pod) => pod.name)}
              getOptionLabel={(option: string) => option}
              disablePortal
              disableClearable
              id="pod-select"
              ListboxProps={{
                sx: { fontSize: "1.6rem" },
              }}
              sx={{
                width: "100%",
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

  if (loading) {
    return (
      <Box data-testid={"pods-loading"} sx={{ my: 2 }}>
        Loading pods view...
        <CircularProgress size={16} sx={{ mx: 2 }} />
      </Box>
    );
  }

  if (podsErr) {
    return (
      <Box
        data-testid={"pods-error"}
        sx={{ mx: 2, my: 2 }}
      >{`Failed to get pods details`}</Box>
    );
  }

  if (!pods?.length) {
    return (
      <Box
        data-testid={"pods-empty"}
        sx={{ mx: 2, my: 2 }}
      >{`No pods found for this vertex`}</Box>
    );
  }

  return (
    <Paper square elevation={0} sx={{ height: "100%" }}>
      <Box sx={{ display: "flex", height: "100%" }}>
        {/*pod details container*/}
        <Box
          sx={{
            display: "flex",
            flexDirection: "column",
            padding: "1rem",
            width: "calc(35% - 2rem)",
            height: "calc(100% - 2rem)",
            justifyContent: "space-between",
            gap: "1rem",
          }}
        >
          {/*pod and container selector*/}
          <Box
            sx={{
              display: "flex",
              width: "100%",
              border: "1px solid #E0E0E0",
            }}
            data-testid={"pods-searchablePodsHeatMap"}
          >
            <Box
              sx={{
                display: "flex",
                flexDirection: "column",
                width: "100%",
                justifyContent: "space-evenly",
                p: "1rem",
              }}
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
          </Box>
          {/*pod and container info*/}
          <Box
            sx={{
              display: "flex",
              height: "100%",
              width: "100%",
              border: "1px solid #E0E0E0",
              overflow: "auto",
            }}
          >
            <Box
              sx={{
                display: "flex",
                flex: 1,
                height: "calc(100% - 2rem)",
                p: "1rem",
              }}
            >
              <ContainerInfo
                namespaceId={namespaceId}
                pipelineId={pipelineId}
                vertexId={vertexId}
                type={type}
                pod={selectedPod}
                podDetails={selectedPodDetails}
                containerName={selectedContainer}
                containerInfo={containerInfo}
                podSpecificInfo={podSpecificInfo}
              />
            </Box>
          </Box>
        </Box>
        {/*logs and metrics container*/}
        <Box
          sx={{
            display: "flex",
            padding: "1rem 0 1rem 1rem",
            width: "calc(65% - 1rem)",
            height: "calc(100% - 2rem)",
          }}
        >
          {podDetail}
        </Box>
      </Box>
    </Paper>
  );
}
