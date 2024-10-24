// eslint-disable-next-line @typescript-eslint/ban-ts-comment
// @ts-nocheck
import {
  ChangeEvent,
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useState,
} from "react";
import Box from "@mui/material/Box";
import Paper from "@mui/material/Paper";
import CircularProgress from "@mui/material/CircularProgress";
import Autocomplete from "@mui/material/Autocomplete";
import TextField from "@mui/material/TextField";
import { EventType } from "@visx/event/lib/types";
import { Containers } from "./partials/Containers";
import { PodDetail } from "./partials/PodDetails";
import { SearchablePodsHeatMap } from "./partials/SearchablePodsHeatMap";
import { ContainerInfo } from "./partials/PodDetails/partials/ContainerInfo";
import { PodInfoNew } from "./partials/PodDetails/partials/PodInfoNew";
import { usePodsViewFetch } from "../../../../../../../../../utils/fetcherHooks/podsViewFetch";
import { notifyError } from "../../../../../../../../../utils/error";
import { AppContext, AppContextProps } from "../../../../../../../../../App";
import { getBaseHref } from "../../../../../../../../../utils/index";
import {
  ContainerInfoProps,
  Hexagon,
  Pod,
  PodSpecificInfoProps,
  PodsProps,
} from "../../../../../../../../../types/declarations/pods";

export function Pods(props: PodsProps) {
  const { namespaceId, pipelineId, vertexId, type } = props;

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
      type,
      setSelectedPod,
      setSelectedContainer
    );

  const [containerInfo, setContainerInfo] = useState<
    ContainerInfoProps | undefined
  >(undefined);
  const [podSpecificInfo, setPodSpecificInfo] = useState<
    PodSpecificInfoProps | undefined
  >(undefined);
  const { host } = useContext<AppContextProps>(AppContext);

  const getContainerInfo = useCallback((podsData, podName, containerName) => {
    const selectedPod = podsData?.find((pod) => pod?.Name === podName);
    if (selectedPod) {
      return selectedPod?.ContainerDetailsMap[containerName];
    } else {
      return null;
    }
  }, []);

  const getPodSpecificInfo = useCallback((podsData, podName) => {
    const podSpecificInfo: PodSpecificInfoProps = {};
    const selectedPod = podsData?.find((pod) => pod?.Name === podName);
    if (selectedPod) {
      podSpecificInfo.Condition = selectedPod?.Condition;
      podSpecificInfo.Name = selectedPod?.Name;
      podSpecificInfo.Reason = selectedPod?.Reason;
      podSpecificInfo.Status = selectedPod?.Status;
      podSpecificInfo.Message = selectedPod?.Message;
      let restartCount = 0;
      for (const container in selectedPod?.ContainerDetailsMap) {
        restartCount +=
          selectedPod?.ContainerDetailsMap?.[container].RestartCount;
      }
      podSpecificInfo.RestartCount = restartCount;
      podSpecificInfo.ContainerCount = Object.keys(
        selectedPod?.ContainerDetailsMap
      ).length;
    }
    return podSpecificInfo;
  }, []);

  useEffect(() => {
    const fetchPodInfo = async () => {
      try {
        const response = await fetch(
          `${host}${getBaseHref()}/api/v1/info/namespaces/${namespaceId}/pods`
        );
        if (!response.ok) {
          throw new Error("Failed to fetch pod details");
        }
        const data = await response.json();
        const containerInfo = getContainerInfo(
          data?.data,
          selectedPod?.name,
          selectedContainer
        );
        const podSpecificInfo = getPodSpecificInfo(
          data?.data,
          selectedPod?.name
        );
        setContainerInfo(containerInfo);
        setPodSpecificInfo(podSpecificInfo);
      } catch (error) {
        setContainerInfo({ error: "Failed to fetch pod details" });
      }
    };
    fetchPodInfo();
  }, [namespaceId, host, selectedPod, selectedContainer]);

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
      <Box data-testid={"pods-poddetails"} sx={{ border: "1px solid #E0E0E0" }}>
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
    <Box
      sx={{
        display: "flex",
        flexDirection: "row",
        justifyContent: "space-between",
      }}
    >
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
          flexGrow: 1,
        }}
      >
        <Box sx={{ paddingBottom: "1rem", width: "100%" }}>
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
      <Box sx={{ display: "flex" }}>
        <Box
          sx={{
            display: "flex",
            flexDirection: "column",
            width: "40%",
          }}
        >
          <Box
            sx={{
              border: "1px solid #E0E0E0",
              display: "flex",
              flexDirection: "column",
              width: "100%",
              justifyContent: "space-evenly",
              mb: "1rem",
            }}
            data-testid={"pods-searchablePodsHeatMap"}
          >
            <Box sx={{ mt: "1.6rem" }}>
              <Box sx={{ p: "1.6rem" }}>
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
          </Box>
          <Box sx={{ width: "100%", border: "1px solid #E0E0E0", mb: "1rem" }}>
            <Box
              sx={{
                display: "flex",
                flexDirection: "column",
                width: "100%",
                marginTop: "1.6rem",
              }}
            >
              <PodInfoNew podSpecificInfo={podSpecificInfo} />
            </Box>
          </Box>
          <Box sx={{ width: "100%", border: "1px solid #E0E0E0" }}>
            <Box
              sx={{
                display: "flex",
                flexDirection: "column",
                width: "100%",
                marginTop: "1.6rem",
              }}
            >
              <ContainerInfo
                pod={selectedPod}
                podDetails={selectedPodDetails}
                containerName={selectedContainer}
                containerInfo={containerInfo}
              />
            </Box>
          </Box>
        </Box>
        <Box sx={{ width: "60%", ml: "1rem" }}>{podDetail}</Box>
      </Box>
    </Paper>
  );
}
