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
import { usePodsViewFetch } from "../../../../../../../../../utils/fetcherHooks/podsViewFetch";
import { notifyError } from "../../../../../../../../../utils/error";
import { AppContext, AppContextProps } from "../../../../../../../../../App";
import { getBaseHref } from "../../../../../../../../../utils";
import {
  ContainerInfoProps,
  Hexagon,
  Pod,
  PodSpecificInfoProps,
  PodsProps,
} from "../../../../../../../../../types/declarations/pods";

export function Pods(props: PodsProps) {
  const { host } = useContext<AppContextProps>(AppContext);
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
  const [requestKey, setRequestKey] = useState(`${Date.now()}`);

  const getContainerInfo = useCallback((podsData, podName, containerName) => {
    const selectedPod = podsData?.find((pod) => pod?.name === podName);
    if (selectedPod) {
      return selectedPod?.containerDetailsMap[containerName];
    } else {
      return null;
    }
  }, []);

  const getPodSpecificInfo = useCallback((podsData, podName) => {
    const podSpecificInfo: PodSpecificInfoProps = {};
    const selectedPod = podsData?.find((pod) => pod?.name === podName);
    if (selectedPod) {
      podSpecificInfo.name = selectedPod?.name;
      podSpecificInfo.reason = selectedPod?.reason;
      podSpecificInfo.status = selectedPod?.status;
      podSpecificInfo.message = selectedPod?.message;
      podSpecificInfo.totalCPU = selectedPod?.totalCPU;
      podSpecificInfo.totalMemory = selectedPod?.totalMemory;
      let restartCount = 0;
      for (const container in selectedPod?.containerDetailsMap) {
        restartCount +=
          selectedPod?.containerDetailsMap?.[container].restartCount;
      }
      podSpecificInfo.restartCount = restartCount;
    }
    return podSpecificInfo;
  }, []);

  useEffect(() => {
    const fetchPodInfo = async () => {
      try {
        const response = await fetch(
          `${host}${getBaseHref()}/api/v1/namespaces/${namespaceId}${
            type === "monoVertex"
              ? `/mono-vertices`
              : `/pipelines/${pipelineId}/vertices`
          }/${vertexId}/pods-info?refreshKey=${requestKey}`
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
  }, [
    namespaceId,
    host,
    getBaseHref,
    type,
    pipelineId,
    vertexId,
    getContainerInfo,
    getPodSpecificInfo,
    requestKey,
    selectedPod,
    selectedContainer,
    setPodSpecificInfo,
    setContainerInfo,
  ]);

  useEffect(() => {
    // Refresh pod details every 30 sec
    const interval = setInterval(() => {
      setRequestKey(`${Date.now()}`);
    }, 30000);
    return () => {
      clearInterval(interval);
    };
  }, []);

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
  }, [selectedPod, selectedContainer]);

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
        />
      </Box>
    );
  }, [namespaceId, pipelineId, type, selectedContainer, selectedPod, vertexId]);

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
            width: "calc(40% - 2rem)",
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
            padding: "1rem",
            width: "calc(60% - 2rem)",
            height: "calc(100% - 2rem)",
          }}
        >
          {podDetail}
        </Box>
      </Box>
    </Paper>
  );
}
