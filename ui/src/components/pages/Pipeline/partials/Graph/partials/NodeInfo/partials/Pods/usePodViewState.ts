import { useCallback, useContext, useEffect, useMemo, useState } from "react";
import { useHistory, useLocation } from "react-router-dom";
import { AppContext } from "../../../../../../../../../App";
import { notifyError } from "../../../../../../../../../utils/error";
import { getBaseHref } from "../../../../../../../../../utils";
import { usePodsViewFetch } from "../../../../../../../../../utils/fetcherHooks/podsViewFetch";
import { replaceObservabilityState } from "../../../../../../../../../utils/observabilityURLState";
import {
  ContainerInfoProps,
  Pod,
  PodRuntimeInfo,
  PodSpecificInfoProps,
  PodsProps,
} from "../../../../../../../../../types/declarations/pods";
import { AppContextProps } from "../../../../../../../../../types/declarations/app";

/** getDefaultContainerName honors the API's sidecar-first container ordering. */
export function getDefaultContainerName(pod: Pod | undefined): string | undefined {
  return pod?.containers?.[0];
}

export function resolveContainerForPod(
  pod: Pod | undefined,
  preferredContainer: string | undefined
): string | undefined {
  if (!pod) return undefined;
  return preferredContainer && pod.containers.includes(preferredContainer)
    ? preferredContainer
    : getDefaultContainerName(pod);
}

/**
 * usePodViewState owns the v1 Pod View data and selection contract shared by
 * Classic and Beta presenters; it deliberately does not own their layout.
 */
export function usePodViewState({
  namespaceId,
  pipelineId,
  vertexId,
  type,
}: PodsProps) {
  const { host } = useContext<AppContextProps>(AppContext);
  const history = useHistory();
  const location = useLocation();
  const [selectedPod, setSelectedPod] = useState<Pod | undefined>(undefined);
  const [selectedContainer, setSelectedContainer] = useState<string | undefined>(
    undefined
  );
  const [podRuntimeByName, setPodRuntimeByName] = useState<
    Map<string, PodRuntimeInfo>
  >(new Map());
  const [podRuntimeError, setPodRuntimeError] = useState<string | undefined>();
  const [runtimeRefreshKey, setRuntimeRefreshKey] = useState(`${Date.now()}`);

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

  const fetchRuntime = useCallback(async () => {
    try {
      const response = await fetch(
        `${host}${getBaseHref()}/api/v1/namespaces/${namespaceId}${
          type === "monoVertex"
            ? "/mono-vertices"
            : `/pipelines/${pipelineId}/vertices`
        }/${vertexId}/pods-info?refreshKey=${runtimeRefreshKey}`
      );
      if (!response.ok) throw new Error("Failed to fetch pod details");
      const body = await response.json();
      const nextRuntime = new Map<string, PodRuntimeInfo>();
      (body?.data || []).forEach((pod: PodRuntimeInfo) => {
        if (pod?.name) nextRuntime.set(pod.name, pod);
      });
      setPodRuntimeByName(nextRuntime);
      setPodRuntimeError(undefined);
    } catch {
      setPodRuntimeError("Failed to fetch pod details");
    }
  }, [host, namespaceId, pipelineId, runtimeRefreshKey, type, vertexId]);

  useEffect(() => {
    fetchRuntime();
  }, [fetchRuntime]);

  useEffect(() => {
    const interval = setInterval(
      () => setRuntimeRefreshKey(`${Date.now()}`),
      30000
    );
    return () => clearInterval(interval);
  }, []);

  useEffect(() => {
    if (podsErr) notifyError(podsErr);
  }, [podsErr]);

  useEffect(() => {
    if (podsDetailsErr) notifyError(podsDetailsErr);
  }, [podsDetailsErr]);

  useEffect(() => {
    if (!pods?.length) return;
    const params = new URLSearchParams(location.search);
    const requestedPodName = params.get("pod");
    const requestedContainer = params.get("container");
    if (!requestedPodName && !requestedContainer) return;
    const requestedPod = requestedPodName
      ? pods.find((pod) => pod.name === requestedPodName)
      : undefined;
    const pod = requestedPod || selectedPod || pods[0];
    const container = resolveContainerForPod(
      pod,
      requestedContainer || selectedContainer
    );
    if (pod?.name !== selectedPod?.name) setSelectedPod(pod);
    if (container !== selectedContainer) setSelectedContainer(container);
    replaceObservabilityState(history, location, {
      pod: pod?.name,
      container,
    });
  }, [history, location, pods, selectedContainer, selectedPod]);

  const selectPod = useCallback(
    (pod: Pod | undefined, preserveContainer: boolean) => {
      if (!pod) return;
      const container = preserveContainer
        ? resolveContainerForPod(pod, selectedContainer)
        : getDefaultContainerName(pod);
      setSelectedPod(pod);
      setSelectedContainer(container);
      replaceObservabilityState(history, location, {
        pod: pod.name,
        container,
      });
    },
    [history, location, selectedContainer]
  );

  const selectContainer = useCallback(
    (container: string) => {
      setSelectedContainer(container);
      replaceObservabilityState(history, location, { container });
    },
    [history, location]
  );

  const selectedRuntime = selectedPod
    ? podRuntimeByName.get(selectedPod.name)
    : undefined;
  const containerInfo = selectedContainer
    ? selectedRuntime?.containerDetailsMap?.[selectedContainer]
    : undefined;
  const podSpecificInfo = useMemo<PodSpecificInfoProps | undefined>(() => {
    if (!selectedRuntime) return undefined;
    const restartCount = Object.values(selectedRuntime.containerDetailsMap || {}).reduce(
      (count, container) => count + (container?.restartCount || 0),
      0
    );
    return {
      name: selectedRuntime.name,
      status: selectedRuntime.status || "",
      message: selectedRuntime.message || "",
      reason: selectedRuntime.reason || "",
      restartCount,
      totalCPU: selectedRuntime.totalCPU || "",
      totalMemory: selectedRuntime.totalMemory || "",
    };
  }, [selectedRuntime]);

  return {
    pods,
    podsDetails,
    podsErr,
    podsDetailsErr,
    loading,
    podRuntimeByName,
    podRuntimeError,
    selectedPod,
    selectedContainer,
    selectedRuntime,
    selectedPodDetails: podsDetails?.get(selectedPod?.name || ""),
    containerInfo: containerInfo as ContainerInfoProps | undefined,
    podSpecificInfo,
    selectPodFromFleet: (pod: Pod) => selectPod(pod, false),
    selectPodFromSearch: (pod: Pod) => selectPod(pod, true),
    selectContainer,
  };
}
