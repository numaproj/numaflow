import { useCallback, useContext, useEffect, useMemo, useRef, useState } from "react";
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

/**
 * getDefaultContainerName returns `defaultContainer` when the pod runs it,
 * otherwise the first container in the API's sidecar-first ordering.
 */
export function getDefaultContainerName(
  pod: Pod | undefined,
  defaultContainer?: string
): string | undefined {
  if (defaultContainer && pod?.containers?.includes(defaultContainer)) {
    return defaultContainer;
  }
  return pod?.containers?.[0];
}

export function resolveContainerForPod(
  pod: Pod | undefined,
  preferredContainer: string | undefined,
  defaultContainer?: string
): string | undefined {
  if (!pod) return undefined;
  return preferredContainer && pod.containers.includes(preferredContainer)
    ? preferredContainer
    : getDefaultContainerName(pod, defaultContainer);
}

export interface PodViewStateOptions {
  /** Classic selects the first pod on load; Beta waits for an explicit click or a `pod` URL param. */
  autoSelectFirstPod?: boolean;
  /** Container chosen when a pod is selected, if the pod runs it. */
  defaultContainer?: string;
  /** When false, keep selection but do not poll runtime or pod metrics. */
  active?: boolean;
}

/**
 * usePodViewState owns the v1 Pod View data and selection contract shared by
 * Classic and Beta presenters; it deliberately does not own their layout.
 */
export function usePodViewState(
  { namespaceId, pipelineId, vertexId, type }: PodsProps,
  { autoSelectFirstPod = true, defaultContainer, active = true }: PodViewStateOptions = {}
) {
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
  const runtimeRequestRef = useRef(0);
  const runtimeAbortRef = useRef<AbortController | undefined>(undefined);

  const { pods, podsErr, loading } =
    usePodsViewFetch(
      namespaceId,
      pipelineId,
      vertexId,
      selectedPod,
      type,
      setSelectedPod,
      setSelectedContainer,
      { autoSelectFirstPod, active }
    );

  const fetchRuntime = useCallback(async () => {
    runtimeAbortRef.current?.abort();
    const controller = new AbortController();
    runtimeAbortRef.current = controller;
    const request = ++runtimeRequestRef.current;
    try {
      const response = await fetch(
        `${host}${getBaseHref()}/api/v1/namespaces/${namespaceId}${
          type === "monoVertex"
            ? "/mono-vertices"
            : `/pipelines/${pipelineId}/vertices`
        }/${vertexId}/pods-info?refreshKey=${runtimeRefreshKey}`,
        { signal: controller.signal }
      );
      if (controller.signal.aborted || request !== runtimeRequestRef.current) return;
      if (!response.ok) throw new Error("Failed to fetch pod details");
      const body = await response.json();
      if (controller.signal.aborted || request !== runtimeRequestRef.current) return;
      const nextRuntime = new Map<string, PodRuntimeInfo>();
      (body?.data || []).forEach((pod: PodRuntimeInfo) => {
        if (pod?.name) nextRuntime.set(pod.name, pod);
      });
      setPodRuntimeByName(nextRuntime);
      setPodRuntimeError(undefined);
    } catch {
      if (controller.signal.aborted || request !== runtimeRequestRef.current) return;
      setPodRuntimeError("Failed to fetch pod details");
    }
  }, [host, namespaceId, pipelineId, runtimeRefreshKey, type, vertexId]);

  // A vertex switch must not keep the previous runtime map until the new request returns.
  useEffect(() => {
    setPodRuntimeByName(new Map());
    setPodRuntimeError(undefined);
  }, [host, namespaceId, pipelineId, vertexId, type]);

  useEffect(() => {
    if (!active) return;
    void fetchRuntime();
    return () => {
      runtimeRequestRef.current += 1;
      runtimeAbortRef.current?.abort();
    };
  }, [active, fetchRuntime]);

  useEffect(() => {
    if (!active) return;
    const interval = setInterval(
      () => setRuntimeRefreshKey(`${Date.now()}`),
      30000
    );
    return () => clearInterval(interval);
  }, [active]);

  useEffect(() => {
    if (podsErr) notifyError(podsErr);
  }, [podsErr]);

  useEffect(() => {
    if (!pods?.length) return;
    const params = new URLSearchParams(location.search);
    const requestedPodName = params.get("pod");
    const requestedContainer = params.get("container");
    if (!requestedPodName && !requestedContainer) return;
    const requestedPod = requestedPodName
      ? pods.find((pod) => pod.name === requestedPodName)
      : undefined;
    const pod = autoSelectFirstPod
      ? requestedPod || selectedPod || pods[0]
      : requestedPod || selectedPod;
    if (!pod) {
      // A stale or container-only link must not leave a selection the view does not show.
      replaceObservabilityState(history, location, { pod: null, container: null });
      return;
    }
    const container = resolveContainerForPod(
      pod,
      requestedContainer || selectedContainer,
      defaultContainer
    );
    if (pod?.name !== selectedPod?.name) setSelectedPod(pod);
    if (container !== selectedContainer) setSelectedContainer(container);
    replaceObservabilityState(history, location, {
      pod: pod?.name,
      container,
    });
  }, [
    autoSelectFirstPod,
    defaultContainer,
    history,
    location,
    pods,
    selectedContainer,
    selectedPod,
  ]);

  const selectPod = useCallback(
    (pod: Pod | undefined, preserveContainer: boolean, preferredContainer?: string) => {
      if (!pod) return;
      const container = preferredContainer
        ? resolveContainerForPod(pod, preferredContainer, defaultContainer)
        : preserveContainer
        ? resolveContainerForPod(pod, selectedContainer, defaultContainer)
        : getDefaultContainerName(pod, defaultContainer);
      setSelectedPod(pod);
      setSelectedContainer(container);
      replaceObservabilityState(history, location, {
        pod: pod.name,
        container,
      });
    },
    [defaultContainer, history, location, selectedContainer]
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
    podsErr,
    loading,
    podRuntimeByName,
    podRuntimeError,
    selectedPod,
    selectedContainer,
    selectedRuntime,
    containerInfo: containerInfo as ContainerInfoProps | undefined,
    podSpecificInfo,
    selectPodFromFleet: (pod: Pod) => selectPod(pod, false),
    selectPodFromSearch: (pod: Pod) => selectPod(pod, true),
    /** Selects a pod and container in one URL write; falls back to the default if the pod lacks it. */
    selectPodWithContainer: (pod: Pod, container: string) => selectPod(pod, false, container),
    selectContainer,
  };
}
