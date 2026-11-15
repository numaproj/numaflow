import {
  Dispatch,
  SetStateAction,
  useContext,
  useEffect,
  useState,
} from "react";
import { getBaseHref, quantityToScalar } from "../index";
import { AppContextProps } from "../../types/declarations/app";
import { AppContext } from "../../App";
import { Pod, PodContainerSpec } from "../../types/declarations/pods";

export const usePodsViewFetch = (
  namespaceId: string | undefined,
  pipelineId: string | undefined,
  vertexId: string | undefined,
  selectedPod: Pod | undefined,
  type: string,
  setSelectedPod: Dispatch<SetStateAction<Pod | undefined>>,
  setSelectedContainer: Dispatch<SetStateAction<string | undefined>>,
  {
    autoSelectFirstPod = true,
    active = true,
  }: { autoSelectFirstPod?: boolean; active?: boolean } = {}
) => {
  const [pods, setPods] = useState<Pod[] | undefined>(undefined);
  const [podsErr, setPodsErr] = useState<any[] | undefined>(undefined);
  const [requestKey, setRequestKey] = useState(`${Date.now()}`);
  const [loading, setLoading] = useState(true);
  const { host } = useContext<AppContextProps>(AppContext);

  // call to get pods for a given vertex
  const fetchPods = async () => {
    try {
      const response = await fetch(
        `${host}${getBaseHref()}/api/v1/namespaces/${namespaceId}${
          type === "monoVertex"
            ? `/mono-vertices`
            : `/pipelines/${pipelineId}/vertices`
        }/${vertexId}/pods?refreshKey=${requestKey}`
      );
      if (response.ok) {
        const json = await response.json();
        if (json?.data) {
          let data = json?.data;
          data = data.filter(
            (pod: any) => !pod?.metadata?.name.includes("-daemon-")
          );
          const pList = data?.map((pod: any) => {
            const containers: string[] = [];
            const containerSpecMap = new Map<string, PodContainerSpec>();

            const containersList: any[] = [];
            pod?.spec?.initContainers
              ?.filter(
                (initContainer: any) =>
                  initContainer?.restartPolicy === "Always"
              )
              ?.forEach((container: any) => containersList.push(container));

            const specContainers = JSON.parse(
              JSON.stringify(pod?.spec?.containers)
            );

            containersList.push(...specContainers);

            containersList?.forEach((container: any) => {
              const cpu = container?.resources?.requests?.cpu;
              let cpuParsed: undefined | number;
              if (cpu) {
                try {
                  cpuParsed = Number(quantityToScalar(cpu));
                } catch (e) {
                  cpuParsed = undefined;
                }
              }
              const memory = container?.resources?.requests?.memory;
              let memoryParsed: undefined | number;
              if (memory) {
                try {
                  memoryParsed = Number(quantityToScalar(memory));
                } catch (e) {
                  memoryParsed = undefined;
                }
              }
              containers.push(container?.name);
              containerSpecMap.set(container?.name, {
                name: container?.name,
                cpu,
                cpuParsed,
                memory,
                memoryParsed,
              });
            });
            return {
              name: pod?.metadata?.name,
              containers,
              containerSpecMap,
              createdAt: pod?.metadata?.creationTimestamp,
            };
          });
          setPods(pList || []); // Ensure we always set an array
          setPodsErr(undefined); // Clear any previous errors
        } else if (json?.errMsg) {
          setPodsErr([
            {
              error: json.errMsg,
              options: {
                toastId: `${vertexId}-pod-fetch-error`,
                autoClose: 5000,
              },
            },
          ]);
        } else {
          // No data and no error means no pods
          setPods([]);
          setPodsErr(undefined);
        }
      } else {
        setPodsErr([
          {
            error: `Failed to get pods for ${vertexId} vertex`,
            options: { toastId: `${vertexId}-pod-fetch`, autoClose: 5000 },
          },
        ]);
      }
    } catch {
      setPodsErr([
        {
          error: `Failed to get pods for ${vertexId} vertex`,
          options: { toastId: `${vertexId}-pod-fetch`, autoClose: 5000 },
        },
      ]);
    }
  };

  useEffect(() => {
    if (!active) return;
    fetchPods();
  }, [vertexId, requestKey, host, active]);

  // Without auto-select, a selection is only cleared when its pod disappears.
  useEffect(() => {
    if (pods?.length) {
      const selectionExists = Boolean(
        selectedPod && pods.find((pod) => pod?.name === selectedPod?.name)
      );
      if (selectionExists) return;
      if (autoSelectFirstPod) {
        setSelectedPod(pods[0]);
        setSelectedContainer(pods[0]?.containers[0]);
      } else if (selectedPod) {
        setSelectedPod(undefined);
        setSelectedContainer(undefined);
      }
    } else {
      setSelectedPod(undefined);
      setSelectedContainer(undefined);
    }
  }, [pods]);

  // Refresh the pod list every 20 sec while the Pods tab is visible.
  // Container CPU and memory come from pods-info, not this list.
  useEffect(() => {
    if (!active) return;
    const interval = setInterval(() => {
      setRequestKey(`${Date.now()}`);
    }, 20000);
    return () => {
      clearInterval(interval);
    };
  }, [active]);

  useEffect(() => {
    setLoading(podsErr ? false : pods === undefined);
  }, [pods, podsErr]);

  return {
    pods,
    podsErr,
    loading,
  };
};
