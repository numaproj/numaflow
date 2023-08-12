import { Dispatch, SetStateAction, useEffect, useState } from "react";
import { quantityToScalar } from "../index";
import {
  Pod,
  PodContainerSpec,
  PodDetail,
} from "../../types/declarations/pods";

export const usePodsViewFetch = (
  namespaceId: string | undefined,
  pipelineId: string | undefined,
  vertexId: string | undefined,
  setSelectedPod: Dispatch<SetStateAction<Pod | undefined>>,
  setSelectedContainer: Dispatch<SetStateAction<string | undefined>>
) => {
  const [pods, setPods] = useState<Pod[] | undefined>(undefined);
  const [podsDetails, setPodsDetails] = useState<
    Map<string, PodDetail> | undefined
  >(undefined);
  const [podsErr, setPodsErr] = useState<any[] | undefined>(undefined);
  const [podsDetailsErr, setPodsDetailsErr] = useState<any[] | undefined>(
    undefined
  );
  const [requestKey, setRequestKey] = useState(`${Date.now()}`);
  const [loading, setLoading] = useState(true);

  // call to get pods for a given vertex
  useEffect(() => {
    const fetchPods = async () => {
      try {
        const response = await fetch(
          `/api/v1/namespaces/${namespaceId}/pipelines/${pipelineId}/vertices/${vertexId}/pods?refreshKey=${requestKey}`
        );
        if (response.ok) {
          const data = await response.json();
          const pList = data?.map((pod: any) => {
            const containers: string[] = [];
            const containerSpecMap = new Map<string, PodContainerSpec>();
            pod?.spec?.containers?.forEach((container: any) => {
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
            };
          });
          setPods(pList);
        } else {
          setPodsErr([
            {
              error: `Failed to get pods for ${vertexId} vertex`,
              options: { toastId: `${vertexId}-pod`, autoClose: 5000 },
            },
          ]);
        }
      } catch {
        setPodsErr([
          {
            error: `Failed to get pods for ${vertexId} vertex`,
            options: { toastId: `${vertexId}-pod`, autoClose: 5000 },
          },
        ]);
      }
    };

    fetchPods();
  }, [vertexId, requestKey]);

  useEffect(() => {
    if (pods?.length) {
      setSelectedPod(pods[0]);
      setSelectedContainer(pods[0]?.containers[0]);
    } else {
      setSelectedPod(undefined);
      setSelectedContainer(undefined);
    }
  }, [pods]);

  useEffect(() => {
    const fetchPods = async () => {
      try {
        const response = await fetch(
          `/api/v1/metrics/namespaces/${namespaceId}/pods?refreshKey=${requestKey}`
        );
        if (response.ok) {
          const data = await response.json();
          const podsMap = new Map<string, PodDetail>();
          data?.forEach((pod: any) => {
            const containerMap = new Map<string, PodContainerSpec>();
            pod?.containers?.forEach((c: any) => {
              const cpu = c?.usage?.cpu;
              let cpuParsed: undefined | number;
              if (cpu) {
                try {
                  cpuParsed = Number(quantityToScalar(cpu));
                } catch (e) {
                  cpuParsed = undefined;
                }
              }
              const memory = c?.usage?.memory;
              let memoryParsed: undefined | number;
              if (memory) {
                try {
                  memoryParsed = Number(quantityToScalar(memory));
                } catch (e) {
                  memoryParsed = undefined;
                }
              }
              const container = {
                name: c?.name,
                cpu,
                cpuParsed,
                memory,
                memoryParsed,
              };
              containerMap.set(container.name, container);
            });
            const podDetail = {
              name: pod?.metadata?.name,
              containerMap,
            };
            podsMap.set(podDetail.name, podDetail);
          });
          setPodsDetails(podsMap);
        } else {
          setPodsDetailsErr([
            {
              error: `Failed to get pods details for ${vertexId} vertex`,
              options: { toastId: `${vertexId}-pod`, autoClose: 5000 },
            },
          ]);
        }
      } catch {
        setPodsDetailsErr([
          {
            error: `Failed to get pods details for ${vertexId} vertex`,
            options: { toastId: `${vertexId}-pod`, autoClose: 5000 },
          },
        ]);
      }
    };

    fetchPods();
  }, [requestKey]);

  useEffect(() => {
    // Refresh pod details every 30 sec
    const interval = setInterval(() => {
      setRequestKey(`${Date.now()}`);
    }, 30000);
    return () => {
      clearInterval(interval);
    };
  }, []);

  //sets loading variable
  useEffect(() => {
    if (pods && podsDetails) {
      setLoading(false);
    }
  }, [pods, podsDetails]);

  return {
    pods,
    podsDetails,
    podsErr,
    podsDetailsErr,
    loading,
  };
};
