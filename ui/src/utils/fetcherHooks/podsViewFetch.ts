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
import {
  Pod,
  PodContainerSpec,
  PodDetail,
} from "../../types/declarations/pods";

export const usePodsViewFetch = (
  namespaceId: string | undefined,
  pipelineId: string | undefined,
  vertexId: string | undefined,
  selectedPod: Pod | undefined,
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
  const { host } = useContext<AppContextProps>(AppContext);

  // call to get pods for a given vertex
  useEffect(() => {
    const fetchPods = async () => {
      try {
        const response = await fetch(
          `${host}${getBaseHref()}/api/v1/namespaces/${namespaceId}/pipelines/${pipelineId}/vertices/${vertexId}/pods?refreshKey=${requestKey}`
        );
        if (response.ok) {
          const json = await response.json();
          if (json?.data) {
            const data = json?.data;
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

    fetchPods();
  }, [vertexId, requestKey, host]);

  useEffect(() => {
    if (pods?.length) {
      if (
        !(selectedPod && pods?.find((pod) => pod?.name === selectedPod?.name))
      ) {
        setSelectedPod(pods[0]);
        setSelectedContainer(pods[0]?.containers[0]);
      }
    } else {
      setSelectedPod(undefined);
      setSelectedContainer(undefined);
    }
  }, [pods]);

  useEffect(() => {
    const fetchPods = async () => {
      try {
        const response = await fetch(
          `${host}${getBaseHref()}/api/v1/metrics/namespaces/${namespaceId}/pods?refreshKey=${requestKey}`
        );
        if (response.ok) {
          const json = await response.json();
          if (json?.data) {
            const data = json?.data;
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
          } else if (json?.errMsg) {
            setPodsDetailsErr([
              {
                error: json.errMsg,
                options: {
                  toastId: `${vertexId}-pod-fetch-error`,
                  autoClose: 5000,
                },
              },
            ]);
          }
        } else {
          setPodsDetailsErr([
            {
              error: `Failed to get pods details for ${vertexId} vertex`,
              options: { toastId: `${vertexId}-pod-fetch`, autoClose: 5000 },
            },
          ]);
        }
      } catch {
        setPodsDetailsErr([
          {
            error: `Failed to get pods details for ${vertexId} vertex`,
            options: { toastId: `${vertexId}-pod-fetch`, autoClose: 5000 },
          },
        ]);
      }
    };

    fetchPods();
  }, [requestKey, host]);

  useEffect(() => {
    // Refresh pod details every 30 sec
    const interval = setInterval(() => {
      setRequestKey(`${Date.now()}`);
    }, 30000);
    return () => {
      clearInterval(interval);
    };
  }, []);

  // checks if all pods are present in podsDetailsMap
  const checkPodDetails = () => {
    if (!pods || !podsDetails) return false;
    for (let i = 0; i < pods.length; i++) {
      if (!podsDetails.has(pods[i]?.name)) return false;
    }
    return true;
  };

  //sets loading variable
  useEffect(() => {
    if (checkPodDetails()) {
      setLoading(false);
    } else if (podsErr || podsDetailsErr) {
      setLoading(false);
    } else {
      setLoading(true);
    }
  }, [pods, podsDetails, podsErr, podsDetailsErr]);

  return {
    pods,
    podsDetails,
    podsErr,
    podsDetailsErr,
    loading,
  };
};
