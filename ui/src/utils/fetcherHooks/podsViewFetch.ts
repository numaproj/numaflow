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
  type: string,
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
  const [lastRetryTime, setLastRetryTime] = useState(0);

  // call to get pods for a given vertex
  const fetchPods = async () => {
    const startedAt = Date.now();
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
            };
          });
          setPods(pList || []); // Ensure we always set an array
          setPodsErr(undefined); // Clear any previous errors
          // #region agent log
          fetch('http://localhost:7291/ingest/5019ad4a-9ec9-4ffe-943a-8dfb3c72b60e',{method:'POST',headers:{'Content-Type':'application/json','X-Debug-Session-Id':'e970b3'},body:JSON.stringify({sessionId:'e970b3',hypothesisId:'H1',location:'podsViewFetch.ts:pods',message:'pod list resolved',data:{ms:Date.now()-startedAt,count:(pList||[]).length,ok:true},timestamp:Date.now()})}).catch(()=>{});
          // #endregion
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
          // #region agent log
          fetch('http://localhost:7291/ingest/5019ad4a-9ec9-4ffe-943a-8dfb3c72b60e',{method:'POST',headers:{'Content-Type':'application/json','X-Debug-Session-Id':'e970b3'},body:JSON.stringify({sessionId:'e970b3',hypothesisId:'H2',location:'podsViewFetch.ts:pods-empty',message:'pod list empty payload',data:{ms:Date.now()-startedAt},timestamp:Date.now()})}).catch(()=>{});
          // #endregion
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

  // call to get pods details (metrics)
  // TODO: deprecate this and gather all metrics from pods-info
  const fetchPodDetails = async () => {
    const startedAt = Date.now();
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
          setPodsDetailsErr(undefined);
          // #region agent log
          fetch('http://localhost:7291/ingest/5019ad4a-9ec9-4ffe-943a-8dfb3c72b60e',{method:'POST',headers:{'Content-Type':'application/json','X-Debug-Session-Id':'e970b3'},body:JSON.stringify({sessionId:'e970b3',hypothesisId:'H1',location:'podsViewFetch.ts:details',message:'pod metrics resolved',data:{ms:Date.now()-startedAt,count:podsMap.size},timestamp:Date.now()})}).catch(()=>{});
          // #endregion
        } else if (json?.errMsg) {
          // #region agent log
          fetch('http://localhost:7291/ingest/5019ad4a-9ec9-4ffe-943a-8dfb3c72b60e',{method:'POST',headers:{'Content-Type':'application/json','X-Debug-Session-Id':'e970b3'},body:JSON.stringify({sessionId:'e970b3',hypothesisId:'H2',location:'podsViewFetch.ts:details-error',message:'pod metrics returned errMsg',data:{ms:Date.now()-startedAt,errMsg:json.errMsg},timestamp:Date.now()})}).catch(()=>{});
          // #endregion
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

  // Fetch pod details whenever requestKey (after every 20 sec) or host changes
  useEffect(() => {
    fetchPodDetails();
  }, [requestKey, host]);

  // Refresh requestKey every 20 sec
  useEffect(() => {
    const interval = setInterval(() => {
      setRequestKey(`${Date.now()}`);
    }, 20000);
    return () => {
      clearInterval(interval);
    };
  }, []);

  // Retry fetching pods details if there is an error
  useEffect(() => {
    const currentTime = Date.now();
    if (podsDetailsErr && currentTime - lastRetryTime > 5000) {
      const retryFetch = setTimeout(() => {
        fetchPodDetails();
        setLastRetryTime(currentTime);
      }, 5000);

      return () => clearTimeout(retryFetch);
    }
  }, [podsDetailsErr, lastRetryTime]);

  // A failed metrics call must not end loading before the pod list exists.
  useEffect(() => {
    const podsReady = pods !== undefined;
    const nextLoading = Boolean(podsErr)
      ? false
      : (!podsReady || !(podsDetails || podsDetailsErr));
    setLoading(nextLoading);
    // #region agent log
    fetch('http://localhost:7291/ingest/5019ad4a-9ec9-4ffe-943a-8dfb3c72b60e',{method:'POST',headers:{'Content-Type':'application/json','X-Debug-Session-Id':'e970b3'},body:JSON.stringify({sessionId:'e970b3',hypothesisId:'H2',location:'podsViewFetch.ts:loading',message:'loading gate',data:{loading:nextLoading,podCount:pods?.length??null,hasDetails:Boolean(podsDetails),podsErr:Boolean(podsErr),detailsErr:Boolean(podsDetailsErr)},timestamp:Date.now()})}).catch(()=>{});
    // #endregion
  }, [pods, podsDetails, podsErr, podsDetailsErr]);

  return {
    pods,
    podsDetails,
    podsErr,
    podsDetailsErr,
    loading,
  };
};
