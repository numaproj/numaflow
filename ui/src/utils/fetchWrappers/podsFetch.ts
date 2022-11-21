import { useState, useEffect } from "react";
import { Pod, PodContainerSpec } from "../models/pods";
import { useFetch } from "./fetch";
import { quantityToScalar } from "../index";

export const usePodsFetch = (
  namespaceId: string,
  pipelineId: string,
  vertexId: string
) => {
  const [pods, setPods] = useState<Pod[]>([]);
  const [loading, setLoading] = useState<boolean>(true);

  const {
    data,
    loading: fetchLoading,
    error,
  } = useFetch(
    `api/v1/namespaces/${namespaceId}/pipelines/${pipelineId}/vertices/${vertexId}/pods`
  );

  useEffect(() => {
    if (fetchLoading) {
      setPods([]);
      setLoading(true);
      return;
    }
    if (error) {
      setLoading(false);
      return;
    }
    if (data) {
      const pList = data.map((raw: any) => {
        const containers: string[] = [];
        const containerSpecMap = new Map<string, PodContainerSpec>();
        raw.spec?.containers?.forEach((rawC: any) => {
          const cpu = rawC.resources?.requests?.cpu;
          let cpuParsed: undefined | number;
          if (cpu) {
            try {
              cpuParsed = Number(quantityToScalar(cpu));
            } catch (e) {
              cpuParsed = undefined;
            }
          }
          const memory = rawC.resources?.requests?.memory;
          let memoryParsed: undefined | number;
          if (memory) {
            try {
              memoryParsed = Number(quantityToScalar(memory));
            } catch (e) {
              memoryParsed = undefined;
            }
          }
          containers.push(rawC.name);
          containerSpecMap.set(rawC.name, {
            name: rawC.name,
            cpu,
            cpuParsed,
            memory,
            memoryParsed,
          });
        });
        return {
          name: raw.metadata.name,
          containers,
          containerSpecMap,
        };
      });
      setPods(pList);
      setLoading(false);
      return;
    }
  }, [data, error, fetchLoading]);

  return { pods, error, loading };
};
