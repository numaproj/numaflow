import { useState, useEffect } from "react";
import { PodContainerSpec, PodDetail } from "../models/pods";
import { useFetch } from "./fetch";
import { quantityToScalar } from "../index";

export const usePodsDetailFetch = (namespaceId: string, requestKey: string) => {
  const [podsDetailMap, setPodsDetailMap] = useState<
    Map<string, PodDetail> | undefined
  >(undefined);
  const [loading, setLoading] = useState<boolean>(true);

  const {
    data,
    loading: fetchLoading,
    error,
  } = useFetch(
    `api/v1/metrics/namespaces/${namespaceId}/pods?refreshKey=${requestKey}`
  );

  useEffect(() => {
    if (fetchLoading) {
      setLoading(true);
      return;
    }
    if (error) {
      setLoading(false);
      return;
    }
    if (data) {
      const podsMap = new Map<string, PodDetail>();
      data.forEach((pod: any) => {
        const containerMap = new Map<string, PodContainerSpec>();
        pod.containers?.forEach((c: any) => {
          const cpu = c.usage?.cpu;
          let cpuParsed: undefined | number;
          if (cpu) {
            try {
              cpuParsed = Number(quantityToScalar(cpu));
            } catch (e) {
              cpuParsed = undefined;
            }
          }
          const memory = c.usage?.memory;
          let memoryParsed: undefined | number;
          if (memory) {
            try {
              memoryParsed = Number(quantityToScalar(memory));
            } catch (e) {
              memoryParsed = undefined;
            }
          }
          const container = {
            name: c.name,
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
      setPodsDetailMap(podsMap);
      setLoading(false);
      return;
    }
  }, [data, error, fetchLoading]);

  return { podsDetailMap, error, loading };
};
