import { useEffect, useState } from "react";
import { useFetch, Options } from "./fetch";
import {
  K8sEvent,
  K8sEventSummary,
  NamespaceK8sEventsFetchProps,
  NamespaceK8sEventsFetchResult,
} from "../../types/declarations/namespace";

const rawDataToEventListing = (
  namespace: string,
  rawData: any[]
): K8sEventSummary | undefined => {
  if (!rawData || !Array.isArray(rawData)) {
    return undefined;
  }
  const defaultValue = "none";
  let normalCount = 0;
  let warningCount = 0;
  const events: K8sEvent[] = [];
  rawData.forEach((event, index) => {
    if (event.type && event.type.toLowerCase() === "normal") {
      normalCount++;
    } else {
      warningCount++;
    }
    events.push({
      eventKey: index,
      namespace,
      timestamp: event.timestamp
        ? `${new Date(event.timestamp).toLocaleDateString()} ${new Date(
            event.timestamp
          ).toLocaleTimeString()}`
        : defaultValue,
      type: event.type || defaultValue,
      object: event.object || defaultValue,
      reason: event.reason || defaultValue,
      message: event.message || defaultValue,
    });
  });
  return {
    normalCount,
    warningCount,
    events,
  };
};

export const useNamespaceK8sEventsFetch = ({
  namespace,
}: NamespaceK8sEventsFetchProps) => {
  const [results, setResults] = useState<NamespaceK8sEventsFetchResult>({
    data: undefined,
    loading: true,
    error: undefined,
  });
  const [options] = useState<Options>({
    skip: false,
    requestKey: "",
  });
  const {
    data: fetchData,
    loading: fetchLoading,
    error: fetchError,
  } = useFetch(`/api/v1/namespaces/${namespace}/events`, undefined, options);

  useEffect(() => {
    if (fetchLoading) {
      setResults({
        data: undefined,
        loading: true,
        error: undefined,
      });
      return;
    }
    if (fetchError) {
      setResults({
        data: undefined,
        loading: false,
        error: fetchError,
      });
      return;
    }
    if (fetchData && fetchData.errMsg) {
      setResults({
        data: undefined,
        loading: false,
        error: fetchData.errMsg,
      });
      return;
    }
    if (fetchData) {
      // const eventList = rawDataToEventListing(MOCK_DATA); // TODO REMOVE MOCK
      const eventList = rawDataToEventListing(namespace, fetchData.data);
      setResults({
        data: eventList,
        loading: false,
        error: undefined,
      });
      return;
    }
  }, [namespace, fetchData, fetchLoading, fetchError]);

  return results;
};
