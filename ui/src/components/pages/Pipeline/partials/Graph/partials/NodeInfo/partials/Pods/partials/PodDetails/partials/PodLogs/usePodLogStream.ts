import { useEffect, useState } from "react";
import "@stardazed/streams-polyfill";
import { getBaseHref } from "../../../../../../../../../../../../../utils";
import { LOADING_LOGS, MAX_LOGS } from "./constants";
import { extractLogChunk, parsePodLogs } from "./parsePodLogs";

export type BuildPodLogsUrlParams = {
  host: string;
  namespaceId: string;
  podName: string;
  containerName: string;
  previous?: boolean;
  tailLines?: number;
};

export function buildPodLogsUrl({
  host,
  namespaceId,
  podName,
  containerName,
  previous = false,
  tailLines = MAX_LOGS,
}: BuildPodLogsUrlParams): string {
  const base = `${host}${getBaseHref()}/api/v1/namespaces/${namespaceId}/pods/${podName}/logs?container=${containerName}&follow=true&tailLines=${tailLines}`;
  return previous ? `${base}&previous=true` : base;
}

export function appendCapped(
  existing: string[],
  incoming: string[],
  max: number = MAX_LOGS
): string[] {
  let updated = [...existing, ...incoming];
  if (updated.length > max) {
    updated = updated.slice(updated.length - max);
  }
  return updated;
}

export type UsePodLogStreamParams = {
  namespaceId: string;
  podName: string;
  containerName: string;
  type: string;
  host: string;
  paused: boolean;
  enableTimestamp: boolean;
  levelFilter: string;
  showPreviousLogs: boolean;
};

export function usePodLogStream({
  namespaceId,
  podName,
  containerName,
  type,
  host,
  paused,
  enableTimestamp,
  levelFilter,
  showPreviousLogs,
}: UsePodLogStreamParams): { logs: string[]; previousLogs: string[] } {
  const [logs, setLogs] = useState<string[]>([]);
  const [previousLogs, setPreviousLogs] = useState<string[]>([]);
  const [logRequestKey, setLogRequestKey] = useState<string>("");
  const [reader, setReader] = useState<
    ReadableStreamDefaultReader<string> | undefined
  >();

  // Reset buffers when the log source changes; also unpause so streaming restarts.
  useEffect(() => {
    setLogs([]);
    setPreviousLogs([]);
  }, [namespaceId, podName, containerName]);

  // Cancel the live reader when pausing (mirrors previous handlePause behavior).
  // Intentionally depends only on `paused` so setting the reader does not re-run this.
  useEffect(() => {
    if (paused && reader) {
      reader.cancel();
      setReader(undefined);
    }
  }, [paused]);

  // Restart the live stream when parse options change.
  // Intentionally omits `reader` so this only runs when parse options change.
  useEffect(() => {
    if (reader) {
      reader.cancel();
      setReader(undefined);
    }
  }, [enableTimestamp, levelFilter]);

  // Live follow stream
  useEffect(() => {
    if (paused) {
      return;
    }
    const requestKey = `${namespaceId}-${podName}-${containerName}`;
    if (logRequestKey && logRequestKey !== requestKey && reader) {
      // Cancel open reader on param change
      reader.cancel();
      setReader(undefined);
      return;
    } else if (reader) {
      // Don't open a new reader if one exists
      return;
    }
    setLogRequestKey(requestKey);
    setLogs([LOADING_LOGS]);
    fetch(
      buildPodLogsUrl({
        host,
        namespaceId,
        podName,
        containerName,
      })
    )
      .then((response) => {
        if (response && response.body) {
          const r = response.body
            .pipeThrough(new TextDecoderStream())
            .getReader();
          setReader(r);
          r.read().then(async function process({
            done,
            value,
          }: ReadableStreamReadResult<string>): Promise<void> {
            if (done) {
              return;
            }
            if (value) {
              const { text, isErrorMessage } = extractLogChunk(value);
              setLogs((current) => {
                const latestLogs = parsePodLogs(text, {
                  enableTimestamp,
                  levelFilter,
                  type,
                  isErrorMessage,
                }).filter((line) => line !== "");
                return appendCapped(current, latestLogs);
              });
            }
            await process(await r.read());
          });
        }
      })
      .catch(console.error);
  }, [
    namespaceId,
    podName,
    containerName,
    reader,
    paused,
    host,
    enableTimestamp,
    levelFilter,
  ]);

  // Previous terminated container stream
  useEffect(() => {
    if (!showPreviousLogs) {
      setPreviousLogs([]);
      return;
    }
    setPreviousLogs([]);
    fetch(
      buildPodLogsUrl({
        host,
        namespaceId,
        podName,
        containerName,
        previous: true,
      })
    )
      .then((response) => {
        if (response && response.body) {
          const prevReader = response.body
            .pipeThrough(new TextDecoderStream())
            .getReader();

          prevReader.read().then(async function process({
            done,
            value,
          }: ReadableStreamReadResult<string>): Promise<void> {
            if (done) {
              return;
            }
            if (value) {
              const { text, isErrorMessage } = extractLogChunk(value);
              setPreviousLogs((prevLogs) => {
                const latestLogs = parsePodLogs(text, {
                  enableTimestamp,
                  levelFilter,
                  type,
                  isErrorMessage,
                }).filter((line) => line !== "");
                return appendCapped(prevLogs, latestLogs);
              });
            }
            await process(await prevReader.read());
          });
        }
      })
      .catch(console.error);
  }, [
    showPreviousLogs,
    namespaceId,
    podName,
    containerName,
    host,
    enableTimestamp,
    levelFilter,
  ]);

  return { logs, previousLogs };
}
