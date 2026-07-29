import { useEffect, useRef, useState } from "react";
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

function cancelReader(
  reader: ReadableStreamDefaultReader<string> | undefined
): void {
  if (!reader) {
    return;
  }
  void reader.cancel().catch(() => {
    // Reader may already be closed/cancelled.
  });
}

function isAbortError(err: unknown): boolean {
  return (
    (err instanceof DOMException && err.name === "AbortError") ||
    (err instanceof Error && err.name === "AbortError")
  );
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

  const liveAbortRef = useRef<AbortController | undefined>();
  const prevAbortRef = useRef<AbortController | undefined>();
  const liveReaderRef = useRef<
    ReadableStreamDefaultReader<string> | undefined
  >();
  const prevReaderRef = useRef<
    ReadableStreamDefaultReader<string> | undefined
  >();
  const liveGenerationRef = useRef(0);
  const prevGenerationRef = useRef(0);

  // Reset buffers when the log source changes; also unpause so streaming restarts.
  useEffect(() => {
    setLogs([]);
    setPreviousLogs([]);
  }, [namespaceId, podName, containerName]);

  // Live follow stream. Cleanup aborts in-flight fetch/reader on pause, unmount,
  // identity change, and parse-option restart.
  useEffect(() => {
    if (paused) {
      return;
    }

    const controller = new AbortController();
    liveAbortRef.current = controller;
    const generation = ++liveGenerationRef.current;

    setLogs([LOADING_LOGS]);

    const run = async () => {
      try {
        const response = await fetch(
          buildPodLogsUrl({
            host,
            namespaceId,
            podName,
            containerName,
          }),
          { signal: controller.signal }
        );
        if (
          controller.signal.aborted ||
          generation !== liveGenerationRef.current
        ) {
          return;
        }
        if (!response?.body) {
          return;
        }
        const reader = response.body
          .pipeThrough(new TextDecoderStream())
          .getReader();
        liveReaderRef.current = reader;

        while (
          !controller.signal.aborted &&
          generation === liveGenerationRef.current
        ) {
          const { done, value } = await reader.read();
          if (done) {
            break;
          }
          if (
            controller.signal.aborted ||
            generation !== liveGenerationRef.current
          ) {
            break;
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
        }
      } catch (err) {
        if (!isAbortError(err)) {
          console.error(err);
        }
      }
    };

    void run();

    return () => {
      controller.abort();
      cancelReader(liveReaderRef.current);
      liveReaderRef.current = undefined;
      if (liveAbortRef.current === controller) {
        liveAbortRef.current = undefined;
      }
      if (generation === liveGenerationRef.current) {
        liveGenerationRef.current++;
      }
    };
  }, [
    namespaceId,
    podName,
    containerName,
    paused,
    host,
    enableTimestamp,
    levelFilter,
    type,
  ]);

  // Previous terminated container stream
  useEffect(() => {
    if (!showPreviousLogs) {
      setPreviousLogs([]);
      return;
    }

    const controller = new AbortController();
    prevAbortRef.current = controller;
    const generation = ++prevGenerationRef.current;

    setPreviousLogs([]);

    const run = async () => {
      try {
        const response = await fetch(
          buildPodLogsUrl({
            host,
            namespaceId,
            podName,
            containerName,
            previous: true,
          }),
          { signal: controller.signal }
        );
        if (
          controller.signal.aborted ||
          generation !== prevGenerationRef.current
        ) {
          return;
        }
        if (!response?.body) {
          return;
        }
        const prevReader = response.body
          .pipeThrough(new TextDecoderStream())
          .getReader();
        prevReaderRef.current = prevReader;

        while (
          !controller.signal.aborted &&
          generation === prevGenerationRef.current
        ) {
          const { done, value } = await prevReader.read();
          if (done) {
            break;
          }
          if (
            controller.signal.aborted ||
            generation !== prevGenerationRef.current
          ) {
            break;
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
        }
      } catch (err) {
        if (!isAbortError(err)) {
          console.error(err);
        }
      }
    };

    void run();

    return () => {
      controller.abort();
      cancelReader(prevReaderRef.current);
      prevReaderRef.current = undefined;
      if (prevAbortRef.current === controller) {
        prevAbortRef.current = undefined;
      }
      if (generation === prevGenerationRef.current) {
        prevGenerationRef.current++;
      }
    };
  }, [
    showPreviousLogs,
    namespaceId,
    podName,
    containerName,
    host,
    enableTimestamp,
    levelFilter,
    type,
  ]);

  return { logs, previousLogs };
}
