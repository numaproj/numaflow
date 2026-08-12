import { useEffect, useMemo, useRef, useState } from "react";
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
  follow?: boolean;
};

export function buildPodLogsUrl({
  host,
  namespaceId,
  podName,
  containerName,
  previous = false,
  tailLines = MAX_LOGS,
  follow = true,
}: BuildPodLogsUrlParams): string {
  const base = `${host}${getBaseHref()}/api/v1/namespaces/${namespaceId}/pods/${podName}/logs?container=${containerName}&follow=${follow}&tailLines=${tailLines}`;
  return previous ? `${base}&previous=true` : base;
}

export function appendCapped(
  existing: string[],
  incoming: string[],
  max: number = MAX_LOGS
): string[] {
  const base =
    incoming.length > 0
      ? existing.filter((line) => line !== LOADING_LOGS)
      : existing;
  let updated = [...base, ...incoming];
  if (updated.length > max) {
    updated = updated.slice(updated.length - max);
  }
  return updated;
}

export function countLoadedLogs(lines: string[]): number {
  return lines.filter((line) => line !== LOADING_LOGS).length;
}

function clearLoadingPlaceholder(lines: string[]): string[] {
  return lines.filter((line) => line !== LOADING_LOGS);
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
  tailLines: number;
};

export type UsePodLogStreamResult = {
  logs: string[];
  previousLogs: string[];
  loadedCount: number;
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
  tailLines,
}: UsePodLogStreamParams): UsePodLogStreamResult {
  const [logs, setLogs] = useState<string[]>([]);
  const [previousLogs, setPreviousLogs] = useState<string[]>([]);

  const liveAbortRef = useRef<AbortController | undefined>();
  const snapshotAbortRef = useRef<AbortController | undefined>();
  const prevAbortRef = useRef<AbortController | undefined>();
  const liveReaderRef = useRef<
    ReadableStreamDefaultReader<string> | undefined
  >();
  const snapshotReaderRef = useRef<
    ReadableStreamDefaultReader<string> | undefined
  >();
  const prevReaderRef = useRef<
    ReadableStreamDefaultReader<string> | undefined
  >();
  const liveGenerationRef = useRef(0);
  const snapshotGenerationRef = useRef(0);
  const prevGenerationRef = useRef(0);
  const skipPausedSnapshotRef = useRef(true);
  // Tail size while last live; used so auto-pause+size-change still snapshots.
  const lastLiveTailLinesRef = useRef(tailLines);
  const tailLinesRef = useRef(tailLines);
  tailLinesRef.current = tailLines;

  // Reset buffers when the log source changes.
  useEffect(() => {
    setLogs([]);
    setPreviousLogs([]);
  }, [namespaceId, podName, containerName]);

  // Live follow stream. Restarts when tailLines or parse options change.
  // Skip while viewing previous/terminated logs so N-lines only refetches previous.
  useEffect(() => {
    if (paused || showPreviousLogs) {
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
            tailLines,
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
              return appendCapped(
                current,
                latestLogs,
                tailLinesRef.current
              );
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
    showPreviousLogs,
    host,
    enableTimestamp,
    levelFilter,
    type,
    tailLines,
  ]);

  // While paused: freeze on enter; refetch absolute window (follow=false) when
  // tailLines changes (including auto-pause + size change). Abort on play so a
  // stale snapshot cannot race live. Skip while viewing terminated logs.
  useEffect(() => {
    if (!paused || showPreviousLogs) {
      if (!paused) {
        skipPausedSnapshotRef.current = true;
        lastLiveTailLinesRef.current = tailLines;
      }
      return;
    }
    if (skipPausedSnapshotRef.current) {
      skipPausedSnapshotRef.current = false;
      // Pure pause: keep the frozen live buffer. Auto-pause with a new window
      // size falls through and fetches follow=false.
      if (tailLines === lastLiveTailLinesRef.current) {
        return;
      }
    }

    const controller = new AbortController();
    snapshotAbortRef.current = controller;
    const generation = ++snapshotGenerationRef.current;

    setLogs([LOADING_LOGS]);

    const run = async () => {
      try {
        const response = await fetch(
          buildPodLogsUrl({
            host,
            namespaceId,
            podName,
            containerName,
            follow: false,
            tailLines,
          }),
          { signal: controller.signal }
        );
        if (
          controller.signal.aborted ||
          generation !== snapshotGenerationRef.current
        ) {
          return;
        }
        if (!response?.ok || !response.body) {
          setLogs((current) => clearLoadingPlaceholder(current));
          return;
        }
        const reader = response.body
          .pipeThrough(new TextDecoderStream())
          .getReader();
        snapshotReaderRef.current = reader;

        while (
          !controller.signal.aborted &&
          generation === snapshotGenerationRef.current
        ) {
          const { done, value } = await reader.read();
          if (done) {
            break;
          }
          if (
            controller.signal.aborted ||
            generation !== snapshotGenerationRef.current
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
              return appendCapped(
                current,
                latestLogs,
                tailLinesRef.current
              );
            });
          }
        }

        if (
          !controller.signal.aborted &&
          generation === snapshotGenerationRef.current
        ) {
          setLogs((current) => {
            const next = clearLoadingPlaceholder(current);
            return next.length ? next : [];
          });
        }
      } catch (err) {
        if (!isAbortError(err)) {
          console.error(err);
          if (generation === snapshotGenerationRef.current) {
            setLogs((current) => clearLoadingPlaceholder(current));
          }
        }
      }
    };

    void run();

    return () => {
      controller.abort();
      cancelReader(snapshotReaderRef.current);
      snapshotReaderRef.current = undefined;
      if (snapshotAbortRef.current === controller) {
        snapshotAbortRef.current = undefined;
      }
      if (generation === snapshotGenerationRef.current) {
        snapshotGenerationRef.current++;
      }
    };
  }, [
    paused,
    showPreviousLogs,
    tailLines,
    namespaceId,
    podName,
    containerName,
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
            follow: false,
            tailLines,
          }),
          { signal: controller.signal }
        );
        if (
          controller.signal.aborted ||
          generation !== prevGenerationRef.current
        ) {
          return;
        }
        if (!response?.ok || !response.body) {
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
              return appendCapped(
                prevLogs,
                latestLogs,
                tailLinesRef.current
              );
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
    tailLines,
  ]);

  const loadedCount = useMemo(
    () => countLoadedLogs(showPreviousLogs ? previousLogs : logs),
    [showPreviousLogs, previousLogs, logs]
  );

  return {
    logs,
    previousLogs,
    loadedCount,
  };
}
