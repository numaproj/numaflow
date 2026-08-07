import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import "@stardazed/streams-polyfill";
import { getBaseHref } from "../../../../../../../../../../../../../utils";
import {
  LOADING_LOGS,
  LOG_HISTORY_BATCH_SIZES,
  MAX_LOGS,
  MAX_TOTAL_LOGS,
} from "./constants";
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

/**
 * Merge a larger historical window into the current chronological buffer.
 * `fetched` is the last N lines from the API; any prefix before the overlap
 * with `existing` is prepended as older history.
 */
export function mergeOlderLogs(
  existing: string[],
  fetched: string[]
): { logs: string[]; prependedCount: number } {
  const base = existing.filter((line) => line !== LOADING_LOGS);
  if (fetched.length === 0) {
    return { logs: base, prependedCount: 0 };
  }
  if (base.length === 0) {
    return { logs: fetched, prependedCount: fetched.length };
  }

  const first = base[0];
  let matchIndex = -1;
  for (let i = 0; i < fetched.length; i++) {
    if (fetched[i] !== first) {
      continue;
    }
    const overlap = Math.min(base.length, fetched.length - i, 20);
    let ok = true;
    for (let j = 0; j < overlap; j++) {
      if (fetched[i + j] !== base[j]) {
        ok = false;
        break;
      }
    }
    if (ok) {
      matchIndex = i;
      break;
    }
  }

  if (matchIndex <= 0) {
    return { logs: base, prependedCount: 0 };
  }

  const older = fetched.slice(0, matchIndex);
  return { logs: [...older, ...base], prependedCount: older.length };
}

export function countLoadedLogs(lines: string[]): number {
  return lines.filter((line) => line !== LOADING_LOGS).length;
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

function normalizeBatchSize(batchSize: number, remainingCapacity: number): number {
  const allowed = LOG_HISTORY_BATCH_SIZES as readonly number[];
  const size = allowed.includes(batchSize)
    ? batchSize
    : LOG_HISTORY_BATCH_SIZES[0];
  return Math.min(size, remainingCapacity);
}

async function readStreamToText(
  reader: ReadableStreamDefaultReader<string>
): Promise<string> {
  let text = "";
  while (true) {
    const { done, value } = await reader.read();
    if (done) {
      break;
    }
    if (value) {
      text += value;
    }
  }
  return text;
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

export type LoadOlderLogsResult = {
  prependedCount: number;
};

export type UsePodLogStreamResult = {
  logs: string[];
  previousLogs: string[];
  loadOlderLogs: (batchSize: number) => Promise<LoadOlderLogsResult>;
  isLoadingOlder: boolean;
  hasMoreOlder: boolean;
  loadedCount: number;
  remainingCapacity: number;
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
}: UsePodLogStreamParams): UsePodLogStreamResult {
  const [logs, setLogs] = useState<string[]>([]);
  const [previousLogs, setPreviousLogs] = useState<string[]>([]);
  const [isLoadingOlder, setIsLoadingOlder] = useState(false);
  const [liveTailLines, setLiveTailLines] = useState(MAX_LOGS);
  const [prevTailLines, setPrevTailLines] = useState(MAX_LOGS);
  const [liveHasMoreOlder, setLiveHasMoreOlder] = useState(true);
  const [prevHasMoreOlder, setPrevHasMoreOlder] = useState(true);

  const liveAbortRef = useRef<AbortController | undefined>();
  const prevAbortRef = useRef<AbortController | undefined>();
  const historyAbortRef = useRef<AbortController | undefined>();
  const liveReaderRef = useRef<
    ReadableStreamDefaultReader<string> | undefined
  >();
  const prevReaderRef = useRef<
    ReadableStreamDefaultReader<string> | undefined
  >();
  const liveGenerationRef = useRef(0);
  const prevGenerationRef = useRef(0);
  const liveTailLinesRef = useRef(MAX_LOGS);
  const prevTailLinesRef = useRef(MAX_LOGS);
  const liveHasMoreOlderRef = useRef(true);
  const prevHasMoreOlderRef = useRef(true);
  const isLoadingOlderRef = useRef(false);
  const logsRef = useRef(logs);
  const previousLogsRef = useRef(previousLogs);

  logsRef.current = logs;
  previousLogsRef.current = previousLogs;

  const abortHistoryFetch = useCallback(() => {
    historyAbortRef.current?.abort();
    historyAbortRef.current = undefined;
    isLoadingOlderRef.current = false;
    setIsLoadingOlder(false);
  }, []);

  const resetLiveScrollback = useCallback(() => {
    liveTailLinesRef.current = MAX_LOGS;
    setLiveTailLines(MAX_LOGS);
    liveHasMoreOlderRef.current = true;
    setLiveHasMoreOlder(true);
    abortHistoryFetch();
  }, [abortHistoryFetch]);

  const resetPrevScrollback = useCallback(() => {
    prevTailLinesRef.current = MAX_LOGS;
    setPrevTailLines(MAX_LOGS);
    prevHasMoreOlderRef.current = true;
    setPrevHasMoreOlder(true);
    abortHistoryFetch();
  }, [abortHistoryFetch]);

  // Reset buffers when the log source changes.
  useEffect(() => {
    setLogs([]);
    setPreviousLogs([]);
    resetLiveScrollback();
    resetPrevScrollback();
  }, [namespaceId, podName, containerName, resetLiveScrollback, resetPrevScrollback]);

  // Live follow stream. Cleanup aborts in-flight fetch/reader on pause, unmount,
  // identity change, and parse-option restart.
  useEffect(() => {
    if (paused) {
      return;
    }

    const controller = new AbortController();
    liveAbortRef.current = controller;
    const generation = ++liveGenerationRef.current;

    resetLiveScrollback();
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
              return appendCapped(
                current,
                latestLogs,
                liveTailLinesRef.current
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
    host,
    enableTimestamp,
    levelFilter,
    type,
    resetLiveScrollback,
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

    resetPrevScrollback();
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
              return appendCapped(
                prevLogs,
                latestLogs,
                prevTailLinesRef.current
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
    resetPrevScrollback,
  ]);

  const currentTailLines = showPreviousLogs ? prevTailLines : liveTailLines;
  const hasMoreOlder = showPreviousLogs ? prevHasMoreOlder : liveHasMoreOlder;
  const remainingCapacity = Math.max(0, MAX_TOTAL_LOGS - currentTailLines);

  const loadedCount = useMemo(
    () => countLoadedLogs(showPreviousLogs ? previousLogs : logs),
    [showPreviousLogs, previousLogs, logs]
  );

  const loadOlderLogs = useCallback(
    async (batchSize: number): Promise<LoadOlderLogsResult> => {
      const tailRef = showPreviousLogs ? prevTailLinesRef : liveTailLinesRef;
      const hasMoreRef = showPreviousLogs
        ? prevHasMoreOlderRef
        : liveHasMoreOlderRef;
      const remaining = Math.max(0, MAX_TOTAL_LOGS - tailRef.current);
      if (!hasMoreRef.current || isLoadingOlderRef.current || remaining === 0) {
        return { prependedCount: 0 };
      }

      const normalized = normalizeBatchSize(batchSize, remaining);
      if (normalized <= 0) {
        return { prependedCount: 0 };
      }

      const nextTail = Math.min(tailRef.current + normalized, MAX_TOTAL_LOGS);

      historyAbortRef.current?.abort();
      const controller = new AbortController();
      historyAbortRef.current = controller;
      isLoadingOlderRef.current = true;
      setIsLoadingOlder(true);

      try {
        const response = await fetch(
          buildPodLogsUrl({
            host,
            namespaceId,
            podName,
            containerName,
            previous: showPreviousLogs,
            follow: false,
            tailLines: nextTail,
          }),
          { signal: controller.signal }
        );
        if (controller.signal.aborted || !response?.body) {
          return { prependedCount: 0 };
        }

        const reader = response.body
          .pipeThrough(new TextDecoderStream())
          .getReader();
        const text = await readStreamToText(reader);
        if (controller.signal.aborted) {
          return { prependedCount: 0 };
        }

        const { text: chunkText, isErrorMessage } = extractLogChunk(text);
        const fetched = parsePodLogs(chunkText, {
          enableTimestamp,
          levelFilter,
          type,
          isErrorMessage,
        }).filter((line) => line !== "");

        const existing = showPreviousLogs
          ? previousLogsRef.current
          : logsRef.current;
        const { logs: merged, prependedCount } = mergeOlderLogs(
          existing,
          fetched
        );

        if (prependedCount === 0) {
          hasMoreRef.current = false;
          if (showPreviousLogs) {
            setPrevHasMoreOlder(false);
          } else {
            setLiveHasMoreOlder(false);
          }
          return { prependedCount: 0 };
        }

        tailRef.current = nextTail;
        if (showPreviousLogs) {
          setPrevTailLines(nextTail);
          setPreviousLogs(merged);
        } else {
          setLiveTailLines(nextTail);
          setLogs(merged);
        }
        return { prependedCount };
      } catch (err) {
        if (!isAbortError(err)) {
          console.error(err);
        }
        return { prependedCount: 0 };
      } finally {
        if (historyAbortRef.current === controller) {
          historyAbortRef.current = undefined;
        }
        isLoadingOlderRef.current = false;
        setIsLoadingOlder(false);
      }
    },
    [
      host,
      namespaceId,
      podName,
      containerName,
      showPreviousLogs,
      enableTimestamp,
      levelFilter,
      type,
    ]
  );

  return {
    logs,
    previousLogs,
    loadOlderLogs,
    isLoadingOlder,
    hasMoreOlder,
    loadedCount,
    remainingCapacity,
  };
}
