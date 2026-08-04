export type ParsePodLogsOptions = {
  enableTimestamp: boolean;
  levelFilter: string;
  type: string;
  isErrorMessage: boolean;
};

export type LogChunk = {
  text: string;
  isErrorMessage: boolean;
};

/**
 * Detect API error payloads ({ errMsg }) vs raw log chunks from the stream.
 */
export function extractLogChunk(value: string): LogChunk {
  try {
    const jsonResponse = JSON.parse(value);
    if (jsonResponse?.errMsg) {
      return { text: jsonResponse.errMsg, isErrorMessage: true };
    }
  } catch {
    // not a JSON error payload
  }
  return { text: value, isErrorMessage: false };
}

export function parsePodLogs(
  value: string,
  opts: ParsePodLogsOptions
): string[] {
  const { enableTimestamp, levelFilter, type, isErrorMessage } = opts;
  const rawLogs = value.split("\n").filter((s) => s.trim().length);
  return rawLogs.map((raw: string) => {
    // 30 characters for RFC 3339 timestamp
    const timestamp =
      raw.length >= 31 && !isErrorMessage ? raw.substring(0, 30) : "";
    const logWithoutTimestamp =
      raw.length >= 31 && !isErrorMessage ? raw.substring(31) : raw;

    let msg = enableTimestamp ? `${timestamp} ` : "";

    if (type === "monoVertex") {
      if (
        levelFilter !== "all" &&
        !logWithoutTimestamp.includes(levelFilter.toUpperCase())
      )
        return "";

      return `${msg}${logWithoutTimestamp}`;
    }

    let obj: unknown;
    try {
      obj = JSON.parse(logWithoutTimestamp);
    } catch {
      obj = logWithoutTimestamp;
    }
    // println log, it is not an object
    if (obj === logWithoutTimestamp) {
      if (levelFilter !== "all" && !obj.toLowerCase().includes(levelFilter))
        return "";
    } else if (
      obj &&
      typeof obj === "object" &&
      "level" in obj &&
      typeof (obj as { level: unknown }).level === "string"
    ) {
      const level = (obj as { level: string }).level;
      // logger log
      msg += `${level.toUpperCase()} `;
      if (levelFilter !== "all" && level !== levelFilter) return "";
    }
    return `${msg}${logWithoutTimestamp}`;
  });
}
