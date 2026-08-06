import { extractLogChunk, parsePodLogs } from "./parsePodLogs";

describe("extractLogChunk", () => {
  it("returns errMsg when the chunk is an API error payload", () => {
    expect(extractLogChunk(JSON.stringify({ errMsg: "boom" }))).toEqual({
      text: "boom",
      isErrorMessage: true,
    });
  });

  it("returns the raw text for normal log chunks", () => {
    const line = "2023-09-04T11:50:19.712416709Z hello";
    expect(extractLogChunk(line)).toEqual({
      text: line,
      isErrorMessage: false,
    });
  });
});

describe("parsePodLogs", () => {
  const ts = "2023-09-04T11:50:19.712416709Z";

  it("parses JSON logger lines and prefixes level", () => {
    const body = JSON.stringify({
      level: "info",
      msg: "hello",
    });
    const result = parsePodLogs(`${ts} ${body}`, {
      enableTimestamp: false,
      levelFilter: "all",
      type: "pipeline",
      isErrorMessage: false,
    });
    expect(result).toEqual([`INFO ${body}`]);
  });

  it("filters JSON logs by level", () => {
    const info = JSON.stringify({ level: "info", msg: "ok" });
    const error = JSON.stringify({ level: "error", msg: "bad" });
    const result = parsePodLogs(`${ts} ${info}\n${ts} ${error}`, {
      enableTimestamp: false,
      levelFilter: "error",
      type: "pipeline",
      isErrorMessage: false,
    }).filter((l) => l !== "");
    expect(result).toEqual([`ERROR ${error}`]);
  });

  it("includes timestamps when enabled", () => {
    const body = JSON.stringify({ level: "warn", msg: "careful" });
    const result = parsePodLogs(`${ts} ${body}`, {
      enableTimestamp: true,
      levelFilter: "all",
      type: "pipeline",
      isErrorMessage: false,
    });
    expect(result[0]).toBe(`${ts} WARN ${body}`);
  });

  it("skips timestamp slicing for API error messages", () => {
    const err = "Failed to get pod logs: not found";
    const result = parsePodLogs(err, {
      enableTimestamp: false,
      levelFilter: "all",
      type: "pipeline",
      isErrorMessage: true,
    });
    expect(result).toEqual([err]);
  });

  it("filters monoVertex plain-text logs by uppercase level token", () => {
    const result = parsePodLogs(
      `${ts} something INFO happened\n${ts} something ERROR happened`,
      {
        enableTimestamp: false,
        levelFilter: "error",
        type: "monoVertex",
        isErrorMessage: false,
      }
    ).filter((l) => l !== "");
    expect(result).toEqual(["something ERROR happened"]);
  });

  it("filters plain (non-JSON) pipeline logs with substring match", () => {
    const result = parsePodLogs(
      `${ts} this is an error line\n${ts} this is fine`,
      {
        enableTimestamp: false,
        levelFilter: "error",
        type: "pipeline",
        isErrorMessage: false,
      }
    ).filter((l) => l !== "");
    expect(result).toEqual(["this is an error line"]);
  });
});
