// eslint-disable-next-line @typescript-eslint/ban-ts-comment
// @ts-nocheck
import {
  ChangeEvent,
  useCallback,
  useContext,
  useEffect,
  useState,
} from "react";
import Box from "@mui/material/Box";
import Typography from "@mui/material/Typography";
import Paper from "@mui/material/Paper";
import Select from "@mui/material/Select";
import MenuItem from "@mui/material/MenuItem";
import InputBase from "@mui/material/InputBase";
import IconButton from "@mui/material/IconButton";
import ClearIcon from "@mui/icons-material/Clear";
import PauseIcon from "@mui/icons-material/Pause";
import PlayArrowIcon from "@mui/icons-material/PlayArrow";
import ArrowUpward from "@mui/icons-material/ArrowUpward";
import ArrowDownward from "@mui/icons-material/ArrowDownward";
import LightMode from "@mui/icons-material/LightMode";
import DarkMode from "@mui/icons-material/DarkMode";
import Download from "@mui/icons-material/Download";
import WrapTextIcon from "@mui/icons-material/WrapText";
import { ClockIcon } from "@mui/x-date-pickers";
import Tooltip from "@mui/material/Tooltip";
import FormControlLabel from "@mui/material/FormControlLabel";
import Checkbox from "@mui/material/Checkbox";
import Highlighter from "react-highlight-words";
import "@stardazed/streams-polyfill";
import { ReadableStreamDefaultReadResult } from "stream/web";
import { getBaseHref } from "../../../../../../../../../../../../../utils";
import { PodLogsProps } from "../../../../../../../../../../../../../types/declarations/pods";
import { AppContextProps } from "../../../../../../../../../../../../../types/declarations/app";
import { AppContext } from "../../../../../../../../../../../../../App";

import "./style.css";

const MAX_LOGS = 1000;
const LOGS_LEVEL_ERROR = "ERROR";
const LOGS_LEVEL_DEBUG = "DEBUG";
const LOGS_LEVEL_WARN = "WARN";

const parsePodLogs = (
  value: string,
  enableTimestamp: boolean,
  levelFilter: string,
  type: string
): string[] => {
  const rawLogs = value.split("\n").filter((s) => s.length);
  return rawLogs.map((raw: string) => {
    try {
      if (type === "monoVertex") {
        const msg = raw;
        if (levelFilter !== "all" && !msg.toLowerCase().includes(levelFilter)) {
          return "";
        }
        if (!enableTimestamp) {
          // remove ISO 8601 timestamp from beginning of log if it exists
          const date = msg.match(
            /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z/
          );
          if (date) {
            return msg.substring(28);
          }
        }
        return msg;
      } else {
        const obj = JSON.parse(raw);
        let msg = ``;
        if (enableTimestamp && obj?.ts) {
          const date = obj.ts.split(/[-T:.Z]/);
          const ds =
            date[0] +
            "/" +
            date[1] +
            "/" +
            date[2] +
            " " +
            date[3] +
            ":" +
            date[4] +
            ":" +
            date[5];
          msg = `${msg}${ds} | `;
        }
        if (obj?.level) {
          msg = `${msg}${obj.level.toUpperCase()} | `;
          if (levelFilter !== "all" && obj.level !== levelFilter) {
            return "";
          }
        }
        msg = `${msg}${raw}`;
        return msg;
      }
    } catch (e) {
      return raw;
    }
  });
};

const logColor = (
  log: string,
  colorMode: string,
  enableTimestamp: boolean,
  type: string
): string => {
  const logLevelColors: { [key: string]: string } = {
    ERROR: "#B80000",
    WARN: "#FFAD00",
    DEBUG: "#81b8ef",
  };

  let startIndex = 0;
  if (enableTimestamp) {
    if (type === "monoVertex") {
      if (log?.includes(LOGS_LEVEL_ERROR) || log?.includes(LOGS_LEVEL_DEBUG))
        startIndex = 28;
      else startIndex = 29;
    } else {
      startIndex = 22;
    }
  } else {
    if (type === "monoVertex") {
      if (log?.includes(LOGS_LEVEL_WARN)) startIndex = 1;
    }
  }

  for (const level in logLevelColors) {
    if (log.startsWith(level, startIndex)) {
      return logLevelColors[level];
    }
  }

  return colorMode === "light" ? "black" : "white";
};

export function PodLogs({
  namespaceId,
  podName,
  containerName,
  type,
}: PodLogsProps) {
  const [logs, setLogs] = useState<string[]>([]);
  const [previousLogs, setPreviousLogs] = useState<string[]>([]);
  const [filteredLogs, setFilteredLogs] = useState<string[]>([]);
  const [logRequestKey, setLogRequestKey] = useState<string>("");
  const [reader, setReader] = useState<
    ReadableStreamDefaultReader | undefined
  >();
  const [search, setSearch] = useState<string>("");
  const [negateSearch, setNegateSearch] = useState<boolean>(false);
  const [wrapLines, setWrapLines] = useState<boolean>(false);
  const [paused, setPaused] = useState<boolean>(false);
  const [colorMode, setColorMode] = useState<string>("light");
  const [logsOrder, setLogsOrder] = useState<string>("desc");
  const [enableTimestamp, setEnableTimestamp] = useState<boolean>(true);
  const [levelFilter, setLevelFilter] = useState<string>("all");
  const [showPreviousLogs, setShowPreviousLogs] = useState(false);
  const { host } = useContext<AppContextProps>(AppContext);

  useEffect(() => {
    // reset logs in memory on any log source change
    setLogs([]);
    setPreviousLogs([]);
    // and start logs again if paused
    setPaused(false);
  }, [namespaceId, podName, containerName]);

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
    setLogs(["Loading logs..."]);
    fetch(
      `${host}${getBaseHref()}/api/v1/namespaces/${namespaceId}/pods/${podName}/logs?container=${containerName}&follow=true&tailLines=${MAX_LOGS}`
    )
      .then((response) => {
        if (response && response.body) {
          const r = response.body
            .pipeThrough(new TextDecoderStream())
            .getReader();
          setReader(r);
          r.read().then(function process({
            done,
            value,
          }): Promise<ReadableStreamDefaultReadResult<string>> {
            if (done) {
              return;
            }
            if (value) {
              setLogs((logs) => {
                const latestLogs = parsePodLogs(
                  value,
                  enableTimestamp,
                  levelFilter,
                  type
                )?.filter((logs) => logs !== "");
                let updated = [...logs, ...latestLogs];
                if (updated.length > MAX_LOGS) {
                  updated = updated.slice(updated.length - MAX_LOGS);
                }
                return updated;
              });
            }
            return r.read().then(process);
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

  useEffect(() => {
    if (showPreviousLogs) {
      setPreviousLogs([]);
      const url = `${host}${getBaseHref()}/api/v1/namespaces/${namespaceId}/pods/${podName}/logs?container=${containerName}&follow=true&tailLines=${MAX_LOGS}&previous=true`;
      fetch(url)
        .then((response) => {
          if (response && response.body) {
            const reader = response.body
              .pipeThrough(new TextDecoderStream())
              .getReader();

            reader.read().then(function process({ done, value }) {
              if (done) {
                return;
              }
              if (value) {
                setPreviousLogs((prevLogs) => {
                  const latestLogs = parsePodLogs(
                    value,
                    enableTimestamp,
                    levelFilter,
                    type
                  )?.filter((logs) => logs !== "");
                  let updated = [...prevLogs, ...latestLogs];
                  if (updated.length > MAX_LOGS) {
                    updated = updated.slice(updated.length - MAX_LOGS);
                  }
                  return updated;
                });
              }
              return reader.read().then(process);
            });
          }
        })
        .catch(console.error);
    } else {
      // Clear previous logs when the checkbox is unchecked
      setPreviousLogs([]);
    }
  }, [
    showPreviousLogs,
    namespaceId,
    podName,
    containerName,
    host,
    enableTimestamp,
    levelFilter,
  ]);

  useEffect(() => {
    if (!search) {
      setFilteredLogs(logs);
      return;
    }
    const searchLowerCase = search.toLowerCase();
    const filtered = logs.filter((log) =>
      negateSearch
        ? !log.toLowerCase().includes(searchLowerCase)
        : log.toLowerCase().includes(searchLowerCase)
    );
    if (!filtered.length) {
      filtered.push("No logs matching search.");
    }
    setFilteredLogs(filtered);
  }, [logs, search, negateSearch]);

  const handleSearchChange = useCallback(
    (event: ChangeEvent<HTMLInputElement>) => {
      setSearch(event.target.value);
    },
    []
  );

  const handleSearchClear = useCallback(() => {
    setSearch("");
  }, []);

  const handleNegateSearchChange = useCallback(
    (event: ChangeEvent<HTMLInputElement>) => {
      setNegateSearch(event.target.checked);
    },
    []
  );

  const handleWrapLines = useCallback(() => {
    setWrapLines((prev) => !prev);
  }, []);

  const handlePause = useCallback(() => {
    setPaused(!paused);
    if (!paused && reader) {
      reader.cancel();
      setReader(undefined);
    }
  }, [paused, reader]);

  const handleColorMode = useCallback(() => {
    setColorMode(colorMode === "light" ? "dark" : "light");
  }, [colorMode]);

  const handleOrder = useCallback(() => {
    setLogsOrder(logsOrder === "asc" ? "desc" : "asc");
  }, [logsOrder]);

  const handleLogsDownload = useCallback(() => {
    const blob = new Blob([logs.join("\n")], {
      type: "text/plain;charset=utf-8",
    });

    const url = URL.createObjectURL(blob);

    const a = document.createElement("a");
    a.href = url;
    a.download = `${podName}-${containerName}-logs.txt`;

    document.body.appendChild(a);

    a.click();

    document.body.removeChild(a);
    URL.revokeObjectURL(url);
  }, [logs]);

  const handleTimestamps = useCallback(() => {
    setEnableTimestamp((prev) => !prev);
    if (reader) {
      reader.cancel();
      setReader(undefined);
    }
  }, [reader]);

  const handleLevelChange = useCallback(
    (e) => {
      setLevelFilter(e.target.value);
      if (reader) {
        reader.cancel();
        setReader(undefined);
      }
    },
    [reader]
  );

  const logsBtnStyle = {
    height: "2.4rem",
    width: "2.4rem",
    color: "rgba(0, 0, 0, 0.54)",
  };

  return (
    <Box sx={{ height: "100%" }}>
      <Box
        sx={{
          display: "flex",
          height: "4.8rem",
        }}
      >
        <Paper
          className="PodLogs-search"
          variant="outlined"
          sx={{
            p: "0.2rem 0.4rem",
            display: "flex",
            alignItems: "center",
            width: 400,
          }}
        >
          <InputBase
            sx={{ ml: 1, flex: 1, fontSize: "1.6rem" }}
            placeholder="Search logs"
            value={search}
            onChange={handleSearchChange}
          />
          <IconButton data-testid="clear-button" onClick={handleSearchClear}>
            <ClearIcon sx={logsBtnStyle} />
          </IconButton>
        </Paper>
        <FormControlLabel
          control={
            <Checkbox
              data-testid="negate-search"
              checked={negateSearch}
              onChange={handleNegateSearchChange}
              sx={{ "& .MuiSvgIcon-root": { fontSize: 24 } }}
            />
          }
          label={
            <Typography sx={{ fontSize: "1.6rem" }}>Negate search</Typography>
          }
        />
        <Tooltip
          title={
            <div className={"icon-tooltip"}>
              {wrapLines ? "Unwrap Lines" : "Wrap Lines"}
            </div>
          }
          placement={"top"}
          arrow
        >
          <IconButton data-testid="wrap-lines-button" onClick={handleWrapLines}>
            <WrapTextIcon
              sx={{
                ...logsBtnStyle,
                background: wrapLines ? "lightgray" : "none",
                borderRadius: "1rem",
              }}
            />
          </IconButton>
        </Tooltip>
        <Tooltip
          title={
            <div className={"icon-tooltip"}>
              {paused ? "Play" : "Pause"} logs
            </div>
          }
          placement={"top"}
          arrow
        >
          <IconButton data-testid="pause-button" onClick={handlePause}>
            {paused ? (
              <PlayArrowIcon sx={logsBtnStyle} />
            ) : (
              <PauseIcon sx={logsBtnStyle} />
            )}
          </IconButton>
        </Tooltip>
        <Tooltip
          title={
            <div className={"icon-tooltip"}>
              {colorMode === "light" ? "Dark" : "Light"} mode
            </div>
          }
          placement={"top"}
          arrow
        >
          <IconButton data-testid="color-mode-button" onClick={handleColorMode}>
            {colorMode === "light" ? (
              <DarkMode sx={logsBtnStyle} />
            ) : (
              <LightMode sx={logsBtnStyle} />
            )}
          </IconButton>
        </Tooltip>
        <Tooltip
          title={
            <div className={"icon-tooltip"}>
              {logsOrder === "asc" ? "Descending" : "Ascending"} order
            </div>
          }
          placement={"top"}
          arrow
        >
          <IconButton data-testid="order-button" onClick={handleOrder}>
            {logsOrder === "asc" ? (
              <ArrowDownward sx={logsBtnStyle} />
            ) : (
              <ArrowUpward sx={logsBtnStyle} />
            )}
          </IconButton>
        </Tooltip>
        <Tooltip
          title={<div className={"icon-tooltip"}>Download logs</div>}
          placement={"top"}
          arrow
        >
          <IconButton
            data-testid="download-logs-button"
            onClick={handleLogsDownload}
          >
            <Download sx={logsBtnStyle} />
          </IconButton>
        </Tooltip>
        <Tooltip
          title={
            <div className={"icon-tooltip"}>
              {enableTimestamp ? "Remove Timestamps" : "Add Timestamps"}
            </div>
          }
          placement={"top"}
          arrow
        >
          <IconButton
            data-testid="toggle-timestamps-button"
            onClick={handleTimestamps}
            disabled={paused}
          >
            <ClockIcon
              sx={{
                height: "2.4rem",
                width: "2.4rem",
                background: enableTimestamp ? "lightgray" : "none",
                borderRadius: "1rem",
              }}
            />
          </IconButton>
        </Tooltip>
        <Select
          labelId="level-filter"
          id="level-filter"
          value={levelFilter}
          onChange={handleLevelChange}
          sx={{ width: "13rem", fontSize: "1.6rem" }}
          disabled={paused}
        >
          <MenuItem sx={{ fontSize: "1.4rem" }} value={"all"}>
            All levels
          </MenuItem>
          <MenuItem sx={{ fontSize: "1.4rem" }} value={"info"}>
            Info
          </MenuItem>
          <MenuItem sx={{ fontSize: "1.4rem" }} value={"error"}>
            Error
          </MenuItem>
          <MenuItem sx={{ fontSize: "1.4rem" }} value={"warn"}>
            Warn
          </MenuItem>
          <MenuItem sx={{ fontSize: "1.4rem" }} value={"debug"}>
            Debug
          </MenuItem>
        </Select>
      </Box>
      <FormControlLabel
        control={
          <Checkbox
            data-testid="previous-logs"
            checked={showPreviousLogs}
            onChange={(event) => setShowPreviousLogs(event.target.checked)}
            sx={{ "& .MuiSvgIcon-root": { fontSize: 24 }, height: "4.2rem" }}
          />
        }
        label={
          <Typography sx={{ fontSize: "1.6rem" }}>
            Show previous terminated container
          </Typography>
        }
      />
      <Box sx={{ height: "calc(100% - 9rem)" }}>
        <Box
          sx={{
            backgroundColor: `${
              colorMode === "light" ? "whitesmoke" : "black"
            }`,
            fontWeight: 600,
            borderRadius: "0.4rem",
            padding: "1rem 0rem",
            height: "calc(100% - 6rem)",
            overflow: "scroll",
          }}
        >
          <Box
            sx={{
              display: "flex",
              flexDirection: "column",
              height: "100%",
            }}
          >
            {logsOrder === "asc" &&
              (showPreviousLogs ? previousLogs : filteredLogs).map(
                (l: string, idx) => (
                  <Box
                    key={`${idx}-${podName}-logs`}
                    component="span"
                    sx={{
                      whiteSpace: wrapLines ? "normal" : "nowrap",
                      height: wrapLines ? "auto" : "1.6rem",
                      lineHeight: "1.6rem",
                    }}
                  >
                    <Highlighter
                      searchWords={[search]}
                      autoEscape={true}
                      textToHighlight={l}
                      style={{
                        // uncomment to add color to logs
                        // color: logColor(l, colorMode, enableTimestamp, type),
                        color: colorMode === "light" ? "black" : "white",
                        fontFamily:
                          "Consolas,Liberation Mono,Courier,monospace",
                        fontWeight: "normal",
                        background:
                          colorMode === "light" ? "#E6E6E6" : "#333333",
                        fontSize: "1.4rem",
                        textWrap: wrapLines ? "wrap" : "nowrap",
                        border: "1px solid #cacaca",
                      }}
                      highlightStyle={{
                        color: `${colorMode === "light" ? "white" : "black"}`,
                        backgroundColor: `${
                          colorMode === "light" ? "black" : "white"
                        }`,
                      }}
                    />
                  </Box>
                )
              )}
            {logsOrder === "desc" &&
              (showPreviousLogs ? previousLogs : filteredLogs)
                .slice()
                .reverse()
                .map((l: string, idx) => (
                  <Box
                    key={`${idx}-${podName}-logs`}
                    component="span"
                    sx={{
                      whiteSpace: wrapLines ? "normal" : "nowrap",
                      height: wrapLines ? "auto" : "1.6rem",
                      lineHeight: "1.6rem",
                    }}
                  >
                    <Highlighter
                      searchWords={[search]}
                      autoEscape={true}
                      textToHighlight={l}
                      style={{
                        // uncomment to add color to logs
                        // color: logColor(l, colorMode, enableTimestamp, type),
                        color: colorMode === "light" ? "black" : "white",
                        fontFamily:
                          "Consolas,Liberation Mono,Courier,monospace",
                        fontWeight: "normal",
                        background:
                          colorMode === "light" ? "#E6E6E6" : "#333333",
                        fontSize: "1.4rem",
                        textWrap: wrapLines ? "wrap" : "nowrap",
                        border: "1px solid #cacaca",
                      }}
                      highlightStyle={{
                        color: `${colorMode === "light" ? "white" : "black"}`,
                        backgroundColor: `${
                          colorMode === "light" ? "black" : "white"
                        }`,
                      }}
                    />
                  </Box>
                ))}
          </Box>
        </Box>
      </Box>
    </Box>
  );
}
