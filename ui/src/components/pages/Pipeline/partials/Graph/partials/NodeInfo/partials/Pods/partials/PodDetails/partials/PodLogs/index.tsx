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
import Paper from "@mui/material/Paper";
import InputBase from "@mui/material/InputBase";
import IconButton from "@mui/material/IconButton";
import ClearIcon from "@mui/icons-material/Clear";
import PauseIcon from "@mui/icons-material/Pause";
import PlayArrowIcon from "@mui/icons-material/PlayArrow";
import ArrowUpward from "@mui/icons-material/ArrowUpward";
import ArrowDownward from "@mui/icons-material/ArrowDownward";
import LightMode from "@mui/icons-material/LightMode";
import DarkMode from "@mui/icons-material/DarkMode";
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

const parsePodLogs = (value: string): string[] => {
  const rawLogs = value.split("\n").filter((s) => s.length);
  return rawLogs.map((raw: string) => {
    try {
      const obj = JSON.parse(raw);
      let msg = ``;
      if (obj?.ts) {
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
      }
      msg = `${msg}${raw}`;
      return msg;
    } catch (e) {
      return raw;
    }
  });
};

const logColor = (log: string, colorMode: string): string => {
  if (log.startsWith("ERROR", 22)) {
    return "#B80000";
  }
  if (log.startsWith("WARN", 22)) {
    return "#FFAD00";
  }
  if (log.startsWith("DEBUG", 22)) {
    return "#81b8ef";
  }
  return colorMode === "light" ? "black" : "white";
};

export function PodLogs({ namespaceId, podName, containerName }: PodLogsProps) {
  const [logs, setLogs] = useState<string[]>([]);
  const [filteredLogs, setFilteredLogs] = useState<string[]>([]);
  const [logRequestKey, setLogRequestKey] = useState<string>("");
  const [reader, setReader] = useState<
    ReadableStreamDefaultReader | undefined
  >();
  const [search, setSearch] = useState<string>("");
  const [negateSearch, setNegateSearch] = useState<boolean>(false);
  const [paused, setPaused] = useState<boolean>(false);
  const [colorMode, setColorMode] = useState<string>("light");
  const [logsOrder, setLogsOrder] = useState<string>("desc");
  const { host } = useContext<AppContextProps>(AppContext);

  useEffect(() => {
    // reset logs in memory on any log source change
    setLogs([]);
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
                const latestLogs = parsePodLogs(value);
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
  }, [namespaceId, podName, containerName, reader, paused, host]);

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

  return (
    <Box>
      <Box sx={{ display: "flex", flexDirection: "row" }}>
        <Paper
          className="PodLogs-search"
          variant="outlined"
          sx={{
            p: "0.125rem 0.25rem",
            display: "flex",
            alignItems: "center",
            width: 400,
          }}
        >
          <InputBase
            sx={{ ml: 1, flex: 1 }}
            placeholder="Search logs"
            value={search}
            onChange={handleSearchChange}
          />
          <IconButton data-testid="clear-button" onClick={handleSearchClear}>
            <ClearIcon />
          </IconButton>
        </Paper>
        <FormControlLabel
          control={
            <Checkbox
              data-testid="negate-search"
              checked={negateSearch}
              onChange={handleNegateSearchChange}
            />
          }
          label="Negate search"
        />
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
            {paused ? <PlayArrowIcon /> : <PauseIcon />}
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
            {colorMode === "light" ? <DarkMode /> : <LightMode />}
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
            {logsOrder === "asc" ? <ArrowDownward /> : <ArrowUpward />}
          </IconButton>
        </Tooltip>
      </Box>
      <Box
        sx={{
          backgroundColor: `${colorMode === "light" ? "whitesmoke" : "black"}`,
          fontWeight: 600,
          overflow: "scroll",
          display: "flex",
          flexDirection: "column",
          borderRadius: "0.25rem",
          padding: "0.625rem 0.3125rem",
          marginTop: "1.5rem",
          height: "25rem",
        }}
      >
        {logsOrder === "asc" &&
          filteredLogs.map((l: string, idx) => (
            <Box
              key={`${idx}-${podName}-logs`}
              component="span"
              sx={{
                whiteSpace: "nowrap",
                paddingTop: "0.5rem",
              }}
            >
              <Highlighter
                searchWords={[search]}
                autoEscape={true}
                textToHighlight={l}
                style={{ color: logColor(l, colorMode) }}
                highlightStyle={{
                  color: `${colorMode === "light" ? "white" : "black"}`,
                  backgroundColor: `${
                    colorMode === "light" ? "black" : "white"
                  }`,
                }}
              />
            </Box>
          ))}
        {logsOrder === "desc" &&
          filteredLogs
            .slice()
            .reverse()
            .map((l: string, idx) => (
              <Box
                key={`${idx}-${podName}-logs`}
                component="span"
                sx={{
                  whiteSpace: "nowrap",
                  paddingTop: "0.5rem",
                }}
              >
                <Highlighter
                  searchWords={[search]}
                  autoEscape={true}
                  textToHighlight={l}
                  style={{ color: logColor(l, colorMode) }}
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
  );
}
