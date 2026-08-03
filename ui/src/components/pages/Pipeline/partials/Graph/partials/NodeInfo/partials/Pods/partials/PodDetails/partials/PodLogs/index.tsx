// eslint-disable-next-line @typescript-eslint/ban-ts-comment
// @ts-nocheck
import {
  ChangeEvent,
  useCallback,
  useContext,
  useEffect,
  useMemo,
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
import { PodLogsProps } from "../../../../../../../../../../../../../types/declarations/pods";
import { AppContextProps } from "../../../../../../../../../../../../../types/declarations/app";
import { AppContext } from "../../../../../../../../../../../../../App";
import { filterLogs } from "./filterLogs";
import { usePodLogStream } from "./usePodLogStream";

import "./style.css";

export function PodLogs({
  namespaceId,
  podName,
  containerName,
  type,
}: PodLogsProps) {
  const [search, setSearch] = useState<string>("");
  const [negateSearch, setNegateSearch] = useState<boolean>(false);
  const [wrapLines, setWrapLines] = useState<boolean>(false);
  const [paused, setPaused] = useState<boolean>(false);
  const [colorMode, setColorMode] = useState<string>("light");
  const [logsOrder, setLogsOrder] = useState<string>("desc");
  const [enableTimestamp, setEnableTimestamp] = useState<boolean>(false);
  const [levelFilter, setLevelFilter] = useState<string>("all");
  const [showPreviousLogs, setShowPreviousLogs] = useState(false);
  const { host } = useContext<AppContextProps>(AppContext);

  // Restart streaming if the source changes while paused
  useEffect(() => {
    setPaused(false);
  }, [namespaceId, podName, containerName]);

  const { logs, previousLogs } = usePodLogStream({
    namespaceId,
    podName,
    containerName,
    type,
    host,
    paused,
    enableTimestamp,
    levelFilter,
    showPreviousLogs,
  });

  const filteredLogs = useMemo(() => {
    const source = showPreviousLogs ? previousLogs : logs;
    return filterLogs(source, search, negateSearch);
  }, [showPreviousLogs, previousLogs, logs, search, negateSearch]);

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
    setPaused((prev) => !prev);
  }, []);

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
  }, [logs, podName, containerName]);

  const handleTimestamps = useCallback(() => {
    setEnableTimestamp((prev) => !prev);
  }, []);

  const handleLevelChange = useCallback((e) => {
    setLevelFilter(e.target.value);
  }, []);

  const logsBtnStyle = {
    height: "2.4rem",
    width: "2.4rem",
    color: "var(--text-secondary)",
  };

  return (
    <Box sx={{ height: "100%" }}>
      <Box
        sx={{
          display: "flex",
          height: "4.8rem",
          overflow: "scroll",
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
          <span>
            <IconButton
              data-testid="wrap-lines-button"
              onClick={handleWrapLines}
            >
              <WrapTextIcon
                sx={{
                  ...logsBtnStyle,
                  background: wrapLines ? "lightgray" : "none",
                  borderRadius: "1rem",
                }}
              />
            </IconButton>
          </span>
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
          <span>
            <IconButton data-testid="pause-button" onClick={handlePause}>
              {paused ? (
                <PlayArrowIcon sx={logsBtnStyle} />
              ) : (
                <PauseIcon sx={logsBtnStyle} />
              )}
            </IconButton>
          </span>
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
          <span>
            <IconButton
              data-testid="color-mode-button"
              onClick={handleColorMode}
            >
              {colorMode === "light" ? (
                <DarkMode sx={logsBtnStyle} />
              ) : (
                <LightMode sx={logsBtnStyle} />
              )}
            </IconButton>
          </span>
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
          <span>
            <IconButton data-testid="order-button" onClick={handleOrder}>
              {logsOrder === "asc" ? (
                <ArrowDownward sx={logsBtnStyle} />
              ) : (
                <ArrowUpward sx={logsBtnStyle} />
              )}
            </IconButton>
          </span>
        </Tooltip>
        <Tooltip
          title={<div className={"icon-tooltip"}>Download logs</div>}
          placement={"top"}
          arrow
        >
          <span>
            <IconButton
              data-testid="download-logs-button"
              onClick={handleLogsDownload}
            >
              <Download sx={logsBtnStyle} />
            </IconButton>
          </span>
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
          <span>
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
          </span>
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
              filteredLogs.map((l: string, idx) => (
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
                      color: colorMode === "light" ? "black" : "white",
                      fontFamily: "Consolas,Liberation Mono,Courier,monospace",
                      fontWeight: "normal",
                      background: colorMode === "light" ? "#E6E6E6" : "#333333",
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
            {logsOrder === "desc" &&
              filteredLogs
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
