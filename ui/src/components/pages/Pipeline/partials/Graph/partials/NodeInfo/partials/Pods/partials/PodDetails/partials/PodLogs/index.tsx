// eslint-disable-next-line @typescript-eslint/ban-ts-comment
// @ts-nocheck
import {
  ChangeEvent,
  MouseEvent,
  ReactNode,
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useRef,
  useState,
} from "react";
import Box from "@mui/material/Box";
import Typography from "@mui/material/Typography";
import Select from "@mui/material/Select";
import MenuItem from "@mui/material/MenuItem";
import Menu from "@mui/material/Menu";
import Button from "@mui/material/Button";
import ButtonGroup from "@mui/material/ButtonGroup";
import InputBase from "@mui/material/InputBase";
import IconButton from "@mui/material/IconButton";
import ClearIcon from "@mui/icons-material/Clear";
import PauseIcon from "@mui/icons-material/Pause";
import PlayArrowIcon from "@mui/icons-material/PlayArrow";
import ArrowUpward from "@mui/icons-material/ArrowUpward";
import ArrowDownward from "@mui/icons-material/ArrowDownward";
import ArrowDropDownIcon from "@mui/icons-material/ArrowDropDown";
import ChevronLeft from "@mui/icons-material/ChevronLeft";
import LightMode from "@mui/icons-material/LightMode";
import DarkMode from "@mui/icons-material/DarkMode";
import Download from "@mui/icons-material/Download";
import WrapTextIcon from "@mui/icons-material/WrapText";
import KeyboardArrowUpIcon from "@mui/icons-material/KeyboardArrowUp";
import KeyboardArrowDownIcon from "@mui/icons-material/KeyboardArrowDown";
import { ClockIcon } from "@mui/x-date-pickers";
import Tooltip from "@mui/material/Tooltip";
import FormControlLabel from "@mui/material/FormControlLabel";
import Checkbox from "@mui/material/Checkbox";
import { PodLogsProps } from "../../../../../../../../../../../../../types/declarations/pods";
import { AppContextProps } from "../../../../../../../../../../../../../types/declarations/app";
import { AppContext } from "../../../../../../../../../../../../../App";
import { DEFAULT_LOG_TAIL_SIZE, LOG_TAIL_SIZES } from "./constants";
import { filterLogs } from "./filterLogs";
import { LogVirtualList, LogVirtualListHandle } from "./LogVirtualList";
import { useLogSearchNavigation } from "./useLogSearchNavigation";
import { usePodLogStream } from "./usePodLogStream";

import "./style.css";

function ToolbarIconButton({
  active,
  title,
  onClick,
  disabled,
  testId,
  children,
}: {
  active?: boolean;
  title: string;
  onClick: () => void;
  disabled?: boolean;
  testId: string;
  children: ReactNode;
}) {
  return (
    <Tooltip
      title={<div className="icon-tooltip">{title}</div>}
      placement="top"
      arrow
    >
      <span>
        <IconButton
          data-testid={testId}
          onClick={onClick}
          disabled={disabled}
          className={`PodLogs-icon-btn${active ? " PodLogs-icon-btn--active" : ""}`}
          size="small"
        >
          {children}
        </IconButton>
      </span>
    </Tooltip>
  );
}

function getShortPodName(podName: string): string {
  const monoVertexMatch = podName.match(
    /(?:^|-)(mv-\d+)(?:-[a-z0-9]{5,})?$/i
  );
  if (monoVertexMatch) {
    return monoVertexMatch[1];
  }

  const withoutPodHash = podName.replace(/-[a-z0-9]{5,}$/i, "");
  const parts = withoutPodHash.split("-");
  return parts.slice(-2).join("-");
}

export function PodLogs({
  namespaceId,
  podName,
  containerName,
  type,
}: PodLogsProps) {
  const [search, setSearch] = useState<string>("");
  const [negateSearch, setNegateSearch] = useState<boolean>(false);
  const [wrapLines, setWrapLines] = useState<boolean>(true);
  const [paused, setPaused] = useState<boolean>(false);
  const [colorMode, setColorMode] = useState<string>("light");
  const [logsOrder, setLogsOrder] = useState<string>("desc");
  const [enableTimestamp, setEnableTimestamp] = useState<boolean>(false);
  const [levelFilter, setLevelFilter] = useState<string>("all");
  const [showPreviousLogs, setShowPreviousLogs] = useState(false);
  const [tailLines, setTailLines] = useState(DEFAULT_LOG_TAIL_SIZE);
  const [tailMenuAnchor, setTailMenuAnchor] = useState<null | HTMLElement>(
    null
  );
  const { host } = useContext<AppContextProps>(AppContext);

  // New container/pod: resume live with the default tail window.
  useEffect(() => {
    setPaused(false);
    setTailLines(DEFAULT_LOG_TAIL_SIZE);
    setTailMenuAnchor(null);
  }, [namespaceId, podName, containerName]);

  // Close the tail menu when resuming live.
  useEffect(() => {
    if (!paused) {
      setTailMenuAnchor(null);
    }
  }, [paused]);

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
    tailLines,
  });

  const tailSelectorDisabled = showPreviousLogs;

  const filteredLogs = useMemo(() => {
    const source = showPreviousLogs ? previousLogs : logs;
    return filterLogs(source, search, negateSearch);
  }, [showPreviousLogs, previousLogs, logs, search, negateSearch]);

  const orderedLogs = useMemo(
    () =>
      logsOrder === "desc" ? filteredLogs.slice().reverse() : filteredLogs,
    [filteredLogs, logsOrder]
  );
  const logVirtualListRef = useRef<LogVirtualListHandle>(null);
  const {
    enabled: searchNavigationEnabled,
    matchCount,
    currentMatch,
    activeIndex,
    goNext,
    goPrev,
  } = useLogSearchNavigation({
    orderedLogs,
    search,
    negateSearch,
    resetKey: `${namespaceId}-${podName}-${containerName}-${showPreviousLogs}-${logsOrder}-${search}-${negateSearch}-${tailLines}`,
  });

  const handleSearchChange = useCallback(
    (event: ChangeEvent<HTMLInputElement>) => {
      setSearch(event.target.value);
    },
    []
  );

  const handleSearchClear = useCallback(() => {
    setSearch("");
  }, []);

  const handleSearchNavigation = useCallback(
    (navigate: () => number | null) => {
      const targetIndex = navigate();
      if (targetIndex !== null) {
        logVirtualListRef.current?.scrollToIndex(targetIndex);
      }
    },
    []
  );

  const handleSearchKeyDown = useCallback(
    (event) => {
      if (event.key !== "Enter" || event.nativeEvent.isComposing) {
        return;
      }

      event.preventDefault();
      handleSearchNavigation(event.shiftKey ? goPrev : goNext);
    },
    [goNext, goPrev, handleSearchNavigation]
  );

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
    if (paused) {
      // Resuming live: always restart with the default tail window.
      setTailLines(DEFAULT_LOG_TAIL_SIZE);
      setPaused(false);
      return;
    }
    setPaused(true);
  }, [paused]);

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

  const handleSelectTailLines = useCallback(
    (size: number) => {
      setTailMenuAnchor(null);
      if (size === tailLines) {
        return;
      }
      setTailLines(size);
      // Changing window size always shows a frozen absolute snapshot.
      if (!paused) {
        setPaused(true);
      }
    },
    [paused, tailLines]
  );

  const handleOpenTailMenu = useCallback(
    (event: MouseEvent<HTMLElement>) => {
      if (tailSelectorDisabled) {
        return;
      }
      setTailMenuAnchor(event.currentTarget);
    },
    [tailSelectorDisabled]
  );

  const handleTogglePreviousLogs = useCallback(() => {
    setShowPreviousLogs((prev) => !prev);
  }, []);

  const logSourceLabel = `${getShortPodName(podName)}/${containerName}`;
  const tailSelectorTooltip = showPreviousLogs
    ? "Unavailable while viewing terminated logs"
    : paused
      ? "Choose how many recent log lines to show"
      : "Choose window size (pauses live stream)";
  const statusBanner = showPreviousLogs
    ? { testId: "previous-container-banner", label: "Previous container" }
    : paused
      ? { testId: "logs-paused-banner", label: "Logs paused" }
      : null;

  return (
    <Box className="PodLogs-root">
      <div className="PodLogs-toolbar">
        <div className="PodLogs-header">
          <div className="PodLogs-header-left">
            <span className="PodLogs-title">Container Logs</span>
            <span
              className="PodLogs-source-badge"
              title={`${podName}/${containerName}`}
              data-testid="log-source-badge"
            >
              {logSourceLabel}
            </span>
          </div>
          <div className="PodLogs-header-right">
            {statusBanner ? (
              <span
                className="PodLogs-status-banner"
                data-testid={statusBanner.testId}
              >
                {statusBanner.label}
              </span>
            ) : null}
            <Tooltip
              title={<div className="icon-tooltip">{tailSelectorTooltip}</div>}
              placement="top"
              arrow
            >
              <span data-testid="log-tail-size-control">
                <ButtonGroup
                  className="PodLogs-tail-size-group"
                  variant="outlined"
                  size="small"
                  disabled={tailSelectorDisabled}
                >
                  <Button
                    data-testid="log-tail-size-button"
                    className="PodLogs-tail-size-main"
                    disabled={tailSelectorDisabled}
                    onClick={handleOpenTailMenu}
                  >
                    {`${tailLines.toLocaleString()} lines`}
                  </Button>
                  <Button
                    data-testid="log-tail-size-menu-button"
                    className="PodLogs-tail-size-menu-btn"
                    aria-label="Choose how many recent log lines to show"
                    disabled={tailSelectorDisabled}
                    onClick={handleOpenTailMenu}
                  >
                    <ArrowDropDownIcon fontSize="small" />
                  </Button>
                </ButtonGroup>
              </span>
            </Tooltip>
            <Menu
              anchorEl={tailMenuAnchor}
              open={Boolean(tailMenuAnchor)}
              onClose={() => setTailMenuAnchor(null)}
              data-testid="log-tail-size-menu"
            >
              {LOG_TAIL_SIZES.map((size) => (
                <MenuItem
                  key={size}
                  data-testid={`log-tail-size-${size}`}
                  selected={size === tailLines}
                  onClick={() => handleSelectTailLines(size)}
                >
                  {`${size.toLocaleString()} lines`}
                </MenuItem>
              ))}
            </Menu>
          </div>
        </div>
        <div className="PodLogs-controls">
          <div className="PodLogs-search">
            <InputBase
              placeholder="Search logs"
              value={search}
              onChange={handleSearchChange}
              onKeyDown={handleSearchKeyDown}
              inputProps={{ "aria-label": "Search logs" }}
            />
            {search ? (
              <IconButton
                data-testid="clear-button"
                className="PodLogs-search-clear"
                onClick={handleSearchClear}
                size="small"
              >
                <ClearIcon />
              </IconButton>
            ) : null}
          </div>
          {searchNavigationEnabled && (
            <div className="PodLogs-match-pill">
              <Typography
                className="PodLogs-match-count"
                data-testid="search-match-count"
              >
                {currentMatch} / {matchCount}
              </Typography>
              <ToolbarIconButton
                testId="search-match-prev"
                title="Previous match (Shift+Enter)"
                onClick={() => handleSearchNavigation(goPrev)}
                disabled={!matchCount}
              >
                <KeyboardArrowUpIcon />
              </ToolbarIconButton>
              <ToolbarIconButton
                testId="search-match-next"
                title="Next match (Enter)"
                onClick={() => handleSearchNavigation(goNext)}
                disabled={!matchCount}
              >
                <KeyboardArrowDownIcon />
              </ToolbarIconButton>
            </div>
          )}
          <FormControlLabel
            className="PodLogs-checkbox-label"
            control={
              <Checkbox
                data-testid="negate-search"
                checked={negateSearch}
                onChange={handleNegateSearchChange}
                size="small"
              />
            }
            label="Negate search"
          />
          <ToolbarIconButton
            testId="wrap-lines-button"
            title={wrapLines ? "Unwrap Lines" : "Wrap Lines"}
            onClick={handleWrapLines}
            active={wrapLines}
          >
            <WrapTextIcon />
          </ToolbarIconButton>
          <ToolbarIconButton
            testId="pause-button"
            title={paused ? "Play logs" : "Pause logs"}
            onClick={handlePause}
            active={paused}
          >
            {paused ? <PlayArrowIcon /> : <PauseIcon />}
          </ToolbarIconButton>
          <ToolbarIconButton
            testId="previous-logs"
            title={
              showPreviousLogs
                ? "Show current container logs"
                : "Show previous terminated container logs"
            }
            onClick={handleTogglePreviousLogs}
            active={showPreviousLogs}
          >
            <ChevronLeft />
          </ToolbarIconButton>
          <ToolbarIconButton
            testId="color-mode-button"
            title={colorMode === "light" ? "Dark mode" : "Light mode"}
            onClick={handleColorMode}
            active={colorMode === "dark"}
          >
            {colorMode === "light" ? <DarkMode /> : <LightMode />}
          </ToolbarIconButton>
          <ToolbarIconButton
            testId="order-button"
            title={
              logsOrder === "asc" ? "Descending order" : "Ascending order"
            }
            onClick={handleOrder}
            active={logsOrder === "asc"}
          >
            {logsOrder === "asc" ? <ArrowDownward /> : <ArrowUpward />}
          </ToolbarIconButton>
          <ToolbarIconButton
            testId="download-logs-button"
            title="Download logs"
            onClick={handleLogsDownload}
          >
            <Download />
          </ToolbarIconButton>
          <ToolbarIconButton
            testId="toggle-timestamps-button"
            title={
              enableTimestamp ? "Remove Timestamps" : "Add Timestamps"
            }
            onClick={handleTimestamps}
            disabled={paused}
            active={enableTimestamp}
          >
            <ClockIcon />
          </ToolbarIconButton>
          <Select
            labelId="level-filter"
            id="level-filter"
            value={levelFilter}
            onChange={handleLevelChange}
            disabled={paused}
            className="PodLogs-level-select"
            sx={{ minWidth: "11rem" }}
            size="small"
          >
            <MenuItem sx={{ fontSize: "1.2rem" }} value={"all"}>
              All levels
            </MenuItem>
            <MenuItem sx={{ fontSize: "1.2rem" }} value={"info"}>
              Info
            </MenuItem>
            <MenuItem sx={{ fontSize: "1.2rem" }} value={"error"}>
              Error
            </MenuItem>
            <MenuItem sx={{ fontSize: "1.2rem" }} value={"warn"}>
              Warn
            </MenuItem>
            <MenuItem sx={{ fontSize: "1.2rem" }} value={"debug"}>
              Debug
            </MenuItem>
          </Select>
        </div>
      </div>
      <Box className="PodLogs-list-wrap">
        <LogVirtualList
          ref={logVirtualListRef}
          logs={orderedLogs}
          search={search}
          wrapLines={wrapLines}
          colorMode={colorMode}
          podName={podName}
          activeIndex={activeIndex}
        />
      </Box>
    </Box>
  );
}
