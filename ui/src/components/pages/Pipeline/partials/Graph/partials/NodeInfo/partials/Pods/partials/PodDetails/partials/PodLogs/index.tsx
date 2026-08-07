// eslint-disable-next-line @typescript-eslint/ban-ts-comment
// @ts-nocheck
import {
  ChangeEvent,
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
import {
  DEFAULT_LOG_HISTORY_BATCH_SIZE,
  END_OF_RETAINED_LOGS,
  LOADING_OLDER_LOGS,
  LOG_HISTORY_BATCH_SIZES,
} from "./constants";
import { filterLogs } from "./filterLogs";
import {
  LogVirtualList,
  LogVirtualListHandle,
  ScrollAnchor,
} from "./LogVirtualList";
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

type PendingRestore = {
  anchor: ScrollAnchor | null;
  prependedCount: number;
  activeLine: string | null;
};

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
  const [selectedBatchSize, setSelectedBatchSize] = useState(
    DEFAULT_LOG_HISTORY_BATCH_SIZE
  );
  const [batchMenuAnchor, setBatchMenuAnchor] = useState<null | HTMLElement>(
    null
  );
  const { host } = useContext<AppContextProps>(AppContext);
  const pendingRestoreRef = useRef<PendingRestore | null>(null);

  // Restart streaming if the source changes while paused
  useEffect(() => {
    setPaused(false);
  }, [namespaceId, podName, containerName]);

  const {
    logs,
    previousLogs,
    loadOlderLogs,
    isLoadingOlder,
    hasMoreOlder,
    loadedCount,
    remainingCapacity,
  } = usePodLogStream({
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
    focusLine,
  } = useLogSearchNavigation({
    orderedLogs,
    search,
    negateSearch,
    resetKey: `${namespaceId}-${podName}-${containerName}-${showPreviousLogs}-${logsOrder}-${search}-${negateSearch}`,
  });

  useEffect(() => {
    const pending = pendingRestoreRef.current;
    if (!pending || pending.prependedCount <= 0) {
      return;
    }
    pendingRestoreRef.current = null;
    if (pending.anchor) {
      logVirtualListRef.current?.restoreScrollAnchor(
        pending.anchor,
        pending.prependedCount,
        logsOrder
      );
    }
    if (pending.activeLine) {
      focusLine(pending.activeLine);
    }
  }, [logs, previousLogs, orderedLogs, logsOrder, focusLine]);

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

  const handleLoadOlder = useCallback(
    async (batchSize: number) => {
      setSelectedBatchSize(batchSize);
      setBatchMenuAnchor(null);
      const activeLine =
        activeIndex !== null ? orderedLogs[activeIndex] ?? null : null;
      const anchor = logVirtualListRef.current?.captureScrollAnchor() ?? null;
      const { prependedCount } = await loadOlderLogs(batchSize);
      if (prependedCount > 0) {
        pendingRestoreRef.current = {
          anchor,
          prependedCount,
          activeLine,
        };
      }
    },
    [activeIndex, orderedLogs, loadOlderLogs]
  );

  const logSourceLabel = `${getShortPodName(podName)}/${containerName}`;
  // Keep the control visible during bootstrap; only treat retention as exhausted
  // after a real loadOlder attempt (hasMoreOlder) or hitting the client ceiling.
  const canLoadOlder = hasMoreOlder && remainingCapacity > 0;
  const loadOlderDisabled =
    isLoadingOlder || !canLoadOlder || loadedCount === 0;
  const effectiveBatchSize = Math.min(selectedBatchSize, remainingCapacity);
  const showEndOfHistory = !hasMoreOlder || remainingCapacity <= 0;

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
            <span
              className="PodLogs-loaded-count"
              data-testid="log-loaded-count"
            >
              {loadedCount.toLocaleString()} loaded
            </span>
            {showEndOfHistory ? (
              <span
                className="PodLogs-end-history"
                data-testid="log-end-of-history"
              >
                {END_OF_RETAINED_LOGS}
              </span>
            ) : (
              <ButtonGroup
                className="PodLogs-load-older-group"
                variant="outlined"
                size="small"
                disabled={loadOlderDisabled}
              >
                <Button
                  data-testid="load-older-logs-button"
                  className="PodLogs-load-older-main"
                  onClick={() => handleLoadOlder(selectedBatchSize)}
                  disabled={loadOlderDisabled || effectiveBatchSize <= 0}
                >
                  {isLoadingOlder
                    ? LOADING_OLDER_LOGS
                    : `Load older ${selectedBatchSize.toLocaleString()}`}
                </Button>
                <Button
                  data-testid="load-older-logs-menu-button"
                  className="PodLogs-load-older-menu-btn"
                  aria-label="Choose how many older lines to load"
                  onClick={(event) => setBatchMenuAnchor(event.currentTarget)}
                  disabled={loadOlderDisabled}
                >
                  <ArrowDropDownIcon fontSize="small" />
                </Button>
              </ButtonGroup>
            )}
            <Menu
              anchorEl={batchMenuAnchor}
              open={Boolean(batchMenuAnchor)}
              onClose={() => setBatchMenuAnchor(null)}
              data-testid="load-older-logs-menu"
            >
              {LOG_HISTORY_BATCH_SIZES.map((size) => (
                <MenuItem
                  key={size}
                  data-testid={`load-older-batch-${size}`}
                  disabled={size > remainingCapacity}
                  selected={size === selectedBatchSize}
                  onClick={() => handleLoadOlder(size)}
                >
                  {size.toLocaleString()}
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
        <div className="PodLogs-footer">
          <FormControlLabel
            className="PodLogs-checkbox-label"
            control={
              <Checkbox
                data-testid="previous-logs"
                checked={showPreviousLogs}
                onChange={(event) =>
                  setShowPreviousLogs(event.target.checked)
                }
                size="small"
              />
            }
            label="Show terminated"
          />
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
