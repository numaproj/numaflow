import { useMemo, useState } from "react";
import SearchIcon from "@mui/icons-material/Search";
import InputAdornment from "@mui/material/InputAdornment";
import MenuItem from "@mui/material/MenuItem";
import TextField from "@mui/material/TextField";
import ToggleButton from "@mui/material/ToggleButton";
import ToggleButtonGroup from "@mui/material/ToggleButtonGroup";
import Tooltip from "@mui/material/Tooltip";
import {
  Pod,
  PodDetail,
  PodFleetSort,
  PodRuntimeInfo,
} from "../../../../../../../types/declarations/pods";
import {
  buildPodFleetItems,
  chipLabel,
  filterAndSortPodFleet,
  podStatusTone,
  PodFleetFilter,
} from "./podFleetModel";

import "./podFleet.css";

interface PodFleetProps {
  pods: Pod[];
  detailsByName?: Map<string, PodDetail>;
  runtimeByName: Map<string, PodRuntimeInfo>;
  selectedPod?: Pod;
  onPodSelect: (pod: Pod) => void;
}

const filters: { value: PodFleetFilter; label: string }[] = [
  { value: "all", label: "All" },
  { value: "critical", label: "Critical" },
  { value: "warning", label: "Warning" },
  { value: "healthy", label: "Healthy" },
];

/** PodFleet renders the selectable, filterable fleet health surface for Beta Pod View. */
export function PodFleet({
  pods,
  detailsByName,
  runtimeByName,
  selectedPod,
  onPodSelect,
}: PodFleetProps) {
  const [filter, setFilter] = useState<PodFleetFilter>("all");
  const [search, setSearch] = useState("");
  const [sort, setSort] = useState<PodFleetSort>("severity");
  const items = useMemo(
    () => buildPodFleetItems(pods, detailsByName, runtimeByName),
    [pods, detailsByName, runtimeByName]
  );
  const visibleItems = useMemo(
    () => filterAndSortPodFleet(items, filter, search, sort),
    [filter, items, search, sort]
  );
  const counts = useMemo(
    () =>
      filters.reduce(
        (allCounts, option) => ({
          ...allCounts,
          [option.value]:
            option.value === "all"
              ? items.length
              : items.filter((item) => item.severity === option.value).length,
        }),
        {} as Record<PodFleetFilter, number>
      ),
    [items]
  );

  return (
    <section className="pod-fleet" data-testid="pod-fleet">
      <div className="pod-fleet-header">
        <div className="pod-fleet-filters">
          <span className="pod-fleet-title">Fleet Health · {pods.length} Pods</span>
          <ToggleButtonGroup
            aria-label="Filter pods by health"
            className="pod-fleet-filter-group"
            exclusive
            value={filter}
            onChange={(_event, value: PodFleetFilter | null) => value && setFilter(value)}
            size="small"
          >
            {filters.map((option) => (
              <ToggleButton
                key={option.value}
                value={option.value}
                data-testid={`pods-filter-${option.value}`}
                aria-label={`${option.label} pods filter`}
              >
                {option.value !== "all" && (
                  <span className={`pod-fleet-filter-dot pod-fleet-filter-dot--${option.value}`} />
                )}
                {option.label} {counts[option.value]}
              </ToggleButton>
            ))}
          </ToggleButtonGroup>
        </div>
        <div className="pod-fleet-actions">
          <TextField
            className="pod-fleet-search"
            value={search}
            onChange={(event) => setSearch(event.target.value)}
            placeholder="Search pods..."
            inputProps={{ "aria-label": "Search pods" }}
            size="small"
            InputProps={{
              startAdornment: (
                <InputAdornment position="start">
                  <SearchIcon fontSize="small" />
                </InputAdornment>
              ),
            }}
          />
          <TextField
            className="pod-fleet-sort"
            select
            value={sort}
            onChange={(event) => setSort(event.target.value as PodFleetSort)}
            inputProps={{ "aria-label": "Sort pods" }}
            size="small"
          >
            <MenuItem value="severity">Severity</MenuItem>
            <MenuItem value="cpu">CPU</MenuItem>
            <MenuItem value="memory">Memory</MenuItem>
            <MenuItem value="restarts">Restarts</MenuItem>
            <MenuItem value="name">Name</MenuItem>
          </TextField>
        </div>
      </div>
      <div className="pod-fleet-grid" data-testid="pod-fleet-grid">
        {visibleItems.map((item) => {
          const isSelected = selectedPod?.name === item.pod.name;
          const status = item.runtime?.status || "Unknown";
          return (
            <Tooltip
              key={item.pod.name}
              title={`${item.pod.name} · ${status}`}
              arrow
            >
              <button
                type="button"
                className={`pod-fleet-cell pod-fleet-cell--${item.severity}${
                  isSelected ? " pod-fleet-cell--selected" : ""
                }`}
                data-testid={`pod-fleet-cell-${item.pod.name}`}
                aria-pressed={isSelected}
                aria-label={`Select ${item.pod.name}, ${status}`}
                onClick={() => onPodSelect(item.pod)}
              >
                <span>{chipLabel(item.pod.name)}</span>
                {item.restartCount > 0 && <span className="pod-fleet-restart-marker" />}
              </button>
            </Tooltip>
          );
        })}
      </div>
      {visibleItems.length === 0 && (
        <p className="pod-fleet-empty" data-testid="pod-fleet-empty">
          No pods match the current filters.
        </p>
      )}
      {selectedPod && (
        <div className="pod-fleet-selected" data-testid="pod-fleet-selected">
          <span>Selected:</span>
          <strong title={selectedPod.name}>{selectedPod.name}</strong>
          <span
            className={`pod-fleet-status pod-fleet-status--${podStatusTone(
              runtimeByName.get(selectedPod.name)?.status
            )}`}
            data-testid="pod-fleet-selected-status"
          >
            {runtimeByName.get(selectedPod.name)?.status || "Unknown"}
          </span>
        </div>
      )}
    </section>
  );
}
