import { History, Location } from "history";

export const OBSERVABILITY_PARAM_NAMES = [
  "vertex",
  "vertexTab",
  "specLine",
  "pod",
  "container",
  "logsSearch",
  "logsNegate",
  "logsWrap",
  "logsColor",
  "logsPaused",
  "logsPrevious",
  "logsOrder",
  "logsTimestamps",
  "logsLevel",
  "logsTail",
  "logsFocus",
  "metric",
  "metricPanels",
  "metricDimension",
  "metricQuantile",
  "metricDuration",
  "metricStart",
  "metricEnd",
  "metricFilter",
] as const;

export type ObservabilityParamName = (typeof OBSERVABILITY_PARAM_NAMES)[number];
export type ObservabilityPatch = Partial<
  Record<ObservabilityParamName, string | number | boolean | null | undefined>
>;

const BOOLEAN_PARAMS = new Set<ObservabilityParamName>([
  "logsNegate",
  "logsWrap",
  "logsPaused",
  "logsPrevious",
  "logsTimestamps",
  "logsFocus",
]);

export const parseBooleanParam = (
  params: URLSearchParams,
  name: ObservabilityParamName,
  defaultValue = false
): boolean => {
  const value = params.get(name);
  return value === null ? defaultValue : value === "1";
};

export const updateObservabilitySearch = (
  search: string,
  patch: ObservabilityPatch
): string => {
  const params = new URLSearchParams(search);
  Object.entries(patch).forEach(([name, value]) => {
    if (value === undefined || value === null || value === "") {
      params.delete(name);
      return;
    }
    params.set(name, BOOLEAN_PARAMS.has(name as ObservabilityParamName) ? (value ? "1" : "0") : String(value));
  });
  const next = params.toString();
  return next ? `?${next}` : "";
};

export const clearObservabilitySearch = (search: string): string =>
  updateObservabilitySearch(
    search,
    OBSERVABILITY_PARAM_NAMES.reduce(
      (patch, name) => ({ ...patch, [name]: null }),
      {} as ObservabilityPatch
    )
  );

export const replaceObservabilityState = (
  history: History,
  location: Location,
  patch: ObservabilityPatch
) => {
  // Nested observability controls are only meaningful for a selected vertex.
  // This also lets these components remain independently testable and reusable.
  if (!new URLSearchParams(location.search).has("vertex")) {
    return;
  }
  const search = updateObservabilitySearch(location.search, patch);
  if (search !== location.search) {
    history.replace({ pathname: location.pathname, search });
  }
};

export const pushObservabilityState = (
  history: History,
  location: Location,
  patch: ObservabilityPatch
) => {
  const search = updateObservabilitySearch(location.search, patch);
  if (search !== location.search) {
    history.push({ pathname: location.pathname, search });
  }
};

export const buildCurrentViewUrl = (location: Location): string =>
  `${window.location.origin}${location.pathname}${location.search}`;

export const buildObservabilityViewUrl = (
  location: Location,
  patch: ObservabilityPatch
): string =>
  `${window.location.origin}${location.pathname}${updateObservabilitySearch(
    location.search,
    patch
  )}`;
