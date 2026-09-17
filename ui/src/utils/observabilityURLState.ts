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

export const parseMetricFilters = (
  value: string | null | undefined
): Record<string, string> =>
  (value || "")
    .split(",")
    .filter(Boolean)
    .reduce((result, item) => {
      const separator = item.indexOf(":");
      if (separator <= 0) return result;
      const key = item.slice(0, separator);
      const filterValue = item.slice(separator + 1);
      return filterValue ? { ...result, [key]: filterValue } : result;
    }, {} as Record<string, string>);

export const serializeMetricFilters = (
  filters: Record<string, string>
): string =>
  Object.entries(filters)
    .filter(([, filterValue]) => filterValue)
    .map(([key, filterValue]) => `${key}:${filterValue}`)
    .join(",");

const METRIC_REQ_PARAMS = {
  metricDimension: "dimension",
  metricQuantile: "quantile",
  metricDuration: "duration",
  metricStart: "start_time",
  metricEnd: "end_time",
} as const;

export type MetricRequestFromUrl = {
  dimension?: string;
  quantile?: string;
  duration?: string;
  start_time?: string;
  end_time?: string;
};

// Missing params stay unset so chart dropdown defaults are not wiped.
export const readMetricRequestFromSearch = (
  search: string
): { req: MetricRequestFromUrl; filters?: Record<string, string> } => {
  const params = new URLSearchParams(
    search.startsWith("?") ? search.slice(1) : search
  );
  const req: MetricRequestFromUrl = {};
  (Object.entries(METRIC_REQ_PARAMS) as [
    keyof typeof METRIC_REQ_PARAMS,
    keyof MetricRequestFromUrl
  ][]).forEach(([param, field]) => {
    const value = params.get(param);
    if (value) req[field] = value;
  });
  return {
    req,
    filters: params.has("metricFilter")
      ? parseMetricFilters(params.get("metricFilter"))
      : undefined,
  };
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
