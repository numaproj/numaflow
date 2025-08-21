import {
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useRef,
  useState,
} from "react";
import {
  CartesianGrid,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";
import Box from "@mui/material/Box";
import CircularProgress from "@mui/material/CircularProgress";
import CloseIcon from "@mui/icons-material/Close";
import IconButton from "@mui/material/IconButton";
import Divider from "@mui/material/Divider";
import Dropdown from "../common/Dropdown";
import EmptyChart from "../EmptyChart";
import FiltersDropdown from "../common/FiltersDropdown";
import TimeSelector from "../common/TimeRange";
import { useMetricsFetch } from "../../../../../../../../../../../../../../../utils/fetchWrappers/metricsFetch";
import {
  CONTAINER_CPU_UTILIZATION,
  CONTAINER_MEMORY_UTILIZATION,
  MONO_VERTEX_PENDING_MESSAGES,
  MONO_VERTEX_PROCESSING_TIME_LATENCY,
  MONO_VERTEX_SINK_WRITE_TIME_LATENCY,
  POD_CPU_UTILIZATION,
  POD_MEMORY_UTILIZATION,
  UDF_PROCESSING_TIME_LATENCY,
  VERTEX_ACK_PROCESSING_TIME_LATENCY,
  VERTEX_PENDING_MESSAGES,
  VERTEX_READ_PROCESSING_TIME_LATENCY,
  VERTEX_WRITE_PROCESSING_TIME_LATENCY,
  VERTEX_PROCESSING_TIME_LATENCY,
  FALLBACK_SINK_WRITE_TIME_LATENCY,
  SOURCE_TRANSFORMER_PROCESSING_TIME_LATENCY,
  MONO_VERTEX_ACK_PROCESSING_TIME_LATENCY,
  MONO_VERTEX_TRANSFORMER_PROCESSING_TIME_LATENCY,
  MONO_VERTEX_READ_TIME_LATENCY,
  MONO_VERTEX_FALLBACK_SINK_WRITE_TIME_LATENCY,
} from "../../utils/constants";
import { AppContext } from "../../../../../../../../../../../../../../../App";
import { AppContextProps } from "../../../../../../../../../../../../../../../types/declarations/app";
import { Pod } from "../../../../../../../../../../../../../../../types/declarations/pods";

import "./style.css";

interface TooltipProps {
  payload?: any[];
  label?: string;
  active?: boolean;
}

const formattedDate = (timestamp: number): string => {
  if (timestamp) {
    try {
      const date = new Date(timestamp * 1000);
      const dayFormatter = new Intl.DateTimeFormat("en-US", {
        weekday: "short",
      });
      const monthFormatter = new Intl.DateTimeFormat("en-US", {
        month: "short",
      });

      const day = dayFormatter.format(date);
      const month = monthFormatter.format(date);
      const year = date.getFullYear();
      let hours = date.getHours();
      const minutes = date.getMinutes().toString().padStart(2, "0");
      const seconds = date.getSeconds().toString().padStart(2, "0");
      const amOrPm = hours >= 12 ? "PM" : "AM";
      hours = hours % 12 || 12;

      const offsetMinutes = date.getTimezoneOffset();
      const offsetSign = offsetMinutes > 0 ? "-" : "+";
      const absOffsetMinutes = Math.abs(offsetMinutes);
      const offsetHours = Math.floor(absOffsetMinutes / 60)
        .toString()
        .padStart(2, "0"); // Integer division
      const offsetRemainingMinutes = (absOffsetMinutes % 60)
        .toString()
        .padStart(2, "0");

      return `${day} ${month} ${date.getDate()} ${year} ${hours}:${minutes}:${seconds} ${amOrPm} ${offsetSign}${offsetHours}${offsetRemainingMinutes}`;
    } catch {
      return "Invalid Date";
    }
  }
  return "Invalid Date";
};

const CustomTooltip = ({
  payload,
  active,
  displayName,
  pinnedTooltip,
  setPinnedTooltip,
  setTooltipX,
  setTooltipY,
}: TooltipProps & {
  displayName: string;
  pinnedTooltip: any;
  setPinnedTooltip: any;
  setTooltipX: any;
  setTooltipY: any;
}) => {
  const [showMessage, setShowMessage] = useState<boolean>(false);

  useEffect(() => {
    setShowMessage(false);
    const timer = setTimeout(() => {
      setShowMessage(true);
    }, 700);

    return () => clearTimeout(timer);
  }, [payload]);

  const tooltipVal = pinnedTooltip ?? payload;
  if (!active || !tooltipVal || !tooltipVal.length) return null;

  // sorting tooltip based on values in descending order
  tooltipVal?.sort((a: { value: any }, b: { value: any }) => {
    const period1 = a?.value;
    const period2 = b?.value;
    return period2 - period1;
  });

  const maxWidth = Math.max(
    ...tooltipVal.map((entry: any) => entry?.name?.length)
  );
  const timestamp = tooltipVal[0]?.payload?.timestamp;

  return (
    <Box className={"tooltip-bg"}>
      {showMessage && !pinnedTooltip && (
        <Box className={"tooltip-pin"}>Click on the chart to pin</Box>
      )}
      <Box className={"tooltip-fixed"}>
        <Box>{formattedDate(timestamp)}</Box>
        {pinnedTooltip && (
          <Box>
            <IconButton
              onClick={() => {
                setPinnedTooltip(null);
                setTooltipX(undefined);
                setTooltipY(undefined);
              }}
            >
              <CloseIcon height={20} width={20} />
            </IconButton>
          </Box>
        )}
      </Box>
      <Divider className={"tooltip-divider"} />
      <Box className={"tooltip-scrollable"}>
        {tooltipVal.map((entry: any, index: any) => {
          const formattedValue = getDefaultFormatter(entry?.value, displayName);
          return (
            <Box className={"tooltip-entry-container"} key={`item-${index}`}>
              <Box
                className={"tooltip-entry"}
                sx={{
                  width: `${maxWidth + 1}rem`,
                  color: entry?.color,
                }}
              >
                {entry?.name}
              </Box>
              <Box sx={{ color: entry?.color }}>{formattedValue}</Box>
            </Box>
          );
        })}
      </Box>
    </Box>
  );
};

const getDefaultFormatter = (value: number, displayName: string) => {
  const formatValue = (value: number, suffix: string) => {
    const formattedValue = parseFloat(value?.toFixed(2));
    return formattedValue % 1 === 0
      ? `${Math.floor(formattedValue)}${suffix}`
      : `${formattedValue}${suffix}`;
  };
  switch (displayName) {
    case MONO_VERTEX_PROCESSING_TIME_LATENCY:
    case MONO_VERTEX_SINK_WRITE_TIME_LATENCY:
    case VERTEX_READ_PROCESSING_TIME_LATENCY:
    case VERTEX_WRITE_PROCESSING_TIME_LATENCY:
    case VERTEX_PROCESSING_TIME_LATENCY:
    case VERTEX_ACK_PROCESSING_TIME_LATENCY:
    case UDF_PROCESSING_TIME_LATENCY:
    case SOURCE_TRANSFORMER_PROCESSING_TIME_LATENCY:
    case FALLBACK_SINK_WRITE_TIME_LATENCY:
    case MONO_VERTEX_TRANSFORMER_PROCESSING_TIME_LATENCY:
    case MONO_VERTEX_READ_TIME_LATENCY:
    case MONO_VERTEX_ACK_PROCESSING_TIME_LATENCY:
    case MONO_VERTEX_FALLBACK_SINK_WRITE_TIME_LATENCY:
      if (value === 0) {
        return "0";
      } else if (value < 1000) {
        return formatValue(value, " μs");
      } else if (value < 1000000) {
        return formatValue(value / 1000, " ms");
      } else {
        return formatValue(value / 1000000, " s");
      }
    case POD_CPU_UTILIZATION:
    case POD_MEMORY_UTILIZATION:
    case CONTAINER_CPU_UTILIZATION:
    case CONTAINER_MEMORY_UTILIZATION:
      if (value === 0) {
        return "0";
      } else if (value < 1000) {
        return formatValue(value, " %");
      } else if (value < 1000000) {
        return formatValue(value / 1000, "k %");
      } else {
        return formatValue(value / 1000000, "M %");
      }
    default:
      if (value === 0) {
        return "0";
      } else if (value < 1000) {
        return formatValue(value, "");
      } else if (value < 1000000) {
        return formatValue(value / 1000, " k");
      } else {
        return formatValue(value / 1000000, " M");
      }
  }
};

const getTickFormatter = (displayName: string) => {
  return (value: number) => {
    return getDefaultFormatter(value, displayName);
  };
};

interface LineChartComponentProps {
  namespaceId: string;
  pipelineId: string;
  type: string;
  metric: any;
  vertexId?: string;
  presets?: any;
  fromModal?: boolean;
  pod?: Pod;
}

// TODO have a check for metricReq against metric object to ensure required fields are passed
const LineChartComponent = ({
  namespaceId,
  pipelineId,
  type,
  metric,
  vertexId,
  presets,
  fromModal,
  pod,
}: LineChartComponentProps) => {
  const { addError } = useContext<AppContextProps>(AppContext);
  const [transformedData, setTransformedData] = useState<any[]>([]);
  const [chartLabels, setChartLabels] = useState<any[]>([]);
  const [metricsReq, setMetricsReq] = useState<any>({
    metric_name: metric?.metric_name,
    pattern_name: metric?.pattern_name,
    display_name: metric?.display_name,
  });
  const [paramsList, setParamsList] = useState<any[]>([]);
  // store all filters for each selected dimension
  const [filtersList, setFiltersList] = useState<any[]>([]);
  const [filters, setFilters] = useState<any>({});
  const [previousDimension, setPreviousDimension] = useState<string>(
    metricsReq?.dimension
  );
  const [pinnedTooltip, setPinnedTooltip] = useState<any>(null);
  const [tooltipX, setTooltipX] = useState<number | undefined>(undefined);
  const [tooltipY, setTooltipY] = useState<number | undefined>(undefined);
  const [isFilterFocused, setFilterFocused] = useState<boolean>(false);
  const chartContainerRef = useRef<HTMLDivElement>(null);
  const [containerWidth, setContainerWidth] = useState(0);
  const resizeObserverRef = useRef<ResizeObserver | null>(null);

  const getRandomColor = useCallback((index: number) => {
    const hue = (index * 137.508) % 360;
    return `hsl(${hue}, 50%, 50%)`;
  }, []);

  useEffect(() => {
    setPinnedTooltip(null);
    setTooltipX(undefined);
    setTooltipY(undefined);
  }, [metricsReq, filters]);

  // required filters
  const getFilterValue = useCallback(
    (filterName: string) => {
      switch (filterName) {
        case "namespace":
          return namespaceId;
        case "mvtx_name":
        case "pipeline":
          return pipelineId;
        case "vertex":
          return vertexId;
        case "pod":
          if (
            [POD_CPU_UTILIZATION, POD_MEMORY_UTILIZATION].includes(
              metric?.display_name
            )
          ) {
            switch (type) {
              case "monoVertex":
                return `${pipelineId}-mv-.*`;
              default:
                return `${pipelineId}-${vertexId}-.*`;
            }
          } else {
            return pod?.name;
          }
        case "replica":
          // Currently "replica" is not used in any of the metrics as a required filter
          // Need may arise in case of pending metrics
          // If so, then set replica to 0, as pending calculation is done only for 0th replica.
          return "0";
        default:
          return "";
      }
    },
    [namespaceId, pipelineId, pod]
  );

  const updateFilterList = useCallback(
    (dimensionVal: string) => {
      const newFilters =
        metric?.dimensions
          ?.find((dimension: any) => dimension?.name === dimensionVal)
          ?.filters?.map((param: any) => ({
            name: param?.Name,
            required: param?.Required,
          })) || [];
      setFiltersList(newFilters);
    },
    [metric, setFiltersList]
  );

  const updateFilters = useCallback(() => {
    const newFilters: any = {};
    filtersList?.forEach((filterElement: any) => {
      if (filterElement?.name && filterElement?.required) {
        newFilters[filterElement.name] = getFilterValue(filterElement.name);
      }
    });
    setFilters(newFilters);
  }, [filtersList, getFilterValue, setFilters]);

  //update filters only when dimension changes in metricsReq
  useEffect(() => {
    if (metricsReq?.dimension !== previousDimension) {
      updateFilterList(metricsReq.dimension);
      setPreviousDimension(metricsReq?.dimension);
    }
  }, [metricsReq, updateFilterList]);

  useEffect(() => {
    if (filtersList?.length) updateFilters();
  }, [filtersList]);

  const updateParams = useCallback(() => {
    const initParams = [{ name: "dimension", required: "true" }];
    // taking dimension[0] as all will have same params
    const newParams =
      metric?.dimensions?.[0]?.params?.map((param: any) => ({
        name: param?.Name,
        required: param?.Required,
      })) || [];

    setParamsList([...initParams, ...newParams]);
  }, [metric, setParamsList]);

  // update params once initially
  useEffect(() => {
    updateParams();
  }, [updateParams]);

  const { chartData, error, isLoading } = useMetricsFetch({
    metricReq: metricsReq,
    filters,
  });

  useEffect(() => {
    if (error) {
      addError(error?.toString());
    }
  }, [error, addError]);

  const groupByLabel = useCallback((dimension: string, displayName: string) => {
    switch (displayName) {
      case POD_CPU_UTILIZATION:
      case POD_MEMORY_UTILIZATION:
        return ["pod"];
      case CONTAINER_CPU_UTILIZATION:
      case CONTAINER_MEMORY_UTILIZATION:
        return ["container"];
      case VERTEX_PENDING_MESSAGES:
      case MONO_VERTEX_PENDING_MESSAGES:
        return dimension === "pod" ? ["pod", "period"] : ["period"];
    }
    switch (dimension) {
      case "mono-vertex":
        return ["mvtx_name"];
      default:
        return [dimension];
    }
  }, []);

  const formatTime = (timestamp: number): string => {
    const date = new Date(timestamp * 1000);
    let hours = date.getHours();
    const minutes = date.getMinutes().toString().padStart(2, "0");
    const amOrPm = hours >= 12 ? "PM" : "AM";
    hours = hours % 12 || 12;
    return `${hours.toString().padStart(2, "0")}:${minutes} ${amOrPm}`;
  };

  const createDataObject = (
    formattedTime: string,
    timestamp: number,
    labelVal: string,
    value: string
  ): Record<string, any> => ({
    time: formattedTime,
    timestamp,
    [labelVal]: parseFloat(value),
  });

  const periodOrder = {
    "1m": 1,
    "5m": 2,
    "15m": 3,
  };

  const updateChartData = useCallback(() => {
    if (!chartData) return;

    const labels: string[] = [];
    const transformedData: Record<string, any>[] = [];
    let filteredChartData = chartData;
    const label = groupByLabel(metricsReq?.dimension, metricsReq?.display_name);

    if (
      [VERTEX_PENDING_MESSAGES, MONO_VERTEX_PENDING_MESSAGES]?.includes(
        metricsReq?.display_name
      )
    )
      filteredChartData = filteredChartData
        // Filter out default period for pending messages
        ?.filter((item) => item?.metric?.["period"] !== "default")
        ?.sort((a, b) => {
          const period1: "1m" | "5m" | "15m" = a?.metric?.["period"];
          const period2: "1m" | "5m" | "15m" = b?.metric?.["period"];
          return (periodOrder[period1] || 0) - (periodOrder[period2] || 0);
        });

    if (
      Array.isArray(label) &&
      label.length === 1 &&
      label[0] === "container"
    ) {
      filteredChartData = filteredChartData?.filter((item) => {
        return pod?.containers?.includes(item?.metric?.["container"]);
      });
    }

    if (Array.isArray(label) && label.length === 1 && label[0] === "pod") {
      filteredChartData = filteredChartData?.filter((item) => {
        return !item?.metric?.["pod"]?.includes("daemon");
      });
    }

    filteredChartData?.forEach((item) => {
      let labelVal = "";
      label?.forEach((eachLabel: string) => {
        if (item?.metric?.[eachLabel] !== undefined) {
          labelVal += (labelVal ? "-" : "") + item.metric[eachLabel];
        }
      });

      // Remove initial hyphen if labelVal is not empty
      if (labelVal.startsWith("-") && labelVal.length > 1) {
        labelVal = labelVal.substring(1);
      }

      labels.push(labelVal);

      item?.values?.forEach(([timestamp, value]: [number, string]) => {
        const formattedTime = formatTime(timestamp);

        const existingElement = transformedData?.find(
          (data) => data?.time === formattedTime
        );
        if (!existingElement) {
          transformedData.push(
            createDataObject(formattedTime, timestamp, labelVal, value)
          );
        } else {
          existingElement[labelVal] = parseFloat(value);
        }
      });
    });
    transformedData.sort((a, b) => {
      return a?.timestamp - b?.timestamp;
    });
    setChartLabels(labels);
    setTransformedData(transformedData);
  }, [chartData, metricsReq, groupByLabel, pod]);

  useEffect(() => {
    if (chartData) updateChartData();
  }, [chartData, updateChartData]);

  const handleResize = useCallback((entries: ResizeObserverEntry[]) => {
    if (entries && entries.length > 0) {
      setContainerWidth(entries[0].contentRect.width);
    }
  }, []);

  useEffect(() => {
    resizeObserverRef.current = new ResizeObserver(handleResize);

    if (
      !isLoading &&
      !error &&
      transformedData?.length > 0 &&
      chartContainerRef.current
    ) {
      resizeObserverRef.current.observe(chartContainerRef.current);
    } else {
      if (resizeObserverRef.current) {
        resizeObserverRef.current.disconnect();
      }
    }

    return () => {
      if (resizeObserverRef.current) {
        resizeObserverRef.current.disconnect();
      }
    };
  }, [isLoading, error, transformedData, handleResize]);

  const calculateInterval = useMemo(() => {
    if (!transformedData) {
      return 0;
    }

    // filtering points with minutes in multiple of 5
    const filteredData = transformedData.filter((td) => {
      const minutes = td?.time?.split(" ")[0]?.split(":")[1];
      return Number(minutes) % 5 === 0;
    });

    const dataLength = filteredData.length;

    const getInterval = (maxPoints: number) => {
      if (dataLength <= maxPoints) return 0;
      const interval = Math.floor(dataLength / maxPoints);
      return interval === 4 ? 3 : Math.max(1, interval);
    };

    if (containerWidth > 1300) return getInterval(24);
    else if (containerWidth > 650) return getInterval(12);
    else return getInterval(6);
  }, [containerWidth, transformedData]);

  const roundTimestampToNearestFiveMinutes = useCallback(
    (timestamp: string): string => {
      const minutes = timestamp?.split(" ")[0]?.split(":")[1];
      return Number(minutes) % 5 === 0 ? timestamp : "";
    },
    []
  );

  const getMetricsModalDesc = useMemo(() => {
    return `This chart represents the above metric at a ${metricsReq?.dimension} level over the selected time period.`;
  }, [metricsReq?.dimension]);

  if (paramsList?.length === 0) return <></>;

  const hasTimeParams = paramsList?.some((param) =>
    ["start_time", "end_time"].includes(param?.name)
  );

  return (
    <Box>
      <Box className={"line-chart-wrapper"}>
        {paramsList
          ?.filter(
            (param) => !["start_time", "end_time"]?.includes(param?.name)
          )
          ?.map((param: any) => {
            return (
              <Box
                display={fromModal ? "none" : "flex"}
                key={`line-chart-${param?.name}`}
                sx={{ gap: "1rem" }}
              >
                <Dropdown
                  metric={metric}
                  type={type}
                  field={param?.name}
                  setMetricReq={setMetricsReq}
                  presets={presets}
                />
              </Box>
            );
          })}
        {filtersList?.filter((filterEle: any) => !filterEle?.required)?.length >
          0 && (
          <Box
            className={"line-chart-filters"}
            sx={{
              display: fromModal ? "none" : "flex",
            }}
          >
            <Box
              className={"line-chart-filters-label"}
              sx={{
                color: isFilterFocused ? "#0077c5" : "rgba(0, 0, 0, 0.54)",
              }}
            >
              Filters
            </Box>
            <FiltersDropdown
              items={filtersList?.filter(
                (filterEle: any) => !filterEle?.required
              )}
              namespaceId={namespaceId}
              pipelineId={pipelineId}
              type={type}
              vertexId={vertexId}
              setFilters={setFilters}
              selectedPodName={pod?.name}
              isFilterFocused={isFilterFocused}
              setFilterFocused={setFilterFocused}
              metric={metric}
            />
          </Box>
        )}
        {fromModal && (
          <Box className={"line-chart-modal-desc"}>{getMetricsModalDesc}</Box>
        )}
        {hasTimeParams && (
          <Box key="line-chart-preset">
            <TimeSelector setMetricReq={setMetricsReq} />
          </Box>
        )}
      </Box>

      {isLoading && (
        <Box className={"line-chart-loading"}>
          <CircularProgress />
        </Box>
      )}

      {!isLoading && error && <EmptyChart message={error?.toString()} />}

      {!isLoading && !error && transformedData?.length > 0 && (
        <ResponsiveContainer ref={chartContainerRef} width="100%" height={400}>
          <LineChart
            data={transformedData}
            margin={{ top: 10, right: 10, left: 5, bottom: 0 }}
            style={{ cursor: "pointer" }}
            onClick={(state) => {
              if (!pinnedTooltip) {
                setPinnedTooltip(state?.activePayload);
                setTooltipX(100);
                setTooltipY(90);
              } else {
                setPinnedTooltip(null);
                setTooltipX(undefined);
                setTooltipY(undefined);
              }
            }}
          >
            <CartesianGrid strokeDasharray="3 3" />
            <XAxis
              dataKey="time"
              padding={{ left: 15, right: 15 }}
              axisLine={{ stroke: "#8D9096" }}
              tickFormatter={roundTimestampToNearestFiveMinutes}
              interval={calculateInterval}
              tick={{
                fontSize: "1rem",
                fontFamily: "Avenir Next, sans-serif",
              }}
            ></XAxis>
            <YAxis
              axisLine={{ stroke: "#8D9096" }}
              tickFormatter={getTickFormatter(metric?.display_name)}
              tick={{
                fontSize: "1rem",
                fontFamily: "Avenir Next, sans-serif",
              }}
            />
            <CartesianGrid stroke="#f5f5f5"></CartesianGrid>

            {chartLabels?.map((value, index) => (
              <Line
                key={`${value}-line-chart`}
                type="monotone"
                dataKey={`${value}`}
                stroke={getRandomColor(index)}
                dot={false}
              />
            ))}

            <Tooltip
              wrapperStyle={{ pointerEvents: "auto" }}
              content={
                <CustomTooltip
                  displayName={metric?.display_name}
                  pinnedTooltip={pinnedTooltip}
                  setPinnedTooltip={setPinnedTooltip}
                  setTooltipX={setTooltipX}
                  setTooltipY={setTooltipY}
                />
              }
              position={{ x: tooltipX, y: tooltipY }}
            />
          </LineChart>
        </ResponsiveContainer>
      )}

      {!isLoading && !error && transformedData?.length === 0 && <EmptyChart />}
    </Box>
  );
};

export default LineChartComponent;
