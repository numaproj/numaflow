import { useCallback, useEffect, useState } from "react";
import {
  CartesianGrid,
  Legend,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";
import Box from "@mui/material/Box";
import CircularProgress from "@mui/material/CircularProgress";
import Dropdown from "../common/Dropdown";
import TimeRange from "../common/TimeRange";
import FiltersDropdown from "../common/FiltersDropdown";
import EmptyChart from "../EmptyChart";
import { useMetricsFetch } from "../../../../../../../../../../../../../../../utils/fetchWrappers/metricsFetch";


const getYAxisLabel = (metricName: string) => {
  console.log("metric name: ", metricName)
  switch(metricName){
    case "forwarder_data_read_total":
    case "monovtx_read_total":
      return { value: 'messages per second', angle: -90, position: 'left', offset: 20 }
    case "monovtx_pending":
        return { value: 'number of pending messages', angle: -90, position: 'left', offset: 20 }
    default:
      return {}
  }
};

const getTickFormatter = (metricName: string) => {
  return (value: number) => {
    switch(metricName){
      case "monovtx_ack_time_bucket":
      case "monovtx_read_time_bucket":
      case "monovtx_processing_time_bucket":
      case "monovtx_sink_time_bucket":
        if (value < 1000) {
          return `${value} μs`;
        } else if (value < 1000000) {
          return `${(value / 1000).toFixed(1)} ms`;
        } else {
          return `${(value / 1000000).toFixed(1)} s`;
        }
      default:
        return value.toString();
    }
  }
};

// TODO have a check for metricReq against metric object to ensure required fields are passed
const LineChartComponent = ({ namespaceId, pipelineId, type, metric, vertexId }: any) => {
  const [transformedData, setTransformedData] = useState<any[]>([]);
  const [chartLabels, setChartLabels] = useState<any[]>([]);
  const [metricsReq, setMetricsReq] = useState<any>({
    metric_name: metric?.metric_name,
  });
  const [paramsList, setParamsList] = useState<any[]>([]);
  // store all filters for each selected dimension
  const [filtersList, setFiltersList] = useState<any[]>([]);
  const [filters, setFilters] = useState<any>({});
  const [previousDimension, setPreviousDimension] = useState(metricsReq?.dimension);

  const getRandomColor = useCallback((index: number) => {
    const hue = (index * 137.508) % 360;
    return `hsl(${hue}, 70%, 50%)`;
  }, []);

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
        default:
          return "";
      }
    },
    [namespaceId, pipelineId]
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

  const groupByLabel = useCallback((dimension: string) => {
    switch (dimension) {
      case "mono-vertex":
        return "mvtx_name";
      default:
        return dimension;
    }
  }, []);

  const updateChartData = useCallback(() => {
    if (chartData) {
      const labels: any[] = [];
      const transformedData: any[] = [];
      const label = groupByLabel(metricsReq?.dimension);
      chartData?.forEach((item) => {
        const labelVal = item?.metric?.[label];
        labels.push(labelVal);
        item?.values?.forEach(([timestamp, value]: [number, string]) => {
          const date = new Date(timestamp * 1000);
          const hours = date.getHours().toString().padStart(2, "0");
          const minutes = date.getMinutes().toString().padStart(2, "0");
          const formattedTime = `${hours}:${minutes}`;
          const ele = transformedData?.find(
            (data) => data?.time === formattedTime
          );
          if (!ele) {
            const dataObject: Record<string, any> = { time: formattedTime };
            dataObject[labelVal] = parseFloat(value);
            transformedData.push(dataObject);
          } else {
            ele[labelVal] = parseFloat(value);
          }
        });
      });
      transformedData.sort((a, b) => {
        const [hoursA, minutesA] = a.time.split(":").map(Number);
        const [hoursB, minutesB] = b.time.split(":").map(Number);
        return hoursA * 60 + minutesA - (hoursB * 60 + minutesB);
      });
      setChartLabels(labels);
      setTransformedData(transformedData);
    }
  }, [chartData, metricsReq, groupByLabel]);

  useEffect(() => {
    if (chartData) updateChartData();
  }, [chartData, updateChartData]);

  if (paramsList?.length === 0) return <></>;

  return (
    <Box>
      <Box
        sx={{
          display: "flex",
          justifyContent: "space-around",
          mt: "1rem",
          mb: "2rem",
        }}
      >
        {paramsList
          ?.filter((param) => !["start_time", "end_time"]?.includes(param.name))
          ?.map((param: any) => {
            return (
              <Box
                key={`line-chart-${param.name}`}
                sx={{ minWidth: 120, fontSize: "2rem" }}
              >
                <Dropdown
                  metric={metric}
                  type={type}
                  field={param.name}
                  setMetricReq={setMetricsReq}
                />
              </Box>
            );
          })}

        {paramsList
          ?.filter((param) => ["start_time", "end_time"]?.includes(param.name))
          ?.map((param: any) => {
            return (
              <Box key={`line-chart-${param.name}`}>
                <TimeRange field={param.name} setMetricReq={setMetricsReq} />
              </Box>
            );
          })}
      </Box>

      {filtersList?.filter((filterEle: any) => !filterEle?.required)?.length >
        0 && (
        <Box
          sx={{
            display: "flex",
            alignItems: "center",
            justifyContent: "space-around",
            mt: "1rem",
            mb: "2rem",
            px: "6rem",
          }}
        >
          <Box sx={{ mr: "1rem" }}>Filters</Box>
          <FiltersDropdown
            items={filtersList?.filter(
              (filterEle: any) => !filterEle?.required
            )}
            namespaceId={namespaceId}
            pipelineId={pipelineId}
            type={type}
            vertexId={vertexId}
            setFilters={setFilters}
          />
        </Box>
      )}

      {isLoading && (
        <Box
          sx={{
            display: "flex",
            justifyContent: "center",
            alignItems: "center",
            height: "100%",
          }}
        >
          <CircularProgress />
        </Box>
      )}

      {!isLoading && error && <EmptyChart />}

      {!isLoading && !error && transformedData?.length > 0 && (
        <ResponsiveContainer width="100%" height={400}>
          <LineChart
            data={transformedData}
            margin={{
              top: 5,
              right: 30,
              left: 30,
              bottom: 5,
            }}
          >
            <CartesianGrid strokeDasharray="3 3" />
            <XAxis dataKey="time" padding={{ left: 30, right: 30 }} >
            </XAxis>
            <YAxis 
            label={getYAxisLabel(metric?.metric_name)}
            tickFormatter={getTickFormatter(metric?.metric_name)}
            />
            <CartesianGrid stroke="#f5f5f5">
            </CartesianGrid>
            
            {chartLabels?.map((value, index) => (
              <Line
                key={`${value}-line-chart`}
                type="monotone"
                dataKey={`${value}`}
                stroke={getRandomColor(index)}
                activeDot={{ r: 8 }}
              />
            ))}

            <Tooltip />
            <Legend />
          </LineChart>
        </ResponsiveContainer>
      )}

      {!isLoading && !error && transformedData?.length === 0 && <EmptyChart />}
    </Box>
  );
};

export default LineChartComponent;
