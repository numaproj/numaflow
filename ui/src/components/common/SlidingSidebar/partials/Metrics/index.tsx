import { useState } from "react";
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, Legend } from 'recharts';
import "./style.css";
import { Filters, useMetricsFetch } from "../../../../../utils/fetchWrappers/metricsFetch";
import { EmptyChartState } from "../../../EmptyStates";
import { Box, CircularProgress, TextField } from "@mui/material";
import { AdapterDayjs } from '@mui/x-date-pickers/AdapterDayjs';
import { DateTimePicker, LocalizationProvider } from '@mui/x-date-pickers';
import dayjs from "dayjs";

export interface MetricsProps {
    namespaceId: string;
    pipelineId: string,
    type: string,
    vertexId?: string,
}

export function Metrics ({
    namespaceId,
    pipelineId,
    type,
    vertexId,
  }: MetricsProps
){

  let transformedData: any[] = [];
  const [dimension, setDimension] = useState('pod'); 
  const [duration, setDuration] = useState('5m');
  const [quantile, setQuantile] = useState('0.95');
  const [metricName, setMetricName] = useState('monovtx_processing_time_bucket');
  const [startTime, setStartTime] = useState<dayjs.Dayjs | null>(null);
  const [endTime, setEndTime] = useState<dayjs.Dayjs | null>(null);

  let filters: Filters = {
    "namespace": namespaceId,
  }

  if (type === "monoVertex"){
    filters = {
      ...filters,
      "mvtx_name": pipelineId
    };

  } else {
    filters = {
      ...filters,
      "pipeline": pipelineId,
    };
  }

  // conditionally add filters based on Discovery API
  filters = {
    ...filters,
    "pod": "simple-mono-vertex-mv-0-usneq"
  }

  const {chartData, error, setShouldFetch, isLoading} = useMetricsFetch({
    metricName: metricName,
    dimension: dimension,
    filters: filters,
    duration: duration,
    quantile: quantile,
    startTime: startTime,
    endTime: endTime
  });


  const handleStartTimeChange = (newValue: dayjs.Dayjs | null) => {
    if (newValue && newValue.isValid()) { 
      setStartTime(newValue);
      setShouldFetch(true); 
    }
  };

  const handleEndTimeChange = (newValue: dayjs.Dayjs | null) => {
    if (newValue && newValue.isValid()) {
      setEndTime(newValue);
      setShouldFetch(true); 
    }
  };


  transformedData = chartData?.map(([timestamp, value]) =>{
    const date = new Date(timestamp * 1000);
    const hours = date.getHours().toString().padStart(2, '0'); 
    const minutes = date.getMinutes().toString().padStart(2, '0');
    const formattedTime = `${hours}:${minutes}`;
    return{
      time: formattedTime,
      value: parseFloat(value), 
    }
  })
  

  return (
    <div>

     { /*Input Controls*/}
     <div>
        <label htmlFor="metricName">Metric Name:</label>
        <select 
          id="metricName" 
          value={metricName} 
          onChange={(e) => {
            setMetricName(e.target.value);
            setShouldFetch(true);
          }}
        >
          {/* Add options for different metric names --> get from discovery API */}
          <option value="monovtx_processing_time_bucket">Mono Vertex Processing Time Latency</option>
          <option value="monovtx_sink_time_bucket">Mono Vertex Sink Latency</option>
        </select>

     </div>

     <div>
        <label htmlFor="dimension">Dimension:</label>
        <select 
          id="dimension" 
          value={dimension} 
          onChange={(e) => {
            setDimension(e.target.value);
            setShouldFetch(true);
          }}
        >
          {/* Add options for different dimensions --> get from Discovery API*/}
          <option value="pod">Pod</option>
          <option value="mono-vertex">MonoVertex</option>
        </select>
     </div>


     <div>
        <label htmlFor="quantile">Quantile:</label>
        <select 
          id="quantile" 
          value={quantile} 
          onChange={(e) => {
            setQuantile(e.target.value);
            setShouldFetch(true);
          }}
        >
          {/* Add options for different quantiles*/}
          <option value="0.90">90th Percentile</option>
          <option value="0.99">99th Percentile</option>
          <option value="0.95">95th Percentile</option>
          <option value="0.50">50th Percentile</option>
          {/* ... more options */}
        </select>
     </div>


     <div>
        <label htmlFor="duration">Duration:</label>
        <select 
          id="duration" 
          value={duration} 
          onChange={(e) => {
            setDuration(e.target.value);
            setShouldFetch(true);
          }}
        >
          {/* Add options for different durations*/}
          <option value="5m">5 mins</option>
          <option value="10m">10 mins</option>
          {/* ... more options */}
        </select>
     </div>


     <LocalizationProvider dateAdapter={AdapterDayjs}>
      <div>
        {/* ... other content */}
          <DateTimePicker 
            label="Start Time" 
            value={startTime} 
            onChange={handleStartTimeChange}
          />
          <DateTimePicker 
            label="End Time" 
            value={endTime} 
            onChange={handleEndTimeChange}
          />
      </div>
    </LocalizationProvider>

      {isLoading ? (<Box
          sx={{
            display: "flex",
            justifyContent: "center",
            alignItems: "center",
            height: "100%",
          }}
        >
          <CircularProgress />
        </Box>) : error ? (<EmptyChartState />) : transformedData?.length > 0 ? ( // should have a different error component
        <LineChart width={800} height={300} data={transformedData}>
          <CartesianGrid strokeDasharray="3 3" />
          <XAxis dataKey="time" padding={{ left: 30, right: 30 }} />
          <YAxis />
          <CartesianGrid stroke="#f5f5f5" />
          <Line type="monotone" dataKey="value" stroke="#8884d8" activeDot={{ r: 8 }} />
          <Tooltip />
          <Legend />
        </LineChart>
      ) : (
        <EmptyChartState /> 
      )}
    </div>
  );
}