import { useCallback, useEffect, useMemo, useState } from "react";
import Box from "@mui/material/Box";
import { DateTimePicker, LocalizationProvider } from "@mui/x-date-pickers";
import { AdapterDayjs } from "@mui/x-date-pickers/AdapterDayjs";
import dayjs from "dayjs";

export interface MetricTimeRangeProps {
  field: string;
  setMetricReq: any;
}

const TimeRange = ({ field, setMetricReq }: MetricTimeRangeProps) => {
  const getInitialValue = useMemo(() => {
    switch (field) {
      case "start_time":
        return dayjs().subtract(1, "hour");
      case "end_time":
        return dayjs();
      default:
        return null;
    }
  }, [field]);

  const [time, setTime] = useState<dayjs.Dayjs | null>(getInitialValue);

  // Update metricsReq with the initial value
  useEffect(() => {
    setMetricReq((prev: any) => ({ ...prev, [field]: getInitialValue }));
  }, [getInitialValue, field, setMetricReq]);

  const handleTimeChange = useCallback(
    (newValue: dayjs.Dayjs | null) => {
      if (newValue && newValue.isValid()) {
        setTime(newValue);
        setMetricReq((prev: any) => ({ ...prev, [field]: newValue }));
      }
    },
    [setTime]
  );

  return (
    <LocalizationProvider dateAdapter={AdapterDayjs}>
      <Box>
        <DateTimePicker
          label="Start Time"
          value={time}
          onChange={handleTimeChange}
          sx={{
            "& .MuiInputLabel-root": {
              fontSize: "1.4rem",
            },
            "& .MuiInputBase-input": {
              fontSize: "1.6rem",
            },
          }}
        />
      </Box>
    </LocalizationProvider>
  );
};

export default TimeRange;
