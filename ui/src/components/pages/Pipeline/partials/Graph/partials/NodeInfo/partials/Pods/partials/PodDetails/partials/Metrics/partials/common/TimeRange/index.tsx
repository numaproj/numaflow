import React, { useEffect, useState } from "react";
import moment from "moment";
import "jquery";
import "bootstrap/dist/css/bootstrap.min.css";
import "bootstrap-daterangepicker/daterangepicker.css";
import DateRangePicker from "react-bootstrap-daterangepicker";
import { ArrowDropDown } from "@mui/icons-material";
import Box from "@mui/material/Box";

import "./style.css";

interface TimeSelectorProps {
  setMetricReq: any;
}

const TimeSelector = ({ setMetricReq }: TimeSelectorProps) => {
  const [startDate, setStartDate] = useState(moment().subtract(1, "hour"));
  const [endDate, setEndDate] = useState(moment());
  const [isOpen, setIsOpen] = useState<boolean>(false);

  const handleToggle = () => {
    setIsOpen(!isOpen);
  };

  const handleCallback = (start: moment.Moment, end: moment.Moment) => {
    setStartDate(start);
    setEndDate(end);
    setMetricReq((prev: any) => ({
      ...prev,
      start_time: start.format(),
      end_time: end.format(),
    }));
  };

  useEffect(() => {
    handleCallback(moment().subtract(1, "hour"), moment());
  }, []);

  const ranges: { [key: string]: [moment.Moment, moment.Moment] } = {
    "Last 10 Minutes": [moment().subtract(10, "minutes"), moment()],
    "Last 30 Minutes": [moment().subtract(30, "minutes"), moment()],
    "Last Hour": [moment().subtract(1, "hour"), moment()],
    "Last 2 Hours": [moment().subtract(2, "hours"), moment()],
    "Last 6 Hours": [moment().subtract(6, "hours"), moment()],
    "Last 12 Hours": [moment().subtract(12, "hours"), moment()],
  };

  return (
    <Box className="date-range-picker-container">
      <Box className="date-range-label">Time Range</Box>
      <DateRangePicker
        initialSettings={{
          startDate: startDate,
          endDate: endDate,
          ranges: ranges,
          timePicker: true,
          locale: {
            format: "MMM DD YYYY hh:mm:ss A",
          },
        }}
        onCallback={handleCallback}
        onShow={handleToggle}
        onHide={handleToggle}
      >
        <input
          type="text"
          id="dateRangeInput"
          className="form-control date-input"
          placeholder="Select a date range"
        />
      </DateRangePicker>
      <span className={`caret ${isOpen ? "open" : ""}`}>
        <ArrowDropDown />
      </span>
    </Box>
  );
};

export default TimeSelector;
