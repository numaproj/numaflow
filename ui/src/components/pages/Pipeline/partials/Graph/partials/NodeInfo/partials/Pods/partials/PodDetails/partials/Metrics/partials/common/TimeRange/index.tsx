import React, { useEffect, useRef, useState } from "react";
import moment from "moment";
import "jquery";
import "bootstrap/dist/css/bootstrap.min.css";
import "bootstrap-daterangepicker/daterangepicker.css";
import DateRangePicker from "react-bootstrap-daterangepicker";
import { ArrowDropDown } from "@mui/icons-material";
import FormControl from "@mui/material/FormControl";
import InputAdornment from "@mui/material/InputAdornment";
import InputLabel from "@mui/material/InputLabel";
import OutlinedInput from "@mui/material/OutlinedInput";

import "./style.css";

interface TimeSelectorProps {
  setMetricReq: any;
  initialStart?: string | null;
  initialEnd?: string | null;
}

const formatMetricTime = (value: moment.Moment): string =>
  value.toDate().toISOString();

const sameMetricInstant = (
  left: string | null | undefined,
  right: string
): boolean => {
  if (!left) return false;
  const leftMs = Date.parse(left);
  const rightMs = Date.parse(right);
  return (
    !Number.isNaN(leftMs) &&
    !Number.isNaN(rightMs) &&
    leftMs === rightMs
  );
};

const TimeSelector = ({
  setMetricReq,
  initialStart,
  initialEnd,
}: TimeSelectorProps) => {
  const [startDate, setStartDate] = useState(
    initialStart ? moment(initialStart) : moment().subtract(1, "hour")
  );
  const [endDate, setEndDate] = useState(initialEnd ? moment(initialEnd) : moment());
  const [isOpen, setIsOpen] = useState<boolean>(false);
  const lastAppliedUrlRangeRef = useRef<string>();

  const handleToggle = () => {
    setIsOpen(!isOpen);
  };

  const handleCallback = (start: moment.Moment, end: moment.Moment) => {
    setStartDate(start);
    setEndDate(end);
    const startTime = formatMetricTime(start);
    const endTime = formatMetricTime(end);
    setMetricReq((prev: any) => {
      if (
        sameMetricInstant(prev.start_time, startTime) &&
        sameMetricInstant(prev.end_time, endTime)
      ) {
        return prev;
      }
      return {
        ...prev,
        start_time: startTime,
        end_time: endTime,
      };
    });
  };

  useEffect(() => {
    if (initialStart || initialEnd) return;
    const startTime = formatMetricTime(startDate);
    const endTime = formatMetricTime(endDate);
    setMetricReq((prev: any) => {
      if (
        sameMetricInstant(prev.start_time, startTime) &&
        sameMetricInstant(prev.end_time, endTime)
      ) {
        return prev;
      }
      return {
        ...prev,
        start_time: startTime,
        end_time: endTime,
      };
    });
  }, [initialStart, initialEnd, setMetricReq]);

  useEffect(() => {
    if (!initialStart || !initialEnd) {
      lastAppliedUrlRangeRef.current = undefined;
      return;
    }
    const urlRange = `${initialStart}\n${initialEnd}`;
    if (lastAppliedUrlRangeRef.current === urlRange) return;

    const nextStart = moment(initialStart);
    const nextEnd = moment(initialEnd);
    if (!nextStart.isValid() || !nextEnd.isValid()) return;
    lastAppliedUrlRangeRef.current = urlRange;

    const startTime = formatMetricTime(nextStart);
    const endTime = formatMetricTime(nextEnd);
    setStartDate((current) =>
      current.isSame(nextStart) ? current : nextStart
    );
    setEndDate((current) => (current.isSame(nextEnd) ? current : nextEnd));
    setMetricReq((prev: any) => {
      if (
        sameMetricInstant(prev.start_time, startTime) &&
        sameMetricInstant(prev.end_time, endTime)
      ) {
        return prev;
      }
      return {
        ...prev,
        start_time: startTime,
        end_time: endTime,
      };
    });
  }, [initialStart, initialEnd, setMetricReq]);

  const ranges: { [key: string]: [moment.Moment, moment.Moment] } = {
    "Last 10 Minutes": [moment().subtract(10, "minutes"), moment()],
    "Last 30 Minutes": [moment().subtract(30, "minutes"), moment()],
    "Last Hour": [moment().subtract(1, "hour"), moment()],
    "Last 2 Hours": [moment().subtract(2, "hours"), moment()],
    "Last 6 Hours": [moment().subtract(6, "hours"), moment()],
    "Last 12 Hours": [moment().subtract(12, "hours"), moment()],
  };

  const rangeLabel = `${startDate.format(
    "MMM DD YYYY hh:mm:ss A"
  )} - ${endDate.format("MMM DD YYYY hh:mm:ss A")}`;

  return (
    <FormControl className="date-range-form" focused={isOpen}>
      <InputLabel
        shrink
        htmlFor="dateRangeInput"
        className="dropdown-input-label"
      >
        Time Range
      </InputLabel>
      <OutlinedInput
        notched
        readOnly
        label="Time Range"
        value={rangeLabel}
        className="date-range-outlined dropdown-select-value"
        endAdornment={
          <InputAdornment
            position="end"
            className={`date-range-caret${isOpen ? " open" : ""}`}
          >
            <ArrowDropDown />
          </InputAdornment>
        }
        inputProps={{ tabIndex: -1, "aria-hidden": true }}
      />
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
          className="date-range-proxy"
          aria-label="Time Range"
          readOnly
        />
      </DateRangePicker>
    </FormControl>
  );
};

export default TimeSelector;
