import moment from 'moment';
import React, { useState } from 'react';
import 'bootstrap/dist/css/bootstrap.min.css';
import 'bootstrap-daterangepicker/daterangepicker.css';
import DateRangePicker from 'react-bootstrap-daterangepicker';
import 'jquery';
import './TimeSelector.css';

interface TimeSelectorProps {
  setMetricReq: any;
}

const TimeSelector = ({setMetricReq}: TimeSelectorProps) => {
  const [startDate, setStartDate] = useState(moment().subtract(1, 'hour'));
  const [endDate, setEndDate] = useState(moment());

  const handleCallback = (start: moment.Moment, end: moment.Moment) => {
    setStartDate(start);
    setEndDate(end);
    setMetricReq((prev: any) => ({ 
      ...prev, 
      start_time: start.format(), 
      end_time: end.format() 
    }));
  };

  const ranges: { [key: string]: [moment.Moment, moment.Moment] } = {
    'Last 10 Minutes': [moment().subtract(10, 'minutes'), moment()],
    'Last 30 Minutes': [moment().subtract(30, 'minutes'), moment()],
    'Last Hour': [moment().subtract(1, 'hour'), moment()],
    'Last 2 Hours': [moment().subtract(2, 'hours'), moment()],
    'Last 6 Hours': [moment().subtract(6, 'hours'), moment()],
    'Last 12 Hours': [moment().subtract(12, 'hours'), moment()],
  };

  return (
    <div className="date-range-picker-container">
      <label htmlFor="dateRangeInput" className="date-range-label">
        Time Range
      </label>
      <DateRangePicker
        initialSettings={{
          startDate: startDate,
          endDate: endDate,
          ranges: ranges,
          timePicker: true,
          timePicker24Hour: true,
          locale: {
            format: 'DD/MM hh:mm A'
          }
        }}
        onCallback={handleCallback}
      >
        <input type="text" 
         id="dateRangeInput"
          className="form-control date-input" 
          style={{ fontSize: '1.6rem', width: '300px', paddingRight: '10px', height: '50px' }} 
        />
      </DateRangePicker>
      <span className="caret">&#9662;</span>
    </div>
  );
};

export default TimeSelector;
