import moment from 'moment';
import React, { useState } from 'react';
import 'bootstrap/dist/css/bootstrap.min.css';
import 'bootstrap-daterangepicker/daterangepicker.css';
import DateRangePicker from 'react-bootstrap-daterangepicker';
import 'jquery';

interface TimeSelectorProps {
  setMetricReq: any;
}

const TimeSelector = ({setMetricReq}: TimeSelectorProps) => {


  const [startDate, setStartDate] = useState(moment().subtract(29, 'days'));
  const [endDate, setEndDate] = useState(moment());

  const handleCallback = (start: moment.Moment, end: moment.Moment) => {
    setStartDate(start);
    setEndDate(end);
    setMetricReq((prev: any) => ({ 
      ...prev, 
      start_time: start, 
      end_time: end 
    }));
  };

  const ranges: { [key: string]: [moment.Moment, moment.Moment] } = {
    'Last 30 Minutes': [moment().subtract(30, 'minutes'), moment()],
    'Last Hour': [moment().subtract(1, 'hour'), moment()],
    'Last 2 Hours': [moment().subtract(2, 'hours'), moment()],
    'Last 6 Hours': [moment().subtract(6, 'hours'), moment()],
    'Last 12 Hours': [moment().subtract(12, 'hours'), moment()],
    'Last 24 Hours': [moment().subtract(24, 'hours'), moment()]
  };

  return (
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
      <input type="text" className="form-control" />
    </DateRangePicker>
  );
};

export default TimeSelector;
