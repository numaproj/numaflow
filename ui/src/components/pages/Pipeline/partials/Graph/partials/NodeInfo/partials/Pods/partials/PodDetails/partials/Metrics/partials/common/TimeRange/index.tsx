import * as React from "react";
import Datetime from 'react-datetime';
import 'react-datetime/css/react-datetime.css';

interface TimeSelectorProps {
  setMetricReq: any;
}

const TimeSelector = ({setMetricReq}: TimeSelectorProps) => {
  const [startValue, setStartValue] = React.useState(new Date(Date.now() - 30 * 60000));
  const [endValue, setEndValue] = React.useState(new Date());

  const handleStartChange = (newValue: any) => {
    setStartValue(newValue);
    setMetricReq((prev: any) => ({ 
      ...prev, 
      start_time: newValue, 
      end_time: endValue 
    }));
  };

  const handleEndChange = (newValue: any) => {
    setEndValue(newValue);
    setMetricReq((prev: any) => ({ 
      ...prev, 
      start_time: startValue, 
      end_time: newValue 
    }));
  };

  const presets = [
    {
      label: '10m',
      value: [new Date(Date.now() - 10 * 60000), new Date()]
    },
    {
      label: '30m',
      value: [new Date(Date.now() - 30 * 60000), new Date()]
    },
    {
      label: '1h',
      value: [new Date(Date.now() - 60 * 60000), new Date()]
    },
    {
      label: '2h',
      value: [new Date(Date.now() - 120 * 60000), new Date()]
    }
  ];

  const applyPreset = (preset: any) => {
    setStartValue(preset.value[0]);
    setEndValue(preset.value[1]);
    setMetricReq((prev: any) => ({ 
      ...prev, 
      start_time: preset.value[0], 
      end_time: preset.value[1] 
    }));
  };

  return (
    <div style={{ display: 'flex', gap: '10px' }}>
      <Datetime
        value={startValue}
        onChange={handleStartChange}
        dateFormat="YYYY-MM-DD"
        timeFormat="HH:mm"
      />
      <Datetime
        value={endValue}
        onChange={handleEndChange}
        dateFormat="YYYY-MM-DD"
        timeFormat="HH:mm"
      />
      <div>
        {presets.map((preset) => (
          <button key={preset.label} onClick={() => applyPreset(preset)}>
            {preset.label}
          </button>
        ))}
      </div>
    </div>
  );
};

export default TimeSelector;
