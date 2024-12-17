import {useState } from "react";
import Box from "@mui/material/Box";

import React from "react";
import Select from "@mui/material/Select";
import MenuItem from "@mui/material/MenuItem";
import FormControl from "@mui/material/FormControl";
import InputLabel from "@mui/material/InputLabel";

interface DropdownProps {
  options: string[];
  selected: string;
  onChange: (value: string) => void;
}

const Dropdown: React.FC<DropdownProps> = ({ options, selected, onChange }) => {
  return (
    <FormControl fullWidth>
      <InputLabel sx={{ fontSize: "1.4rem" }}>Time Range </InputLabel>
      <Select
        value={selected}
        onChange={(event) => onChange(event.target.value as string)}
        label="Preset"
        sx={{ fontSize: "1.6rem" }}
      >
        {options.map((option) => (
          <MenuItem key={option} value={option} sx={{ fontSize: "1.4rem" }}>
            {option}
          </MenuItem>
        ))}
        
      </Select>
    </FormControl>
  );
};

interface TimePreset {
  label: string;
  start: Date;
  end: Date;
}

interface TimePresetSelectorProps {
  setMetricReq: any;
}

const timePresets: TimePreset[] = [
  // { label: "2H Dec 13 2024 05:45 am to Dec 13 2024 07:45 am", start: new Date("2024-12-13T05:45:00"), end: new Date("2024-12-13T07:45:00") },
  { label: "1 H", start: new Date(Date.now() - 3600 * 1000), end: new Date() },
  { label: "30 mins", start: new Date(Date.now() - 1800 * 1000), end: new Date() },
];

const TimePresetSelector = ({setMetricReq}: TimePresetSelectorProps) => {
  const [selectedPreset, setSelectedPreset] = useState<TimePreset>(timePresets[0]);

  const handlePresetChange = (preset: TimePreset) => {
    setSelectedPreset(preset);
    setMetricReq((prev: any) => ({ ...prev, ["start_time"]: preset.start, ["end_time"]: preset.end }));
  };

  return (
    <Box>
      <Dropdown
        options={timePresets.map((preset) => preset.label)}
        selected={selectedPreset.label}
        onChange={(label: string) => {
          const preset = timePresets.find((p) => p.label === label);
          if (preset) {
            handlePresetChange(preset);
          }
        }}
      />
      {/* Add custom time window setter here*/}
    </Box>
  );
};

export default TimePresetSelector;
