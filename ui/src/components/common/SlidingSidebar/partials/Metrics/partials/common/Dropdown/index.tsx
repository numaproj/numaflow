import { useEffect, useMemo, useState } from "react";
import FormControl from "@mui/material/FormControl";
import InputLabel from "@mui/material/InputLabel";
import Select from "@mui/material/Select";
import MenuItem from "@mui/material/MenuItem";
import {
  dimensionMap,
  dimensionReverseMap,
  durationMap,
  durationOptions,
  quantileMap,
  quantileOptions,
} from "../../../utils/constants";

export interface MetricDropDownProps {
  metric: any;
  type: string;
  field: string;
  setMetricReq: any;
}

const Dropdown = ({
  metric,
  type,
  field,
  setMetricReq,
}: MetricDropDownProps) => {
  const getInitialValue = useMemo(() => {
    switch (field) {
      case "dimension":
        return dimensionReverseMap[type];
      case "quantile":
        return quantileOptions[0];
      case "duration":
        return durationOptions[0];
      default:
        return "";
    }
  }, [field, dimensionReverseMap, type, quantileOptions, durationOptions]);

  const [value, setValue] = useState<string>(getInitialValue);
  const fieldName = field.charAt(0).toUpperCase() + field.slice(1);

  // Update metricsReq with the initial value
  useEffect(() => {
    setMetricReq((prev: any) => ({ ...prev, [field]: getInitialValue }));
  }, [getInitialValue, field, setMetricReq]);

  const getDropDownEntries = useMemo(() => {
    switch (field) {
      case "dimension":
        return metric?.dimensions?.map((dimension: any) => (
          <MenuItem key={`dropdown-${dimension?.name}`} value={dimension?.name}>
            {dimensionMap[dimension?.name]}
          </MenuItem>
        ));
      case "quantile":
        return quantileOptions?.map((quantile: string) => (
          <MenuItem key={`dropdown-${quantile}`} value={quantile}>
            {quantileMap[quantile]}
          </MenuItem>
        ));
      case "duration":
        return durationOptions?.map((duration: string) => (
          <MenuItem key={`dropdown-${duration}`} value={duration}>
            {durationMap[duration]}
          </MenuItem>
        ));
      default:
        return <></>;
    }
  }, [
    field,
    metric,
    dimensionMap,
    quantileOptions,
    quantileMap,
    durationOptions,
    durationMap,
  ]);

  return (
    <FormControl fullWidth>
      <InputLabel id={`${field}-select-label`} sx={{ fontSize: "1.4rem" }}>
        {fieldName}
      </InputLabel>
      <Select
        labelId={`${field}-select-label`}
        id={`${field}-select`}
        value={value}
        label={fieldName}
        onChange={(e) => {
          setValue(e.target.value);
          setMetricReq((prev: any) => ({ ...prev, [field]: e.target.value }));
        }}
        sx={{ fontSize: "1.6rem" }}
      >
        {getDropDownEntries}
      </Select>
    </FormControl>
  );
};

export default Dropdown;
