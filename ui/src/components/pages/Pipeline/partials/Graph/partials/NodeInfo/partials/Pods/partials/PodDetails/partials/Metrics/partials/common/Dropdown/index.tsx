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
  presets?: any;
}

const Dropdown = ({
  metric,
  type,
  field,
  setMetricReq,
  presets,
}: MetricDropDownProps) => {
  // to handle cases there is no "mono-vertex" as dimension at top level (for eg: container level cpu/memory)
  const initialDimensionValue = useMemo(() => {
    if (!metric?.dimensions?.length) return metric?.dimensions[0]?.name;

    return metric?.dimensions?.find(
      (val: any) => val?.name === dimensionReverseMap[type]
    )?.name;
  }, [metric, type]);

  const getInitialValue = useMemo(() => {
    switch (field) {
      case "dimension":
        return initialDimensionValue;
      case "quantile":
        return presets?.quantile ?? quantileOptions[quantileOptions.length - 1];
      case "duration":
        return presets?.duration ?? durationOptions[0];
      default:
        return "";
    }
  }, [field, initialDimensionValue, quantileOptions, durationOptions, presets]);

  const [value, setValue] = useState<string>(getInitialValue);

  const fieldName = useMemo(() => {
    const capitalizedField = field.charAt(0).toUpperCase() + field.slice(1);
    return capitalizedField === "Duration" ? "Query Window" : capitalizedField;
  }, [field]);

  // Update metricsReq with the initial value
  useEffect(() => {
    setMetricReq((prev: any) => ({ ...prev, [field]: getInitialValue }));
  }, [getInitialValue, field, setMetricReq]);

  const getDropDownEntries = useMemo(() => {
    switch (field) {
      case "dimension":
        return metric?.dimensions?.map((dimension: any) => (
          <MenuItem
            key={`dropdown-${dimension?.name}`}
            value={dimension?.name}
            sx={{ fontSize: "1.4rem" }}
          >
            {dimensionMap[dimension?.name]}
          </MenuItem>
        ));
      case "quantile":
        return quantileOptions?.map((quantile: string) => (
          <MenuItem
            key={`dropdown-${quantile}`}
            value={quantile}
            sx={{ fontSize: "1.4rem" }}
          >
            {quantileMap[quantile]}
          </MenuItem>
        ));
      case "duration":
        return durationOptions?.map((duration: string) => (
          <MenuItem
            key={`dropdown-${duration}`}
            value={duration}
            sx={{ fontSize: "1.4rem" }}
          >
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
        sx={{ fontSize: "1.6rem", height: "50px" }}
      >
        {getDropDownEntries}
      </Select>
    </FormControl>
  );
};

export default Dropdown;
