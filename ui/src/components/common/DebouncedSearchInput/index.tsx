import React, { useCallback, useState, useEffect } from "react";
import TextField from "@mui/material/TextField";
import InputAdornment from "@mui/material/InputAdornment";
import SearchIcon from "@mui/icons-material/Search";

import "./style.css";

export interface DebouncedSearchInputProps {
  disabled?: boolean;
  placeHolder?: string;
  onChange: (value: string) => void;
}

export function DebouncedSearchInput({
  disabled = false,
  placeHolder,
  onChange,
}: DebouncedSearchInputProps) {
  const [timerId, setTimerId] = useState<any | undefined>();

  const debounceValue = useCallback(
    (updatedValue: string) => {
      if (timerId) {
        clearTimeout(timerId);
      }
      setTimerId(setTimeout(() => onChange(updatedValue), 500));
    },
    [onChange, timerId]
  );

  const handleInputChange = useCallback(
    (event: { target: { value: string } }) => {
      debounceValue(event.target.value);
    },
    [debounceValue]
  );

  useEffect(() => {
    // Clear timer on dismount
    return () => {
      if (timerId) {
        clearTimeout(timerId);
      }
    };
  }, [timerId]);

  return (
    <TextField
      sx={{
        background: "#FFFFFF",
        flexGrow: "2",
        maxWidth: "39.375rem",
        minWidth: "15.625rem",
        border: "1px solid #6B6C72",
        borderRadius: "0.25rem",
      }}
      variant="outlined"
      placeholder={placeHolder}
      disabled={disabled}
      InputProps={{
        startAdornment: (
          <InputAdornment position="start">
            <SearchIcon sx={{ color: "#241C15" }} />
          </InputAdornment>
        ),
      }}
      onChange={handleInputChange}
      data-testid="debounced-search-input"
    />
  );
}
