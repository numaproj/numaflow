import React, { useCallback, useState, useEffect } from "react";
import TextField from "@mui/material/TextField";
import { styled } from "@mui/material/styles";
import InputAdornment from "@mui/material/InputAdornment";
import SearchIcon from "@mui/icons-material/Search";

import "./style.css";

const CssTextField = styled(TextField)({
  background: "#FFFFFF !important",
  border:
    "1px solid var(--neutral-peppercorn-a-30, rgba(36, 28, 21, 0.30)) !important",
  "& label.Mui-focused": {
    border: 0,
  },
  "& .MuiInput-underline:after": {
    border: 0,
  },
  "& .MuiOutlinedInput-root": {
    "& fieldset": {
      border: 0,
    },
    "&:hover fieldset": {
      border: 0,
    },
    "&.Mui-focused fieldset": {
      border: 0,
    },
  },
});

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
  const [timerId, setTimerId] = useState<number | undefined>();

  const debounceValue = useCallback((updatedValue: string) => {
    if (timerId) { 
      clearTimeout(timerId);
    }
    setTimerId(setTimeout(() => onChange(updatedValue), 500));
  }, [onChange, timerId]);

  const handleInputChange = useCallback(
    (event: { target: { value: string } }) => {
      debounceValue(event.target.value);
    },
    [debounceValue]
  );

  useEffect(() => {
    // Clear timer on dismount
    return () =>{
      if (timerId) {
        clearTimeout(timerId);
      }
    };
  }, [timerId]);

  return (
    <CssTextField
      sx={{
        background: "#FFFFFF",
        width: "50%",
        maxWidth: "39.375rem",
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
    />
  );
}
