import React, { useCallback, useContext, useMemo } from "react";
import Box from "@mui/material/Box";
import Grid from "@mui/material/Grid";
import Paper from "@mui/material/Paper";
import Button from "@mui/material/Button";
import { AppContextProps } from "../../../../../types/declarations/app";
import { AppContext } from "../../../../../App";

import "./style.css";

export function Errors() {
  const { errors, clearErrors } = useContext<AppContextProps>(AppContext);

  const handleClear = useCallback(() => {
    clearErrors();
  }, [clearErrors]);

  const content = useMemo(() => {
    const paperStyle = {
      display: "flex",
      flexDirection: "column",
      padding: "1rem",
    };
    return (
      <Grid
        container
        spacing={2}
        sx={{ marginTop: "0.5rem", justifyContent: "center" }}
      >
        {!errors.length && (
          <Grid item xs={12}>
            <Paper elevation={0} sx={paperStyle}>
              No errors
            </Paper>
          </Grid>
        )}
        {errors.map((error) => (
          <Grid item xs={12}>
            <Paper elevation={0} sx={paperStyle}>
              <span className="errors-message-text">{error.message}</span>
              <span>{error.date.toLocaleTimeString()}</span>
            </Paper>
          </Grid>
        ))}
        {!!errors.length && (
          <Button
            sx={{ marginTop: "1rem" }}
            onClick={handleClear}
            variant="outlined"
            color="primary"
          >
            Clear
          </Button>
        )}
      </Grid>
    );
  }, [errors]);

  return (
    <Box
      sx={{
        display: "flex",
        flexDirection: "column",
        height: "100%",
      }}
    >
      <span className="errors-header-text">Errors</span>
      {content}
    </Box>
  );
}
