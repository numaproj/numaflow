import React, { useMemo } from "react";
import Box from "@mui/material/Box";
import { Slide, ToastContainer } from "react-toastify";

import "./style.css";

export interface ErrorsProps {
  errors: boolean;
}

// eslint-disable-next-line @typescript-eslint/no-unused-vars
export function Errors({ errors }: ErrorsProps) {
  const header = useMemo(() => {
    const headerContainerStyle = {
      display: "flex",
      flexDirection: "row",
    };
    const textClass = "vertex-details-header-text";

    return (
      <Box sx={headerContainerStyle}>
        <span className={textClass}>Errors</span>
      </Box>
    );
  }, []);

  return (
    <Box
      sx={{
        display: "flex",
        flexDirection: "column",
        height: "100%",
        borderColor: "divider",
      }}
    >
      <Box sx={{ marginTop: "1rem", borderBottom: 1, borderColor: "divider" }}>
        {header}
      </Box>
      <ToastContainer
        position="bottom-right"
        autoClose={5000}
        hideProgressBar={false}
        newestOnTop={false}
        limit={11}
        closeOnClick={false}
        rtl={false}
        draggable={true}
        pauseOnHover={true}
        transition={Slide}
        theme="light"
      />
    </Box>
  );
}
