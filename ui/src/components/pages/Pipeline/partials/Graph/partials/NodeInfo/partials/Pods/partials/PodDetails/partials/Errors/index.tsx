import React, { useState } from "react";
import Box from "@mui/material/Box";
import { ContainerDropdown } from "./partials/ContainerDropdown";
import { ErrorDetails } from "../../../../../../../../../../../../../types/declarations/pods";

interface ErrorsProps {
  containers?: string[];
  details?: ErrorDetails[];
}

export const Errors = ({ containers, details }: ErrorsProps) => {
  const [refreshKey, setRefreshKey] = useState<string>("");

  const handleRefresh = () => {
    setRefreshKey(Math.random().toString(16).slice(2));
  };

  const filteredContainers = containers?.filter((c) => c !== "numa");

  if (!filteredContainers || filteredContainers.length === 0)
    return <Box sx={{ fontSize: "1.4rem", p: "1.5rem" }}>No errors found</Box>;

  return (
    <Box>
      {filteredContainers?.map((c, idx) => (
        <Box key={`container-${idx}`} sx={{ my: "1rem" }}>
          <ContainerDropdown
            container={c}
            details={details?.filter((d) => d.container === c)}
            onRefresh={handleRefresh}
          />
        </Box>
      ))}
    </Box>
  );
};
