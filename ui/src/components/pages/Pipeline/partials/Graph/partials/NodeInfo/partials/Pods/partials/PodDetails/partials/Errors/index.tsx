import React from "react";
import Box from "@mui/material/Box";
import { ContainerDropdown } from "./partials/ContainerDropdown";

export type ErrorDetails = {
  container: string;
  timestamp: string;
  code: string;
  message: string;
  details: string;
};

interface ErrorsProps {
  containers: string[];
}

const detailsTest: ErrorDetails[] = [
  {
    container: "container1",
    timestamp: "2023-10-27T10:00:00Z",
    code: "Internal",
    message: "Failed to connect to database",
    details: "Connection timed out after 5 seconds.",
  },
  {
    container: "container2",
    timestamp: "2023-10-27T10:15:00Z",
    code: "Internal",
    message: "Invalid input data",
    details: "The provided input data does not match the expected format.",
  },
];

export const Errors = ({ containers }: ErrorsProps) => {
  return (
    <Box>
      {containers
        ?.filter((c) => c != "numa")
        ?.map((c) => (
          <Box sx={{ mb: "1rem" }}>
            <ContainerDropdown container={c} details={detailsTest} />
          </Box>
        ))}
    </Box>
  );
};
