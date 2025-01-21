import React, { useCallback, useState } from "react";
import Tooltip from "@mui/material/Tooltip";
import Box from "@mui/material/Box";
import { MetricsModal } from "./partials/MetricsModal";

import "./style.css";

interface MetricsModalWrapperProps {
  namespaceId: string;
  pipelineId: string;
  vertexId: string;
  type: string;
  metricName: string;
  value: any;
}

export function MetricsModalWrapper({
  namespaceId,
  pipelineId,
  vertexId,
  type,
  metricName,
  value,
}: MetricsModalWrapperProps) {
  const [open, setOpen] = useState<boolean>(false);

  const handleOpen = useCallback(() => {
    setOpen(true);
  }, []);
  const handleClose = useCallback(() => {
    setOpen(false);
  }, []);

  return (
    <Box>
      <Tooltip
        title={
          <Box sx={{ fontSize: "1rem" }}>
            Click to get more information about the trend
          </Box>
        }
        placement={"top-start"}
        arrow
      >
        <Box className="metrics-hyperlink" onClick={() => handleOpen()}>
          {value}
        </Box>
      </Tooltip>
      <MetricsModal
        open={open}
        handleClose={handleClose}
        metricName={metricName}
        namespaceId={namespaceId}
        pipelineId={pipelineId}
        vertexId={vertexId}
        type={type}
      />
    </Box>
  );
}
