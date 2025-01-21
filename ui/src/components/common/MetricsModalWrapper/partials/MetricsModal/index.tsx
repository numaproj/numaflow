import React, { useCallback, useContext, useState } from "react";
import Box from "@mui/material/Box";
import Modal from "@mui/material/Modal";
import IconButton from "@mui/material/IconButton";
import CloseIcon from "@mui/icons-material/Close";
import { Metrics } from "../../../../pages/Pipeline/partials/Graph/partials/NodeInfo/partials/Pods/partials/PodDetails/partials/Metrics";
import {
  VertexDetailsContext,
  VertexDetailsContextProps,
} from "../../../SlidingSidebar/partials/VertexDetails";
import { metricNameMap } from "../../../../pages/Pipeline/partials/Graph/partials/NodeInfo/partials/Pods/partials/PodDetails/partials/Metrics/utils/constants";

const style = {
  position: "absolute",
  top: "50%",
  left: "50%",
  transform: "translate(-50%, -50%)",
  height: "60%",
  width: "80%",
  bgcolor: "background.paper",
  boxShadow: 24,
  p: 4,
};

interface MetricsModalProps {
  open: boolean;
  handleClose: () => void;
  metricName: string;
  namespaceId: string;
  pipelineId: string;
  vertexId: string;
  type: string;
}

export function MetricsModal({
  open,
  handleClose,
  metricName,
  namespaceId,
  pipelineId,
  vertexId,
  type,
}: MetricsModalProps) {
  const vertexDetailsContext =
    useContext<VertexDetailsContextProps>(VertexDetailsContext);
  const { setVertexTab, setPodsViewTab, setExpanded } = vertexDetailsContext;

  const [metricsFound, setMetricsFound] = useState<boolean>(false);

  const handleRedirect = useCallback(() => {
    handleClose();
    setVertexTab(0);
    setPodsViewTab(1);
    const panelId = `${metricName}-panel`;
    setExpanded((prevExpanded) => {
      const newExpanded = new Set(prevExpanded);
      newExpanded.add(panelId);
      return newExpanded;
    });
  }, [handleClose, setVertexTab, setPodsViewTab, metricName, setExpanded]);

  return (
    <Modal
      open={open}
      onClose={handleClose}
      aria-labelledby="buffer-details-title"
      aria-describedby="buffer-details-description"
    >
      <Box sx={style}>
        <Box
          sx={{
            display: "flex",
            justifyContent: "space-between",
          }}
        >
          <Box sx={{ fontSize: "1.6rem" }}>{metricNameMap[metricName]}</Box>
          <IconButton onClick={handleClose}>
            <CloseIcon fontSize="large" />
          </IconButton>
        </Box>
        <Box>
          <Metrics
            namespaceId={namespaceId}
            pipelineId={pipelineId}
            vertexId={vertexId}
            type={type}
            metricName={metricName}
            setMetricsFound={setMetricsFound}
          />
        </Box>
        {metricsFound && (
          <Box
            sx={{
              display: "flex",
              flexDirection: "row-reverse",
              textDecoration: "underline",
              color: "#0077C5",
              cursor: "pointer",
              mt: "0.5rem",
            }}
            onClick={handleRedirect}
          >
            Click to see detailed view with additional filters
          </Box>
        )}
      </Box>
    </Modal>
  );
}
