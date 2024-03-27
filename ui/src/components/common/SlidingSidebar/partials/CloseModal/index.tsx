import React, { useMemo } from "react";
import Box from "@mui/material/Box";
import Modal from "@mui/material/Modal";
import Button from "@mui/material/Button";
import SuccessIcon from "@mui/icons-material/CheckCircle";
import WarnIcon from "../../../../../images/warning-triangle.png";

import "./style.css";

export interface CloseModalProps {
  onConfirm: () => void;
  onCancel: () => void;
  message?: string;
  iconType?: "info" | "warn";
}

export function CloseModal({
  onConfirm,
  onCancel,
  message = "Are sure you want to close this sidebar?",
  iconType,
}: CloseModalProps) {
  const icon = useMemo(() => {
    switch (iconType) {
      case "info":
        return (
          <SuccessIcon
            data-testid="info-icon"
            fontSize="large"
            color="success"
          />
        );
      case "warn":
        return (
          <img
            data-testid="warn-icon"
            src={WarnIcon}
            alt="Warn"
            className="close-modal-warn-icon"
          />
        );
      default:
        return null;
    }
  }, [iconType]);

  return (
    <Modal open={true}>
      <Box
        sx={{
          display: "flex",
          flexDirection: "column",
          position: "absolute",
          top: "50%",
          left: "50%",
          transform: "translate(-50%, -50%)",
          bgcolor: "background.paper",
          borderRadius: "0.5rem",
          boxShadow: 24,
          padding: "3.2rem",
        }}
      >
        <Box
          sx={{
            display: "flex",
            flexDirection: "row",
            alignItems: "center",
            justifyContent: "space-around",
            marginBottom: "1.6rem",
          }}
        >
          {icon}
          <span className="close-modal-message">{message}</span>
        </Box>
        <Box
          sx={{
            display: "flex",
            flexDirection: "row",
            alignItems: "center",
            justifyContent: "space-evenly",
          }}
        >
          <Button
            data-testid="close-modal-confirm"
            onClick={onConfirm}
            variant="contained"
            color="primary"
            sx={{
              fontSize: "1.4rem",
            }}
          >
            Confirm
          </Button>
          <Button
            data-testid="close-modal-cancel"
            onClick={onCancel}
            variant="outlined"
            color="primary"
            sx={{
              fontSize: "1.4rem",
            }}
          >
            Cancel
          </Button>
        </Box>
      </Box>
    </Modal>
  );
}
