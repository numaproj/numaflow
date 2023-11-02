import React, { useCallback, useMemo, useState } from "react";
import Box from "@mui/material/Box";
import Modal from "@mui/material/Modal";
import Button from "@mui/material/Button";
import { getAPIResponseError } from "../../../../../utils";
import { ValidationMessage } from "../../../../common/SpecEditor/partials/ValidationMessage";
import CircularProgress from "@mui/material/CircularProgress";

import "./style.css";

export interface DeleteModalProps {
  onDeleteCompleted: () => void;
  onCancel: () => void;
  type: "pipeline" | "isb";
  namespaceId: string;
  pipelineId?: string;
  isbId?: string;
}

export function DeleteModal({
  onDeleteCompleted,
  onCancel,
  type,
  namespaceId,
  pipelineId,
  isbId,
}: DeleteModalProps) {
  const [success, setSuccess] = useState<boolean>(false);
  const [error, setError] = useState<string | undefined>(undefined);
  const [loading, setLoading] = useState<boolean>(false);

  const handleDelete = useCallback(async () => {
    try {
      setLoading(true);
      setError(undefined);
      let url: string;
      switch (type) {
        case "pipeline":
          url = `/api/v1/namespaces/${namespaceId}/pipelines/${pipelineId}`;
          break;
        case "isb":
          url = `/api/v1/namespaces/${namespaceId}/isb-services/${isbId}`;
          break;
        default:
          return;
      }
      const response = await fetch(url, {
        method: "DELETE",
      });
      const error = await getAPIResponseError(response);
      if (error) {
        setError(error);
      } else {
        setSuccess(true);
        // Call complete after some time to view status update
        setTimeout(() => {
          onDeleteCompleted();
        }, 1000);
      }
    } catch (e: any) {
      setError(e.message);
    } finally {
      setLoading(false);
    }
  }, [type, namespaceId, pipelineId, isbId, onDeleteCompleted]);

  const content = useMemo(() => {
    const containerStyle = {
      display: "flex",
      flexDirection: "column",
      position: "absolute",
      top: "50%",
      left: "50%",
      transform: "translate(-50%, -50%)",
      bgcolor: "background.paper",
      borderRadius: "0.3125rem",
      boxShadow: 24,
      padding: "2rem",
    };
    const buttonContainerStyle = {
      display: "flex",
      flexDirection: "row",
      alignItems: "center",
      justifyContent: "space-evenly",
      marginTop: "1rem",
    };
    if ((type === "pipeline" && !pipelineId) || (type === "isb" && !isbId)) {
      return <Box sx={containerStyle}>Missing Props</Box>;
    }
    if (error) {
      return (
        <Box sx={containerStyle}>
          <ValidationMessage
            type="error"
            title={error}
            content={`Delete ${type === "pipeline" ? "Pipeline" : "ISB"}: ${
              type === "pipeline" ? pipelineId : isbId
            }`}
          />
          <Box sx={buttonContainerStyle}>
            <Button
              onClick={handleDelete}
              variant="contained"
              color="secondary"
            >
              Try again
            </Button>
            <Button onClick={onCancel} variant="outlined" color="primary">
              Cancel
            </Button>
          </Box>
        </Box>
      );
    }
    if (loading) {
      return (
        <Box sx={containerStyle}>
          <Box sx={{ display: "flex", justifyContent: "center" }}>
            <CircularProgress />
          </Box>
          <Box sx={buttonContainerStyle}>
            <Button variant="contained" color="secondary" disabled>
              Delete
            </Button>
            <Button variant="outlined" color="primary" disabled>
              Cancel
            </Button>
          </Box>
        </Box>
      );
    }
    if (success) {
      return (
        <Box sx={containerStyle}>
          <ValidationMessage
            type="success"
            title="Successfully deleted"
            content=""
          />
        </Box>
      );
    }
    return (
      <Box sx={containerStyle}>
        <ValidationMessage
          type="error"
          title="Are you sure?"
          content={`Delete ${type === "pipeline" ? "Pipeline" : "ISB"}: ${
            type === "pipeline" ? pipelineId : isbId
          }`}
        />
        <Box sx={buttonContainerStyle}>
          <Button
            onClick={handleDelete}
            variant="contained"
            color="secondary"
            data-testid="delete-confirmation-button"
          >
            Delete
          </Button>
          <Button onClick={onCancel} variant="outlined" color="primary">
            Cancel
          </Button>
        </Box>
      </Box>
    );
  }, [type, namespaceId, pipelineId, isbId, loading, error, success]);

  return <Modal open={true}>{content}</Modal>;
}
