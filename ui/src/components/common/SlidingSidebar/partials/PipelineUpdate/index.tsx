import React, { useCallback, useContext, useEffect, useState } from "react";
import YAML from "yaml";
import Box from "@mui/material/Box";
import {
  SpecEditor,
  Status,
  StatusIndicator,
  ValidationMessage,
} from "../../../SpecEditor";
import { SpecEditorSidebarProps } from "../..";
import { AppContextProps } from "../../../../../types/declarations/app";
import { AppContext } from "../../../../../App";
import { getAPIResponseError, getBaseHref } from "../../../../../utils";
import { usePipelineUpdateFetch } from "../../../../../utils/fetchWrappers/pipelineUpdateFetch";

import "./style.css";

export function PipelineUpdate({
  initialYaml,
  namespaceId,
  pipelineId,
  viewType,
  titleOverride,
  onUpdateComplete,
  setModalOnClose,
}: SpecEditorSidebarProps) {
  const [loading, setLoading] = useState(false);
  const [validationPayload, setValidationPayload] = useState<any>(undefined);
  const [submitPayload, setSubmitPayload] = useState<any>(undefined);
  const [validationMessage, setValidationMessage] = useState<
    ValidationMessage | undefined
  >();
  const [status, setStatus] = useState<StatusIndicator | undefined>();
  const [updatedPipelineId, setUpdatedPipelineId] = useState<
    string | undefined
  >();
  const { host } = useContext<AppContextProps>(AppContext);

  const { pipelineAvailable } = usePipelineUpdateFetch({
    namespaceId,
    pipelineId: updatedPipelineId,
    active: !!updatedPipelineId,
  });

  // Call update complete on dismount if pipeline was updated
  useEffect(() => {
    return () => {
      if (updatedPipelineId) {
        onUpdateComplete && onUpdateComplete();
      }
    };
  }, [updatedPipelineId, onUpdateComplete]);

  // Track update process and close on completion
  useEffect(() => {
    if (!updatedPipelineId) {
      return;
    }
    let timer: any;
    if (pipelineAvailable) {
      setStatus((prev) => {
        const existing = prev ? { ...prev } : {};
        return {
          ...existing,
          processing: {
            status: Status.SUCCESS,
            message: "Pipeline updated successfully",
          },
        };
      });
      timer = setTimeout(() => {
        onUpdateComplete && onUpdateComplete();
      }, 1000);
    } else {
      setStatus((prev) => {
        const existing = prev ? { ...prev } : {};
        return {
          ...existing,
          processing: {
            status: Status.LOADING,
            message: "Pipeline is being updated...",
          },
        };
      });
    }
    return () => {
      clearTimeout(timer);
    };
  }, [updatedPipelineId, pipelineAvailable, onUpdateComplete]);

  // Submit API call
  useEffect(() => {
    const postData = async () => {
      setStatus({
        submit: {
          status: Status.LOADING,
          message: "Submitting pipeline update...",
          allowRetry: false,
        },
      });
      try {
        const response = await fetch(
          `${host}${getBaseHref()}/api/v1/namespaces/${namespaceId}/pipelines/${pipelineId}?dry-run=false`,
          {
            method: "PUT",
            headers: {
              "Content-Type": "application/json",
            },
            body: JSON.stringify(submitPayload),
          }
        );
        const error = await getAPIResponseError(response);
        if (error) {
          setValidationMessage({
            type: "error",
            message: error,
          });
          setStatus(undefined);
        } else {
          setStatus({
            submit: {
              status: Status.SUCCESS,
              message: "Pipeline update submitted successfully",
              allowRetry: false,
            },
          });
          setUpdatedPipelineId(submitPayload.metadata?.name || "default");
          setModalOnClose && setModalOnClose(undefined);
        }
      } catch (e: any) {
        setValidationMessage({
          type: "error",
          message: e.message,
        });
        setStatus(undefined);
      }
    };

    if (submitPayload) {
      postData();
    }
  }, [namespaceId, pipelineId, submitPayload, setModalOnClose, host]);

  // Validation API call
  useEffect(() => {
    const postData = async () => {
      setLoading(true);
      try {
        const response = await fetch(
          `${host}${getBaseHref()}/api/v1/namespaces/${namespaceId}/pipelines/${pipelineId}?dry-run=true`,
          {
            method: "PUT",
            headers: {
              "Content-Type": "application/json",
            },
            body: JSON.stringify(validationPayload),
          }
        );
        const error = await getAPIResponseError(response);
        if (error) {
          setValidationMessage({
            type: "error",
            message: error,
          });
        } else {
          setValidationMessage({
            type: "success",
            message: "Successfully validated",
          });
        }
      } catch (e: any) {
        setValidationMessage({
          type: "error",
          message: `Error: ${e.message}`,
        });
      } finally {
        setLoading(false);
        setValidationPayload(undefined);
      }
    };

    if (validationPayload) {
      postData();
    }
  }, [namespaceId, pipelineId, validationPayload, host]);

  const handleValidate = useCallback((value: string) => {
    let parsed: any;
    try {
      parsed = YAML.parse(value);
    } catch (e: any) {
      setValidationMessage({
        type: "error",
        message: `Invalid YAML: ${e.message}`,
      });
      return;
    }
    if (!parsed) {
      setValidationMessage({
        type: "error",
        message: "Error: no spec provided.",
      });
      return;
    }
    setValidationPayload(parsed);
    setValidationMessage(undefined);
  }, []);

  const handleSubmit = useCallback((value: string) => {
    let parsed: any;
    try {
      parsed = YAML.parse(value);
    } catch (e: any) {
      setValidationMessage({
        type: "error",
        message: `Invalid YAML: ${e.message}`,
      });
      return;
    }
    if (!parsed) {
      setValidationMessage({
        type: "error",
        message: "Error: no spec provided.",
      });
      return;
    }
    setSubmitPayload(parsed);
    setValidationMessage(undefined);
  }, []);

  const handleReset = useCallback(() => {
    setStatus(undefined);
    setValidationMessage(undefined);
  }, []);

  const handleMutationChange = useCallback(
    (mutated: boolean) => {
      if (!setModalOnClose) {
        return;
      }
      if (mutated) {
        setModalOnClose({
          message: "Are you sure you want to discard your changes?",
          iconType: "warn",
        });
      } else {
        setModalOnClose(undefined);
      }
    },
    [setModalOnClose]
  );

  return (
    <Box
      sx={{
        display: "flex",
        flexDirection: "column",
        height: "100%",
      }}
    >
      <Box
        sx={{
          display: "flex",
          flexDirection: "row",
          marginBottom: "3.2rem",
        }}
      >
        <span className="pipeline-spec-header-text">
          {titleOverride ? titleOverride : `Edit Pipeline: ${pipelineId}`}
        </span>
      </Box>
      <SpecEditor
        initialYaml={initialYaml}
        viewType={viewType}
        loading={loading}
        onValidate={handleValidate}
        onSubmit={handleSubmit}
        onResetApplied={handleReset}
        onMutatedChange={handleMutationChange}
        statusIndicator={status}
        validationMessage={validationMessage}
      />
    </Box>
  );
}
