import React, { useState, useEffect, useCallback, useContext } from "react";
import Box from "@mui/material/Box";
import YAML from "yaml";
import { getAPIResponseError, getBaseHref } from "../../../../../../../utils";
import {
  SpecEditor,
  ViewType,
  Status,
  StatusIndicator,
  ValidationMessage,
} from "../../../../../SpecEditor";
import { SpecEditorModalProps } from "../../../..";
import { AppContextProps } from "../../../../../../../types/declarations/app";
import { AppContext } from "../../../../../../../App";

import "./style.css";

export interface VertexUpdateProps {
  namespaceId: string;
  pipelineId: string;
  vertexId: string;
  vertexSpec: any;
  setModalOnClose?: (props: SpecEditorModalProps | undefined) => void;
  refresh: () => void;
}

export function VertexUpdate({
  namespaceId,
  pipelineId,
  vertexId,
  vertexSpec,
  setModalOnClose,
  refresh,
}: VertexUpdateProps) {
  const [loading, setLoading] = useState(false);
  const [validationPayload, setValidationPayload] = useState<any>(undefined);
  const [submitPayload, setSubmitPayload] = useState<any>(undefined);
  const [validationMessage, setValidationMessage] = useState<
    ValidationMessage | undefined
  >();
  const [status, setStatus] = useState<StatusIndicator | undefined>();
  const [mutationKey, setMutationKey] = useState<string>("");
  const [currentSpec, setCurrentSpec] = useState<any>(vertexSpec);
  const { host, isReadOnly } = useContext<AppContextProps>(AppContext);

  // Submit API call
  useEffect(() => {
    const postData = async () => {
      setStatus({
        submit: {
          status: Status.LOADING,
          message: "Submitting vertex update...",
          allowRetry: false,
        },
      });
      try {
        const response = await fetch(
          `${host}${getBaseHref()}/api/v1/namespaces/${namespaceId}/pipelines/${pipelineId}/vertices/${vertexId}?dry-run=false`,
          {
            method: "PUT",
            headers: {
              "Content-Type": "application/json",
            },
            body: JSON.stringify(submitPayload.parsed),
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
              message: "Vertex updated successfully",
              allowRetry: false,
            },
          });
          // Set current spec to submitted spec so any additional edits are noticed
          setCurrentSpec(submitPayload.value);
          // Set to not mutated on successful submit
          setMutationKey("id" + Math.random().toString(16).slice(2));
          // Clear success message after some time
          setTimeout(() => {
            setStatus(undefined);
            setValidationMessage(undefined);
            refresh();
          }, 1000);
        }
      } catch (e: any) {
        setValidationMessage({
          type: "error",
          message: e.message,
        });
        setStatus(undefined);
      } finally {
        setSubmitPayload(undefined);
      }
    };

    if (submitPayload) {
      postData();
    }
  }, [namespaceId, pipelineId, vertexId, submitPayload, host]);

  // Validation API call
  useEffect(() => {
    const postData = async () => {
      setLoading(true);
      try {
        const response = await fetch(
          `${host}${getBaseHref()}/api/v1/namespaces/${namespaceId}/pipelines/${pipelineId}/vertices/${vertexId}?dry-run=true`,
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
  }, [namespaceId, pipelineId, vertexId, validationPayload, host]);

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
    setSubmitPayload({ parsed, value });
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
      <SpecEditor
        initialYaml={currentSpec}
        viewType={isReadOnly ? ViewType.READ_ONLY : ViewType.TOGGLE_EDIT}
        loading={loading}
        mutationKey={mutationKey}
        editResetKey={mutationKey}
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
