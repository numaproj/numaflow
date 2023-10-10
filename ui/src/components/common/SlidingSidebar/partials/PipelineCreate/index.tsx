import React, { useCallback, useEffect, useState } from "react";
import YAML from "yaml";
import Box from "@mui/material/Box";
import {
  SpecEditor,
  Status,
  StatusIndicator,
  ValidationMessage,
} from "../../../SpecEditor";
import { SpecEditorSidebarProps } from "../..";
import { getAPIResponseError } from "../../../../../utils";
import { usePipelineUpdateFetch } from "../../../../../utils/fetchWrappers/pipelineUpdateFetch";

import "./style.css";

const INITIAL_VALUE = `apiVersion: numaflow.numaproj.io/v1alpha1
kind: Pipeline
metadata:
  name: simple-pipeline
spec:
  vertices:
    - name: in
      source:
        # A self data generating source
        generator:
          rpu: 5
          duration: 1s
    - name: cat
      udf:
        builtin:
          name: cat # A built-in UDF which simply cats the message
    - name: out
      sink:
        # A simple log printing sink
        log: {}
  edges:
    - from: in
      to: cat
    - from: cat
      to: out`;

export function PiplineCreate({
  namespaceId,
  viewType,
  onUpdateComplete,
  setModalOnClose,
}: SpecEditorSidebarProps) {
  const [loading, setLoading] = useState<boolean>(false);
  const [validationPayload, setValidationPayload] = useState<any>(undefined);
  const [submitPayload, setSubmitPayload] = useState<any>(undefined);
  const [validationMessage, setValidationMessage] = useState<
    ValidationMessage | undefined
  >();
  const [status, setStatus] = useState<StatusIndicator | undefined>();
  const [createdPipelineId, setCreatedPipelineId] = useState<
    string | undefined
  >();

  const { pipelineAvailable } = usePipelineUpdateFetch({
    namespaceId,
    pipelineId: createdPipelineId,
    active: !!createdPipelineId,
  });

  // Track creation process and close on completion
  useEffect(() => {
    if (!createdPipelineId) {
      return;
    }
    let timer: number;
    if (pipelineAvailable) {
      setStatus((prev) => {
        const existing = prev ? { ...prev } : {};
        return {
          ...existing,
          processing: {
            status: Status.SUCCESS,
            message: "Pipeline created successfully",
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
            message: "Pipeline is being created...",
          },
        };
      });
    }
    return () => {
      clearTimeout(timer);
    };
  }, [createdPipelineId, pipelineAvailable, onUpdateComplete]);

  // Submit API call
  useEffect(() => {
    const postData = async () => {
      setStatus({
        submit: {
          status: Status.LOADING,
          message: "Submitting pipeline...",
          allowRetry: false,
        },
      });
      try {
        const response = await fetch(
          `/api/v1/namespaces/${namespaceId}/pipelines?dry-run=false`,
          {
            method: "POST",
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
              message: "Pipeline submitted successfully",
              allowRetry: false,
            },
          });
          setCreatedPipelineId(submitPayload.metadata?.name || "default");
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
  }, [namespaceId, submitPayload, setModalOnClose]);

  // Validation API call
  useEffect(() => {
    const postData = async () => {
      setLoading(true);
      try {
        const response = await fetch(
          `/api/v1/namespaces/${namespaceId}/pipelines?dry-run=true`,
          {
            method: "POST",
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
  }, [namespaceId, validationPayload]);

  const handleValidate = useCallback((value: string) => {
    let parsed: any;
    try {
      parsed = YAML.parse(value);
    } catch (e) {
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
    } catch (e) {
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
          marginBottom: "2rem",
        }}
      >
        <span className="pipeline-spec-header-text">Create Pipeline</span>
      </Box>
      <SpecEditor
        initialYaml={INITIAL_VALUE}
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
