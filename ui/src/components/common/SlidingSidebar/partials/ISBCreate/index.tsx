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

import "./style.css";

const INITIAL_VALUE = `#
# This manifest is intended for demonstration purpose, it's not suitable for production.
# Check https://numaflow.numaproj.io/core-concepts/inter-step-buffer-service/ to figure out reliable configuration for production.
#
apiVersion: numaflow.numaproj.io/v1alpha1
kind: InterStepBufferService
metadata:
  name: default
spec:
  jetstream:
    version: latest # Do NOT use "latest" in real deployment, check "numaflow-controller-config" ConfigMap to get available versions.
    # Optional. Specifying "persistence" will create a PersistentVolumeClaim for data persistence, it works for most of Kubernetes 
    # clusters, including Kind, Minikube, K3s, etc, it's needed to run production workloads.
    persistence:
      volumeSize: 3Gi
`;

export function ISBCreate({
  namespaceId,
  viewType,
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
  const { host } = useContext<AppContextProps>(AppContext);

  // Submit API call
  useEffect(() => {
    const postData = async () => {
      setStatus({
        submit: {
          status: Status.LOADING,
          message: "Submitting isb service...",
          allowRetry: false,
        },
      });
      try {
        const response = await fetch(
          `${host}${getBaseHref()}/api/v1/namespaces/${namespaceId}/isb-services?dry-run=false`,
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
              message: "ISB Service created successfully",
              allowRetry: false,
            },
          });
          if (onUpdateComplete) {
            // Give small grace period before callling complete (allows user to see message)
            setTimeout(() => {
              onUpdateComplete();
            }, 1000);
          }
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
  }, [namespaceId, submitPayload, onUpdateComplete, host]);

  // Validation API call
  useEffect(() => {
    const postData = async () => {
      setLoading(true);
      try {
        const response = await fetch(
          `${host}${getBaseHref()}/api/v1/namespaces/${namespaceId}/isb-services?dry-run=true`,
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
  }, [namespaceId, validationPayload, host]);

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
          marginBottom: "2rem",
        }}
      >
        <span className="isb-spec-header-text">Create ISB Service</span>
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
        allowNonMutatedSubmit
      />
    </Box>
  );
}
