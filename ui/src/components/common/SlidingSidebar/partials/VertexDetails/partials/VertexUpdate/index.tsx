import React, { useState, useEffect, useCallback } from "react";
import Box from "@mui/material/Box";
import YAML from "yaml";
import { getAPIResponseError } from "../../../../../../../utils";
import { ValidationMessage } from "../../../../../SpecEditor/partials/ValidationMessage";
import { SpecEditor, ViewType } from "../../../../../SpecEditor";
import { SpecEditorModalProps } from "../../../..";

import "./style.css";

export interface VertexUpdateProps {
  namespaceId: string;
  pipelineId: string;
  vertexId: string;
  vertexSpec: any;
  setModalOnClose?: (props: SpecEditorModalProps | undefined) => void;
}

export function VertexUpdate({
  namespaceId,
  pipelineId,
  vertexId,
  vertexSpec,
  setModalOnClose,
}: VertexUpdateProps) {
  const [loading, setLoading] = useState(false);
  const [validationPayload, setValidationPayload] = useState<any>(undefined);
  const [submitPayload, setSubmitPayload] = useState<any>(undefined);
  const [contextComponent, setContextComponent] = useState<
    React.ReactNode | undefined
  >();
  const [mutationKey, setMutationKey] = useState<string>("");
  const [currentSpec, setCurrentSpec] = useState<any>(vertexSpec);

  // Submit API call
  useEffect(() => {
    const postData = async () => {
      setLoading(true);
      try {
        const response = await fetch(
          `/api/v1/namespaces/${namespaceId}/pipelines/${pipelineId}/vertices/${vertexId}?dry-run=false`,
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
          setContextComponent(
            <ValidationMessage
              type="error"
              title="Submission Error"
              content={error}
            />
          );
        } else {
          setContextComponent(
            <ValidationMessage
              type="success"
              title="Successfully submitted"
              content=""
            />
          );
          // Set current spec to submitted spec so any additional edits are noticed
          setCurrentSpec(submitPayload.value);
          // Set to not mutated on successful submit
          setMutationKey("id" + Math.random().toString(16).slice(2));
          // Clear success message after some time
          setTimeout(() => {
            setContextComponent(undefined);
          }, 2000);
        }
      } catch (e: any) {
        setContextComponent(
          <ValidationMessage
            type="error"
            title="Submission Error"
            content={`Error: ${e.message}`}
          />
        );
      } finally {
        setLoading(false);
        setSubmitPayload(undefined);
      }
    };

    if (submitPayload) {
      postData();
    }
  }, [namespaceId, pipelineId, vertexId, submitPayload]);

  // Validation API call
  useEffect(() => {
    const postData = async () => {
      setLoading(true);
      try {
        const response = await fetch(
          `/api/v1/namespaces/${namespaceId}/pipelines/${pipelineId}/vertices/${vertexId}?dry-run=true`,
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
          setContextComponent(
            <ValidationMessage
              type="error"
              title="Validation Error"
              content={error}
            />
          );
        } else {
          setContextComponent(
            <ValidationMessage
              type="success"
              title="Successfully validated"
              content=""
            />
          );
        }
      } catch (e: any) {
        setContextComponent(
          <ValidationMessage
            type="error"
            title="Validation Error"
            content={`Error: ${e.message}`}
          />
        );
      } finally {
        setLoading(false);
        setValidationPayload(undefined);
      }
    };

    if (validationPayload) {
      postData();
    }
  }, [namespaceId, pipelineId, vertexId, validationPayload]);

  const handleValidate = useCallback((value: string) => {
    let parsed;
    try {
      parsed = YAML.parse(value);
    } catch (e) {
      setContextComponent(
        <ValidationMessage
          type="error"
          title="Validation Error"
          content={`Invalid YAML: ${e.message}`}
        />
      );
      return;
    }
    if (!parsed) {
      setContextComponent(
        <ValidationMessage
          type="error"
          title="Validation Error"
          content="No spec provided."
        />
      );
      return;
    }
    setValidationPayload(parsed);
    setContextComponent(undefined);
  }, []);

  const handleSubmit = useCallback((value: string) => {
    let parsed;
    try {
      parsed = YAML.parse(value);
    } catch (e) {
      setContextComponent(
        <ValidationMessage
          type="error"
          title="Validation Error"
          content={`Invalid YAML: ${e.message}`}
        />
      );
      return;
    }
    if (!parsed) {
      setContextComponent(
        <ValidationMessage
          type="error"
          title="Validation Error"
          content="No spec provided."
        />
      );
      return;
    }
    setSubmitPayload({ parsed, value });
    setContextComponent(undefined);
  }, []);

  const handleReset = useCallback(() => {
    setContextComponent(undefined);
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
        viewType={ViewType.TOGGLE_EDIT}
        loading={loading}
        onValidate={handleValidate}
        onSubmit={handleSubmit}
        onResetApplied={handleReset}
        contextComponent={contextComponent}
        mutationKey={mutationKey}
        editResetKey={mutationKey}
        onMutatedChange={handleMutationChange}
      />
    </Box>
  );
}
