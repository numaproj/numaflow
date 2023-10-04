import React, { useCallback, useEffect, useState } from "react";
import YAML from "yaml";
import Box from "@mui/material/Box";
import { SpecEditor } from "../../../SpecEditor";
import { SpecEditorSidebarProps } from "../..";
import { ValidationMessage } from "../../../SpecEditor/partials/ValidationMessage";
import { getAPIResponseError } from "../../../../../utils";

import "./style.css";

const INITIAL_VALUE =
  "# Add pipeline spec and submit to create a new pipeline.\n";

export function PiplineCreate({
  namespaceId,
  viewType,
  onUpdateComplete,
  setModalOnClose,
}: SpecEditorSidebarProps) {
  const [loading, setLoading] = useState(false);
  const [validationPayload, setValidationPayload] = useState<any>(undefined);
  const [submitPayload, setSubmitPayload] = useState<any>(undefined);
  const [contextComponent, setContextComponent] = useState<
    React.ReactNode | undefined
  >();

  // Submit API call
  useEffect(() => {
    const postData = async () => {
      setLoading(true);
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
          if (onUpdateComplete) {
            // Give small grace period before callling complete (allows user to see message)
            setTimeout(() => {
              onUpdateComplete();
            }, 1000);
          }
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
  }, [namespaceId, submitPayload, onUpdateComplete]);

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
  }, [namespaceId, validationPayload]);

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
    setSubmitPayload(parsed);
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
        contextComponent={contextComponent}
      />
    </Box>
  );
}
