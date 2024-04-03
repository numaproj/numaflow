import React, { useMemo, useState, useEffect, useCallback } from "react";
import Box from "@mui/material/Box";
import Paper from "@mui/material/Paper";
import YAML from "yaml";
import * as monaco from "monaco-editor";
import Editor, { loader } from "@monaco-editor/react";
import CircularProgress from "@mui/material/CircularProgress";
import Button from "@mui/material/Button";
import SuccessIcon from "@mui/icons-material/CheckCircle";
import ErrorIcon from "../../../images/warning-triangle.png";

import "./style.css";

loader.config({ monaco });

export enum ViewType {
  READ_ONLY,
  TOGGLE_EDIT,
  EDIT,
}

export interface ValidationMessage {
  type: "error" | "success";
  message: string;
}

export enum Status {
  LOADING,
  SUCCESS,
  ERROR,
}

export interface StatusIndicator {
  submit?: {
    status: Status;
    message: string;
    allowRetry?: boolean;
  };
  processing?: {
    status: Status;
    message: string;
  };
}

export interface SpecEditorProps {
  initialYaml?: any; // Value initally loaded into view. Object instance of spec or string.
  loading?: boolean; // Show spinner
  viewType?: ViewType; // Allow editing
  allowNonMutatedSubmit?: boolean; // Allow submit without content being changed
  onValidate?: (value: string) => void;
  onSubmit?: (value: string) => void;
  onResetApplied?: () => void;
  onMutatedChange?: (mutated: boolean) => void;
  validationMessage?: ValidationMessage;
  statusIndicator?: StatusIndicator;
  mutationKey?: string;
  editResetKey?: string;
}

export function SpecEditor({
  initialYaml,
  loading = false,
  viewType = ViewType.READ_ONLY,
  allowNonMutatedSubmit = false,
  onValidate,
  onSubmit,
  onResetApplied,
  onMutatedChange,
  statusIndicator,
  validationMessage,
  mutationKey,
  editResetKey,
}: SpecEditorProps) {
  const [editable, setEditable] = useState(viewType === ViewType.EDIT);
  const [mutated, setMutated] = useState(false);
  const [value, setValue] = useState<string>(
    typeof initialYaml === "string"
      ? initialYaml
      : YAML.stringify(initialYaml) || ""
  );
  const [editorRef, setEditorRef] = useState<any>(undefined);

  useEffect(() => {
    if (onMutatedChange) {
      onMutatedChange(mutated);
    }
  }, [mutated, onMutatedChange]);

  useEffect(() => {
    if (editable) {
      editorRef?.focus();
    }
  }, [editorRef, editable]);

  useEffect(() => {
    setMutated(false);
  }, [mutationKey]);

  useEffect(() => {
    if (viewType === ViewType.TOGGLE_EDIT) {
      setEditable(false);
    }
  }, [viewType, editResetKey]);

  // Update editable on view type change
  useEffect(() => {
    // Set editable for non-toggle types. Toggle type editable maintain via toggle.
    if (viewType === ViewType.EDIT) {
      setEditable(true);
    } else if (viewType === ViewType.READ_ONLY) {
      setEditable(false);
    }
  }, [viewType]);

  // Track if mutation has occurred
  useEffect(() => {
    if (!initialYaml && !value) {
      // Both empty. Check needed as other comparisons does not catch this.
      setMutated(false);
      return;
    }
    if ((initialYaml && !value) || (!initialYaml && value)) {
      // One defined and other is not
      setMutated(true);
      return;
    }
    if (typeof initialYaml === "string" && initialYaml !== value) {
      // Both defined, different value (initial is string)
      setMutated(true);
      return;
    }
    if (
      typeof initialYaml !== "string" &&
      YAML.stringify(initialYaml) !== value
    ) {
      // Both defined, different value (initial is object)
      setMutated(true);
      return;
    }
    // No changes
    setMutated(false);
  }, [initialYaml, value]);

  const handleEditorDidMount = useCallback((editor: any) => {
    setEditorRef(editor);
  }, []);

  const handleValueChange = useCallback((newValue: string | undefined) => {
    setValue(newValue ? newValue : "");
  }, []);

  const handleReset = useCallback(() => {
    setValue(
      typeof initialYaml === "string"
        ? initialYaml
        : YAML.stringify(initialYaml) || ""
    );
    onResetApplied && onResetApplied();
  }, [initialYaml, onResetApplied]);

  const handleEditToggle = useCallback(() => {
    const updated = !editable;
    setEditable(updated);
    if (!updated) {
      handleReset(); // Reset back to original
    }
  }, [handleReset, editable]);

  const handleValidate = useCallback(() => {
    if (!onValidate || !value) {
      return;
    }
    onValidate(value);
  }, [onValidate, value]);

  const handleSubmit = useCallback(() => {
    if (!onSubmit || !value) {
      return;
    }
    onSubmit(value);
  }, [onSubmit, value]);

  const spinner = useMemo(() => {
    if (!loading) {
      return undefined;
    }
    return (
      <Box
        sx={{
          display: "flex",
          justifyContent: "center",
          position: "absolute",
          width: "100%",
          height: "75%",
          alignItems: "center",
          zIndex: 999,
        }}
      >
        <CircularProgress />
      </Box>
    );
  }, [loading]);

  const actionButtons = useMemo(() => {
    if (viewType === ViewType.READ_ONLY) {
      return undefined;
    }
    const btnStyle = {
      height: "fit-content",
      fontSize: "1.4rem",
    };
    const statusShowing =
      !!statusIndicator &&
      (!!statusIndicator.submit || !!statusIndicator.processing);
    const retryAllowed =
      !!statusIndicator &&
      !!statusIndicator.submit &&
      !!statusIndicator.submit.allowRetry;
    return (
      <Box
        sx={{
          display: "flex",
          flexDirection: "row",
          marginBottom: "2.4rem",
          marginLeft: "2rem",
          justifyContent: validationMessage ? "space-between" : "flex-end",
        }}
      >
        {validationMessage && (
          <Box
            sx={{
              display: "flex",
              flexDirection: "row",
              alignItems: "center",
              overflow: "hidden",
              textOverflow: "ellipsis",
              whiteSpace: "nowrap",
            }}
          >
            {validationMessage.type === "error" && (
              <img
                src={ErrorIcon}
                alt="Error"
                className="spec-editor-validation-message-icon"
              />
            )}
            {validationMessage.type === "success" && (
              <SuccessIcon
                fontSize="large"
                color="success"
                sx={{
                  marginRight: "0.8rem",
                  height: "3.5rem",
                  width: "3.5rem",
                }}
              />
            )}
            <span
              className="spec-editor-validation-message"
              style={{
                overflow: "hidden",
                textOverflow: "ellipsis",
                whiteSpace: "nowrap",
              }}
            >
              {validationMessage.message}
            </span>
          </Box>
        )}
        <Box
          sx={{
            display: "flex",
            flexDirection: "row",
          }}
        >
          {viewType === ViewType.TOGGLE_EDIT && (
            <Button
              onClick={handleEditToggle}
              variant="contained"
              disabled={loading || statusShowing}
              sx={btnStyle}
              data-testid="spec-editor-edit-btn"
            >
              {editable ? "View" : "Edit"}
            </Button>
          )}
          <Button
            disabled={!mutated || loading || !editable || statusShowing}
            onClick={handleReset}
            variant="contained"
            sx={{ marginLeft: "0.8rem", ...btnStyle }}
          >
            Reset
          </Button>
          <Button
            disabled={
              (!mutated && !allowNonMutatedSubmit) ||
              loading ||
              !editable ||
              statusShowing
            }
            onClick={handleValidate}
            variant="contained"
            sx={{ marginLeft: "0.8rem", ...btnStyle }}
            data-testid="spec-editor-validate-button"
          >
            Validate
          </Button>
          <Button
            disabled={
              (!mutated && !allowNonMutatedSubmit) ||
              loading ||
              !editable ||
              (statusShowing && !retryAllowed)
            }
            onClick={handleSubmit}
            variant="contained"
            sx={{ marginLeft: "0.8rem", ...btnStyle }}
          >
            Submit
          </Button>
        </Box>
      </Box>
    );
  }, [
    statusIndicator,
    viewType,
    editable,
    mutated,
    loading,
    handleEditToggle,
    handleReset,
    handleValidate,
    handleSubmit,
    validationMessage,
  ]);

  const status = useMemo(() => {
    if (
      !statusIndicator ||
      (!statusIndicator.submit && !statusIndicator.processing)
    ) {
      // No status to display
      return undefined;
    }
    const errorIcon = (
      <img
        src={ErrorIcon}
        alt="Error"
        className="spec-editor-status-message-icon"
      />
    );
    const successIcon = (
      <SuccessIcon
        fontSize="large"
        color="success"
        sx={{ marginRight: "0.8rem", height: "3.5rem", width: "3.5rem" }}
      />
    );
    const loadingIcon = (
      <CircularProgress
        size={27}
        sx={{ marginRight: "1.6rem", marginLeft: "0.3rem" }}
      />
    );
    const containerStyle = {
      display: "flex",
      flexDirection: "row",
      alignItems: "center",
      margin: "0.8rem 0",
    };
    const messageContainerStyle = {
      display: "flex",
      flexDirection: "column",
      alignItems: "flex-start",
    };
    return (
      <Box
        sx={{
          display: "flex",
          flexDirection: "column",
          alignItems: "center",
          justifyContent: "center",
          height: "100%",
        }}
      >
        <Box>
          {statusIndicator.submit && (
            <Box sx={containerStyle}>
              {statusIndicator.submit.status === Status.ERROR && errorIcon}
              {statusIndicator.submit.status === Status.SUCCESS && successIcon}
              {statusIndicator.submit.status === Status.LOADING && loadingIcon}
              <Box sx={messageContainerStyle}>
                <span className="spec-editor-status-message">
                  {statusIndicator.submit.message}
                </span>
              </Box>
            </Box>
          )}
          {statusIndicator.processing && (
            <Box sx={containerStyle}>
              {statusIndicator.processing.status === Status.ERROR && errorIcon}
              {statusIndicator.processing.status === Status.SUCCESS &&
                successIcon}
              {statusIndicator.processing.status === Status.LOADING &&
                loadingIcon}
              <Box sx={messageContainerStyle}>
                <span className="spec-editor-status-message">
                  {statusIndicator.processing.message}
                </span>
              </Box>
            </Box>
          )}
        </Box>
      </Box>
    );
  }, [statusIndicator]);

  const editor = useMemo(() => {
    return (
      <Box
        sx={
          status // Hide editor if status is displayed
            ? {
                display: "none",
              }
            : {
                height: "100%",
              }
        }
      >
        <Editor
          height="100%"
          defaultLanguage="yaml"
          defaultValue={value}
          value={value}
          onMount={handleEditorDidMount}
          onChange={handleValueChange}
          theme="github"
          options={
            !loading && editable
              ? { domReadOnly: false, readOnly: false }
              : { domReadOnly: true, readOnly: true }
          }
          loading={
            <Box sx={{ display: "flex", justifyContent: "center" }}>
              <CircularProgress />
            </Box>
          }
        />
      </Box>
    );
  }, [status, value, editable, loading, handleValueChange]);

  return (
    <Box
      sx={{
        display: "flex",
        flexDirection: "column",
        height: "100%",
      }}
      data-testid="spec-editor"
    >
      <Paper
        elevation={0}
        sx={{
          display: "flex",
          flexDirection: "column",
          height: "100%",
          padding: "2.4rem 2.4rem 2.4rem 0",
        }}
      >
        {spinner}
        {actionButtons}
        {status}
        {editor}
      </Paper>
    </Box>
  );
}
