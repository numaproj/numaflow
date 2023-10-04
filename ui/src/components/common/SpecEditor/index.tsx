import React, { useMemo, useState, useEffect, useCallback } from "react";
import Box from "@mui/material/Box";
import Paper from "@mui/material/Paper";
import YAML from "yaml";
import Editor from "@monaco-editor/react";
import CircularProgress from "@mui/material/CircularProgress";
import Button from "@mui/material/Button";

import "./style.css";

export enum ViewType {
  READ_ONLY,
  TOGGLE_EDIT,
  EDIT,
}

export interface SpecEditorProps {
  initialYaml?: any; // Value initally loaded into view. Object instance of spec or string.
  loading?: boolean; // Show spinner
  viewType?: ViewType; // Allow editing
  onValidate?: (value: string) => void;
  onSubmit?: (value: string) => void;
  onResetApplied?: () => void;
  onMutatedChange?: (mutated: boolean) => void;
  contextComponent?: React.ReactNode; // Component provided by parent to show error/validation context
  mutationKey?: string;
  editResetKey?: string;
}

export function SpecEditor({
  initialYaml,
  loading = false,
  viewType = ViewType.READ_ONLY,
  onValidate,
  onSubmit,
  onResetApplied,
  onMutatedChange,
  contextComponent,
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
    return (
      <Box
        sx={{
          display: "flex",
          flexDirection: "row",
          justifyContent: "flex-end",
          marginBottom: "1.5rem",
        }}
      >
        {viewType === ViewType.TOGGLE_EDIT && (
          <Button
            onClick={handleEditToggle}
            variant="contained"
            disabled={loading}
          >
            {editable ? "View" : "Edit"}
          </Button>
        )}
        <Button
          disabled={!mutated || loading || !editable}
          onClick={handleReset}
          variant="contained"
          sx={{ marginLeft: "0.5rem" }}
        >
          Reset
        </Button>
        <Button
          disabled={!mutated || loading || !editable}
          onClick={handleValidate}
          variant="contained"
          sx={{ marginLeft: "0.5rem" }}
        >
          Validate
        </Button>
        <Button
          disabled={!mutated || loading || !editable}
          onClick={handleSubmit}
          variant="contained"
          sx={{ marginLeft: "0.5rem" }}
        >
          Submit
        </Button>
      </Box>
    );
  }, [
    viewType,
    editable,
    mutated,
    loading,
    handleEditToggle,
    handleReset,
    handleValidate,
    handleSubmit,
  ]);

  const editor = useMemo(() => {
    return (
      <Box
        sx={{
          height: "100%",
        }}
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
  }, [value, editable, loading, handleValueChange]);

  return (
    <Box
      sx={{
        display: "flex",
        flexDirection: "column",
        height: "100%",
      }}
    >
      <Paper
        elevation={0}
        sx={{
          display: "flex",
          flexDirection: "column",
          height: "100%",
          padding: "1.5rem 1.5rem 1.5rem 0",
        }}
      >
        {spinner}
        {actionButtons}
        {contextComponent && (
          <Box
            sx={{
              marginLeft: "1.5rem",
              marginBottom: "1.5rem",
            }}
          >
            {contextComponent}
          </Box>
        )}
        {editor}
      </Paper>
    </Box>
  );
}
