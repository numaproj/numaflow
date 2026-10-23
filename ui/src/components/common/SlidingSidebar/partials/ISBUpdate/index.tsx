import React, {
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useState,
} from "react";
import YAML from "yaml";
import Box from "@mui/material/Box";
import Button from "@mui/material/Button";
import Tab from "@mui/material/Tab";
import Tabs from "@mui/material/Tabs";
import { useHistory, useLocation } from "react-router-dom";
import {
  SpecEditor,
  Status,
  StatusIndicator,
  ValidationMessage,
  ViewType,
} from "../../../SpecEditor";
import TabPanel from "../../../Tab-Panel";
import { SpecEditorSidebarProps } from "../..";
import { AppContextProps } from "../../../../../types/declarations/app";
import { AppContext } from "../../../../../App";
import {
  a11yProps,
  getAPIResponseError,
  getBaseHref,
} from "../../../../../utils";
import { useISBServiceDebugFetch } from "../../../../../utils/fetchWrappers/isbServiceDebugFetch";
import { CopyViewLinkButton } from "../../../CopyViewLinkButton";
import {
  buildObservabilityViewUrl,
  updateObservabilitySearch,
} from "../../../../../utils/observabilityURLState";
import { ISBDebugInfo } from "./partials/ISBDebugInfo";

import "./style.css";

const JETSTREAM_TAB_INDEX = 0;
const SPEC_TAB_INDEX = 1;
const ISB_TAB_KEY_BY_INDEX: Record<number, string> = {
  [JETSTREAM_TAB_INDEX]: "jetstream",
  [SPEC_TAB_INDEX]: "spec",
};
const ISB_TAB_INDEX_BY_KEY: Record<string, number> = {
  jetstream: JETSTREAM_TAB_INDEX,
  spec: SPEC_TAB_INDEX,
};

const formatSpecLine = (startLine: number, endLine: number) =>
  startLine === endLine ? `${startLine}` : `${startLine}-${endLine}`;

export function ISBUpdate({
  initialYaml,
  namespaceId,
  isbId,
  viewType,
  titleOverride,
  initialTab,
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
  const [tabValue, setTabValue] = useState(
    initialTab
      ? ISB_TAB_INDEX_BY_KEY[initialTab] ?? JETSTREAM_TAB_INDEX
      : JETSTREAM_TAB_INDEX
  );
  const { host } = useContext<AppContextProps>(AppContext);
  const history = useHistory();
  const location = useLocation();
  const params = new URLSearchParams(location.search);
  const specLineParam = params.get("specLine") || "";
  const [initialStartLine, initialEndLine] = specLineParam
    .split("-", 2)
    .map((line) => Number(line));
  const [selectedSpecLine, setSelectedSpecLine] = useState<string | undefined>(
    specLineParam || undefined
  );
  const showDebugTabs =
    viewType === ViewType.READ_ONLY && !!initialYaml?.spec?.jetstream;
  const isDeepLinkedISB = params.get("isb") === isbId;
  const debugFetch = useISBServiceDebugFetch({
    namespaceId,
    isbId,
    enabled: showDebugTabs,
  });

  useEffect(() => {
    if (!showDebugTabs || !isDeepLinkedISB) {
      return;
    }
    const requestedTab = new URLSearchParams(location.search).get("isbTab");
    const requestedTabIndex = requestedTab
      ? ISB_TAB_INDEX_BY_KEY[requestedTab]
      : undefined;
    if (requestedTabIndex !== undefined && requestedTabIndex !== tabValue) {
      setTabValue(requestedTabIndex);
    }
  }, [showDebugTabs, isDeepLinkedISB, location.search, tabValue]);

  useEffect(() => {
    setSelectedSpecLine(specLineParam || undefined);
  }, [specLineParam]);

  // Submit API call
  useEffect(() => {
    const postData = async () => {
      setStatus({
        submit: {
          status: Status.LOADING,
          message: "Submitting isb service update...",
          allowRetry: false,
        },
      });
      try {
        const response = await fetch(
          `${host}${getBaseHref()}/api/v1/namespaces/${namespaceId}/isb-services/${isbId}?dry-run=false`,
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
              message: "ISB Service updated successfully",
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
  }, [namespaceId, isbId, submitPayload, onUpdateComplete, host]);

  // Validation API call
  useEffect(() => {
    const postData = async () => {
      setLoading(true);
      try {
        const response = await fetch(
          `${host}${getBaseHref()}/api/v1/namespaces/${namespaceId}/isb-services/${isbId}?dry-run=true`,
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
  }, [namespaceId, isbId, validationPayload, host]);

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

  const handleTabsChange = useCallback(
    (_: any, newValue: any) => {
      setTabValue(newValue);
      if (!isDeepLinkedISB) {
        return;
      }
      const search = updateObservabilitySearch(location.search, {
        isb: isbId,
        isbTab: ISB_TAB_KEY_BY_INDEX[newValue],
        specLine: newValue === SPEC_TAB_INDEX ? selectedSpecLine : null,
      });
      if (search !== location.search) {
        history.replace({ pathname: location.pathname, search });
      }
    },
    [history, isDeepLinkedISB, isbId, location, selectedSpecLine]
  );

  const handleSelectionLineChange = useCallback(
    (startLine: number, endLine: number) => {
      const nextSpecLine = formatSpecLine(startLine, endLine);
      setSelectedSpecLine(nextSpecLine);

      if (!isDeepLinkedISB || tabValue !== SPEC_TAB_INDEX) {
        return;
      }
      const search = updateObservabilitySearch(location.search, {
        isb: isbId,
        isbTab: "spec",
        specLine: nextSpecLine,
      });
      if (search !== location.search) {
        history.replace({ pathname: location.pathname, search });
      }
    },
    [history, isDeepLinkedISB, isbId, location, tabValue]
  );

  const specViewUrl = useMemo(
    () =>
      buildObservabilityViewUrl(location, {
        isb: isbId,
        isbTab: "spec",
        specLine: selectedSpecLine,
      }),
    [isbId, location, selectedSpecLine]
  );

  const specEditor = (
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
      initialLine={
        Number.isInteger(initialStartLine) && initialStartLine > 0
          ? initialStartLine
          : undefined
      }
      initialEndLine={
        Number.isInteger(initialEndLine) && initialEndLine >= initialStartLine
          ? initialEndLine
          : undefined
      }
      onSelectionLineChange={handleSelectionLineChange}
    />
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
        <span className="isb-spec-header-text">
          {titleOverride ? titleOverride : `Edit ISB Service: ${isbId}`}
        </span>
      </Box>
      {showDebugTabs ? (
        <Box>
          <Box
            sx={{
              display: "flex",
              alignItems: "center",
              justifyContent: "space-between",
            }}
          >
            <Tabs
              value={tabValue}
              onChange={handleTabsChange}
              aria-label="ISB service tabs"
              sx={{
                "& .MuiTab-root": {
                  fontSize: "1.2rem",
                  fontWeight: 600,
                  minWidth: "12rem",
                },
              }}
            >
              <Tab label="JetStream" {...a11yProps(JETSTREAM_TAB_INDEX)} />
              <Tab label="Spec" {...a11yProps(SPEC_TAB_INDEX)} />
            </Tabs>
            <Box className="isb-service-tabs-actions">
              {tabValue === SPEC_TAB_INDEX ? (
                <CopyViewLinkButton url={specViewUrl} />
              ) : (
                <Button
                  variant="outlined"
                  size="medium"
                  onClick={debugFetch.refresh}
                  disabled={debugFetch.loading}
                  sx={{
                    fontSize: "1.2rem",
                    fontWeight: 600,
                    minWidth: "10rem",
                  }}
                >
                  {debugFetch.loading ? "Refreshing..." : "Refresh"}
                </Button>
              )}
            </Box>
          </Box>
          <TabPanel value={tabValue} index={JETSTREAM_TAB_INDEX}>
            <ISBDebugInfo
              jetStream={debugFetch.data?.jetStream}
              loading={debugFetch.loading}
              error={debugFetch.error}
            />
          </TabPanel>
          <TabPanel value={tabValue} index={SPEC_TAB_INDEX}>
            <Box
              sx={{
                height: "calc(100vh - 20rem)",
                minHeight: "45rem",
              }}
            >
              {specEditor}
            </Box>
          </TabPanel>
        </Box>
      ) : (
        specEditor
      )}
    </Box>
  );
}
