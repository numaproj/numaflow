import React, { useCallback, useContext, useEffect, useState } from "react";
import Paper from "@mui/material/Paper";
import { Link } from "react-router-dom";
import { MonoVertexCardProps } from "../../../../../types/declarations/namespace";
import {
  Box,
  Button,
  CircularProgress,
  Grid,
  MenuItem,
  Select,
  SelectChangeEvent,
} from "@mui/material";
import ArrowForwardIcon from "@mui/icons-material/ArrowForward";
import { DeleteModal } from "../DeleteModal";
import {
  getAPIResponseError,
  IconsStatusMap,
  StatusString,
  timeAgo,
  UNKNOWN,
  PAUSED,
  RUNNING,
  // PAUSING,
  DELETING,
  getBaseHref,
  GetConsolidatedHealthStatus,
} from "../../../../../utils";
import { useMonoVertexUpdateFetch } from "../../../../../utils/fetchWrappers/monoVertexUpdateFetch";
import { useMonoVertexHealthFetch } from "../../../../../utils/fetchWrappers/monoVertexHealthFetch";
import { AppContextProps } from "../../../../../types/declarations/app";
import { AppContext } from "../../../../../App";
import { SidebarType } from "../../../../common/SlidingSidebar";
import { ViewType } from "../../../../common/SpecEditor";
import pipelineIcon from "../../../../../images/pipeline.png";

import "./style.css";

export interface DeleteProps {
  type: "pipeline";
  pipelineId?: string;
}

export function MonoVertexCard({
  namespace,
  data,
  statusData,
  refresh,
  setMonoVertexHealthMap,
}: MonoVertexCardProps) {
  const { addError, setSidebarProps, host, isReadOnly } =
    useContext<AppContextProps>(AppContext);
  const [viewOption] = useState("view");
  const [editOption] = useState("edit");
  const [deleteOption] = useState("delete");
  const [deleteProps, setDeleteProps] = useState<DeleteProps | undefined>();
  const [statusPayload, setStatusPayload] = useState<any>(undefined);
  const [error, setError] = useState<string | undefined>(undefined);
  const [successMessage, setSuccessMessage] = useState<string | undefined>(
    undefined
  );
  const [timerDateStamp, setTimerDateStamp] = useState<any>(undefined);
  const [timer, setTimer] = useState<any>(undefined);
  const [pipelineAbleToLoad, setPipelineAbleToLoad] = useState<boolean>(false);
  const { pipelineAvailable } = useMonoVertexUpdateFetch({
    namespaceId: namespace,
    pipelineId: data?.name,
    active: !pipelineAbleToLoad,
    refreshInterval: 5000, // 5 seconds
  });

  useEffect(() => {
    if (pipelineAvailable) {
      setPipelineAbleToLoad(true);
    }
  }, [pipelineAvailable]);

  const handleUpdateComplete = useCallback(() => {
    refresh();
    setPipelineAbleToLoad(false);
    if (!setSidebarProps) {
      return;
    }
    // Close sidebar
    setSidebarProps(undefined);
  }, [setSidebarProps, refresh]);

  const handleViewChange = useCallback(
    (event: SelectChangeEvent<string>) => {
      if (event.target.value === "pipeline" && setSidebarProps) {
        setSidebarProps({
          type: SidebarType.PIPELINE_UPDATE,
          specEditorProps: {
            titleOverride: `View Pipeline: ${data?.name}`,
            initialYaml: statusData?.monoVertex,
            namespaceId: namespace,
            pipelineId: data?.name,
            viewType: ViewType.READ_ONLY,
            onUpdateComplete: handleUpdateComplete,
          },
        });
      }
    },
    [setSidebarProps, handleUpdateComplete, data]
  );

  const handleEditChange = useCallback(
    (event: SelectChangeEvent<string>) => {
      if (event.target.value === "pipeline" && setSidebarProps) {
        setSidebarProps({
          type: SidebarType.PIPELINE_UPDATE,
          specEditorProps: {
            initialYaml: statusData?.monoVertex,
            namespaceId: namespace,
            pipelineId: data?.name,
            viewType: ViewType.EDIT,
            onUpdateComplete: handleUpdateComplete,
          },
        });
      }
    },
    [setSidebarProps, handleUpdateComplete, data]
  );

  const handleDeleteChange = useCallback(
    (event: SelectChangeEvent<string>) => {
      if (event.target.value === "pipeline") {
        setDeleteProps({
          type: "pipeline",
          pipelineId: data?.name,
        });
      }
    },
    [data]
  );

  const handleDeleteComplete = useCallback(() => {
    refresh();
    setDeleteProps(undefined);
  }, [refresh]);

  const handeDeleteCancel = useCallback(() => {
    setDeleteProps(undefined);
  }, []);

  const handleTimer = useCallback(() => {
    const dateString = new Date().toISOString();
    const time = timeAgo(dateString);
    setTimerDateStamp(time);
    const pauseTimer = setInterval(() => {
      const time = timeAgo(dateString);
      setTimerDateStamp(time);
    }, 1000);
    setTimer(pauseTimer);
  }, []);

  const handlePlayClick = useCallback(() => {
    handleTimer();
    setStatusPayload({
      spec: {
        lifecycle: {
          desiredPhase: RUNNING,
        },
      },
    });
  }, []);

  const handlePauseClick = useCallback(() => {
    handleTimer();
    setStatusPayload({
      spec: {
        lifecycle: {
          desiredPhase: PAUSED,
        },
      },
    });
  }, []);

  useEffect(() => {
    const patchStatus = async () => {
      try {
        const response = await fetch(
          `${host}${getBaseHref()}/api/v1/namespaces/${namespace}/mono-vertices/${
            data?.name
          }`,
          {
            method: "PATCH",
            headers: {
              "Content-Type": "application/json",
            },
            body: JSON.stringify(statusPayload),
          }
        );
        const error = await getAPIResponseError(response);
        if (error) {
          setError(error);
        } else {
          refresh();
          setSuccessMessage("Status updated successfully");
        }
      } catch (e: any) {
        setError(e);
      }
    };
    if (statusPayload) {
      patchStatus();
    }
  }, [statusPayload, host]);

  useEffect(() => {
    if (
      statusPayload?.spec?.lifecycle?.desiredPhase === PAUSED &&
      statusData?.monoVertex?.status?.phase === PAUSED
    ) {
      clearInterval(timer);
      setStatusPayload(undefined);
    }
    if (
      statusPayload?.spec?.lifecycle?.desiredPhase === RUNNING &&
      statusData?.monoVertex?.status?.phase === RUNNING
    ) {
      clearInterval(timer);
      setStatusPayload(undefined);
    }
  }, [statusData]);

  const {
    data: healthData,
    loading: healthLoading,
    error: healthError,
  } = useMonoVertexHealthFetch({
    namespaceId: namespace,
    monoVertexId: data?.name,
    addError,
    pipelineAbleToLoad,
  });

  useEffect(() => {
    if (healthError) {
      addError(healthError);
    }
  }, [healthError]);

  const pipelineStatus = statusData?.monoVertex?.status?.phase || UNKNOWN;
  const getHealth = useCallback(
    (pipelineStatus: string) => {
      if (healthData) {
        const { resourceHealthStatus, dataHealthStatus } = healthData;
        return GetConsolidatedHealthStatus(
          pipelineStatus,
          resourceHealthStatus,
          dataHealthStatus
        );
      }
      return UNKNOWN;
    },
    [healthData]
  );

  // Set health status in map when healthData changes
  useEffect(() => {
    if (healthData && data?.name && setMonoVertexHealthMap) {
      const healthStatus = getHealth(pipelineStatus);
      setMonoVertexHealthMap((prev) => ({
        ...prev,
        [data.name]: healthStatus,
      }));
    }
  }, [
    healthData,
    pipelineStatus,
    data?.name,
    setMonoVertexHealthMap,
    getHealth,
  ]);

  return (
    <>
      <Paper
        sx={{
          display: "flex",
          flexDirection: "column",
          // padding: "1.5rem",
          width: "100%",
          borderRadius: "1.6rem",
        }}
      >
        <Box
          sx={{
            display: "flex",
            flexDirection: "row",
            flexGrow: 1,
            paddingTop: "1.6rem",
            paddingLeft: "1.6rem",
            paddingRight: "1.6rem",
            paddingBottom: "1.28rem",
            alignItems: "center",
          }}
        >
          <img
            className="pipeline-card-icon"
            src={pipelineIcon}
            alt="pipeline icon"
          />
          <Box
            sx={{
              display: "flex",
              flexDirection: "row",
              flexGrow: 1,
              marginLeft: "1.6rem",
            }}
          >
            <Link
              to={`?namespace=${namespace}&pipeline=${data.name}&type=monoVertex`}
              style={
                pipelineStatus === DELETING || !pipelineAbleToLoad
                  ? { pointerEvents: "none", textDecoration: "none" }
                  : { textDecoration: "none" }
              }
            >
              <span className="pipeline-card-name">{data?.name}</span>
            </Link>
          </Box>
          {!isReadOnly && (
            <Box
              sx={{
                display: "flex",
                flexDirection: "row",
                flexGrow: 1,
                justifyContent: "flex-end",
                alignItems: "center",
                height: "6.4rem",
              }}
            >
              {error && statusPayload ? (
                <div
                  style={{
                    borderRadius: "1.3rem",
                    padding: "0.8rem",
                    height: "6.4rem",
                    width: "22.8rem",
                    background: "#F0F0F0",
                    display: "flex",
                    flexDirection: "column",
                    fontSize: "1.6rem",
                    overflowX: "hidden",
                    overflowY: "scroll",
                    textOverflow: "ellipsis",
                    wordWrap: "break-word",
                    scrollbarWidth: "none",
                    msOverflowStyle: "none",
                  }}
                >
                  {error}
                </div>
              ) : successMessage &&
                statusPayload &&
                ((statusPayload.spec.lifecycle.desiredPhase === PAUSED &&
                  statusData?.monoVertex?.status?.phase !== PAUSED) ||
                  (statusPayload.spec.lifecycle.desiredPhase === RUNNING &&
                    statusData?.monoVertex?.status?.phase !== RUNNING)) ? (
                <div
                  style={{
                    borderRadius: "1.3rem",
                    width: "22.8rem",
                    background: "#F0F0F0",
                    display: "flex",
                    flexDirection: "row",
                    marginLeft: "1.6rem",
                    padding: "0.8rem",
                    color: "#516F91",
                    alignItems: "center",
                  }}
                >
                  <CircularProgress
                    sx={{
                      width: "2rem !important",
                      height: "2rem !important",
                    }}
                  />{" "}
                  <Box
                    sx={{
                      display: "flex",
                      flexDirection: "column",
                    }}
                  >
                    <span style={{ marginLeft: "1.6rem", fontSize: "1.6rem" }}>
                      {statusPayload?.spec?.lifecycle?.desiredPhase === PAUSED
                        ? "Pipeline Pausing..."
                        : "Pipeline Resuming..."}
                    </span>
                    <span style={{ marginLeft: "1.6rem", fontSize: "1.6rem" }}>
                      {timerDateStamp}
                    </span>
                  </Box>
                </div>
              ) : (
                ""
              )}

              <Button
                variant="contained"
                sx={{
                  marginRight: "2.08rem",
                  marginLeft: "1.6rem",
                  height: "3.4rem",
                  fontSize: "1.4rem",
                }}
                onClick={handlePlayClick}
                disabled
                // disabled={
                //   statusData?.monoVertex?.status?.phase === RUNNING ||
                //   pipelineStatus === DELETING
                // }
              >
                Resume
              </Button>
              <Button
                variant="contained"
                sx={{
                  marginRight: "7.8rem",
                  height: "3.4rem",
                  fontSize: "1.4rem",
                }}
                onClick={handlePauseClick}
                disabled
                // disabled={
                //   statusData?.monoVertex?.status?.phase === PAUSED ||
                //   statusData?.monoVertex?.status?.phase === PAUSING ||
                //   pipelineStatus === DELETING
                // }
              >
                Pause
              </Button>
            </Box>
          )}
          <Link
            to={`?namespace=${namespace}&pipeline=${data.name}&type=monoVertex`}
            style={
              pipelineStatus === DELETING || !pipelineAbleToLoad
                ? { pointerEvents: "none", textDecoration: "none" }
                : { textDecoration: "none" }
            }
          >
            {pipelineAbleToLoad ? (
              <ArrowForwardIcon
                sx={{
                  height: "2.4rem",
                  width: "2.4rem",
                  ...(pipelineStatus === DELETING
                    ? { color: "#D52B1E" }
                    : { color: "#0077C5" }),
                }}
              />
            ) : (
              <CircularProgress size={24} />
            )}
          </Link>
        </Box>

        <Box
          sx={{
            display: "flex",
            background: "#F9F9F9",
            flexDirection: "row",
            flexGrow: 1,
            padding: "1.6rem",
            paddingTop: "0",
            width: "100%",
            borderBottomLeftRadius: "1.6rem",
            borderBottomRightRadius: "1.6rem",
          }}
        >
          <Grid
            container
            spacing={2}
            sx={{
              background: "#F9F9F9",
              marginTop: "1rem",
              marginLeft: "0",
              flexWrap: "no-wrap",
            }}
          >
            <Box
              sx={{
                display: "flex",
                flexDirection: "column",
                paddingTop: "1.6rem",
                paddingLeft: "1.6rem",
                fontSize: "1.6rem",
              }}
            >
              <span>Status:</span>
              <span>Health:</span>
            </Box>
            <Box
              sx={{
                display: "flex",
                flexDirection: "column",
                paddingTop: "1.6rem",
                paddingLeft: "1.6rem",
              }}
            >
              <img
                src={IconsStatusMap[pipelineStatus]}
                alt="Status"
                className={"pipeline-logo"}
              />
              <img
                src={
                  IconsStatusMap[
                    healthLoading ? UNKNOWN : getHealth(pipelineStatus)
                  ]
                }
                alt="Health"
                className={"pipeline-logo"}
              />
            </Box>
            <Box
              sx={{
                display: "flex",
                flexDirection: "column",
                paddingTop: "1.6rem",
                paddingLeft: "1.6rem",
                fontSize: "1.6rem",
                marginTop: "0.1rem",
              }}
            >
              <span>{StatusString[pipelineStatus]}</span>
              <span>
                {
                  StatusString[
                    healthLoading ? UNKNOWN : getHealth(pipelineStatus)
                  ]
                }
              </span>
            </Box>
          </Grid>

          <Grid
            container
            spacing={0.5}
            sx={{
              background: "#F9F9F9",
              marginTop: "1rem",
              alignItems: "center",
              justifyContent: "end",
              marginRight: "1.2rem",
            }}
          >
            {isReadOnly && (
              <Grid item>
                <Select
                  defaultValue="view"
                  onChange={handleViewChange}
                  value={viewOption}
                  variant="outlined"
                  data-testid="pipeline-card-view-select"
                  disabled={pipelineStatus === DELETING}
                  sx={{
                    color: "#0077C5",
                    height: "3.4rem",
                    background: "#fff",
                    marginRight: "2rem",
                    fontSize: "1.6rem",
                  }}
                >
                  <MenuItem sx={{ display: "none" }} hidden value="view">
                    VIEW
                  </MenuItem>
                  <MenuItem value="pipeline" sx={{ fontSize: "1.6rem" }}>
                    Pipeline
                  </MenuItem>
                </Select>
              </Grid>
            )}
            {!isReadOnly && (
              <Grid item>
                <Select
                  defaultValue="edit"
                  onChange={handleEditChange}
                  value={editOption}
                  variant="outlined"
                  data-testid="pipeline-card-edit-select"
                  disabled
                  // disabled={pipelineStatus === DELETING}
                  sx={{
                    color: "#0077C5",
                    height: "3.4rem",
                    background: "#fff",
                    marginRight: "2rem",
                    fontSize: "1.6rem",
                  }}
                >
                  <MenuItem sx={{ display: "none" }} hidden value="edit">
                    EDIT
                  </MenuItem>
                  <MenuItem value="pipeline" sx={{ fontSize: "1.6rem" }}>
                    Pipeline
                  </MenuItem>
                </Select>
              </Grid>
            )}
            {!isReadOnly && (
              <Grid item>
                <Select
                  defaultValue="delete"
                  onChange={handleDeleteChange}
                  value={deleteOption}
                  disabled
                  // disabled={pipelineStatus === DELETING}
                  sx={{
                    color: "#0077C5",
                    height: "3.4rem",
                    marginRight: "6.4rem",
                    background: "#fff",
                    fontSize: "1.6rem",
                  }}
                >
                  <MenuItem value="delete" sx={{ display: "none" }}>
                    DELETE
                  </MenuItem>
                  <MenuItem value="pipeline" sx={{ fontSize: "1.6rem" }}>
                    Pipeline
                  </MenuItem>
                </Select>
              </Grid>
            )}
          </Grid>
        </Box>
        {deleteProps && (
          <DeleteModal
            {...deleteProps}
            namespaceId={namespace}
            onDeleteCompleted={handleDeleteComplete}
            onCancel={handeDeleteCancel}
          />
        )}
      </Paper>
    </>
  );
}
