import React, { useCallback, useContext, useEffect, useState } from "react";
import Paper from "@mui/material/Paper";
import { Link } from "react-router-dom";
import { PipelineCardProps } from "../../../../../types/declarations/namespace";
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
  GetISBType,
  getAPIResponseError,
  IconsStatusMap,
  ISBStatusString,
  StatusString,
  timeAgo,
  UNKNOWN,
  PAUSED,
  RUNNING,
  PAUSING,
  DELETING,
} from "../../../../../utils";
import { usePipelineUpdateFetch } from "../../../../../utils/fetchWrappers/pipelineUpdateFetch";
import { AppContextProps } from "../../../../../types/declarations/app";
import { AppContext } from "../../../../../App";
import { SidebarType } from "../../../../common/SlidingSidebar";
import { ViewType } from "../../../../common/SpecEditor";
import pipelineIcon from "../../../../../images/pipeline.png";

import "./style.css";

export interface DeleteProps {
  type: "pipeline" | "isb";
  pipelineId?: string;
  isbId?: string;
}

export function PipelineCard({
  namespace,
  data,
  statusData,
  isbData,
  refresh,
}: PipelineCardProps) {
  const { setSidebarProps, systemInfo } =
    useContext<AppContextProps>(AppContext);
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
  const { pipelineAvailable } = usePipelineUpdateFetch({
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

  const handleEditChange = useCallback(
    (event: SelectChangeEvent<string>) => {
      if (event.target.value === "pipeline" && setSidebarProps) {
        setSidebarProps({
          type: SidebarType.PIPELINE_UPDATE,
          specEditorProps: {
            initialYaml: statusData?.pipeline,
            namespaceId: namespace,
            pipelineId: data?.name,
            viewType: ViewType.EDIT,
            onUpdateComplete: handleUpdateComplete,
          },
        });
      } else if (event.target.value === "isb" && setSidebarProps) {
        setSidebarProps({
          type: SidebarType.ISB_UPDATE,
          specEditorProps: {
            initialYaml: isbData?.isbService?.spec,
            namespaceId: namespace,
            isbId: isbData?.name,
            viewType: ViewType.EDIT,
            onUpdateComplete: handleUpdateComplete,
          },
        });
      }
    },
    [setSidebarProps, handleUpdateComplete, isbData, data]
  );

  const handleDeleteChange = useCallback(
    (event: SelectChangeEvent<string>) => {
      if (event.target.value === "pipeline") {
        setDeleteProps({
          type: "pipeline",
          pipelineId: data?.name,
        });
      } else if (event.target.value === "isb") {
        setDeleteProps({
          type: "isb",
          isbId: isbData?.name,
        });
      }
    },
    [isbData, data]
  );

  const handleDeleteComplete = useCallback(() => {
    refresh();
    setDeleteProps(undefined);
  }, [refresh]);

  const handeDeleteCancel = useCallback(() => {
    setDeleteProps(undefined);
  }, []);

  const isbType = GetISBType(isbData?.isbService?.spec) || UNKNOWN;
  const isbStatus = isbData?.isbService?.status?.phase || UNKNOWN;
  const isbHealthStatus = isbData?.status || UNKNOWN;
  const pipelineStatus = statusData?.pipeline?.status?.phase || UNKNOWN;
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
          `/api/v1/namespaces/${namespace}/pipelines/${data?.name}`,
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
      } catch (e) {
        setError(e);
      }
    };
    if (statusPayload) {
      patchStatus();
    }
  }, [statusPayload]);

  useEffect(() => {
    if (
      statusPayload?.spec?.lifecycle?.desiredPhase === PAUSED &&
      statusData?.pipeline?.status?.phase === PAUSED
    ) {
      clearInterval(timer);
      setStatusPayload(undefined);
    }
    if (
      statusPayload?.spec?.lifecycle?.desiredPhase === RUNNING &&
      statusData?.pipeline?.status?.phase === RUNNING
    ) {
      clearInterval(timer);
      setStatusPayload(undefined);
    }
  }, [statusData]);

  return (
    <>
      <Paper
        sx={{
          display: "flex",
          flexDirection: "column",
          padding: "1.5rem",
          width: "100%",
        }}
      >
        <Box
          sx={{
            display: "flex",
            flexDirection: "row",
            flexGrow: 1,
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
              marginLeft: "1rem",
            }}
          >
            <Link
              to={
                systemInfo?.namespaced
                  ? `/pipelines/${data.name}`
                  : `/namespaces/${namespace}/pipelines/${data.name}`
              }
              style={
                pipelineStatus === DELETING || !pipelineAbleToLoad
                  ? { pointerEvents: "none", textDecoration: "none" }
                  : { textDecoration: "none" }
              }
            >
              <span className="pipeline-card-name">{data?.name}</span>
            </Link>
          </Box>
          <Box
            sx={{
              display: "flex",
              flexDirection: "row",
              flexGrow: 1,
              justifyContent: "flex-end",
              alignItems: "center",
              height: "4rem",
            }}
          >
            {error && statusPayload ? (
              <div
                style={{
                  borderRadius: "0.8125rem",
                  width: "14.25rem",
                  background: "#F0F0F0",
                  display: "flex",
                  flexDirection: "row",
                }}
              >
                {error}
              </div>
            ) : successMessage &&
              statusPayload &&
              ((statusPayload.spec.lifecycle.desiredPhase === PAUSED &&
                statusData?.pipeline?.status?.phase !== PAUSED) ||
                (statusPayload.spec.lifecycle.desiredPhase === RUNNING &&
                  statusData?.pipeline?.status?.phase !== RUNNING)) ? (
              <div
                style={{
                  borderRadius: "0.8125rem",
                  width: "14.25rem",
                  background: "#F0F0F0",
                  display: "flex",
                  flexDirection: "row",
                  marginLeft: "1rem",
                  padding: "0.5rem",
                  color: "#516F91",
                  alignItems: "center",
                }}
              >
                <CircularProgress
                  sx={{
                    width: "1.25rem !important",
                    height: "1.25rem !important",
                  }}
                />{" "}
                <Box
                  sx={{
                    display: "flex",
                    flexDirection: "column",
                  }}
                >
                  <span style={{ marginLeft: "1rem" }}>
                    {statusPayload?.spec?.lifecycle?.desiredPhase === PAUSED
                      ? "Pipeline Pausing..."
                      : "Pipeline Resuming..."}
                  </span>
                  <span style={{ marginLeft: "1rem" }}>{timerDateStamp}</span>
                </Box>
              </div>
            ) : (
              ""
            )}

            <Button
              variant="contained"
              sx={{
                marginRight: "1.3rem",
                marginLeft: "1rem",
                height: "2.125rem",
              }}
              onClick={handlePlayClick}
              disabled={
                statusData?.pipeline?.status?.phase === RUNNING ||
                pipelineStatus === DELETING
              }
            >
              Resume
            </Button>
            <Button
              variant="contained"
              sx={{ marginRight: "4.875rem", height: "2.125rem" }}
              onClick={handlePauseClick}
              disabled={
                statusData?.pipeline?.status?.phase === PAUSED ||
                statusData?.pipeline?.status?.phase === PAUSING ||
                pipelineStatus === DELETING
              }
            >
              Pause
            </Button>
          </Box>
          <Link
            to={
              systemInfo?.namespaced
                ? `/pipelines/${data.name}`
                : `/namespaces/${namespace}/pipelines/${data.name}`
            }
            style={
              pipelineStatus === DELETING || !pipelineAbleToLoad
                ? { pointerEvents: "none", textDecoration: "none" }
                : { textDecoration: "none" }
            }
          >
            {pipelineAbleToLoad ? (
              <ArrowForwardIcon
                sx={
                  pipelineStatus === DELETING
                    ? { color: "#D52B1E" }
                    : { color: "#0077C5" }
                }
              />
            ) : (
              <CircularProgress size={24} />
            )}
          </Link>
        </Box>

        <Box
          sx={{
            display: "flex",
            flexDirection: "row",
            flexGrow: 1,
            width: "100%",
          }}
        >
          <Grid
            container
            spacing={2}
            sx={{
              background: "#F9F9F9",
              marginTop: "0.625rem",
              flexWrap: "no-wrap",
            }}
          >
            <Box
              sx={{
                display: "flex",
                flexDirection: "column",
                paddingTop: "1rem",
                paddingLeft: "1rem",
              }}
            >
              <span>Status:</span>
              <span>Health:</span>
            </Box>
            <Box
              sx={{
                display: "flex",
                flexDirection: "column",
                paddingTop: "1rem",
                paddingLeft: "1rem",
              }}
            >
              <img
                src={IconsStatusMap[pipelineStatus]}
                alt="Status"
                className={"pipeline-logo"}
              />
              <img
                src={IconsStatusMap[statusData?.status]}
                alt="Health"
                className={"pipeline-logo"}
              />
            </Box>
            <Box
              sx={{
                display: "flex",
                flexDirection: "column",
                paddingTop: "1rem",
                paddingLeft: "1rem",
              }}
            >
              <span>{StatusString[pipelineStatus]}</span>
              <span>{StatusString[statusData?.status]}</span>
            </Box>
          </Grid>

          <Grid
            container
            spacing={2}
            sx={{
              background: "#F9F9F9",
              marginTop: "0.625rem",
              flexWrap: "no-wrap",
            }}
          >
            <Box
              sx={{
                display: "flex",
                flexDirection: "column",
                paddingTop: "1rem",
                paddingLeft: "1rem",
              }}
            >
              <span>ISB Services:</span>
              <span>ISB Type:</span>
              <span>ISB Size:</span>
            </Box>
            <Box
              sx={{
                display: "flex",
                flexDirection: "column",
                paddingTop: "1rem",
                paddingLeft: "1rem",
              }}
            >
              <span>{isbData?.name}</span>
              <span>{isbType}</span>
              <span>
                {isbType !== UNKNOWN && isbData?.isbService?.spec[isbType]
                  ? isbData?.isbService?.spec[isbType].replicas
                  : UNKNOWN}
              </span>
            </Box>
          </Grid>

          <Grid
            container
            spacing={2}
            sx={{
              background: "#F9F9F9",
              marginTop: "0.625rem",
              flexWrap: "no-wrap",
            }}
          >
            <Box
              sx={{
                display: "flex",
                flexDirection: "column",
                paddingTop: "1rem",
                paddingLeft: "1rem",
              }}
            >
              <span>Status:</span>
              <span>Health:</span>
            </Box>
            <Box
              sx={{
                display: "flex",
                flexDirection: "column",
                paddingTop: "1rem",
                paddingLeft: "1rem",
              }}
            >
              <img
                src={IconsStatusMap[isbStatus]}
                alt="Status"
                className={"pipeline-logo"}
              />
              <img
                src={IconsStatusMap[isbHealthStatus]}
                alt="Health"
                className={"pipeline-logo"}
              />
            </Box>
            <Box
              sx={{
                display: "flex",
                flexDirection: "column",
                paddingTop: "1rem",
                paddingLeft: "1rem",
              }}
            >
              <span>{ISBStatusString[isbStatus]}</span>
              <span>{ISBStatusString[isbHealthStatus]}</span>
            </Box>
          </Grid>
          <Grid
            container
            spacing={0.5}
            sx={{
              background: "#F9F9F9",
              marginTop: "0.625rem",
              alignItems: "center",
              justifyContent: "end",
              marginRight: "0.75rem",
            }}
          >
            <Grid item>
              <Select
                defaultValue="edit"
                onChange={handleEditChange}
                value={editOption}
                variant="outlined"
                data-testid="pipeline-card-edit-select"
                disabled={pipelineStatus === DELETING}
                sx={{
                  color: "#0077C5",
                  height: "2.125rem",
                  background: "#fff",
                  marginRight: "1.25rem",
                }}
              >
                <MenuItem sx={{ display: "none" }} hidden value="edit">
                  EDIT
                </MenuItem>
                <MenuItem value="pipeline">Pipeline</MenuItem>
                <MenuItem value="isb">ISB Service</MenuItem>
              </Select>
            </Grid>
            <Grid item>
              <Select
                defaultValue="delete"
                onChange={handleDeleteChange}
                value={deleteOption}
                disabled={pipelineStatus === DELETING}
                sx={{
                  color: "#0077C5",
                  height: "2.125rem",
                  marginRight: "4rem",
                  background: "#fff",
                }}
              >
                <MenuItem value="delete" sx={{ display: "none" }}>
                  DELETE
                </MenuItem>
                <MenuItem value="pipeline">Pipeline</MenuItem>
                <MenuItem value="isb">ISB Service</MenuItem>
              </Select>
            </Grid>
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
