import React, { useCallback, useContext, useEffect, useState } from "react";
import Paper from "@mui/material/Paper";
import { Link } from "react-router-dom";
import { PipelineCardProps } from "../../../../../types/declarations/namespace";
import {
  Alert,
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
} from "../../../../../utils";
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
  const { setSidebarProps } = useContext<AppContextProps>(AppContext);
  const [editOption] = useState("view");
  const [deleteOption] = useState("delete");
  const [deleteProps, setDeleteProps] = useState<DeleteProps | undefined>();
  const [statusPayload, setStatusPayload] = useState<any>(undefined);
  const [error, setError] = useState<string | undefined>(undefined);
  const [successMessage, setSuccessMessage] = useState<string | undefined>(
    undefined
  );

  const handleUpdateComplete = useCallback(() => {
    refresh();
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
  const pipelineStatus = statusData?.pipeline?.status?.phase || UNKNOWN;

  const handlePlayClick = useCallback((e) => {
    setStatusPayload({
      spec: {
        lifecycle: {
          desiredPhase: RUNNING,
        },
      },
    });
  }, []);

  const handlePauseClick = useCallback((e) => {
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
      } finally {
        const timer = setTimeout(() => {
          setStatusPayload(undefined);
          clearTimeout(timer);
        }, 5000);
      }
    };
    if (statusPayload) {
      patchStatus();
    }
  }, [statusPayload]);
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
            <span className="pipeline-card-name">{data?.name}</span>
          </Box>
          <Box
            sx={{
              display: "flex",
              flexDirection: "row",
              flexGrow: 1,
              justifyContent: "flex-end",
              alignItems: "center",
              height: "64px",
            }}
          >
            {error && statusPayload ? (
              <div
                style={{
                  borderRadius: "13px",
                  width: "228px",
                  background: "#F0F0F0",
                  display: "flex",
                  flexDirection: "row",
                }}
              >
                {error}
              </div>
            ) : successMessage &&
              statusPayload &&
              statusPayload?.spec?.lifecycle?.desiredPhase === PAUSED &&
              statusData?.pipeline?.status?.phase !== PAUSED ? (
              <div
                style={{
                  borderRadius: "13px",
                  width: "228px",
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
                  sx={{ width: "20px !important", height: "20px !important" }}
                />{" "}
                <Box
                  sx={{
                    display: "flex",
                    flexDirection: "column",
                  }}
                >
                  <span style={{ marginLeft: "1rem" }}>
                    Pipeline Pausing...
                  </span>
                  <span style={{ marginLeft: "1rem" }}>
                    {statusData?.pipeline?.status?.lastUpdated
                      ? timeAgo(statusData?.pipeline?.status?.lastUpdated)
                      : ""}
                  </span>
                </Box>
              </div>
            ) : (
              ""
            )}

            <Button
              variant="contained"
              sx={{ marginRight: "1.3rem", marginLeft: "1rem", height: "34px" }}
              onClick={handlePlayClick}
              disabled={statusData?.pipeline?.status?.phase === RUNNING}
            >
              Resume
            </Button>
            <Button
              variant="contained"
              sx={{ marginRight: "5.1rem", height: "34px" }}
              onClick={handlePauseClick}
              disabled={
                statusData?.pipeline?.status?.phase === PAUSED ||
                statusData?.pipeline?.status?.phase === PAUSING
              }
            >
              Pause
            </Button>
          </Box>
          <Link
            to={`/namespaces/${namespace}/pipelines/${data.name}`}
            style={{ textDecoration: "none" }}
          >
            <ArrowForwardIcon sx={{ color: "#0077C5" }} />
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
              marginTop: "10px",
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
              marginTop: "10px",
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
                {isbType && isbData?.isbService?.spec[isbType]
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
              marginTop: "10px",
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
                src={IconsStatusMap[isbData?.status]}
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
              <span>{ISBStatusString[isbData?.status]}</span>
            </Box>
          </Grid>
          <Grid
            container
            spacing={0.5}
            sx={{
              background: "#F9F9F9",
              marginTop: "10px",
              alignItems: "center",
              justifyContent: "end",
            }}
          >
            <Grid item>
              <Select
                defaultValue="edit"
                onChange={handleEditChange}
                value={editOption}
                variant="outlined"
                sx={{
                  color: "#0077C5",
                  height: "34px",
                  background: "#fff",
                  marginRight: "20px",
                }}
              >
                <MenuItem sx={{ display: "none" }} hidden value="view">
                  Edit
                </MenuItem>
                <MenuItem value="pipeline">Pipeline</MenuItem>
                <MenuItem value="isb">ISB</MenuItem>
              </Select>
            </Grid>
            <Grid item>
              <Select
                defaultValue="delete"
                onChange={handleDeleteChange}
                value={deleteOption}
                sx={{
                  color: "#0077C5",
                  height: "34px",
                  marginRight: "4rem",
                  background: "#fff",
                }}
              >
                <MenuItem value="delete" sx={{ display: "none" }}>
                  Delete
                </MenuItem>
                <MenuItem value="pipeline">Pipeline</MenuItem>
                <MenuItem value="isb">ISB</MenuItem>
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
