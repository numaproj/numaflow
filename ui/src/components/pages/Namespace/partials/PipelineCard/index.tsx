import React, { useCallback, useContext } from "react";
import Paper from "@mui/material/Paper";
import { Link } from "react-router-dom";
import { PipelineCardProps } from "../../../../../types/declarations/namespace";
import {
  Box,
  Button,
  Grid,
  MenuItem,
  Select,
  SelectChangeEvent,
} from "@mui/material";
import ArrowForwardIcon from "@mui/icons-material/ArrowForward";
import {
  IconsStatusMap,
  ISBStatusString,
  StatusString,
} from "../../../../../utils";
import { AppContextProps } from "../../../../../types/declarations/app";
import { AppContext } from "../../../../../App";
import { SidebarType } from "../../../../common/SlidingSidebar";
import { ViewType } from "../../../../common/SpecEditor";
import pipelineIcon from "../../../../../images/pipeline.png";

import "./style.css";

export function PipelineCard({
  namespace,
  data,
  statusData,
  isbData,
  refresh,
}: PipelineCardProps) {
  const { setSidebarProps } = useContext<AppContextProps>(AppContext);
  const [editOption] = React.useState("view");
  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  const [deleteOption, setDeleteOption] = React.useState("Delete");

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

  const handleDeleteChange = useCallback((event: SelectChangeEvent<string>) => {
    setDeleteOption(event.target.value);
  }, []);
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
            }}
          >
            <Button variant="contained" sx={{ marginRight: "1.3rem" }} disabled>
              Resume
            </Button>
            <Button variant="contained" disabled sx={{ marginRight: "5.1rem" }}>
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
                src={IconsStatusMap[statusData?.pipeline?.status?.phase]}
                alt={statusData?.pipeline?.status?.phase}
                className={"pipeline-logo"}
              />
              <img
                src={IconsStatusMap[statusData?.status]}
                alt={statusData?.status}
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
              <span>{StatusString[statusData?.pipeline?.status?.phase]}</span>
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
              <span>{isbData?.isbService?.status?.type}</span>
              <span>
                {
                  isbData?.isbService?.spec[isbData?.isbService?.status?.type]
                    .replicas
                }
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
                src={IconsStatusMap[isbData?.isbService?.status?.phase]}
                alt={isbData?.isbService?.status?.phase}
                className={"pipeline-logo"}
              />
              <img
                src={IconsStatusMap[isbData?.status]}
                alt={isbData?.status}
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
              <span>{ISBStatusString[isbData?.isbService?.status?.phase]}</span>
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
                disabled
                onChange={handleDeleteChange}
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
      </Paper>
    </>
  );
}
