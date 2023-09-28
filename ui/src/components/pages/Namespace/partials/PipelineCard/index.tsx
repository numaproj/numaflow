import React, { useCallback, useContext } from "react";
import Paper from "@mui/material/Paper";
import { Link } from "react-router-dom";
import { PipelineCardProps } from "../../../../../types/declarations/namespace";
import {
  Box,
  Button,
  Chip,
  Grid,
  MenuItem,
  Select,
  SelectChangeEvent,
} from "@mui/material";
import {
  IndicatorStatus,
  StatusIndicator,
} from "../../../../common/StatusIndicator/StatusIndicator";
import { AppContextProps } from "../../../../../types/declarations/app";
import { AppContext } from "../../../../../App";
import { SidebarType } from "../../../../common/SlidingSidebar";

import "./style.css";

export function PipelineCard({
  namespace,
  data,
  statusData,
  isbData,
}: PipelineCardProps) {
  const { setSidebarProps } = useContext<AppContextProps>(AppContext);
  const [editOption] = React.useState("edit");
  const [deleteOption, setDeleteOption] = React.useState("Delete");

  const handleEditChange = useCallback(
    (event: SelectChangeEvent<string>) => {
      if (event.target.value === "pipeline" && setSidebarProps) {
        setSidebarProps({
          type: SidebarType.PIPELINE_SPEC,
          pipelineSpecProps: { spec: statusData?.pipeline?.spec },
        });
      } else if (event.target.value === "isb" && setSidebarProps) {
        setSidebarProps({
          type: SidebarType.PIPELINE_SPEC,
          pipelineSpecProps: { spec: isbData?.isbService?.spec, titleOverride: "ISB Spec" },
        });
      }
    },
    [setSidebarProps, statusData, isbData]
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
        <Link
          to={`/namespaces/${namespace}/pipelines/${data.name}`}
          style={{ textDecoration: "none" }}
        >
          <Box
            sx={{
              display: "flex",
              flexDirection: "row",
              flexGrow: 1,
            }}
          >
            <Box
              sx={{
                display: "flex",
                flexDirection: "row",
                flexGrow: 1,
              }}
            >
              <span className="pipeline-card-name">{data.name}</span>
            </Box>
            <Box
              sx={{
                display: "flex",
                flexDirection: "row",
                flexGrow: 1,
                justifyContent: "flex-end",
              }}
            >
              <Button variant="contained" sx={{ marginRight: "10px" }}>
                Resume
              </Button>
              <Button variant="contained">Pause</Button>
            </Box>
          </Box>
        </Link>
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
            sx={{ background: "#F9F9F9", marginTop: "10px" }}
          >
            <Grid item xs={6} md={8}>
              <Box>
                Status :{" "}
                <StatusIndicator
                  status={statusData?.pipeline?.status?.phase.toUpperCase()}
                />{" "}
                {statusData?.pipeline?.status?.phase}
              </Box>
            </Grid>
            {/* <Grid item xs={5} md={5}>
              <Box>Maximum Lag : 10 s</Box>
            </Grid> */}
            <Grid item xs={6} md={8}>
              <Box>
                Health :{" "}
                <StatusIndicator status={statusData?.status?.toUpperCase()} />{" "}
                {statusData?.status}
              </Box>
            </Grid>
          </Grid>

          <Grid
            container
            spacing={2}
            sx={{ background: "#F9F9F9", marginTop: "10px" }}
          >
            <Grid item xs={12}>
              <Box>ISB Services : {isbData?.name}</Box>
            </Grid>
            <Grid item xs={12}>
              <Box>
                ISB Type :{" "}
                <Chip
                  label={isbData?.isbService?.status?.type}
                  sx={{ background: "#B3F3F3" }}
                />{" "}
              </Box>
            </Grid>
            <Grid item xs={12}>
              <Box>
                ISB Size :{" "}
                {
                  isbData?.isbService?.spec[isbData?.isbService.status?.type]
                    .replicas
                }
              </Box>
            </Grid>
          </Grid>

          <Grid
            container
            spacing={2}
            sx={{ background: "#F9F9F9", marginTop: "10px" }}
          >
            <Grid item xs={12}>
              <Box>
                Status :{" "}
                <StatusIndicator
                  status={isbData?.isbService?.status?.phase.toUpperCase()}
                />{" "}
                {isbData?.isbService?.status?.phase}
              </Box>
            </Grid>
            <Grid item xs={12}>
              <Box>
                Health :{" "}
                <StatusIndicator status={isbData?.status?.toUpperCase()} />{" "}
                {isbData?.status}
              </Box>
            </Grid>
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
                sx={{
                  color: "#0077C5",
                  border: "1px solid #0077C5",
                  height: "34px",
                  background: "#fff",
                }}
              >
                <MenuItem sx={{ display: "none" }} hidden value="edit">
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
                sx={{
                  color: "#0077C5",
                  border: "1px solid #0077C5",
                  height: "34px",
                  marginRight: "10px",
                  background: "#fff",
                }}
              >
                <MenuItem value="delete">Delete</MenuItem>
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
