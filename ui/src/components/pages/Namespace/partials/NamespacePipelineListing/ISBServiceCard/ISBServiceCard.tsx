import React, { useCallback, useContext, useState } from "react";
import Paper from "@mui/material/Paper";
import { Box, Button, Grid } from "@mui/material";
import { DeleteModal } from "../../DeleteModal";
import {
  GetISBType,
  IconsStatusMap,
  ISBStatusString,
  UNKNOWN,
} from "../../../../../../utils";
import { AppContextProps } from "../../../../../../types/declarations/app";
import { AppContext } from "../../../../../../App";
import { SidebarType } from "../../../../../common/SlidingSidebar";
import { ViewType } from "../../../../../common/SpecEditor";
import pipelineIcon from "../../../../../../images/pipeline.png";
import { ISBServicesListing } from "../ISBServiceTypes";

export interface DeleteProps {
  type: "pipeline" | "isb";
  pipelineId?: string;
  isbId?: string;
}

export interface ISBServiceCardProps {
  namespace: string;
  data: ISBServicesListing;
  refresh: () => void;
}

export function ISBServiceCard({
  namespace,
  data,
  refresh,
}: ISBServiceCardProps) {
  const { setSidebarProps } = useContext<AppContextProps>(AppContext);
  const [deleteProps, setDeleteProps] = useState<DeleteProps | undefined>();

  const handleUpdateComplete = useCallback(() => {
    refresh();
    if (!setSidebarProps) {
      return;
    }
    // Close sidebar
    setSidebarProps(undefined);
  }, [setSidebarProps, refresh]);

  const handleEditChange = useCallback(() => {
    setSidebarProps({
      type: SidebarType.ISB_UPDATE,
      specEditorProps: {
        initialYaml: data?.isbService,
        namespaceId: namespace,
        isbId: data?.name,
        viewType: ViewType.EDIT,
        onUpdateComplete: handleUpdateComplete,
      },
    });
  }, [setSidebarProps, handleUpdateComplete, data]);

  const handleDeleteChange = useCallback(() => {
    setDeleteProps({
      type: "isb",
      isbId: data?.name,
    });
  }, [data]);

  const handleDeleteComplete = useCallback(() => {
    refresh();
    setDeleteProps(undefined);
  }, [refresh]);

  const handeDeleteCancel = useCallback(() => {
    setDeleteProps(undefined);
  }, []);

  const isbType = GetISBType(data?.isbService?.spec) || UNKNOWN;
  const isbStatus = data?.isbService?.status?.phase || UNKNOWN;
  const isbSize =
    isbType !== UNKNOWN && data?.isbService?.spec[isbType]
      ? data?.isbService?.spec[isbType].replicas
        ? data?.isbService?.spec[isbType].replicas
        : 3
      : UNKNOWN;

  return (
    <>
      <Paper
        sx={{
          display: "flex",
          flexDirection: "column",
          width: "100%",
          borderRadius: "1rem",
        }}
      >
        <Box
          sx={{
            display: "flex",
            flexDirection: "row",
            flexGrow: 1,
            paddingTop: "1rem",
            paddingLeft: "1rem",
            paddingRight: "1rem",
            paddingBottom: "0.8rem",
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
        </Box>

        <Box
          sx={{
            display: "flex",
            background: "#F9F9F9",
            flexDirection: "row",
            flexGrow: 1,
            padding: "1rem",
            paddingTop: "0",
            width: "100%",
            borderBottomLeftRadius: "1rem",
            borderBottomRightRadius: "1rem",
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
                src={IconsStatusMap[isbStatus]}
                alt="Status"
                className={"pipeline-logo"}
              />
              <img
                src={IconsStatusMap[data?.status]}
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
              <span>{ISBStatusString[data?.status]}</span>
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
              <span>Name:</span>
              <span>Type:</span>
              <span>Size:</span>
            </Box>
            <Box
              sx={{
                display: "flex",
                flexDirection: "column",
                paddingTop: "1rem",
                paddingLeft: "1rem",
              }}
            >
              <span>{data?.name}</span>
              <span>{isbType}</span>
              <span>{isbSize}</span>
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
              <Button
                onClick={handleEditChange}
                variant="contained"
                data-testid="edit-isb"
              >
                Edit
              </Button>
            </Grid>
            <Grid item>
              <Button
                onClick={handleDeleteChange}
                variant="contained"
                sx={{ marginRight: "1rem" }}
                data-testid="delete-isb"
              >
                Delete
              </Button>
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
