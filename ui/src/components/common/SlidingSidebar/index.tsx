import React, { useState, useEffect, useCallback, useMemo } from "react";
import Box from "@mui/material/Box";
import { K8sEvents, K8sEventsProps } from "./partials/K8sEvents";
import { VertexDetails, VertexDetailsProps } from "./partials/VertexDetails";
import { PiplineSpec, PiplineSpecProps } from "./partials/PipelineSpec";
import { EdgeDetails, EdgeDetailsProps } from "./partials/EdgeDetails";
import {
  GeneratorDetails,
  GeneratorDetailsProps,
} from "./partials/GeneratorDetails";
import { Errors, ErrorsProps } from "./partials/Errors";
import { PiplineCreate } from "./partials/PipelineCreate";
import { PiplineUpdate } from "./partials/PipelineUpdate";
import { ISBCreate } from "./partials/ISBCreate";
import { ISBUpdate } from "./partials/ISBUpdate";
import { ViewType } from "../SpecEditor";
import IconButton from "@mui/material/IconButton";
import CloseIcon from "@mui/icons-material/Close";
import slider from "../../../images/slider.png";

import "./style.css";

export enum SidebarType {
  NAMESPACE_K8s,
  PIPELINE_K8s,
  PIPELINE_CREATE,
  PIPELINE_UPDATE,
  PIPELINE_SPEC,
  ISB_CREATE,
  ISB_UPDATE,
  VERTEX_DETAILS,
  EDGE_DETAILS,
  GENERATOR_DETAILS,
  ERRORS,
}

const MIN_WIDTH_BY_TYPE = {
  [SidebarType.NAMESPACE_K8s]: 750,
  [SidebarType.PIPELINE_K8s]: 750,
  [SidebarType.PIPELINE_CREATE]: 750,
  [SidebarType.PIPELINE_UPDATE]: 750,
  [SidebarType.PIPELINE_SPEC]: 750,
  [SidebarType.ISB_CREATE]: 750,
  [SidebarType.ISB_UPDATE]: 750,
  [SidebarType.VERTEX_DETAILS]: 750,
  [SidebarType.EDGE_DETAILS]: 750,
  [SidebarType.GENERATOR_DETAILS]: 750,
  [SidebarType.ERRORS]: 350,
};

export interface SpecEditorSidebarProps {
  initialYaml?: any;
  namespaceId?: string;
  pipelineId?: string;
  isbId?: string;
  viewType?: ViewType;
  onUpdateComplete?: () => void;
}

export interface SlidingSidebarProps {
  pageWidth: number;
  slide?: boolean;
  type: SidebarType;
  k8sEventsProps?: K8sEventsProps;
  vertexDetailsProps?: VertexDetailsProps;
  edgeDetailsProps?: EdgeDetailsProps;
  pipelineSpecProps?: PiplineSpecProps;
  generatorDetailsProps?: GeneratorDetailsProps;
  errorsProps?: ErrorsProps;
  specEditorProps?: SpecEditorSidebarProps;
  onClose: () => void;
}

export function SlidingSidebar({
  pageWidth,
  slide = true,
  type,
  k8sEventsProps,
  vertexDetailsProps,
  edgeDetailsProps,
  pipelineSpecProps,
  generatorDetailsProps,
  errorsProps,
  specEditorProps,
  onClose,
}: SlidingSidebarProps) {
  const [width, setWidth] = useState<number>(
    errorsProps
      ? MIN_WIDTH_BY_TYPE[SidebarType.ERRORS]
      : pageWidth
      ? pageWidth / 2
      : 0
  );
  const [minWidth, setMinWidth] = useState<number>(0);

  // Set min width by type
  useEffect(() => {
    setMinWidth(MIN_WIDTH_BY_TYPE[type] || 0);
  }, [type]);

  // Don't allow width greater then pageWidth
  useEffect(() => {
    if (width > pageWidth) {
      setWidth(pageWidth);
    }
  }, [width, pageWidth]);

  const dragHandler = useCallback(
    (mouseDownEvent) => {
      const startWidth = width;
      const startPosition = mouseDownEvent.pageX;
      const onMouseMove = (mouseMoveEvent) => {
        const result = startWidth + startPosition - mouseMoveEvent.pageX;
        if (!minWidth || result >= minWidth) {
          setWidth(result);
        }
      };
      const onMouseUp = () => {
        document.body.removeEventListener("mousemove", onMouseMove);
      };
      document.body.addEventListener("mousemove", onMouseMove);
      document.body.addEventListener("mouseup", onMouseUp, { once: true });
    },
    [width, minWidth]
  );

  const content = useMemo(() => {
    switch (type) {
      case SidebarType.NAMESPACE_K8s:
      case SidebarType.PIPELINE_K8s:
        if (!k8sEventsProps) {
          break;
        }
        return <K8sEvents {...k8sEventsProps} />;
      case SidebarType.PIPELINE_CREATE:
        if (!specEditorProps || !specEditorProps.namespaceId) {
          break;
        }
        return <PiplineCreate {...specEditorProps} />;
      case SidebarType.PIPELINE_UPDATE:
        if (
          !specEditorProps ||
          !specEditorProps.namespaceId ||
          !specEditorProps.pipelineId
        ) {
          break;
        }
        return <PiplineUpdate {...specEditorProps} />;
      case SidebarType.ISB_CREATE:
        if (!specEditorProps || !specEditorProps.namespaceId) {
          break;
        }
        return <ISBCreate {...specEditorProps} />;
      case SidebarType.ISB_UPDATE:
        if (!specEditorProps || !specEditorProps.namespaceId || !specEditorProps.isbId) {
          break;
        }
        return <ISBUpdate {...specEditorProps} />;
      case SidebarType.VERTEX_DETAILS:
        if (!vertexDetailsProps) {
          break;
        }
        return <VertexDetails {...vertexDetailsProps} />;
      case SidebarType.EDGE_DETAILS:
        if (!edgeDetailsProps) {
          break;
        }
        return <EdgeDetails {...edgeDetailsProps} />;
      case SidebarType.PIPELINE_SPEC:
        if (!pipelineSpecProps) {
          break;
        }
        return <PiplineSpec {...pipelineSpecProps} />;
      case SidebarType.GENERATOR_DETAILS:
        if (!generatorDetailsProps) {
          break;
        }
        return <GeneratorDetails {...generatorDetailsProps} />;
      case SidebarType.ERRORS:
        if (!errorsProps) {
          break;
        }
        return <Errors {...errorsProps} />;
      default:
        break;
    }
    return <div>Missing Props</div>;
  }, [
    type,
    k8sEventsProps,
    specEditorProps,
    vertexDetailsProps,
    edgeDetailsProps,
    pipelineSpecProps,
    generatorDetailsProps,
    errorsProps,
  ]);

  return (
    <Box
      sx={{
        display: "flex",
        flexDirection: "row",
        backgroundColor: "#F8F8FB",
        width: width,
        minWidth: minWidth,
        height: "100%",
      }}
    >
      {slide && (
        <Box
          sx={{
            display: "flex",
            flexDirection: "column",
            justifyContent: "center",
            height: "100vh",
          }}
        >
          <img
            onMouseDown={dragHandler}
            src={slider}
            alt="slider"
            className={"sidebar-drag-icon"}
            draggable={false}
          />
        </Box>
      )}
      <Box
        sx={{
          display: "flex",
          flexDirection: "column",
          paddingTop: "5.25rem",
          paddingRight: "1.5rem",
          paddingBottom: "1.5rem",
          paddingLeft: "1rem",
          width: "100%",
          height: "calc(100% - 6.75rem)",
          overflowX: "scroll",
        }}
      >
        <Box
          sx={{
            display: "flex",
            flexDirection: "row",
            justifyContent: "flex-end",
          }}
        >
          <IconButton data-testid="close-button" onClick={onClose}>
            <CloseIcon />
          </IconButton>
        </Box>
        {content}
      </Box>
    </Box>
  );
}
