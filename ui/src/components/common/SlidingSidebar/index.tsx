import React, { useState, useEffect, useCallback, useMemo } from "react";
import Box from "@mui/material/Box";
import { K8sEvents, K8sEventsProps } from "./partials/K8sEvents";
import { VertexDetails, VertexDetailsProps } from "./partials/VertexDetails";
import { PiplineSpecs, PiplineSpecsProps } from "./partials/PipelineSpecs";
import { EdgeDetails, EdgeDetailsProps } from "./partials/EdgeDetails";
import IconButton from "@mui/material/IconButton";
import CloseIcon from "@mui/icons-material/Close";
import slider from "../../../images/slider.png";

import "./style.css";

export enum SidebarType {
  NAMESPACE_K8s,
  PIPELINE_K8s,
  VERTEX_DETAILS,
  EDGE_DETAILS,
  PIPELINE_SPECS,
}

const MIN_WIDTH_BY_TYPE = {
  [SidebarType.NAMESPACE_K8s]: 750,
  [SidebarType.PIPELINE_K8s]: 750,
  [SidebarType.VERTEX_DETAILS]: 750,
  [SidebarType.EDGE_DETAILS]: 750,
  [SidebarType.PIPELINE_SPECS]: 750,
};

export interface SlidingSidebarProps {
  pageWidth: number;
  type: SidebarType;
  k8sEventsProps?: K8sEventsProps;
  vertexDetailsProps?: VertexDetailsProps;
  edgeDetailsProps?: EdgeDetailsProps;
  pipelineSpecsProps?: PiplineSpecsProps;
  onClose: () => void;
}

export function SlidingSidebar({
  pageWidth,
  type,
  k8sEventsProps,
  vertexDetailsProps,
  edgeDetailsProps,
  pipelineSpecsProps,
  onClose,
}: SlidingSidebarProps) {
  const [width, setWidth] = useState<number>(pageWidth ? pageWidth / 2 : 0);
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
      case SidebarType.PIPELINE_SPECS:
        if (!pipelineSpecsProps) {
          break;
        }
        return <PiplineSpecs {...pipelineSpecsProps} />;
      default:
        break;
    }
    return <div>Missing Props</div>;
  }, [type, k8sEventsProps]);

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
