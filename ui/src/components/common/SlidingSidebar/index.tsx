import React, { useState, useEffect, useCallback, useMemo } from "react";
import Box from "@mui/material/Box";
import { NamespaceK8s, NamespaceK8sProps } from "./partials/NamespaceK8s";
import { VertexDetails, VertexDetailsProps } from "./partials/VertexDetails";
import IconButton from "@mui/material/IconButton";
import CloseIcon from "@mui/icons-material/Close";
import slider from "../../../images/slider.png";

import "./style.css";

export enum SidebarType {
  NAMESPACE_K8s,
  VERTEX_DETAILS,
}

const MIN_WIDTH_BY_TYPE = {
  [SidebarType.NAMESPACE_K8s]: 750,
  [SidebarType.VERTEX_DETAILS]: 750,
};

export interface SlidingSidebarProps {
  pageWidth: number;
  type: SidebarType;
  namespaceK8sProps?: NamespaceK8sProps;
  vertexDetailsProps?: VertexDetailsProps;
  onClose: () => void;
}

export function SlidingSidebar({
  pageWidth,
  type,
  namespaceK8sProps,
  vertexDetailsProps,
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
        if (!namespaceK8sProps) {
          break;
        }
        return <NamespaceK8s {...namespaceK8sProps} />;
      case SidebarType.VERTEX_DETAILS:
        if (!vertexDetailsProps) {
          break;
        }
        return <VertexDetails {...vertexDetailsProps} />;
      default:
        break;
    }
    return <div>Missing Props</div>;
  }, [type, namespaceK8sProps]);

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
