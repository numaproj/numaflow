import React, { useState, useEffect, useCallback, useMemo } from "react";
import Box from "@mui/material/Box";

import "./style.css";
import { NamespaceK8s, NamespaceK8sProps } from "./partials/NamespaceK8s";

export enum SideBarType {
  NAMESPACE_K8s,
}

export interface SideBarProps {
  pageWidth: number;
  minWidth?: number;
  type: SideBarType;
  namespaceK8sProps?: NamespaceK8sProps;
}

export function SideBarContent({
  pageWidth,
  minWidth,
  type,
  namespaceK8sProps,
}: SideBarProps) {
  const [width, setWidth] = useState<number>(pageWidth ? pageWidth / 2 : 0);

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
        setWidth(startWidth + startPosition - mouseMoveEvent.pageX);
      };
      const onMouseUp = () => {
        document.body.removeEventListener("mousemove", onMouseMove);
      };
      document.body.addEventListener("mousemove", onMouseMove);
      document.body.addEventListener("mouseup", onMouseUp, { once: true });
    },
    [width]
  );

  const content = useMemo(() => {
    switch (type) {
      case SideBarType.NAMESPACE_K8s:
        if (!namespaceK8sProps) {
          break;
        }
        return <NamespaceK8s namespaceId={namespaceK8sProps.namespaceId} />;
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
        <div onMouseDown={dragHandler} className="sidebar-drag-icon"></div>
      </Box>
      <Box
        sx={{
          display: "flex",
          flexDirection: "column",
          paddingTop: "5.8125rem",
        }}
      >
        {content}
      </Box>
    </Box>
  );
}
