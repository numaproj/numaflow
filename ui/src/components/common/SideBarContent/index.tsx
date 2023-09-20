import React, { useState, useEffect, useCallback } from "react";
import Box from "@mui/material/Box";

import "./style.css";

export enum SideBarType {
  NAMESPACE_K8s,
}

export interface NamespaceK8sProps {
  namespaceId: string;
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
        TODO CONTENT
      </Box>
    </Box>
  );
}
