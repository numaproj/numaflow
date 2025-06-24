import React from "react";
import { FC, memo, useCallback, useContext, useEffect, useMemo } from "react";
import { Tooltip } from "@mui/material";
import {
  BaseEdge,
  Edge,
  EdgeProps,
  EdgeLabelRenderer,
  getSimpleBezierPath,
} from "@xyflow/react";
import { duration } from "moment";
import { HighlightContext } from "../../index";
import { HighlightContextProps } from "../../../../../../../types/declarations/graph";
import error from "../../../../../../../images/error.svg";

import "@xyflow/react/dist/style.css";
import "./style.css";

const CustomEdge: FC<EdgeProps<Edge<Record<string, any>>>> = ({
  id,
  sourceX,
  sourceY,
  targetX,
  targetY,
  sourcePosition,
  targetPosition,
  data,
  markerEnd,
}) => {
  const straightWidth = 50;

  // eslint-disable-next-line prefer-const
  let [sX, sY, tX, tY] = [
    sourceX + straightWidth,
    sourceY,
    targetX - straightWidth,
    targetY,
  ];

  if (data?.sideInputEdge) {
    tX = tX + straightWidth;
    tY = tY + straightWidth;
  }

  const obj = getSimpleBezierPath({
    sourceX: sX,
    sourceY: sY,
    sourcePosition,
    targetX: tX,
    targetY: tY,
    targetPosition,
  });

  let [edgePath] = obj;

  let labelRenderer = "";

  edgePath = `M ${
    sX - straightWidth
  } ${sY} L ${sX} ${sY} ${edgePath} M ${tX} ${tY} L ${
    data?.sideInputEdge ? tX : tX + straightWidth
  } ${data?.sideInputEdge ? tY - straightWidth : tY}`;

  if (data?.fwdEdge) {
    const centerX = (sourceX + targetX) / 2;
    const centerY = (sourceY + targetY) / 2;
    labelRenderer = `translate(-50%, -50%) translate(${centerX}px,${centerY}px)`;
  }

  if (data?.backEdge) {
    const height = data?.backEdgeHeight * 35 + 30;
    edgePath = `M ${sourceX} ${sourceY} L ${sourceX} ${
      -height + 5
    } Q ${sourceX} ${-height} ${sourceX - 5} ${-height} L ${
      targetX + 5
    } ${-height} Q ${targetX} ${-height} ${targetX} ${
      -height + 5
    } L ${targetX} ${targetY}`;
    const centerX = (sourceX + targetX) / 2;
    labelRenderer = `translate(-50%, -50%) translate(${centerX}px,${-height}px)`;
  }

  if (data?.selfEdge) {
    edgePath = `M ${sourceX} ${sourceY} L ${sourceX} ${
      sourceY - 25
    } Q ${sourceX} ${sourceY - 30} ${sourceX - 5} ${sourceY - 30} L ${
      targetX + 5
    } ${targetY - 30} Q ${targetX} ${sourceY - 30} ${targetX} ${
      sourceY - 25
    } L ${targetX} ${sourceY - 8}`;
    const centerX = (sourceX + targetX) / 2;
    labelRenderer = `translate(-50%, -50%) translate(${centerX}px,${
      sourceY - 30
    }px)`;
  }

  const {
    highlightValues,
    setHighlightValues,
    sideInputNodes,
    sideInputEdges,
    hoveredEdge,
  } = useContext<HighlightContextProps>(HighlightContext);

  const getColor = useMemo(() => {
    return data?.isFull ? "#DB334D" : "#8D9096";
  }, [data]);

  useEffect(() => {
    if (data?.sideInputEdge) return;
    let sourceClass = `source-handle-${data?.source}`;
    let targetClass = `target-handle-${data?.target}`;
    if (data?.backEdge) {
      sourceClass = `center-${sourceClass}`;
      targetClass = `center-${targetClass}`;
    } else if (data?.selfEdge) {
      sourceClass = `quad-${sourceClass}`;
      targetClass = `quad-${targetClass}`;
    }
    const handleElements = Array.from(
      document.querySelectorAll(`.${targetClass}, .${sourceClass}`)
    ) as HTMLElement[];

    handleElements.forEach((handle) => {
      handle.style.background = getColor;
    });
  }, [data, getColor]);

  const commonStyle = useMemo(() => {
    const style: any = {};
    if (
      !sideInputEdges.has(id) &&
      sideInputNodes.has(Object.keys(highlightValues)[0]) &&
      highlightValues["---"]
    ) {
      style["opacity"] = 0.5;
    }
    return style;
  }, [highlightValues, sideInputNodes, sideInputEdges]);

  const edgeStyle = useMemo(() => {
    return {
      stroke: highlightValues[id]
        ? "black"
        : data?.sideInputEdge
        ? "#274C77"
        : getColor,
      strokeWidth: 2,
      ...commonStyle,
    };
  }, [highlightValues, getColor, data, commonStyle]);

  const getMinWM = useMemo(() => {
    if (data?.edgeWatermark?.watermarks) {
      return Math.min.apply(null, data?.edgeWatermark?.watermarks);
    } else {
      return -1;
    }
  }, [data]);

  const getDelay = useMemo(() => {
    const str = " behind now";
    const fetchWMTime = data?.edgeWatermark?.WMFetchTime;
    const minWM = getMinWM;
    const startTime = minWM === -1 ? 0 : minWM;
    const endTime = fetchWMTime || Date.now();
    const diff = duration(endTime - startTime);

    const years = diff.years();
    const months = diff.months();
    const days = diff.days();
    const hours = diff.hours();
    const minutes = diff.minutes();
    const seconds = diff.seconds();
    const milliseconds = diff.milliseconds();

    if (years > 0) {
      return `${years}yr ${months}mo` + str;
    } else if (months > 0) {
      return `${months}mo ${days}d` + str;
    } else if (days > 0) {
      return `${days}d ${hours}hr` + str;
    } else if (hours > 0) {
      return `${hours}hr ${minutes}min` + str;
    } else if (minutes > 0) {
      return `${minutes}min ${seconds}sec` + str;
    } else if (seconds > 0) {
      return `${seconds}sec ${milliseconds}ms` + str;
    } else {
      return `${milliseconds}ms` + str;
    }
  }, [data, getMinWM]);

  const handleClick = useCallback(() => {
    const updatedEdgeHighlightValues: any = {};
    updatedEdgeHighlightValues[id] = true;
    setHighlightValues(updatedEdgeHighlightValues);
  }, [setHighlightValues]);

  return (
    <>
      <BaseEdge
        id={id}
        path={edgePath}
        style={edgeStyle}
        data-testid={id}
        markerEnd={markerEnd}
        interactionWidth={40}
      />
      <EdgeLabelRenderer>
        <div
          className={"edge"}
          style={{
            transform: labelRenderer,
            flexDirection: sourceY === targetY ? "column" : "row",
          }}
          onClick={handleClick}
        >
          {data?.edgeWatermark?.isWaterMarkEnabled && (
            <Tooltip
              title={
                <div className={"edge-tooltip"}>
                  <div>Watermark</div>
                  {getMinWM < 0 ? (
                    <div>Not Available</div>
                  ) : (
                    <>
                      <div style={{ fontWeight: 700 }}>{getMinWM}</div>
                      <div style={{ fontWeight: 700 }}>
                        {new Date(getMinWM).toISOString()}
                      </div>
                      <div style={{ fontWeight: 700 }}>{getDelay}</div>
                    </>
                  )}
                </div>
              }
              arrow
              placement={sourceY === targetY ? "top" : "left"}
              open={hoveredEdge === id}
            >
              <div />
            </Tooltip>
          )}
          <Tooltip
            title={
              <div className={"edge-tooltip"}>
                <div
                  style={{
                    display: "flex",
                    alignItems: "center",
                    justifyContent: "center",
                    width: "100%",
                  }}
                >
                  {data?.isFull && <img src={error} alt={"error"} />}
                  <div style={{ display: "flex", flexDirection: "column", alignItems: "center", gap: "2px" }}>
                    <span>Pending: {data?.pendingLabel || 0}</span>
                    <span>AckPending: {data?.ackPendingLabel || 0}</span>
                  </div>
                </div>
              </div>
            }
            arrow
            placement={sourceY === targetY ? "bottom" : "right"}
            open={hoveredEdge === id}
          >
            <div />
          </Tooltip>
        </div>
      </EdgeLabelRenderer>
    </>
  );
};

export default memo(CustomEdge);
