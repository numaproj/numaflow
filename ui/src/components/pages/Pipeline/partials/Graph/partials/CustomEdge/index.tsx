import React from "react";
import { FC, memo, useCallback, useContext, useEffect, useMemo } from "react";
import { Tooltip } from "@mui/material";
import { EdgeProps, EdgeLabelRenderer, getSimpleBezierPath } from "reactflow";
import { duration } from "moment";
import { HighlightContext } from "../../index";
import { HighlightContextProps } from "../../../../../../../types/declarations/graph";
import error from "../../../../../../../images/error.svg";

import "reactflow/dist/style.css";
import "./style.css";

const CustomEdge: FC<EdgeProps> = ({
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
  const straightWidth = 50
  const obj = getSimpleBezierPath({
    sourceX: sourceX + straightWidth,
    sourceY,
    sourcePosition,
    targetX: targetX - straightWidth,
    targetY,
    targetPosition,
  });

  const debug = false

  let [edgePath, labelX, labelY] = obj;

  // console.log( sourceX, sourceY, edgePath, sourcePosition, data)
  let labelRenderer = "";

  // connect center of nodes
  // edgePath = `M ${sourceX-252/2} ${sourceY} L ${targetX+252/2} ${targetY}`;

  // connect small straight lines
  // edgePath = `M ${sourceX} ${sourceY} L ${sourceX+straightWidth} ${sourceY}
  // M ${sourceX+straightWidth} ${sourceY} L ${targetX-straightWidth} ${targetY}
  // M ${targetX-straightWidth} ${targetY} L ${targetX} ${targetY}`;

  // connect small straight lines with curve
  // edgePath = `M ${sourceX} ${sourceY} L ${sourceX+straightWidth} ${sourceY}
  // M ${sourceX+straightWidth} ${sourceY} C ${targetX-straightWidth} ${targetY}
  // M ${targetX-straightWidth} ${targetY} L ${targetX} ${targetY}`;

  // 259.1953125 125.8046875 'M259.1953125,125.8046875 C372,125.8046875 372,35.8046875 484.8046875,35.8046875'

  edgePath = `M ${sourceX} ${sourceY} L ${sourceX+straightWidth} ${sourceY}
  M ${sourceX+straightWidth} ${sourceY} ${edgePath} M ${targetX - straightWidth} ${targetY} L ${targetX} ${targetY}`;

  if (data?.fwdEdge) {
    if (sourceY !== targetY) {
      if (data?.fromNodeOutDegree > 1) {
        labelY = targetY;
        labelX = targetX - 63;
      } else {
        labelY = sourceY;
        labelX = sourceX + 63;
      }
    } else if (data?.fromNodeOutDegree > 1) {
      labelX = sourceX + ((targetX - sourceX) * 3) / 4;
    }
    labelRenderer = `translate(-50%, -50%) translate(${labelX}px,${labelY}px)`;
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
      labelY - 10
    }px)`;
  }

  const {
    highlightValues,
    setHighlightValues,
    sideInputNodes,
    sideInputEdges,
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

  const wmStyle = useMemo(() => {
    return {
      color: getColor,
      ...commonStyle,
    };
  }, [getColor, commonStyle]);

  const pendingStyle = useMemo(() => {
    return {
      color: getColor,
      fontWeight: data?.isFull ? 700 : 400,
      ...commonStyle,
    };
  }, [data, getColor, commonStyle]);

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
      <svg>
        <path
          id={id}
          className="react-flow__edge-path"
          d={edgePath}
          style={edgeStyle}
          data-testid={id}
          markerEnd={markerEnd}
          onClick={handleClick}
        />
      </svg>
      {debug && (<EdgeLabelRenderer>
        <div
          className={"edge"}
          style={{
            transform: labelRenderer,
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
                      <div>{new Date(getMinWM).toISOString()}</div>
                      <div>{getDelay}</div>
                    </>
                  )}
                </div>
              }
              arrow
              placement={"top"}
            >
              <div className={"edge-watermark-label"} style={wmStyle}>
                {getMinWM}
              </div>
            </Tooltip>
          )}
          <Tooltip
            title={<div className={"edge-tooltip"}>Pending</div>}
            arrow
            placement={"bottom"}
          >
            <div className={"edge-pending-label"} style={pendingStyle}>
              {data?.isFull && <img src={error} alt={"error"} />}
              {data?.backpressureLabel}
            </div>
          </Tooltip>
        </div>
      </EdgeLabelRenderer>)}
    </>
  );
};

export default memo(CustomEdge);
