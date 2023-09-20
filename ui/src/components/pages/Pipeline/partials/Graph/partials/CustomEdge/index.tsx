import { FC, memo, useCallback, useContext, useEffect, useMemo } from "react";
import { Tooltip } from "@mui/material";
import { EdgeProps, EdgeLabelRenderer, getSmoothStepPath } from "reactflow";
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
  const obj = getSmoothStepPath({
    sourceX,
    sourceY,
    sourcePosition,
    targetX,
    targetY,
    targetPosition,
  });

  let [edgePath, labelX, labelY] = obj;
  let labelRenderer = "";

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

  const { highlightValues, setHighlightValues } =
    useContext<HighlightContextProps>(HighlightContext);

  const getColor = useMemo(() => {
    return data?.isFull ? "#DB334D" : "#8D9096";
  }, [data]);

  useEffect(() => {
    const handles = Array.from(
      document.getElementsByClassName(
        "react-flow__handle"
      ) as HTMLCollectionOf<HTMLElement>
    );
    handles.forEach((handle) => {
      handle.style.background = getColor;
    });
  }, [getColor]);

  const edgeStyle = useMemo(() => {
    return {
      stroke: highlightValues[id] ? "black" : getColor,
      strokeWidth: 2,
    };
  }, [highlightValues, getColor]);

  const wmStyle = useMemo(() => {
    return {
      color: getColor,
    };
  }, [getColor]);

  const pendingStyle = useMemo(() => {
    return {
      color: getColor,
      fontWeight: data?.isFull ? 700 : 400,
    };
  }, [data, getColor]);

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
    const updatedNodeHighlightValues = {};
    updatedNodeHighlightValues[id] = true;
    setHighlightValues(updatedNodeHighlightValues);
  }, []);

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
      <EdgeLabelRenderer>
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
                  <div>{new Date(getMinWM).toISOString()}</div>
                  <div>{getDelay}</div>
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
      </EdgeLabelRenderer>
    </>
  );
};

export default memo(CustomEdge);
