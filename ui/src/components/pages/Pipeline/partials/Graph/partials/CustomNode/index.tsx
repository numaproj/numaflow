import { FC, memo, useCallback, useContext, useMemo } from "react";
import { Tooltip } from "@mui/material";
import { Handle, NodeProps, Position } from "reactflow";
import { HighlightContext } from "../../index";
import { HighlightContextProps } from "../../../../../../../types/declarations/graph";
import healthy from "../../../../../../../images/heart-fill.svg";
import unhealthy from "../../../../../../../images/unhealthy.svg";
import source from "../../../../../../../images/source.svg";
import map from "../../../../../../../images/map.svg";
import reduce from "../../../../../../../images/reduce.svg";
import sink from "../../../../../../../images/sink.svg";
import input from "../../../../../../../images/input.svg";
import generator from "../../../../../../../images/generator.svg";

import "reactflow/dist/style.css";
import "./style.css";

const getBorderColor = (nodeType: string) => {
  return nodeType === "source"
    ? "#3874CB"
    : nodeType === "udf"
    ? "#009EAC"
    : "#577477";
};

const isSelected = (selected: boolean) => {
  return selected ? "0.1875rem solid" : "0.0625rem solid";
};

const CustomNode: FC<NodeProps> = ({
  data,
  isConnectable,
  sourcePosition = Position.Bottom,
  targetPosition = Position.Top,
}: NodeProps) => {
  //TODO add check for healthy/unhealthy node and update imported images accordingly

  const {
    highlightValues,
    setHighlightValues,
    setHidden,
    sideInputNodes,
    handleNodeClick,
    sideInputEdges,
  } = useContext<HighlightContextProps>(HighlightContext);

  const handleClick = useCallback(() => {
    const updatedNodeHighlightValues = {};
    updatedNodeHighlightValues[data?.name] = true;
    setHighlightValues(updatedNodeHighlightValues);
  }, [data, setHighlightValues]);

  const commonStyle = useMemo(() => {
    const style = {};
    if (
      !sideInputNodes.has(data?.name) &&
      sideInputNodes.has(Object.keys(highlightValues)[0])
    ) {
      style["opacity"] = 0.5;
    }
    return style;
  }, [highlightValues, sideInputNodes, data]);

  const blurHandle = (id: string) => {
    const style = {};
    const sourceVertex = Object.keys(highlightValues)[0];
    if (sideInputNodes.has(sourceVertex)) {
      const edgeId = sourceVertex + "-" + data?.name;
      if (!sideInputEdges.has(edgeId)) {
        style["opacity"] = 0.5;
      } else {
        if (sideInputEdges.get(edgeId) !== id) {
          style["opacity"] = 0.5;
        }
      }
    }
    return style;
  };

  const nodeStyle = useMemo(() => {
    return {
      border: `${isSelected(highlightValues[data?.name])} ${getBorderColor(
        data?.type
      )}`,
      ...commonStyle,
    };
  }, [highlightValues, data]);

  if (data?.type === "sideInput") {
    return (
      <Tooltip
        title={<div className={"node-tooltip"}>{data?.name}</div>}
        arrow
        placement={"top-end"}
      >
        <div className={"sideInput_node"} onClick={handleClick}>
          <img src={generator} alt={"generator"} width={22} height={24} />
          <Handle
            type="source"
            id="2"
            position={Position.Right}
            style={{ top: "60%", left: "35%" }}
          />
        </div>
      </Tooltip>
    );
  }

  const handleInputClick = useCallback(
    (e) => {
      e.stopPropagation();
      const targetId = e.target.id;
      let source: string;
      sideInputNodes.forEach((_, node) => {
        const possibleEdge = `${node}-${data?.name}`;
        if (
          sideInputEdges.has(possibleEdge) &&
          sideInputEdges.get(possibleEdge) === targetId
        ) {
          source = node;
        }
      });
      handleNodeClick(e, sideInputNodes.get(source));
    },
    [sideInputEdges, data, sideInputNodes, handleNodeClick]
  );

  const handleMouseOver = useCallback(
    (e) => {
      const targetId = e.target.id;
      let source: string;

      sideInputNodes.forEach((_, node) => {
        const possibleEdge = `${node}-${data?.name}`;
        if (
          sideInputEdges.has(possibleEdge) &&
          sideInputEdges.get(possibleEdge) === targetId
        ) {
          source = node;
        }
      });
      setHidden((prevState) => {
        const updatedState = {};
        Object.keys(prevState).forEach((key) => {
          updatedState[key] = true;
        });
        updatedState[source] = false;
        return updatedState;
      });
      const updatedHighlightedState = {};
      updatedHighlightedState[source] = true;
      setHighlightValues(updatedHighlightedState);
    },
    [data, sideInputNodes, sideInputEdges, setHidden, setHighlightValues]
  );

  const handleMouseOut = useCallback(() => {
    setHidden((prevState) => {
      const updatedState = {};
      Object.keys(prevState).forEach((key) => {
        updatedState[key] = true;
      });
      return updatedState;
    });
    setHighlightValues({});
  }, [setHidden, setHighlightValues]);

  return (
    <div data-testid={data?.name}>
      <div
        className={"react-flow__node-input"}
        onClick={handleClick}
        style={nodeStyle}
      >
        <div className="node-info">{data?.name}</div>

        <Tooltip
          title={
            <div className={"node-tooltip"}>
              {data?.podnum <= 1 ? "pod" : "pods"}
            </div>
          }
          placement={"top-end"}
          arrow
        >
          <div className={"node-pods"}>
            {data?.type === "source" && (
              <img src={source} alt={"source-vertex"} />
            )}
            {data?.type === "udf" && data?.nodeInfo?.udf?.groupBy === null && (
              <img src={map} alt={"map-vertex"} />
            )}
            {data?.type === "udf" && data?.nodeInfo?.udf?.groupBy && (
              <img src={reduce} alt={"reduce-vertex"} />
            )}
            {data?.type === "sink" && <img src={sink} alt={"sink-vertex"} />}
            {data?.podnum}
          </div>
        </Tooltip>

        <div className={"node-status"}>
          <img src={healthy} alt={"healthy"} />
        </div>

        <Tooltip
          title={
            <div className={"node-tooltip"}>
              <div>Processing Rates</div>
              <div>1 min: {data?.vertexMetrics?.ratePerMin}/sec</div>
              <div>5 min: {data?.vertexMetrics?.ratePerFiveMin}/sec</div>
              <div>15 min: {data?.vertexMetrics?.ratePerFifteenMin}/sec</div>
            </div>
          }
          arrow
          placement={"bottom-end"}
        >
          <div className={"node-rate"}>
            {data?.vertexMetrics?.ratePerMin}/sec
          </div>
        </Tooltip>

        {(data?.type === "udf" || data?.type === "sink") && (
          <Handle
            type="target"
            id="0"
            className={`target-handle-${data?.name}`}
            position={targetPosition}
            isConnectable={isConnectable}
          />
        )}
        {(data?.type === "source" || data?.type === "udf") && (
          <Handle
            type="source"
            id="0"
            className={`source-handle-${data?.name}`}
            position={sourcePosition}
            isConnectable={isConnectable}
          />
        )}
        {data?.centerSourceHandle && (
          <Handle
            type="source"
            id="1"
            className={`center-source-handle-${data?.name}`}
            position={Position.Top}
          />
        )}
        {data?.centerTargetHandle && (
          <Handle
            type="target"
            id="1"
            className={`center-target-handle-${data?.name}`}
            position={Position.Top}
          />
        )}
        {data?.quadHandle && (
          <>
            <Handle
              type="source"
              id="2"
              className={`quad-source-handle-${data?.name}`}
              position={Position.Top}
              style={{
                left: "75%",
              }}
            />
            <Handle
              type="target"
              id="2"
              className={`quad-target-handle-${data?.name}`}
              position={Position.Top}
              style={{
                left: "25%",
              }}
            />
          </>
        )}
        {data?.nodeInfo?.sideInputs?.map((_, idx) => {
          return (
            <Handle
              key={idx}
              type="target"
              id={`3-${idx}`}
              position={Position.Bottom}
              style={{
                left: `${50 - idx * 10}%`,
              }}
            />
          );
        })}
      </div>
      {data?.nodeInfo?.sideInputs?.map((_, idx) => {
        return (
          <img
            key={idx}
            src={input}
            alt={"input"}
            id={`3-${idx}`}
            className={"sideInput_handle"}
            style={{
              left: `${44.2 - idx * 10}%`,
              ...blurHandle(`3-${idx}`),
            }}
            onMouseOver={handleMouseOver}
            onMouseOut={handleMouseOut}
            onClick={handleInputClick}
          />
        );
      })}
    </div>
  );
};
export default memo(CustomNode);
