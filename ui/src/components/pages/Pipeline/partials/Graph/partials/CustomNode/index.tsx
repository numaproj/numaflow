import { FC, memo, useCallback, useContext } from "react";
import { Tooltip } from "@mui/material";
import { Handle, NodeProps, Position } from "reactflow";
import { HighlightContext } from "../../index";
import healthy from "../../../../../../../images/heart-fill.svg";
import unhealthy from "../../../../../../../images/unhealthy.svg";
import source from "../../../../../../../images/source.svg";
import map from "../../../../../../../images/map.svg";
import reduce from "../../../../../../../images/reduce.svg";
import sink from "../../../../../../../images/sink.svg";

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

  const { highlightValues, setHighlightValues } =
    useContext<any>(HighlightContext);

  const handleClick = useCallback(() => {
    const updatedNodeHighlightValues = {};
    updatedNodeHighlightValues[data?.name] = true;
    setHighlightValues(updatedNodeHighlightValues);
  }, []);

  return (
    <div data-testid={data?.name}>
      <div
        className={"react-flow__node-input"}
        onClick={handleClick}
        style={{
          border: `${isSelected(highlightValues[data?.name])} ${getBorderColor(
            data?.type
          )}`,
        }}
      >
        <div className="node-info">{data?.name}</div>

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
          {data?.podnum} {data?.podnum <= 1 ? "pod" : "pods"}
        </div>

        {data?.type !== "source" && (
          <div className={"node-status"}>
            <img src={healthy} alt={"healthy"} />
          </div>
        )}

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
            position={targetPosition}
            isConnectable={isConnectable}
          />
        )}
        {(data?.type === "source" || data?.type === "udf") && (
          <Handle
            type="source"
            id="0"
            position={sourcePosition}
            isConnectable={isConnectable}
          />
        )}
        {data?.centerSourceHandle && (
          <Handle type="source" id="1" position={Position.Top} />
        )}
        {data?.centerTargetHandle && (
          <Handle type="target" id="1" position={Position.Top} />
        )}
        {data?.quadHandle && (
          <>
            <Handle
              type="source"
              id="2"
              position={Position.Top}
              style={{
                left: "75%",
              }}
            />
            <Handle
              type="target"
              id="2"
              position={Position.Top}
              style={{
                left: "25%",
              }}
            />
          </>
        )}
      </div>
    </div>
  );
};
export default memo(CustomNode);
